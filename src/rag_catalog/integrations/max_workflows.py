"""Employee workflows for verified private MAX conversations."""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from functools import cached_property
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, uuid5

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.cloud_drive.registry import CloudFilenameConflict
from rag_catalog.core.roles import (
    ROLE_LABELS,
    can_manage_shifts,
    can_use_catalog,
    can_use_shifts,
    manages_employees,
    user_roles,
)
from rag_catalog.core.shift_attachments import MAX_FILE_BYTES, ShiftAttachments, safe_filename
from rag_catalog.core.user_auth_db import UserAuthDB
from rag_catalog.core.work_shifts import STATUS_LABELS, WorkShiftJournal

from .employee_onboarding import OnboardingFlow
from .max_api import MaxAPIError
from .max_waybill import MaxWaybillRequests

if TYPE_CHECKING:
    from .max_api import MaxClient, MaxEvent


def _button(text: str, payload: str) -> dict:
    return {"type": "callback", "text": text, "payload": payload}


def _link(text: str, url: str) -> dict:
    return {"type": "link", "text": text, "url": url}


def _role_labels(user: dict) -> str:
    scoped = user.get("organization_roles") or {}
    roles = {role for organization_roles in scoped.values() for role in organization_roles} or user_roles(user)
    if user.get("is_platform_admin"):
        roles.add("admin")
    return ", ".join(label for role, label in ROLE_LABELS.items() if role in roles)


def _integer(value: str, *, minimum: int = 1) -> int:
    if not re.fullmatch(r"[0-9]{1,18}", value) or int(value) < minimum:
        raise ValueError("Укажите корректный номер")
    return int(value)


class _JournalAuth:
    """Only MAX events go through this adapter. It never issues a web session."""

    def __init__(self, auth: UserAuthDB):
        self.auth = auth

    def get_user_by_session(self, key: str):
        max_id, username, linked_at = json.loads(key)
        user = self.auth.get_user_by_max_user_id(max_id)
        return user if user and user["username"] == username and user["max_linked_at"] == linked_at else None

    def get_user(self, *, username: str):
        return self.auth.get_user(username=username)

    def list_users(self):
        return self.auth.list_users()

    def list_employee_organizations(self):
        return self.auth.list_employee_organizations()


class MaxWorkflows:
    def __init__(self, cfg: dict, auth_db: UserAuthDB, client: MaxClient):
        self.cfg = cfg
        self.auth = auth_db
        self.client = client
        base = str(cfg.get("max_office_url") or "").strip().rstrip("/")
        parsed = urlsplit(base)
        self.office_url = base if (parsed.scheme == "https" and parsed.hostname and not parsed.username
                                   and not parsed.password and not parsed.query and not parsed.fragment) else ""

    @cached_property
    def journal(self):
        path = Path(self.cfg.get("work_shifts_db_path") or "data/work_shifts.db")
        return WorkShiftJournal(Path(__file__).resolve().parents[3] / path, _JournalAuth(self.auth))

    @cached_property
    def attachments(self):
        return ShiftAttachments(self.journal)

    @cached_property
    def waybill_requests(self):
        return MaxWaybillRequests(self.journal)

    @cached_property
    def drive(self):
        return CloudDriveService.from_config(self.cfg)

    def _active(self, max_id: int):
        user = self.auth.get_user_by_max_user_id(max_id)
        if user and user.get("status") == "active" and not user.get("must_change_password"):
            return user
        return None

    def _reply(self, uid: int, text: str, *, buttons: list[list[dict]] | None = None) -> None:
        rows = (buttons or []) + [[_button("Главное меню", "/menu")]]
        self.client.send_message(uid, text, buttons=rows)

    def handle(self, event: MaxEvent) -> None:
        """The runner authenticates the Bot API, normalizes private events and acknowledges callbacks."""
        try:
            self._handle(event)
        except (ValueError, PermissionError) as exc:
            self._reply(event.user_id, str(exc))

    def _handle(self, event: MaxEvent) -> None:
        text = event.text.strip()
        if text.casefold() in {"/menu", "/help", "/start menu", "меню", "главное меню"}:
            text = "/menu"
        elif text.casefold() == "/start":
            text = "/start"
        parts = text.split(maxsplit=1)
        command = parts[0].lower() if parts else ""
        args = parts[1].strip() if len(parts) > 1 else ""
        uid = event.user_id
        if command == "/start" and args.startswith("link_"):
            result = self.auth.consume_max_link_token(token=args[5:], max_user_id=uid)
            if not result["ok"]:
                existing = self._active(uid)
                if existing:
                    self.client.send_message(uid, f"Этот MAX уже привязан к учётной записи {existing['username']}.")
                    self._menu(uid, existing)
                    return
                raise ValueError("Привязка не удалась. В кабинете нажмите «MAX» вверху → «Открыть бота MAX».")
            self.client.send_message(uid, "MAX привязан к учётной записи сотрудника.")
        user = self._active(uid)
        reply = OnboardingFlow(self.auth, provider="max", bot_link=self.cfg.get("max_bot_link", ""),
                               office_url=self.office_url).handle(
            sender_id=uid, text=text, display_name=event.display_name, user=user,
            is_callback=bool(event.callback_id), event_id=event.event_id, contact=event.contact,
            has_files=bool(event.files))
        if reply is not None:
            if reply.notice:
                from .employee_portal import dispatch_notice
                dispatch_notice(self.cfg, reply.notice)
            buttons = [[_link(b["text"], b["url"]) if "url" in b else _button(b["text"], b["payload"])
                        for b in row] for row in reply.buttons]
            if reply.is_main_menu:
                self.client.send_message(uid, reply.text, buttons=buttons)
            else:
                self._reply(uid, reply.text, buttons=buttons)
            return
        if not user:
            return
        if command == "/start" and args.startswith("login_"):
            token = args[6:]
            if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", token):
                raise ValueError("Некорректная ссылка для входа")
            self._reply(uid, "На другом экране запрошен вход в кабинет под вашей учётной записью. "
                        "Нажмите кнопку, только если вы сами начали этот вход.",
                        buttons=[[_button("Подтвердить мой вход", "/confirm_login " + token)]])
            return
        if command == "/confirm_login":
            if not event.callback_id:
                raise ValueError("Подтвердите свой запрос кнопкой в сообщении бота")
            result = self.auth.confirm_max_login(token=args, max_user_id=uid)
            self._reply(uid, "Вход подтверждён. Вернитесь в кабинет." if result["ok"]
                        else "Запрос уже использован или истёк. Начните вход заново.")
            return
        binding = json.dumps([uid, user["username"], user["max_linked_at"]], separators=(",", ":"))
        if event.files and not command and can_use_shifts(user):
            selected_shift = self.waybill_requests.claim(binding, event.event_id)
            if selected_shift is not None:
                self._waybill(event, binding, str(selected_shift))
                return
        if event.files and command not in ("/waybill", "/upload"):
            hints = []
            if can_use_shifts(user):
                hints.append("К смене: отправьте файл с подписью /waybill НОМЕР_ЗАПИСИ. См. /shifts.")
            if can_use_catalog(user):
                hints.append("В личную папку: отправьте файл с подписью /upload.")
            raise ValueError("\n".join(hints) or "Загрузка файлов недоступна для вашей роли")
        greeting = text.casefold().rstrip(".!") in {"привет", "здравствуйте", "доброе утро", "добрый день",
                                                    "добрый вечер", "начать"}
        if command in ("", "/start", "/menu", "/help") or greeting:
            self._menu(uid, user)
        elif command == "/whoami":
            self._reply(uid, f"Логин: {user['username']}\nИмя: {user.get('display_name') or '—'}"
                        f"\nПрава: {_role_labels(user)}")
        elif command == "/logout":
            if args == "confirm " + user["username"] and event.callback_id:
                self.auth.unlink_max_user(uid)
                self._reply(uid, "MAX отвязан от рабочего кабинета.")
            else:
                self.client.send_message(uid, "Отвязать MAX от вашей учётной записи? Для повторной привязки "
                                         "понадобится войти в кабинет.", buttons=[[
                                             _button("Отвязать MAX", "/logout confirm " + user["username"]),
                                             _button("Оставить", "/menu")]])
        elif command in ("/shifts", "/new_shift", "/waybill", "/attachment", "/attach_next", "/cancel_waybill"):
            if not can_use_shifts(user):
                raise PermissionError("Журнал смен недоступен для вашей роли")
            if command == "/shifts":
                self._shifts(uid, binding, args)
            elif command == "/new_shift":
                self._new_shift(event, binding, user, args)
            elif command == "/waybill":
                self._waybill(event, binding, args)
            elif command == "/attach_next":
                shift_id = _integer(args)
                prompt, waiting = self.waybill_requests.select(binding, shift_id, event.event_id)
                if waiting:
                    self.client.send_message(uid, f"Отправьте следующий файл или фото без подписи в течение "
                                             f"10 минут. Он будет приложен к смене #{shift_id}.",
                                             buttons=[[_button("Отменить выбор", "/cancel_waybill " + prompt)]])
                else:
                    self._reply(uid, "Этот выбор уже завершён. Чтобы приложить ещё файл, выберите смену снова.")
            elif command == "/cancel_waybill":
                self.waybill_requests.cancel(binding, args)
                self._reply(uid, "Ожидание этого путевого листа завершено.")
            else:
                self._attachment(uid, binding, args)
        elif command in ("/office", "/files", "/search", "/upload", "/download"):
            if not can_use_catalog(user):
                raise PermissionError("Для доступа к офисному каталогу нужна роль пользователя облака")
            if command == "/office":
                if not self.office_url:
                    raise ValueError("Администратор ещё не указал адрес рабочего кабинета")
                self._reply(uid, "Откройте рабочий кабинет и выберите вход через MAX.",
                            buttons=[[_link("Открыть офис", self.office_url)]])
            elif command == "/download":
                self._download(uid, user, args)
            elif command == "/files":
                self._files(uid, user, args)
            elif command == "/upload":
                self._upload(event, user)
            else:
                self._search(uid, user, args)
        elif not command.startswith("/") and can_use_catalog(user):
            self._search(uid, user, text)
        else:
            self._menu(uid, user)

    def _menu(self, uid, user, *, notice=""):
        rows = []
        if can_use_catalog(user):
            rows.append([_button("Офис", "/office"), _button("Поиск по содержимому", "/search")])
            rows.append([_button("Поиск по имени", "/files"), _button("Загрузить файл", "/upload")])
        elif can_use_shifts(user) and self.office_url:
            rows.append([_link("Открыть кабинет", self.office_url + "/shifts")])
        if can_use_shifts(user):
            rows.append([_button("Ближайшие смены", "/shifts"), _button("Записать смену", "/new_shift")])
        if manages_employees(user):
            rows.append([_button("Добавить сотрудника", "/add_user"), _button("Заявки", "/requests")])
        rows.append([_button("Мой профиль", "/whoami"), _button("Отвязать MAX", "/logout")])
        name = user.get("display_name") or user["username"]
        header = f"Рабочий кабинет\n{name} · {_role_labels(user)}"
        paragraphs = [header, notice, "Нажмите кнопку под сообщением."]
        self.client.send_message(uid, "\n\n".join(filter(None, paragraphs)), buttons=rows)

    def _shifts(self, uid, key, args):
        today = date.today()
        parts = args.split()
        if len(parts) > 3:
            raise ValueError("Формат: /shifts ДАТА_С ДАТА_ПО (ГГГГ-ММ-ДД)")
        first = parts[0] if parts else today.isoformat()
        last = parts[1] if len(parts) > 1 else (first if parts else (today + timedelta(days=6)).isoformat())
        offset = _integer(parts[2], minimum=0) if len(parts) > 2 else 0
        page = self.journal.list(key, date_from=first, date_to=last, offset=offset, limit=6)
        lines = [f"Смены с {first} по {last}"]
        rows = []
        for item in page["rows"]:
            shift_id = item["id"]
            lines.append(f"#{shift_id} · {item['work_date']} · смена {item['shift_number']} · "
                         f"{item['employee_name'][:70]} · {item['equipment'][:60]} · {item['workplace'][:60]} · "
                         f"{STATUS_LABELS[item['status']]}")
            rows.append([_button(f"Путевые листы #{shift_id}", f"/waybill {shift_id}")])
        if page["count"] > offset + len(page["rows"]):
            rows.append([_button("Следующие смены", f"/shifts {first} {last} {offset + len(page['rows'])}")])
        if not page["rows"]:
            lines.append("Записей за этот период нет.")
        lines.append("Другой период: /shifts ГГГГ-ММ-ДД ГГГГ-ММ-ДД")
        self._reply(uid, "\n".join(lines), buttons=rows)

    def _new_shift(self, event, key, user, args):
        fields = [v.strip() for v in args.split("|")]
        if len(fields) not in (7, 8):
            example = f"/new_shift {date.today().isoformat()} | 1 | Организация | Кран | Объект | 8 | 123"
            if can_manage_shifts(user):
                example += " | логин_сотрудника"
            raise ValueError("Введите дату, номер смены, организацию, технику, объект, часы и номер "
                             "путевого листа через символ |.\nПример:\n" + example)
        day, number, organization, equipment, workplace, hours, waybill, *employee = fields
        row = self.journal.save(key, dict(work_date=day, shift_number=_integer(number),
                    organization=organization, equipment=equipment, workplace=workplace,
                    hours=hours.replace(",", "."), waybill_number=waybill,
                    employee=employee[0] if employee else user["username"]), source_event_id="max:" + event.event_id)
        self._reply(event.user_id, f"Смена #{row['id']} записана. Можно приложить путевой лист.",
                    buttons=[[_button("Путевой лист", f"/waybill {row['id']}")]])

    def _waybill(self, event, key, args):
        parts = args.split()
        if not 1 <= len(parts) <= 2 or (event.files and len(parts) != 1):
            raise ValueError("Укажите номер записи из /shifts. Например: /waybill 12")
        shift_id = _integer(parts[0])
        offset = _integer(parts[1], minimum=0) if len(parts) > 1 else 0
        attachments = self.attachments.list(key, shift_id)  # Authorize before requesting any file bytes.
        uid = event.user_id
        if event.files:
            if any(a.kind not in ("file", "image") for a in event.files):
                raise ValueError("Отправьте путевой лист как документ или фотографию")
            if any(a.size is not None and a.size > MAX_FILE_BYTES for a in event.files):
                raise ValueError("Максимальный размер одного путевого листа — 16 МБ")
            for index, attachment in enumerate(event.files):
                with TemporaryDirectory() as directory:
                    dest = Path(directory) / "incoming"
                    self._receive_file(attachment, dest, MAX_FILE_BYTES)
                    self.attachments.add(key, shift_id, f"{event.event_id}:{index}", attachment.name, dest.read_bytes())
            self._reply(uid, f"Путевые листы приложены к смене #{shift_id}. Файлов: {len(event.files)}.")
            return
        lines = [f"Смена #{shift_id}. Приложено файлов: {len(attachments)}."]
        lines.append(f"Нажмите «Приложить путевой лист» или отправьте файл с подписью /waybill {shift_id}. "
                     "Лимит одного файла — 16 МБ.")
        rows = [[_button("Приложить путевой лист", f"/attach_next {shift_id}")]]
        rows.extend([_button(item["filename"][:60], f"/attachment {shift_id} {item['id']}")]
                    for item in attachments[offset:offset + 8])
        if len(attachments) > offset + 8:
            rows.append([_button("Следующие вложения", f"/waybill {shift_id} {offset + 8}")])
        self._reply(uid, "\n".join(lines), buttons=rows)

    def _attachment(self, uid, key, args):
        ids = args.split()
        if len(ids) != 2:
            raise ValueError("Выберите вложение в списке путевых листов смены")
        doc = self.attachments.read(key, _integer(ids[0]), _integer(ids[1]))
        with TemporaryDirectory() as directory:
            path = Path(directory) / safe_filename(doc["filename"])
            path.write_bytes(doc["content"])
            self.client.send_document(uid, path)

    def _files(self, uid, user, query):
        if not query:
            raise ValueError("Введите название файла или команду /files часть_названия")
        if not self.cfg.get("cloud_drive_db_path"):
            raise ValueError("Облачный каталог ещё не настроен")
        from rag_catalog.ui.helpers import _cd_registry_acl_filter
        offset = 0
        files = []
        while len(files) < 8:
            page = self.drive.search_nodes(query=query[:256], node_type="file", limit=100, offset=offset)
            files.extend(_cd_registry_acl_filter(self.cfg, user, page["items"], service=self.drive))
            if page["next_offset"] is None:
                break
            offset = page["next_offset"]
        fresh = self._active(uid)
        if not fresh or fresh["username"] != user["username"] or not can_use_catalog(fresh):
            raise PermissionError("Учётная запись больше не имеет доступа к каталогу")
        current_files = []
        for item in files[:8]:
            node = self.drive.registry.get_file_by_id(item["id"])
            if node and not node.deleted_at:
                current_files.append({"node_type": "file", "id": node.id, "path": node.path})
        files = _cd_registry_acl_filter(self.cfg, fresh, current_files, service=self.drive)
        rows = [[_button(item["path"][-80:], "/download " + item["id"])] for item in files[:8]]
        text = ("Выберите файл для скачивания. Для более точного поиска добавьте часть пути или имени."
                if rows else "Доступных файлов по этому запросу не найдено.")
        self._reply(uid, text, buttons=rows)

    def _search(self, uid, user, query):
        if not query:
            raise ValueError("Опишите искомый документ или введите /search и текст запроса.")
        if not self.cfg.get("cloud_drive_db_path"):
            self._menu(uid, user, notice="Облачный каталог ещё не настроен.")
            return
        from rag_catalog.ui.helpers import (
            _cached_searcher_if_ready,
            _cd_registry_acl_allows,
            _filter_cloud_drive_search_results,
        )
        searcher = _cached_searcher_if_ready(self.cfg, connect_on_miss=True)
        if searcher is None:
            self._menu(uid, user, notice="Поиск по содержимому временно недоступен. Попробуйте «Поиск по имени».")
            return

        def allowed(results):
            current = self._active(uid)
            if not current or current["username"] != user["username"] or not can_use_catalog(current):
                return []
            return _filter_cloud_drive_search_results(self.cfg, current, results, service=self.drive)

        results = searcher.search(query[:500], limit=6, source="max_bot", username=user["username"],
                                  result_filter=allowed)
        fresh = self._active(uid)
        if not fresh or fresh["username"] != user["username"] or not can_use_catalog(fresh):
            raise PermissionError("Учётная запись больше не имеет доступа к каталогу")
        lines, rows = [], []
        for item in allowed(results or [])[:6]:
            file_id = str(item.get("cloud_file_id") or "")
            node = self.drive.registry.get_file_by_id(file_id) if file_id else None
            if (not node or node.deleted_at or not _cd_registry_acl_allows(
                    self.cfg, fresh, node.path, service=self.drive, file_id=node.id)
                    or (item.get("cloud_version_id") and item["cloud_version_id"] != node.current_version_id)):
                continue
            label = str(item.get("filename") or node.name)
            snippet = str(item.get("text") or item.get("chunk_text") or "")
            lines.append(f"{len(lines) + 1}. {label[:180]}\n{' '.join(snippet.split())[:460]}".rstrip())
            rows.append([_button(f"Скачать {len(lines)} · {label[:50]}", "/download " + node.id)])
        if rows:
            self._reply(uid, "\n\n".join(lines), buttons=rows)
        else:
            self._menu(uid, fresh, notice="Доступных документов по запросу не найдено.")

    def _receive_file(self, attachment, path, limit):
        try:
            self.client.download_file(attachment, path, max_bytes=limit)
        except MaxAPIError as exc:
            if exc.code in {"media.invalid", "media.too_large", "media.size_mismatch", "media.expired"}:
                raise ValueError(f"{exc}. Прикрепите файл заново (лимит {limit // 1048576} МБ).") from None
            raise

    def _upload(self, event, user):
        if not event.files:
            raise ValueError("Чтобы сохранить файл в личную папку облака, отправьте документ с подписью /upload.")
        if not self.cfg.get("cloud_drive_db_path"):
            raise ValueError("Облачный каталог ещё не настроен")
        from rag_catalog.ui.helpers import _cd_registry_acl_allows
        home = self.drive.ensure_user_home_folder(username=user["username"])["path"]
        size_limit = max(1, min(int(self.cfg.get("cloud_drive_max_upload_mb") or 512), 102400)) * 1048576

        def authorize():
            fresh = self._active(event.user_id)
            if (not fresh or fresh["username"] != user["username"] or not can_use_catalog(fresh)
                    or not _cd_registry_acl_allows(self.cfg, fresh, home, service=self.drive, required_level="editor")):
                raise PermissionError("Личная папка недоступна для загрузки")

        authorize()
        for index, attachment in enumerate(event.files):
            create_id = str(uuid5(NAMESPACE_URL, f"rag-catalog:max:{event.event_id}:{index}"))
            if self.drive.registry.get_file_by_id(create_id):
                continue
            filename = safe_filename(attachment.name)
            with TemporaryDirectory() as directory:
                source = Path(directory) / filename
                self._receive_file(attachment, source, size_limit)
                authorize()
                kwargs = dict(parent_path=home, source_path=str(source), create_id=create_id,
                              created_by=user["username"])
                try:
                    self.drive.upload_file(filename=filename, **kwargs)
                except CloudFilenameConflict:
                    name = Path(filename)
                    alternative = f"{name.stem} ({create_id[:12]}){name.suffix}"
                    try:
                        self.drive.upload_file(filename=alternative, **kwargs)
                    except CloudFilenameConflict:
                        raise ValueError("Файл с таким именем уже существует. Переименуйте его и пришлите заново.") from None
        self._reply(event.user_id, f"Файлы приняты в личную папку облака: {len(event.files)}.")

    def _download(self, uid, user, file_id):
        from rag_catalog.ui.helpers import _cd_registry_acl_allows
        if not self.cfg.get("cloud_drive_db_path"):
            raise ValueError("Облачный каталог ещё не настроен")
        node = self.drive.registry.get_file_by_id(file_id)
        if not node or node.deleted_at or not _cd_registry_acl_allows(
            self.cfg, user, node.path, file_id=node.id, service=self.drive
        ):
            raise PermissionError("Файл недоступен")
        with TemporaryDirectory() as directory:
            path = Path(directory) / safe_filename(node.name)
            self.drive.storage.download_file(node.storage_key, path)
            # A large download can outlive a role change or file move.
            current = self.drive.registry.get_file_by_id(file_id)
            fresh = self._active(uid)
            if (not fresh or fresh["username"] != user["username"] or not can_use_catalog(fresh)
                    or not current or current.deleted_at or current.storage_key != node.storage_key
                    or not _cd_registry_acl_allows(self.cfg, fresh, current.path, file_id=current.id,
                                                   service=self.drive)):
                raise PermissionError("Файл недоступен")
            self.client.send_document(uid, path)
