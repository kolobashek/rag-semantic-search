"""Shared employee onboarding dialogs for private MAX and Telegram conversations."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
import time
from dataclasses import dataclass, field
from urllib.parse import quote, urlsplit

from rag_catalog.core.roles import ROLE_LABELS
from rag_catalog.core.sqlite_runtime import ClosingConnection, cached_database

from .employee_portal import existing_account_instructions, reviewer_notice

_PAGE_SIZE = 7


@dataclass(frozen=True)
class OnboardingContact:
    name: str = ""
    sender_id: str = ""
    phone: str = field(default="", repr=False)


@dataclass
class OnboardingReply:
    text: str
    buttons: list[list[dict]] = field(default_factory=list)
    pick_contact: bool = False
    notice: dict | None = None
    is_main_menu: bool = False


class _Drafts:
    def __init__(self, path: str):
        self.path = path
        with self.connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS employee_bot_drafts (
                       provider TEXT NOT NULL, sender TEXT NOT NULL, actor TEXT NOT NULL,
                       data TEXT NOT NULL, expires INTEGER NOT NULL, PRIMARY KEY(provider,sender))""")

    def connect(self):
        return sqlite3.connect(self.path, factory=ClosingConnection, timeout=5)

    def read(self, provider, sender, actor):
        with self.connect() as db:
            row = db.execute("""SELECT data FROM employee_bot_drafts
                             WHERE provider=? AND sender=? AND actor=? AND expires>?""",
                             (provider, sender, actor, int(time.time()))).fetchone()
        return json.loads(row[0]) if row else {}

    def write(self, provider, sender, actor, draft):
        now = int(time.time())
        with self.connect() as db:
            db.execute("DELETE FROM employee_bot_drafts WHERE expires<=?", (now,))
            db.execute("""INSERT OR REPLACE INTO employee_bot_drafts(provider,sender,actor,data,expires)
                       VALUES(?,?,?,?,?)""", (provider, sender, actor, json.dumps(draft), now + 1800))

    def clear(self, provider, sender):
        with self.connect() as db:
            db.execute("DELETE FROM employee_bot_drafts WHERE provider=? AND sender=?", (provider, sender))


def _button(label, payload):
    return {"text": label, "payload": payload}


def _name(value):
    value = " ".join(str(value or "").split())
    if not 2 <= len(value) <= 150 or not any(c.isalpha() for c in value):
        raise ValueError("Введите имя и фамилию сотрудника, до 150 символов.")
    return value


def _https(value):
    parsed = urlsplit(str(value or ""))
    return bool(parsed.scheme == "https" and parsed.hostname and not parsed.username and not parsed.password
                and not parsed.query and not parsed.fragment)


class OnboardingFlow:
    def __init__(self, auth_db, *, provider: str, bot_link: str, office_url: str):
        self.auth = auth_db
        self.provider = provider
        self.bot_link = str(bot_link or "").strip().rstrip("/")
        self.office = str(office_url or "").strip().rstrip("/")
        self.menu_action = "нажмите «Главное меню»" if provider == "max" else "откройте /menu"

    def handle(self, *, sender_id, text, display_name="", user=None, is_callback=False,
               contact: OnboardingContact | None = None, event_id="", has_files=False) -> OnboardingReply | None:
        sender = str(sender_id)
        text = str(text or "").strip()
        verb, _, args = text.partition(" ")
        args = args.strip()
        if verb == "/start" and args.startswith("invite_"):
            claimed = self.auth.claim_employee_invite(provider=self.provider, sender_id=sender, token=args[7:])
            if claimed.get("ok"):
                return self._activation(claimed)
            return OnboardingReply("Приглашение недоступно для этого аккаунта или истекло. Попросите "
                                   "администратора выдать новое. Если у вас уже есть кабинет, войдите в него.",
                                   self._existing_buttons())
        aliases = {"Заявка": "/request", "Заявки": "/requests", "Добавить пользователя": "/add_user",
                   "Добавить сотрудника": "/add_user", "/register": "/request"}
        verb = aliases.get(text, aliases.get(verb, verb))
        admin_cmd = verb in ("/add_user", "/reinvite", "/requests", "/approve", "/reject", "ob:admin", "ob:queue")
        menu = text in ("/menu", "/start", "Меню", "Главное меню")
        if not (admin_cmd or menu or verb.startswith("ob:") or verb in ("/request", "/status") or contact
                or not user or not verb.startswith("/")):
            return None
        actor = (user or {}).get("username", "")
        drafts = cached_database(_Drafts, self.auth.db_path)
        # A draft made by an authenticated account cannot be used after unlinking.
        binding = json.dumps([actor, (user or {}).get(self.provider + "_linked_at", "")])
        draft = drafts.read(self.provider, sender, binding)
        stamp = hashlib.sha256((str(event_id) + "\n" + text).encode()).hexdigest() if event_id else ""
        admin = self.auth.list_employee_organizations(admin_username=actor) if actor and (
            admin_cmd or contact or draft.get("flow") in ("invite", "review")) else []
        try:
            if menu:
                if draft:
                    drafts.clear(self.provider, sender)
                if user:
                    return None
                if text != "/start":
                    return self._welcome()
                status = self.auth.employee_request_status(provider=self.provider, sender_id=sender)
                return self._status(sender, status) if status.get("status") not in (None, "none") else self._welcome()
            if stamp and draft.get("last_event") == stamp:
                return self._render(draft, actor)
            if admin_cmd and not admin:
                return OnboardingReply("Добавление сотрудников и рассмотрение заявок доступно администратору.")
            if contact and not admin:
                if not user and draft.get("stage") == "name" and draft.get("flow") == "request":
                    return OnboardingReply("Введите ваши имя и фамилию текстом для заявки.", [])
                return (OnboardingReply("Добавлять сотрудников по контакту может только администратор.")
                        if user else self._welcome())
            if verb in ("/status", "ob:status"):
                return self._status(sender)
            if verb == "ob:existing":
                messenger = "MAX" if self.provider == "max" else "Telegram"
                return OnboardingReply(f"Войдите в свой рабочий кабинет привычным способом. В верхней панели "
                                       f"нажмите «{messenger}», затем «Открыть бота {messenger}».",
                                       self._existing_buttons())
            if verb in ("/requests", "ob:queue") or verb.startswith(("ob:page:", "ob:queue:")):
                if not actor:
                    return self._welcome()
                if verb.startswith("ob:queue:"):
                    match = re.fullmatch(r"ob:queue:(pending|use_existing|correctable):([0-9]{1,9})", verb)
                    if not match:
                        raise ValueError("Откройте список заявок заново.")
                    status, offset = match.groups()
                    return self._queue(actor, int(offset), status)
                return self._queue(actor, int(verb[8:]) if verb.startswith("ob:page:") else 0)
            if verb in ("/approve", "/reject") or verb.startswith("ob:view:"):
                request_id = verb[8:] if verb.startswith("ob:view:") else args.strip()
                if not actor or not request_id.isascii() or not request_id.isdigit():
                    raise ValueError("Выберите заявку в списке /requests.")
                row = None
                for status in ("pending", "use_existing", "correctable"):
                    rows = self.auth.list_employee_requests(admin_username=actor, status=status)
                    row = next((r for r in rows if str(r["id"]) == request_id), None)
                    if row:
                        break
                if not row or (verb == "/reject" and status != "pending"):
                    raise ValueError("Заявка уже рассмотрена или недоступна.")
                if status == "correctable":
                    stage = "reopen"
                elif status == "use_existing":
                    stage = "referred"
                elif verb == "/reject":
                    stage = "reject"
                else:
                    stage = "decision" if row["provider"] in ("telegram", "max") else "roles"
                draft = {"flow": "review", "stage": stage, "review_status": status,
                         "id": row["id"], "name": row["display_name"], "org": row["organization_id"],
                         "roles": ["driver"]}
            elif verb in ("/add_user", "ob:admin") or (contact and admin and not verb.startswith("ob:")):
                draft = {"flow": "invite", "stage": "name", "roles": ["driver"]}
                if contact:
                    candidate = str(contact.sender_id or "")
                    if candidate.isascii() and candidate.isdigit() and 0 < int(candidate) < 2**63:
                        draft["expected"] = candidate
                    if contact.name:
                        draft.update(name=_name(contact.name), stage="org")
                elif args:
                    fields = args.split(maxsplit=2)
                    if not re.fullmatch(r"[a-z][a-z0-9._-]{2,63}", fields[0]):
                        raise ValueError("Для добавления по имени нажмите «Добавить сотрудника» и введите ФИО.")
                    draft["username"] = fields[0]
                    if len(fields) >= 2:
                        roles = fields[1].split(",")
                        if not all(role in ROLE_LABELS for role in roles):
                            raise ValueError("Допустимые роли: driver, user, dispatcher, admin.")
                        draft["roles"] = sorted(set(roles))
                    if len(fields) == 3:
                        draft.update(name=_name(fields[2]), stage="org")
            elif verb == "/reinvite":
                if not re.fullmatch(r"[a-z][a-z0-9._-]{2,63}", args.strip()):
                    raise ValueError("Для повторного приглашения введите /reinvite логин.")
                draft = {"flow": "invite", "stage": "org", "username": args.strip(), "name": args.strip(),
                         "roles": ["driver"], "renew": True}
            elif verb in ("/request", "ob:request"):
                if user:
                    return OnboardingReply(f"Ваш аккаунт уже привязан. Чтобы посмотреть права, {self.menu_action}.")
                known = self.auth.employee_request_status(provider=self.provider, sender_id=sender)
                if known.get("status") in ("pending", "approved", "blocked", "linked", "use_existing"):
                    return self._status(sender, known)
                draft = {"flow": "request", "stage": "org", "roles": ["driver"]}
                if args:
                    draft["name"] = _name(args)
                elif display_name and len(display_name.strip()) >= 2 and any(c.isalpha() for c in display_name):
                    draft["suggested"] = " ".join(display_name.split())[:150]
            elif verb.startswith("ob:"):
                pieces = verb.split(":")
                if not is_callback or len(pieces) < 3 or pieces[1] != draft.get("nonce"):
                    raise ValueError("Эта кнопка устарела. Откройте /menu и выберите действие снова.")
                if draft.get("flow") in ("review", "invite") and not admin:
                    raise PermissionError("Недостаточно прав для этой организации.")
                action = pieces[2]
                choice = pieces[3] if len(pieces) == 4 else ""
                stage = draft["stage"]
                if action == "cancel":
                    drafts.clear(self.provider, sender)
                    if self.provider == "max":
                        return OnboardingReply("Действие отменено. Нажмите «Главное меню».")
                    return OnboardingReply("Действие отменено. Главное меню можно открыть командой /menu.")
                if stage in ("decision", "referred", "confirm_profile") and action == "profiles":
                    self._load_profiles(draft, actor, "", 0)
                elif stage == "decision" and action == "new":
                    draft["stage"] = "roles"
                elif stage == "referred" and action == "reopen":
                    draft["stage"] = "reopen"
                elif stage == "reopen" and action == "reopen_yes":
                    result = self.auth.reopen_employee_request(admin_username=actor, request_id=draft["id"])
                    if not result.get("ok"):
                        raise ValueError("Вернуть заявку не удалось. Откройте список заявок и проверьте её статус.")
                    draft.update(stage="decision", review_status="pending")
                elif stage == "profiles" and action in ("prev", "more"):
                    offset = max(0, draft["offset"] + (_PAGE_SIZE if action == "more" else -_PAGE_SIZE))
                    self._load_profiles(draft, actor, draft["query"], offset)
                elif stage == "profiles" and action == "pick" and choice.isdecimal() and int(choice) < len(draft["profiles"]):
                    draft.update(stage="confirm_profile", selected=draft["profiles"][int(choice)]["username"])
                elif stage == "org" and action == "page" and choice.isdecimal() and int(choice) < len(draft["org_ids"]):
                    draft["offset"] = int(choice)
                elif action == "editname" and draft["flow"] == "invite" and not draft.get("renew"):
                    draft["stage"] = "name"
                elif stage == "name" and action == "profile" and draft.get("suggested") and draft["flow"] == "request":
                    draft.update(name=_name(draft["suggested"]), stage="confirm")
                elif stage == "org" and action == "org" and choice.isdecimal() and int(choice) < len(draft["org_ids"]):
                    choice = draft["org_ids"][int(choice)]
                    allowed = self._organizations(actor, draft["flow"])
                    if choice not in {str(o["id"]) for o in allowed}:
                        raise PermissionError("Организация больше недоступна.")
                    draft.update(org=choice, stage=self._after_org(draft))
                elif stage == "roles" and action == "role" and choice in ROLE_LABELS:
                    selected = set(draft["roles"])
                    selected.symmetric_difference_update([choice])
                    draft["roles"] = sorted(selected)
                elif stage == "roles" and action == "next":
                    if not draft["roles"]:
                        raise ValueError("Выберите хотя бы одну роль.")
                    draft["stage"] = "confirm"
                elif stage in ("decision", "roles") and action == "reject" and draft["flow"] == "review":
                    draft["stage"] = "reject"
                elif stage in ("confirm", "reject", "confirm_profile") and action == "yes":
                    used = draft.get("applied_event")
                    if used and (not event_id or used != event_id):
                        raise ValueError(f"Решение уже принято. Для следующего действия {self.menu_action}.")
                    reply = self._apply(draft, actor, sender)
                    draft["applied_event"] = event_id or "applied"
                    drafts.write(self.provider, sender, binding, draft)
                    return reply
                else:
                    raise ValueError("Эта кнопка устарела. Откройте /menu.")
            elif draft.get("stage") == "profiles" and admin and text and not text.startswith("/") and not has_files:
                self._load_profiles(draft, actor, text[:150], 0)
            elif draft.get("stage") == "name" and text and not text.startswith("/") and not contact and not has_files:
                draft.update(name=_name(text), stage="confirm" if draft["flow"] == "request" else "org")
            elif (self.provider == "max" and draft and not draft.get("applied_event")
                  and not text.startswith("/") and (not has_files or not user)
                  and (draft["flow"] == "request" or admin)):
                return self._render(draft, actor)
            elif not user:
                return self._welcome()
            else:
                return None
            if draft["stage"] == "org":
                orgs = self._organizations(actor, draft["flow"])
                if not orgs:
                    raise ValueError("Нет доступных организаций. Обратитесь к администратору кабинета.")
                draft["org_ids"] = [str(o["id"]) for o in orgs]
                if len(orgs) == 1:
                    draft.update(org=str(orgs[0]["id"]), stage=self._after_org(draft))
            draft.update(nonce=secrets.token_hex(5), last_event=stamp)
            drafts.write(self.provider, sender, binding, draft)
            return self._render(draft, actor)
        except (ValueError, PermissionError) as error:
            return OnboardingReply(str(error))

    def _organizations(self, actor, flow):
        return self.auth.list_employee_organizations(admin_username=actor if flow != "request" else None)

    @staticmethod
    def _after_org(draft):
        if draft["flow"] == "request":
            return "confirm" if draft.get("name") else "name"
        return "confirm" if draft.get("renew") else "roles"

    def _existing_buttons(self):
        return [[{"text": "Открыть кабинет", "url": self.office}]] if _https(self.office) else []

    def _welcome(self):
        if self.provider == "max":
            return OnboardingReply("Здравствуйте! Нажмите кнопку под сообщением. Если вы новый сотрудник, "
                                   "выберите «Подать заявку». Если уже пользуетесь кабинетом, "
                                   "выберите «У меня уже есть кабинет» и привяжите MAX.",
                                   [[_button("Подать заявку", "ob:request")],
                                    [_button("У меня уже есть кабинет", "ob:existing")],
                                    [_button("Статус заявки", "ob:status")]], is_main_menu=True)
        return OnboardingReply("Здравствуйте! Это рабочий бот для сотрудников. Если у вас уже есть кабинет, "
                               "войдите в него и привяжите Telegram. Если вы новый сотрудник, подайте заявку.",
                               [[_button("У меня уже есть кабинет", "ob:existing")],
                                [_button("Подать заявку", "ob:request"), _button("Статус заявки", "ob:status")]],
                               is_main_menu=True)

    def _activation(self, result):
        token = result.get("activation_token")
        if token and _https(self.office):
            return OnboardingReply(f"Учётная запись: {result['username']}. Откройте личную ссылку и задайте пароль. "
                                   f"После активации {self.menu_action}.",
                                   [[{"text": "Задать пароль", "url": self.office + "/auth/activate#token=" + quote(token)}]])
        return OnboardingReply("Проверьте состояние подключения через /status.", self._existing_buttons())

    def _status(self, sender, row=None):
        result = row or self.auth.employee_request_status(provider=self.provider, sender_id=sender)
        if result.get("activation_token"):
            return self._activation(result)
        status = result.get("status")
        if status == "pending":
            return OnboardingReply("Заявка ожидает решения администратора.", [[_button("Обновить статус", "ob:status")]])
        if status == "use_existing":
            if result.get("reason") == "referral_unconfirmed":
                return OnboardingReply("Привязка к выбранному администратором кабинету ещё не подтверждена. "
                                       "Если вы уже вошли в свой кабинет, попросите администратора проверить выбор.",
                                       self._existing_buttons())
            return OnboardingReply(existing_account_instructions(self.provider), self._existing_buttons())
        if status == "rejected":
            return OnboardingReply("Администратор отклонил заявку. Уточните данные и при необходимости подайте новую.",
                                   [[_button("Подать заявку", "ob:request")]])
        if status in ("approved", "active", "linked"):
            return OnboardingReply(f"Заявка одобрена. Для доступа {self.menu_action}.", self._existing_buttons())
        if status == "blocked":
            return OnboardingReply("Доступ к учётной записи приостановлен. Обратитесь к администратору.")
        return OnboardingReply("Заявки от вашего аккаунта пока нет.", [[_button("Подать заявку", "ob:request")]])

    def _queue(self, actor, offset, status="pending"):
        if status not in ("pending", "use_existing", "correctable"):
            raise ValueError("Откройте список заявок заново.")
        rows = self.auth.list_employee_requests(admin_username=actor, status=status)
        offset = max(0, offset)
        buttons = [[_button(f"#{r['id']} · {r['display_name']} · {r['organization_name']}"[:75], f"ob:view:{r['id']}")]
                   for r in rows[offset:offset + _PAGE_SIZE]]
        if offset:
            buttons.append([_button("Предыдущие заявки", f"ob:queue:{status}:{max(0, offset - _PAGE_SIZE)}")])
        if offset + _PAGE_SIZE < len(rows):
            buttons.append([_button("Следующие заявки", f"ob:queue:{status}:{offset + _PAGE_SIZE}")])
        if status == "pending":
            for other, label in [("use_existing", "Ожидают привязки"), ("correctable", "Исправить одобрение")]:
                if self.auth.list_employee_requests(admin_username=actor, status=other):
                    buttons.append([_button(label, f"ob:queue:{other}:0")])
        else:
            buttons.append([_button("К новым заявкам", "ob:queue")])
        return OnboardingReply("Выберите заявку сотрудника." if rows else "Заявок на рассмотрении нет.", buttons)

    def _load_profiles(self, draft, actor, query, offset):
        page = self.auth.list_employee_request_candidates(admin_username=actor, request_id=draft["id"],
                                                          query=query, offset=offset, limit=_PAGE_SIZE)
        if not page.get("ok"):
            raise ValueError("Заявка изменилась или больше недоступна. Откройте её заново.")
        draft.update(stage="profiles", profiles=page["items"], query=query, offset=offset, has_more=page["has_more"])

    def _render(self, draft, actor):
        def button(label, action):
            return _button(label, f"ob:{draft['nonce']}:{action}")

        cancel = [button("Отмена", "cancel")]
        stage = draft["stage"]
        orgs = self._organizations(actor, draft["flow"])
        if stage == "org":
            names = {str(org["id"]): org["name"] for org in orgs}
            offset = draft.get("offset", 0)
            ids = draft["org_ids"]
            buttons = [[button(names[org][:70], f"org:{i}")] for i, org in enumerate(ids[offset:offset + 7], offset)
                       if org in names]
            nav = []
            if offset:
                nav.append(button("Назад", f"page:{max(0, offset-7)}"))
            if offset + 7 < len(ids):
                nav.append(button("Далее", f"page:{offset+7}"))
            if nav:
                buttons.append(nav)
            return OnboardingReply("Выберите организацию сотрудника.", buttons + [cancel])
        org_name = next((o["name"] for o in orgs if str(o["id"]) == str(draft.get("org"))), "")
        if stage == "name":
            if draft["flow"] == "invite":
                return OnboardingReply("Перешлите контакт сотрудника или введите его имя и фамилию. "
                                       "Приглашение вы получите для личной передачи.", [cancel], pick_contact=True)
            buttons = [[button("Использовать: " + draft["suggested"][:50], "profile")]] if draft.get("suggested") else []
            return OnboardingReply(f"Организация: {org_name}. Введите ваши имя и фамилию для администратора.",
                                   buttons + [cancel])
        if not org_name:
            raise PermissionError("Организация больше недоступна.")
        details = f"Сотрудник: {draft['name']}\nОрганизация: {org_name}"
        if stage == "decision":
            return OnboardingReply(details + "\nЕсли сотрудник уже работает в кабинете, выберите его профиль.",
                                   [[button("Привязать к кабинету", "profiles")], [button("Создать новый кабинет", "new")],
                                    [button("Отклонить заявку", "reject")], cancel])
        if stage == "referred":
            return OnboardingReply(details + "\nСотруднику предложено войти в существующий кабинет.",
                                   [[button("Выбрать другой кабинет", "profiles")],
                                    [button("Вернуть на рассмотрение", "reopen")], cancel])
        if stage == "reopen":
            text = "Вернуть заявку на рассмотрение?"
            if draft["review_status"] == "correctable":
                text += " Прежняя ссылка задания пароля перестанет работать."
            return OnboardingReply(details + "\n" + text, [[button("Да, вернуть заявку", "reopen_yes")], cancel])
        if stage == "profiles":
            choices = []
            for i, profile in enumerate(draft["profiles"]):
                username = profile["username"]
                name = profile.get("display_name") or username
                choices.append([button(f"{username} · {name}"[:75], f"pick:{i}")])
            if draft["offset"]:
                choices.append([button("Предыдущие профили", "prev")])
            if draft["has_more"]:
                choices.append([button("Следующие профили", "more")])
            instruction = ("Выберите кабинет из списка или напишите логин или имя для поиска." if draft["profiles"]
                           else "Подходящих кабинетов нет. Для поиска введите другой логин или имя.")
            return OnboardingReply(details + "\n" + instruction, choices + [cancel])
        if stage == "confirm_profile":
            return OnboardingReply(details + f"\nЛогин кабинета: {draft['selected']}\n"
                                   "Сотрудник получит приглашение войти в этот кабинет и подтвердить привязку. "
                                   "Действующие права сотрудника сохранятся.",
                                   [[button("Подтвердить привязку к кабинету", "yes")],
                                    [button("Выбрать другой кабинет", "profiles")], cancel])
        if stage == "roles":
            buttons = [[button(("✓ " if r in draft["roles"] else "○ ") + label, "role:" + r)]
                       for r, label in ROLE_LABELS.items()]
            buttons.append([button("Далее", "next")])
            if draft["flow"] == "review":
                buttons.append([button("Отклонить заявку", "reject")])
            elif draft["flow"] == "invite":
                buttons.append([button("Изменить имя", "editname")])
            return OnboardingReply(details + "\nВыберите права сотрудника.", buttons + [cancel])
        if stage == "reject":
            return OnboardingReply(details + "\nОтклонить эту заявку?", [[button("Да, отклонить", "yes")], cancel])
        labels = ", ".join(ROLE_LABELS[r] for r in draft["roles"])
        if draft.get("renew"):
            return OnboardingReply(details + "\nОбновить приглашение с ранее назначенными правами?",
                                   [[button("Обновить приглашение", "yes")], cancel])
        action = {"request": "Отправить заявку", "review": "Одобрить заявку", "invite": "Создать приглашение"}
        return OnboardingReply(details + f"\nПрава: {labels}", [[button(action[draft["flow"]], "yes")], cancel])

    def _apply(self, draft, actor, sender):
        org = draft["org"]
        if org not in {str(o["id"]) for o in self._organizations(actor, draft["flow"])}:
            raise PermissionError("Организация больше недоступна.")
        if draft["flow"] == "request":
            result = self.auth.submit_employee_request(provider=self.provider, sender_id=sender, organization_id=org,
                                                       display_name=draft["name"])
            if result.get("ok"):
                reply = self._status(sender, result)
                if result.get("created"):
                    reply.notice = reviewer_notice(self.auth, org, result["id"], preferred_channel=self.provider)
                return reply
        elif draft["flow"] == "review":
            if draft["stage"] == "confirm_profile":
                result = self.auth.refer_employee_request(admin_username=actor, request_id=draft["id"],
                                                          username=draft["selected"])
                if not result.get("ok"):
                    return OnboardingReply("Выбранный кабинет или заявка больше недоступны. Откройте список заново.",
                                           [[_button("Заявки", "ob:queue")]])
                linked = result["status"] == "linked"
                message = ("Ваш мессенджер уже привязан к рабочему кабинету. Откройте главное меню бота."
                           if linked else existing_account_instructions(result["provider"]))
                notice = {"provider": result["provider"], "sender_id": result["sender_id"],
                          "text": message, "open_office": not linked}
                outcome = "Привязка уже подтверждена." if linked else "Сотрудник сможет подтвердить привязку из кабинета."
                return OnboardingReply(f"Заявка #{draft['id']}. Кабинет: {draft['selected']}. {outcome}", notice=notice)
            approve = draft["stage"] != "reject"
            result = self.auth.review_employee_request(admin_username=actor, request_id=draft["id"], approve=approve,
                                                       roles=draft["roles"])
            if result.get("ok"):
                status = "одобрена" if approve else "отклонена"
                note = None
                if result["provider"] in ("max", "telegram") and result["sender_id"]:
                    note = {"provider": result["provider"], "sender_id": result["sender_id"],
                            "text": "Ваша заявка " + status + ". Откройте /status для подробностей."}
                elif approve and result.get("activation_token") and _https(self.office):
                    url = self.office + "/auth/activate#token=" + quote(result["activation_token"])
                    return OnboardingReply(f"Заявка #{draft['id']} одобрена. Передайте сотруднику личную ссылку "
                                           f"для задания пароля:\n{url}")
                return OnboardingReply(f"Заявка #{draft['id']} {status}.", notice=note)
        else:
            if not _https(self.bot_link) or not _https(self.office):
                raise ValueError("Для приглашений нужно настроить адрес бота и рабочего кабинета.")
            result = self.auth.create_employee_invite(admin_username=actor, username=draft.get("username", ""),
                display_name=draft["name"], provider=self.provider, expected_sender_id=draft.get("expected", ""),
                organization_id=org, roles=draft["roles"], renew_pending=bool(draft.get("renew")),
                request_key=draft["nonce"])
            if result.get("ok"):
                url = self.bot_link + "?start=invite_" + quote(result["token"])
                return OnboardingReply(f"Личное приглашение для {draft['name']} ({result['username']}). "
                                       f"Передайте ссылку этому сотруднику:\n{url}")
        reason = result.get("reason")
        if reason in ("already_exists", "already_linked", "existing_account"):
            return OnboardingReply("Учётная запись уже существует. Сотруднику нужно войти в свой кабинет "
                                   "и привязать этот мессенджер.", self._existing_buttons())
        return OnboardingReply("Действие недоступно. Проверьте организацию и актуальный статус заявки или приглашения.")
