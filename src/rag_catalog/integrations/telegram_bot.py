"""
telegram_bot.py — Telegram бот для RAG-поиска по документам.

Запуск:
    python telegram_bot.py
"""

import logging
import re
import secrets
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Any, Dict, Iterator, List
from urllib.parse import quote

import requests

from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.log_history import install_env_log_handler
from rag_catalog.core.rag_core import RAGSearcher, load_config
from rag_catalog.core.roles import (
    ROLE_LABELS,
    can_manage_shifts,
    can_use_catalog,
    can_use_shifts,
    manages_employees,
    user_roles,
)
from rag_catalog.core.user_auth_db import UserAuthDB
from rag_catalog.integrations.employee_onboarding import OnboardingContact, OnboardingFlow
from rag_catalog.integrations.employee_portal import dispatch_notice, employee_office_url

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
install_env_log_handler()
logger = logging.getLogger(__name__)

API_TIMEOUT = 40
POLL_SLEEP_SEC = 1.0
SEARCH_PAGE_SIZE = 5
OPEN_FILE_FEEDBACK = 0
SEARCH_SESSION_TTL_SEC = 30 * 60
CHAT_ACTION_INTERVAL_SEC = 4.0
SEARCH_SESSIONS: Dict[str, Dict[str, Any]] = {}
PENDING_REFINEMENTS: Dict[str, str] = {}


# ─── Cloud Drive ACL ──────────────────────────────────────────────────────────
# Бот обязан применять ту же модель прав, что и веб-интерфейс. Без этого любой
# привязанный пользователь получает весь индекс: поиск, «Каталог» и выдачу
# файлов кнопкой — в обход настроенных ACL.

def _cloud_drive_acl_active(cfg: Dict[str, Any]) -> bool:
    return bool(str(cfg.get("cloud_drive_db_path") or "").strip())


def acl_filter_results(
    cfg: Dict[str, Any],
    user: Dict[str, Any] | None,
    results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Отфильтровать результаты по правам Cloud Drive (тот же путь, что в вебе)."""
    if not results or not _cloud_drive_acl_active(cfg):
        return list(results or [])
    try:
        from rag_catalog.ui.helpers import _filter_cloud_drive_search_results
    except Exception:
        logger.exception("ACL Cloud Drive недоступен — выдача закрыта")
        return []
    try:
        return list(_filter_cloud_drive_search_results(cfg, user, list(results)))
    except Exception:
        logger.exception("Ошибка проверки ACL Cloud Drive — выдача закрыта")
        return []


def acl_filter_paths(
    cfg: Dict[str, Any],
    user: Dict[str, Any] | None,
    paths: List[str],
) -> List[str]:
    if not paths or not _cloud_drive_acl_active(cfg):
        return list(paths or [])
    allowed = acl_filter_results(cfg, user, [{"full_path": str(path)} for path in paths])
    return [str(item.get("full_path") or "") for item in allowed if item.get("full_path")]


def acl_allows_path(cfg: Dict[str, Any], user: Dict[str, Any] | None, path: str) -> bool:
    clean = str(path or "").strip()
    if not clean:
        return False
    return bool(acl_filter_paths(cfg, user, [clean]))


def _answer_source_paths(answer: Dict[str, Any]) -> List[str]:
    sources: List[Dict[str, Any]] = list(answer.get("sources") or [])
    single = answer.get("source")
    if not sources and isinstance(single, dict):
        sources = [single]
    paths: List[str] = []
    for src in sources:
        if not isinstance(src, dict):
            continue
        raw = str(src.get("full_path") or src.get("path") or "").strip()
        if raw:
            paths.append(raw)
    return paths


def acl_result_filter(cfg: Dict[str, Any], user: Dict[str, Any] | None):
    """Фильтр для передачи внутрь RAGSearcher: ответы строятся по своему поиску.

    Возвращает None, если Cloud Drive не настроен — тогда ограничивать нечего.
    """
    if not _cloud_drive_acl_active(cfg):
        return None

    def _filter(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return acl_filter_results(cfg, user, results)

    return _filter


def acl_allows_answer(cfg: Dict[str, Any], user: Dict[str, Any] | None, answer: Dict[str, Any]) -> bool:
    """LLM-ответ строится по всему индексу, поэтому проверяем каждый источник.

    Если хотя бы один источник недоступен пользователю, ответ подавляется
    целиком: пересказ закрытого документа — та же утечка, что и сам документ.
    """
    if not _cloud_drive_acl_active(cfg):
        return True
    paths = _answer_source_paths(answer)
    if not paths:
        return False
    return len(acl_filter_paths(cfg, user, paths)) == len(paths)


def _api_url(token: str, method: str) -> str:
    return f"https://api.telegram.org/bot{token}/{method}"


class TelegramAPIError(RuntimeError):
    pass


@contextmanager
def _telegram_errors(method: str) -> Iterator[None]:
    """Requests exceptions contain credential-bearing URLs, including HTTP errors."""
    try:
        yield
    except requests.RequestException as exc:
        status = getattr(exc.response, "status_code", None)
        detail = f"HTTP {status}" if isinstance(status, int) and status >= 400 else type(exc).__name__
        raise TelegramAPIError(f"Telegram API {method} unavailable: {detail}") from None


def _telegram_data(resp: requests.Response, method: str) -> Dict[str, Any]:
    resp.raise_for_status()
    try:
        data = resp.json()
    except ValueError:
        raise TelegramAPIError(f"Telegram API {method}: invalid JSON response") from None
    if not isinstance(data, dict) or not data.get("ok"):
        code = data.get("error_code") if isinstance(data, dict) else None
        code = code if isinstance(code, int) else "unknown"
        raise TelegramAPIError(f"Telegram API {method} rejected request (code={code})")
    return data


def tg_call(token: str, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    with _telegram_errors(method):
        resp = requests.post(_api_url(token, method), json=payload, timeout=API_TIMEOUT)
        return _telegram_data(resp, method)


def send_message(token: str, chat_id: str, text: str, reply_markup: Dict[str, Any] | None = None) -> int:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    data = tg_call(token, "sendMessage", payload)
    return int((data.get("result") or {}).get("message_id") or 0)


def delete_message(token: str, chat_id: str, message_id: int) -> None:
    if not message_id:
        return
    try:
        tg_call(token, "deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception:
        pass


def edit_message_reply_markup(token: str, *, chat_id: str, message_id: int, reply_markup: Dict[str, Any] | None) -> None:
    try:
        payload: Dict[str, Any] = {"chat_id": chat_id, "message_id": message_id}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        tg_call(token, "editMessageReplyMarkup", payload)
    except Exception:
        pass


def send_chat_action(token: str, chat_id: str, action: str = "typing") -> None:
    if not chat_id:
        return
    try:
        tg_call(token, "sendChatAction", {"chat_id": chat_id, "action": action})
    except Exception as exc:
        logger.debug("Не удалось отправить chat action %s в чат %s: %s", action, chat_id, exc)


@contextmanager
def chat_action(token: str, chat_id: str, action: str = "typing") -> Iterator[None]:
    """Показывать статус Telegram (typing/upload_document), пока выполняется блок."""
    stop = threading.Event()
    send_chat_action(token, chat_id, action)

    def _keep_alive() -> None:
        while not stop.wait(CHAT_ACTION_INTERVAL_SEC):
            send_chat_action(token, chat_id, action)

    worker = threading.Thread(target=_keep_alive, name=f"telegram-{action}", daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()


def edit_message_text(
    token: str,
    *,
    chat_id: str,
    message_id: int,
    text: str,
    reply_markup: Dict[str, Any] | None = None,
) -> None:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_call(token, "editMessageText", payload)


def answer_callback_query(token: str, callback_query_id: str, text: str = "") -> None:
    payload: Dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = False
    tg_call(token, "answerCallbackQuery", payload)


def send_document(token: str, chat_id: str, path: str, caption: str = "") -> None:
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        send_message(token, chat_id, f"Файл недоступен:\n{path}")
        return
    send_chat_action(token, chat_id, "upload_document")
    with _telegram_errors("sendDocument"), file_path.open("rb") as fh:
        resp = requests.post(
            _api_url(token, "sendDocument"),
            data={"chat_id": chat_id, "caption": caption[:1000]},
            files={"document": (file_path.name, fh)},
            timeout=API_TIMEOUT,
        )
        _telegram_data(resp, "sendDocument")


def get_file_info(token: str, file_id: str) -> Dict[str, Any]:
    data = tg_call(token, "getFile", {"file_id": file_id})
    result = data.get("result") or {}
    if not result.get("file_path"):
        raise TelegramAPIError("Telegram getFile did not return file_path")
    return result


def download_file(token: str, file_path: str, destination: Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://api.telegram.org/file/bot{token}/{file_path}"
    with _telegram_errors("downloadFile"), requests.get(url, stream=True, timeout=API_TIMEOUT) as resp:
        resp.raise_for_status()
        total = 0
        with destination.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                fh.write(chunk)
                total += len(chunk)
    return total


def get_updates(token: str, offset: int) -> List[Dict[str, Any]]:
    data = tg_call(
        token,
        "getUpdates",
        {
            "offset": offset,
            "timeout": 25,
            "allowed_updates": ["message", "callback_query"],
        },
    )
    return data.get("result", [])


_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _clean_tg_text(text: str, max_len: int = 1000) -> str:
    """Remove control characters and truncate for safe Telegram output."""
    text = _CTRL_RE.sub("", str(text or ""))
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if len(text) > max_len:
        text = text[:max_len] + "…"
    return text.strip()


def _file_uri(path: str) -> str:
    """Convert a Windows path to a percent-encoded file:// URI."""
    if not path:
        return ""
    try:
        p = PureWindowsPath(path)
        if not p.drive or not p.parts:
            return ""
        rest = list(p.parts[1:])
        encoded = "/".join(quote(part, safe="") for part in rest)
        return f"file:///{p.drive}/{encoded}"
    except Exception:
        return ""


def format_fact_answer(result: Dict[str, Any]) -> str:
    answer = _clean_tg_text(result.get("answer", "Ответ не найден"), 800)
    lines = [answer]
    sources: List[Dict[str, Any]] = list(result.get("sources") or [])
    if not sources and result.get("source"):
        sources = [result["source"]]
    if len(sources) == 1:
        src = sources[0]
        filename = _clean_tg_text(str(src.get("filename") or "неизвестный файл"), 200)
        full_path = str(src.get("full_path", "") or "")
        excerpt = _clean_tg_text(str(src.get("text_excerpt") or src.get("excerpt") or ""), 300)
        lines.append(f"Файл: {filename}")
        if excerpt:
            lines.append(f"Фрагмент: {excerpt}")
        if full_path:
            lines.append(f"Путь: {_clean_tg_text(full_path, 500)}")
            uri = _file_uri(full_path)
            if uri:
                lines.append(f"Ссылка: {uri}")
    elif sources:
        lines.append("Источники:")
        for src in sources[:3]:
            filename = _clean_tg_text(str(src.get("filename") or "неизвестный файл"), 200)
            full_path = str(src.get("full_path") or "")
            lines.append(f"- {filename}")
            if full_path:
                uri = _file_uri(full_path)
                if uri:
                    lines.append(f"  {uri}")
    return "\n".join(lines)


def is_allowed_chat(chat_id: str, allowed_chat_id: str) -> bool:
    if not allowed_chat_id:
        return True
    return str(chat_id).strip() == str(allowed_chat_id).strip()


def get_authorized_telegram_user(auth_db: UserAuthDB, chat_id: str) -> Dict[str, Any] | None:
    try:
        user = auth_db.get_user_by_telegram_chat_id(str(chat_id).strip())
        if user and user.get("status", "active") == "active" and not user.get("must_change_password"):
            return user
        return None
    except Exception:
        return None


def _log_tg_auth_event(
    auth_db: UserAuthDB,
    *,
    username: str = "",
    chat_id: str = "",
    event_type: str,
    ok: bool,
    error: str = "",
) -> None:
    try:
        auth_db.log_auth_event(
            username=username,
            event_type=event_type,
            ok=ok,
            user_agent=f"telegram:{chat_id}",
            error=error,
        )
    except Exception:
        logger.debug("Не удалось записать событие авторизации Telegram", exc_info=True)


def _users_db_path(cfg: Dict[str, Any]) -> str:
    value = str(cfg.get("users_db_path", "") or "").strip()
    if value:
        return value
    return f"{cfg.get('qdrant_db_path', '.')}/rag_users.db"


def _app_auth_link(cfg: Dict[str, Any]) -> str:
    return str(cfg.get("telegram_bot_link") or "").strip()


_SAFE_FILENAME_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')


def _safe_filename(value: str, fallback: str = "telegram_file") -> str:
    name = Path(str(value or "")).name.strip()
    name = _SAFE_FILENAME_RE.sub("_", name)
    name = re.sub(r"\s+", " ", name).strip(" ._")
    if not name:
        name = fallback
    if len(name) > 160:
        stem = Path(name).stem[:120].strip(" ._") or fallback
        suffix = Path(name).suffix[:20]
        name = f"{stem}{suffix}"
    return name


def _upload_root(cfg: Dict[str, Any], username: str) -> Path:
    base = Path(str(cfg.get("telegram_upload_path") or "").strip() or str(Path(str(cfg.get("catalog_path") or ".")) / "Telegram Uploads"))
    user_dir = _safe_filename(username or "unknown_user", "unknown_user")
    day = datetime.now().strftime("%Y-%m-%d")
    return base / user_dir / day


def _unique_destination(folder: Path, filename: str) -> Path:
    candidate = folder / _safe_filename(filename)
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    for index in range(1, 10_000):
        next_candidate = folder / f"{stem} ({index}){suffix}"
        if not next_candidate.exists():
            return next_candidate
    raise RuntimeError("Не удалось подобрать свободное имя файла")


def _message_file_info(msg: Dict[str, Any]) -> Dict[str, Any] | None:
    document = msg.get("document") if isinstance(msg.get("document"), dict) else None
    if document:
        return {
            "file_id": str(document.get("file_id") or ""),
            "file_name": _safe_filename(str(document.get("file_name") or "telegram_document")),
            "file_size": int(document.get("file_size") or 0),
            "kind": "document",
        }
    photos = msg.get("photo") if isinstance(msg.get("photo"), list) else []
    if photos:
        photo = max(
            (item for item in photos if isinstance(item, dict)),
            key=lambda item: int(item.get("file_size") or 0),
            default=None,
        )
        if photo:
            suffix = ".jpg"
            return {
                "file_id": str(photo.get("file_id") or ""),
                "file_name": f"telegram_photo_{datetime.now().strftime('%H%M%S')}{suffix}",
                "file_size": int(photo.get("file_size") or 0),
                "kind": "photo",
            }
    return None


def save_telegram_upload(
    *,
    token: str,
    cfg: Dict[str, Any],
    auth_db: UserAuthDB,
    chat_id: str,
    msg: Dict[str, Any],
    user: Dict[str, Any],
) -> Dict[str, Any]:
    current = get_authorized_telegram_user(auth_db, chat_id)
    if not can_use_catalog(current) or current["username"] != user.get("username"):
        return {"ok": False, "reason": "access_denied"}
    info = _message_file_info(msg)
    if not info or not info.get("file_id"):
        return {"ok": False, "reason": "no_file"}
    username = str(user.get("username") or "").strip().lower()
    destination = _unique_destination(_upload_root(cfg, username), str(info.get("file_name") or "telegram_file"))
    tg_file = get_file_info(token, str(info["file_id"]))
    bytes_written = download_file(token, str(tg_file["file_path"]), destination)
    _log_tg_auth_event(
        auth_db,
        username=username,
        chat_id=chat_id,
        event_type="telegram_file_uploaded",
        ok=True,
        error=f"bytes={bytes_written}",
    )
    return {
        "ok": True,
        "path": str(destination),
        "bytes": bytes_written,
        "kind": str(info.get("kind") or "file"),
    }


def _employee_admin(user: Dict[str, Any] | None) -> bool:
    return manages_employees(user) or "admin" in user_roles(user)


def _main_menu(user: Dict[str, Any] | None = None) -> Dict[str, Any]:
    rows = []
    if not user:
        rows.append([{"text": "Заявка"}, {"text": "/status"}])
    if can_use_catalog(user):
        rows.extend([[{"text": "Офис"}, {"text": "Каталог"}], [{"text": "Поиск"}, {"text": "Добавить файл"}]])
    if can_use_shifts(user):
        rows.append([{"text": "Смены"}, {"text": "Путевой лист"}])
    if _employee_admin(user):
        rows.append([{"text": "Добавить пользователя"}, {"text": "Заявки"}])
    rows.append([{"text": "Кто я"}, {"text": "Помощь"}])
    return {
        "keyboard": rows,
        "resize_keyboard": True,
        "is_persistent": True,
        "one_time_keyboard": False,
    }


def set_bot_commands(token: str) -> None:
    # Глобальный список команд не должен показывать административные команды
    # обычным пользователям. Админские действия доступны администраторам через
    # reply-меню и текст помощи после авторизации.
    tg_call(
        token,
        "setMyCommands",
        {
            "commands": [
                {"command": "start", "description": "Главное меню"},
                {"command": "whoami", "description": "Текущий пользователь"},
                {"command": "status", "description": "Проверить статус заявки"},
                {"command": "register", "description": "Подать заявку"},
                {"command": "logout", "description": "Отвязать Telegram"},
            ],
        },
    )


def _user_help_text(user: Dict[str, Any] | None, chat_id: str) -> str:
    if not user:
        return (
            "Откройте приглашение администратора или подайте заявку через /register. "
            "Проверить решение можно через /status.\n"
            f"Ваш Telegram ID: {chat_id}"
        )
    lines = [f"Вы вошли как {user['username']}."]
    if can_use_catalog(user):
        lines.append("Офис — /office. Для поиска напишите название или фрагмент документа. "
                     "Чтобы сохранить документ, отправьте его сюда.")
    if can_use_shifts(user):
        lines.append("Смены и путевые листы — /shifts. Откройте смену для просмотра её вложений.")
    if _employee_admin(user):
        lines.append("Команды администратора\n/add_user — пригласить сотрудника\n/requests — рассмотреть заявки")
    lines.append("/whoami — мои роли\n/logout — отвязать Telegram")
    return "\n\n".join(lines)


def _private_sender(message: dict, sender: dict, allowed_chat_id: str) -> str:
    chat = message.get("chat") or {}
    actor = str(sender.get("id") or "")
    if (chat.get("type") == "private" and actor.isascii() and actor.isdigit() and int(actor) > 0
            and actor == str(chat.get("id")) and not sender.get("is_bot") and is_allowed_chat(actor, allowed_chat_id)):
        return actor
    return ""


_PICK_EMPLOYEE_ID = 71


def _contact_hint(message: dict) -> OnboardingContact | None:
    shared = message.get("users_shared") or {}
    if shared and shared.get("request_id") == _PICK_EMPLOYEE_ID and len(shared.get("users") or []) == 1:
        contact = shared["users"][0]
    else:
        contact = message.get("contact")
    if not isinstance(contact, dict):
        return None
    name = " ".join(str(contact[k]) for k in ("first_name", "last_name") if contact.get(k))
    return OnboardingContact(name=name, sender_id=str(contact.get("user_id") or ""),
                             phone=str(contact.get("phone_number") or ""))


def _onboarding(auth_db, cfg, chat_id, user, text, sender, *, event_id, contact=None, is_callback=False):
    flow = OnboardingFlow(auth_db, provider="telegram", bot_link=_app_auth_link(cfg), office_url=employee_office_url(cfg))
    return flow.handle(sender_id=chat_id, text=text, display_name=" ".join(
        str(sender[k]) for k in ("first_name", "last_name") if sender.get(k)
    ), user=user, is_callback=is_callback, contact=contact, event_id=event_id)


def _deliver_onboarding(token, cfg, chat_id, reply, user):
    if reply.notice:
        dispatch_notice(cfg, reply.notice)
    if reply.pick_contact:
        send_message(token, chat_id, "Можно выбрать сотрудника в Telegram.", {"keyboard": [[{
            "text": "Выбрать сотрудника", "request_users": {
                "request_id": _PICK_EMPLOYEE_ID, "user_is_bot": False, "max_quantity": 1,
                "request_name": True, "request_username": True,
            },
        }]], "resize_keyboard": True, "one_time_keyboard": True})
    rows = [[{"text": b["text"], **({"url": b["url"]} if "url" in b else {"callback_data": b["payload"]})}
             for b in row] for row in reply.buttons]
    send_message(token, chat_id, reply.text, {"inline_keyboard": rows} if rows else _main_menu(user))


def process_query(
    searcher: RAGSearcher,
    text: str,
    source: str = "telegram_bot",
    username: str = "",
    *,
    cfg: Dict[str, Any] | None = None,
    user: Dict[str, Any] | None = None,
) -> str:
    q = (text or "").strip()
    if not q:
        return "Пустой запрос."
    acl_cfg: Dict[str, Any] = dict(cfg or {})

    # Фильтр уходит внутрь searcher: ответ строится только по разрешённым
    # документам, а не подавляется целиком из-за одного закрытого источника.
    result_filter = acl_result_filter(acl_cfg, user)
    filter_kwargs: Dict[str, Any] = {"result_filter": result_filter} if result_filter else {}
    try:
        fact = searcher.answer_fact_question(q, limit=30, **filter_kwargs)
    except (ConnectionError, RuntimeError) as exc:
        return f"Ошибка инфраструктуры поиска: {exc}"

    if fact.get("ok") and acl_allows_answer(acl_cfg, user, fact):
        return format_fact_answer(fact)

    if _looks_like_question(q) and hasattr(searcher, "answer_documents"):
        try:
            answer = searcher.answer_documents(
                q, limit=20, source=source, username=username, **filter_kwargs
            )
        except (ConnectionError, RuntimeError) as exc:
            return f"Ошибка инфраструктуры поиска: {exc}"
        if isinstance(answer, dict) and answer.get("ok") and acl_allows_answer(acl_cfg, user, answer):
            return format_rag_answer(answer)

    try:
        results = searcher.search(q, limit=3, content_only=False, source=source, username=username, **filter_kwargs)
    except (ConnectionError, RuntimeError) as exc:
        return f"Ошибка инфраструктуры поиска: {exc}"
    results = acl_filter_results(acl_cfg, user, [item for item in (results or []) if isinstance(item, dict)])
    if not results:
        return "Ничего не найдено."

    lines = ["Точный факт не извлечён. Ближайшие результаты:"]
    for i, r in enumerate(results, 1):
        lines.append(
            f"{i}. {r.get('filename', '')} | score={r.get('score', 0)} | путь={r.get('full_path', '')}"
        )
    return "\n".join(lines)


def _looks_like_question(text: str) -> bool:
    q = (text or "").strip().lower()
    if not q:
        return False
    if "?" in q:
        return True
    first = q.split(maxsplit=1)[0]
    return first in {
        "что",
        "кто",
        "где",
        "когда",
        "какой",
        "какая",
        "какое",
        "какие",
        "почему",
        "зачем",
        "как",
        "сколько",
        "чем",
    }


def format_rag_answer(result: Dict[str, Any]) -> str:
    answer = _clean_tg_text(str(result.get("answer") or "Модель не дала ответа."), 2500)
    prefix = "Ответ по документам:" if result.get("ok") else "Ответ не подтверждён:"
    lines = [prefix, answer]
    sources = result.get("sources") or []
    if sources:
        lines.append("")
        lines.append("Источники:")
        for idx, src in enumerate(sources[:5], 1):
            filename = _clean_tg_text(str(src.get("filename") or src.get("path") or "источник"), 120)
            path = _clean_tg_text(str(src.get("full_path") or src.get("path") or ""), 180)
            citation = _clean_tg_text(str(src.get("citation") or ""), 220)
            label = citation or f"{idx}. {filename}" + (f" — {path}" if path else "")
            lines.append(label)
    return "\n".join(lines)


def _result_title(result: Dict[str, Any]) -> str:
    return _clean_tg_text(
        str(result.get("filename") or result.get("name") or Path(str(result.get("full_path") or "")).name or "результат"),
        80,
    )


def _result_path(result: Dict[str, Any]) -> str:
    return str(result.get("full_path") or result.get("path") or "")


def _result_ext(result: Dict[str, Any]) -> str:
    ext = str(result.get("extension") or "").lower()
    if ext:
        return ext
    return Path(_result_path(result)).suffix.lower()


def _result_mtime(result: Dict[str, Any]) -> float:
    for key in ("modified_time", "mtime", "last_modified", "modified_at"):
        value = result.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    path = _result_path(result)
    try:
        return Path(path).stat().st_mtime if path else 0.0
    except Exception:
        return 0.0


def _session_id() -> str:
    return secrets.token_urlsafe(6).replace("-", "_")


def _cleanup_search_sessions() -> None:
    now = time.time()
    expired = [
        sid for sid, session in SEARCH_SESSIONS.items()
        if now - float(session.get("created_at") or now) > SEARCH_SESSION_TTL_SEC
    ]
    for sid in expired:
        SEARCH_SESSIONS.pop(sid, None)
    # CATALOG_SESSIONS хранит полный список путей индекса (мегабайты на сессию),
    # поэтому их тоже нужно вычищать по TTL, иначе память бота растёт до OOM.
    expired_catalog = [
        sid for sid, session in CATALOG_SESSIONS.items()
        if now - float(session.get("created_at") or now) > SEARCH_SESSION_TTL_SEC
    ]
    for sid in expired_catalog:
        CATALOG_SESSIONS.pop(sid, None)
    expired_refinements = [
        chat_id for chat_id, sid in PENDING_REFINEMENTS.items()
        if sid not in SEARCH_SESSIONS
    ]
    for chat_id in expired_refinements:
        PENDING_REFINEMENTS.pop(chat_id, None)


def _filter_session_results(session: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = list(session.get("results") or [])
    file_type = str(session.get("file_type") or "")
    date_sort = str(session.get("date_sort") or "")
    if file_type:
        groups = {
            "pdf": {".pdf"},
            "doc": {".doc", ".docx", ".rtf", ".odt"},
            "xls": {".xls", ".xlsx", ".csv", ".ods"},
            "img": {".jpg", ".jpeg", ".png", ".gif", ".webp", ".tif", ".tiff"},
        }
        allowed = groups.get(file_type, set())
        if allowed:
            results = [item for item in results if _result_ext(item) in allowed]
    if date_sort == "new":
        results.sort(key=_result_mtime, reverse=True)
    elif date_sort == "old":
        results.sort(key=_result_mtime)
    return results


def _format_single_result(result: Dict[str, Any], *, index: int) -> str:
    title = _result_title(result)
    path = _clean_tg_text(_result_path(result), 220)
    size_mb = result.get("size_mb")
    modified = str(result.get("modified") or "")[:10]
    lines = [f"📄 {title}"]
    if path:
        lines.append(path)
    if size_mb:
        lines.append(f"{float(size_mb):.1f} МБ" + (f"  •  {modified}" if modified else ""))
    elif modified:
        lines.append(modified)
    return "\n".join(lines)


def _result_keyboard(session_id: str, index: int, *, feedback_given: bool) -> Dict[str, Any]:
    row: List[Dict[str, str]] = [
        {"text": "📥 Получить", "callback_data": f"open:{session_id}:{index}"},
    ]
    if feedback_given:
        row.append({"text": "✓ Оценено", "callback_data": f"fb_noop:{session_id}:{index}"})
    else:
        row += [
            {"text": "👍", "callback_data": f"fb:{session_id}:{index}:pos"},
            {"text": "👎", "callback_data": f"fb:{session_id}:{index}:neg"},
        ]
    return {"inline_keyboard": [row]}


def _nav_keyboard(session_id: str) -> Dict[str, Any]:
    return {"inline_keyboard": [
        [
            {"text": "Ещё ↓", "callback_data": f"more:{session_id}"},
            {"text": "Уточнить запрос", "callback_data": f"refine:{session_id}"},
        ],
        [
            {"text": "PDF",   "callback_data": f"ft:{session_id}:pdf"},
            {"text": "Word",  "callback_data": f"ft:{session_id}:doc"},
            {"text": "Excel", "callback_data": f"ft:{session_id}:xls"},
            {"text": "Фото",  "callback_data": f"ft:{session_id}:img"},
            {"text": "Все",   "callback_data": f"ft:{session_id}:all"},
        ],
        [
            {"text": "Новые",    "callback_data": f"date:{session_id}:new"},
            {"text": "Старые",   "callback_data": f"date:{session_id}:old"},
            {"text": "↺ Сброс", "callback_data": f"date:{session_id}:none"},
        ],
    ]}


def _send_result_messages(
    token: str, chat_id: str, session_id: str, session: Dict[str, Any], results: List[Dict[str, Any]]
) -> None:
    """Send one message per result; append their message_ids to session."""
    base = len(session.get("result_message_ids") or [])
    feedback_given: set = session.setdefault("feedback_given", set())
    for i, result in enumerate(results):
        idx = base + i
        text = _format_single_result(result, index=idx)
        markup = _result_keyboard(session_id, idx, feedback_given=idx in feedback_given)
        try:
            msg_id = send_message(token, chat_id, text, markup)
            session.setdefault("result_message_ids", []).append(msg_id)
        except Exception as exc:
            logger.warning("Не удалось отправить результат %d: %s", idx, exc)


def _send_nav_message(token: str, chat_id: str, session_id: str, session: Dict[str, Any]) -> None:
    """Delete previous nav message (if any) and send a fresh one at the bottom."""
    results = _filter_session_results(session)
    shown = len(session.get("result_message_ids") or [])
    total = len(results)
    query = str(session.get("query") or "")

    filters: List[str] = []
    ft = session.get("file_type", "")
    ds = session.get("date_sort", "")
    if ft:
        filters.append(ft.upper())
    if ds:
        filters.append("новые" if ds == "new" else "старые")
    filter_str = f"  [{', '.join(filters)}]" if filters else ""

    if total == 0:
        text = f"По запросу «{query}» ничего не найдено{filter_str}."
    elif shown < total:
        text = f"Показано {shown} из {total}{filter_str}."
    else:
        text = f"Все {total} результатов{filter_str}."

    old_nav_id = int(session.get("nav_message_id") or 0)
    if old_nav_id:
        delete_message(token, chat_id, old_nav_id)

    msg_id = send_message(token, chat_id, text, _nav_keyboard(session_id))
    session["nav_message_id"] = msg_id


def _reset_result_messages(token: str, chat_id: str, session: Dict[str, Any]) -> None:
    """Delete all sent result messages and clear session tracking."""
    for msg_id in session.get("result_message_ids") or []:
        delete_message(token, chat_id, msg_id)
    session["result_message_ids"] = []
    session["feedback_given"] = set()


# ---------------------------------------------------------------------------
# Catalog browser
# ---------------------------------------------------------------------------

CATALOG_SESSIONS: Dict[str, Dict[str, Any]] = {}
CATALOG_PAGE_SIZE = 8


def _catalog_state_db_path(cfg: Dict[str, Any]) -> str:
    qdrant_db_path = str(cfg.get("qdrant_db_path") or "").strip()
    return str(Path(qdrant_db_path) / "index_state.db") if qdrant_db_path else ""


def _catalog_load_paths(cfg: Dict[str, Any]) -> List[str]:
    db_path = _catalog_state_db_path(cfg)
    if not db_path or not Path(db_path).exists():
        return []
    try:
        entries = IndexStateDB(db_path).iter_entries()
        return [str(e["full_path"]) for e in entries if e.get("full_path")]
    except Exception:
        return []


def _catalog_children(all_paths: List[str], current: str) -> tuple[List[str], List[str]]:
    folders: set[str] = set()
    files: List[str] = []
    current_p = Path(current)
    for p_str in all_paths:
        try:
            rel = Path(p_str).relative_to(current_p)
        except ValueError:
            continue
        parts = rel.parts
        if len(parts) == 1:
            files.append(p_str)
        elif len(parts) > 1:
            folders.add(str(current_p / parts[0]))
    return sorted(folders), sorted(files)


def _catalog_update_session(session: Dict[str, Any], current: str, offset: int = 0) -> None:
    folders, files = _catalog_children(session["all_paths"], current)
    session["current"] = current
    session["items"] = [("d", p) for p in folders] + [("f", p) for p in files]
    session["offset"] = offset


def _catalog_text(session: Dict[str, Any]) -> str:
    root = session["root"]
    current = session["current"]
    items: List[tuple[str, str]] = session["items"]
    offset: int = session["offset"]
    try:
        rel_parts = Path(current).relative_to(Path(root)).parts
        parts = [p for p in rel_parts if p not in (".", "")]
        breadcrumb = " / ".join(parts) if parts else "Корень"
    except ValueError:
        breadcrumb = Path(current).name or current
    total = len(items)
    if total == 0:
        return f"📂 {breadcrumb}\n\nПапка пуста."
    end = min(offset + CATALOG_PAGE_SIZE, total)
    return f"📂 {breadcrumb}\n{offset + 1}–{end} из {total}"


def _catalog_keyboard(sid: str, session: Dict[str, Any]) -> Dict[str, Any]:
    items: List[tuple[str, str]] = session["items"]
    offset: int = session["offset"]
    root: str = session["root"]
    current: str = session["current"]
    rows: List[List[Dict[str, str]]] = []
    page = items[offset : offset + CATALOG_PAGE_SIZE]
    for i, (kind, path) in enumerate(page):
        name = _clean_tg_text(Path(path).name, 55)
        abs_idx = offset + i
        label = f"📁 {name}" if kind == "d" else f"📄 {name}"
        rows.append([{"text": label, "callback_data": f"cat:{sid}:{kind}:{abs_idx}"}])
    nav: List[Dict[str, str]] = []
    if Path(current) != Path(root):
        nav.append({"text": "⬆ Назад", "callback_data": f"cat:{sid}:up"})
    if offset + CATALOG_PAGE_SIZE < len(items):
        nav.append({"text": "Ещё ↓", "callback_data": f"cat:{sid}:pg:{offset + CATALOG_PAGE_SIZE}"})
    if offset > 0:
        nav.append({"text": "↑ Начало", "callback_data": f"cat:{sid}:pg:0"})
    if nav:
        rows.append(nav)
    return {"inline_keyboard": rows}


def _send_catalog_view(token: str, chat_id: str, sid: str, session: Dict[str, Any]) -> None:
    text = _catalog_text(session)
    keyboard = _catalog_keyboard(sid, session)
    msg_id = int(session.get("message_id") or 0)
    if msg_id:
        try:
            edit_message_text(token, chat_id=chat_id, message_id=msg_id, text=text, reply_markup=keyboard)
            return
        except Exception:
            pass
    session["message_id"] = send_message(token, chat_id, text, keyboard)


def open_catalog(
    token: str,
    cfg: Dict[str, Any],
    chat_id: str,
    user: Dict[str, Any] | None = None,
    auth_db: UserAuthDB | None = None,
) -> None:
    if not can_use_catalog(user):
        send_message(token, chat_id, "Доступ к каталогу для этой роли закрыт.", _main_menu(user))
        return
    root = str(cfg.get("catalog_path") or "").strip()
    if not root or not Path(root).exists():
        send_message(token, chat_id, "Каталог документов недоступен.")
        return
    all_paths = _catalog_load_paths(cfg)
    if not all_paths:
        send_message(token, chat_id, "Каталог пуст — индексация ещё не выполнялась.")
        return
    # Дерево строится из state БД индексатора, где нет никаких прав: без этого
    # фильтра «Каталог» отдавал весь индекс любому привязанному пользователю.
    if auth_db is not None:
        current = get_authorized_telegram_user(auth_db, chat_id)
        if not can_use_catalog(current) or current.get("username") != user.get("username"):
            send_message(token, chat_id, "Доступ к каталогу изменился.")
            return
        user = current
    all_paths = acl_filter_paths(cfg, user, all_paths)
    if not all_paths:
        send_message(token, chat_id, "Нет доступных вам документов.")
        return
    sid = _session_id()
    session: Dict[str, Any] = {
        "session_id": sid,
        "chat_id": str(chat_id),
        "root": root,
        "current": root,
        "all_paths": all_paths,
        "items": [],
        "offset": 0,
        "message_id": 0,
        "created_at": time.time(),
        "cfg": cfg,
        "user": user,
    }
    CATALOG_SESSIONS[sid] = session
    _cleanup_search_sessions()
    _catalog_update_session(session, root)
    _send_catalog_view(token, chat_id, sid, session)


def _handle_catalog_callback(
    token: str, chat_id: str, user: Dict[str, Any], sid: str, parts: List[str]
) -> None:
    session = CATALOG_SESSIONS.get(sid)
    if not session:
        send_message(token, chat_id, "Сессия каталога устарела. Откройте каталог снова.", _main_menu(user))
        return
    if str(session.get("chat_id") or "") != chat_id:
        return
    if (session.get("user") or {}).get("username") != user.get("username"):
        return
    paths = session.get("all_paths") or []
    if len(acl_filter_paths(dict(session.get("cfg") or {}), user, paths)) != len(paths):
        CATALOG_SESSIONS.pop(sid, None)
        send_message(token, chat_id, "Доступ к документам изменился. Откройте каталог снова.", _main_menu(user))
        return
    subaction = parts[2] if len(parts) > 2 else ""
    if subaction in {"d", "f", "pg"} and (len(parts) < 4 or not parts[3].isascii() or not parts[3].isdigit()):
        return
    if subaction == "up":
        current = session["current"]
        root = session["root"]
        parent = Path(current).parent
        try:
            parent.relative_to(Path(root))
            _catalog_update_session(session, str(parent))
            _send_catalog_view(token, chat_id, sid, session)
        except ValueError:
            pass  # already at root or above — ignore
    elif subaction in ("d", "f") and len(parts) >= 4:
        idx = int(parts[3])
        items: List[tuple[str, str]] = session.get("items") or []
        if idx < len(items):
            kind, path = items[idx]
            if subaction == "d" and kind == "d":
                _catalog_update_session(session, path)
                _send_catalog_view(token, chat_id, sid, session)
            elif subaction == "f" and kind == "f":
                # Повторная проверка прав перед выдачей: список в сессии мог
                # устареть после отзыва доступа.
                if not acl_allows_path(dict(session.get("cfg") or {}), user, path):
                    send_message(token, chat_id, "Файл недоступен: нет прав.", _main_menu(user))
                    return
                try:
                    with chat_action(token, chat_id, "upload_document"):
                        send_document(token, chat_id, path, caption=Path(path).name)
                except Exception as exc:
                    send_message(token, chat_id, f"Не удалось отправить файл:\n{path}\n{exc}", _main_menu(user))
    elif subaction == "pg" and len(parts) >= 4:
        session["offset"] = int(parts[3])
        _send_catalog_view(token, chat_id, sid, session)


def send_search_results(
    token: str,
    searcher: RAGSearcher,
    *,
    chat_id: str,
    query: str,
    username: str,
    previous_session_id: str = "",
    auth_db: UserAuthDB | None = None,
    cfg: Dict[str, Any] | None = None,
    user: Dict[str, Any] | None = None,
) -> str:
    """Run search, send individual result messages + nav message. Returns session_id."""
    if not can_use_catalog(user):
        send_message(token, chat_id, "Доступ к поиску для этой роли закрыт.", _main_menu(user))
        return ""
    q = (query or "").strip()
    if previous_session_id and previous_session_id in SEARCH_SESSIONS:
        old = SEARCH_SESSIONS[previous_session_id]
        if old.get("username") == username and old.get("chat_id") == chat_id:
            q = f"{old.get('query', '')} {q}".strip()
    try:
        results = searcher.search(
            q, limit=30, content_only=False,
            source=f"telegram_bot:{chat_id}", username=username,
            result_filter=acl_result_filter(dict(cfg or {}), user),
        )
    except (ConnectionError, RuntimeError) as exc:
        send_message(token, chat_id, f"Ошибка поиска: {exc}")
        return ""
    if auth_db is not None:
        user = get_authorized_telegram_user(auth_db, chat_id)
        if not can_use_catalog(user) or user.get("username") != username:
            send_message(token, chat_id, "Доступ к поиску изменился.", _main_menu(user))
            return ""
    results = [item for item in (results or []) if isinstance(item, dict)]
    results = acl_filter_results(dict(cfg or {}), user, results)

    sid = _session_id()
    SEARCH_SESSIONS[sid] = {
        "session_id": sid,
        "query": q,
        "chat_id": str(chat_id),
        "username": username,
        "results": results,
        "visible": SEARCH_PAGE_SIZE,
        "created_at": time.time(),
        "file_type": "",
        "date_sort": "",
        "telemetry": getattr(searcher, "telemetry", None),
        "cfg": dict(cfg or {}),
        "user": user,
        "result_message_ids": [],
        "nav_message_id": 0,
        "feedback_given": set(),
    }
    session = SEARCH_SESSIONS[sid]
    _cleanup_search_sessions()

    shown = _filter_session_results(session)[:SEARCH_PAGE_SIZE]
    if shown:
        _send_result_messages(token, chat_id, sid, session, shown)
    _send_nav_message(token, chat_id, sid, session)
    return sid


def _record_result_feedback(
    *,
    auth_db: UserAuthDB | None,
    session: Dict[str, Any],
    result: Dict[str, Any],
    username: str,
    chat_id: str,
    value: int,
    rank: int,
    reason: str,
) -> None:
    telemetry = session.get("telemetry")
    if telemetry is not None and hasattr(telemetry, "log_search_feedback"):
        telemetry.log_search_feedback(
            username=username,
            source=f"telegram_bot:{chat_id}",
            query=str(session.get("query") or ""),
            result_path=_result_path(result),
            result_title=_result_title(result),
            feedback=value,
            result_rank=rank,
            result_score=float(result.get("score") or 0),
            details={"session_id": str(session.get("session_id") or ""), "reason": reason},
        )
    if auth_db is not None:
        _log_tg_auth_event(
            auth_db,
            username=username,
            chat_id=chat_id,
            event_type="telegram_search_feedback",
            ok=True,
            error=f"value={value} | reason={reason} | path={_result_path(result)}",
        )


def _record_visible_feedback(
    *,
    auth_db: UserAuthDB | None,
    session: Dict[str, Any],
    username: str,
    chat_id: str,
    value: int,
    reason: str,
) -> None:
    results = _filter_session_results(session)
    limit = int(session.get("visible") or SEARCH_PAGE_SIZE)
    for index, result in enumerate(results[:limit]):
        _record_result_feedback(
            auth_db=auth_db,
            session=session,
            result=result,
            username=username,
            chat_id=chat_id,
            value=value,
            rank=index + 1,
            reason=reason,
        )



def handle_callback_query(
    *, token: str, auth_db: UserAuthDB, callback_query: Dict[str, Any],
    cfg: Dict[str, Any] | None = None, event_id: str = "",
) -> None:
    cfg = cfg or {}
    data = str(callback_query.get("data") or "")
    sender = callback_query.get("from") or {}
    chat_id = _private_sender(callback_query.get("message") or {}, sender, str(cfg.get("telegram_allowed_chat_id") or ""))
    if not chat_id:
        return
    callback_id = str(callback_query.get("id") or "")
    if callback_id:
        try:
            answer_callback_query(token, callback_id)
        except TelegramAPIError:
            # Telegram may expire the spinner before a queued private event is read.
            pass
    user = get_authorized_telegram_user(auth_db, chat_id)
    if data.startswith("ob:"):
        reply = _onboarding(auth_db, cfg, chat_id, user, data, sender, is_callback=True, event_id=event_id or callback_id)
        if reply:
            _deliver_onboarding(token, cfg, chat_id, reply, user)
        return
    if data.startswith("tglogin:"):
        challenge = data.removeprefix("tglogin:")
        if not user or not re.fullmatch(r"[a-zA-Z0-9_-]{1,53}", challenge):
            send_message(token, chat_id, "Для подтверждения нужен активный привязанный аккаунт.")
            return
        out = auth_db.confirm_telegram_login(token=challenge, telegram_chat_id=chat_id)
        message = "Вход подтверждён. Вернитесь в браузер." if out.get("ok") else "Запрос недействителен. Начните вход заново."
        send_message(token, chat_id, message, _main_menu(user))
        return
    if not can_use_catalog(user):
        send_message(token, chat_id, "Доступ к каталогу для этой роли закрыт.", _main_menu(user))
        return
    _cleanup_search_sessions()
    parts = data.split(":")
    if len(parts) < 2:
        return
    action, sid = parts[0], parts[1]

    if action == "cat":
        _handle_catalog_callback(token, chat_id, user, sid, parts)
        return

    session = SEARCH_SESSIONS.get(sid)
    if not session:
        send_message(token, chat_id, "Сессия поиска устарела. Повторите запрос.", _main_menu(user))
        return
    if str(session.get("chat_id") or "") != chat_id:
        send_message(token, chat_id, "Эти результаты принадлежат другому чату.", _main_menu(user))
        return

    if session.get("username") != user.get("username"):
        return
    saved = session.get("results") or []
    if len(acl_filter_results(dict(session.get("cfg") or {}), user, saved)) != len(saved):
        SEARCH_SESSIONS.pop(sid, None)
        send_message(token, chat_id, "Доступ к документам изменился. Повторите поиск.", _main_menu(user))
        return
    if action in {"open", "fb"} and (len(parts) < 3 or not parts[2].isascii() or not parts[2].isdigit()):
        return
    if action == "open" and len(parts) >= 3:
        index = int(parts[2])
        results = _filter_session_results(session)
        if index >= len(results):
            send_message(token, chat_id, "Результат уже недоступен.", _main_menu(user))
            return
        result = results[index]
        path = _result_path(result)
        if not acl_allows_path(dict(session.get("cfg") or {}), user, path):
            send_message(token, chat_id, "Файл недоступен.", _main_menu(user))
            return
        _record_result_feedback(
            auth_db=auth_db,
            session=session,
            result=result,
            username=str(user.get("username") or ""),
            chat_id=chat_id,
            value=OPEN_FILE_FEEDBACK,
            rank=index + 1,
            reason="open_file",
        )
        try:
            with chat_action(token, chat_id, "upload_document"):
                send_document(token, chat_id, path, caption=_result_title(result))
        except Exception as exc:
            send_message(token, chat_id, f"Не удалось отправить файл:\n{path}\n{exc}", _main_menu(user))
        return

    if action == "fb" and len(parts) >= 4:
        index = int(parts[2])
        value = 1 if parts[3] == "pos" else -1
        results = _filter_session_results(session)
        if index < len(results):
            result = results[index]
            _record_result_feedback(
                auth_db=auth_db,
                session=session,
                result=result,
                username=str(user.get("username") or ""),
                chat_id=chat_id,
                value=3 if value > 0 else -3,
                rank=index + 1,
                reason="explicit",
            )
            feedback_given: set = session.setdefault("feedback_given", set())
            feedback_given.add(index)
            result_msg_ids = session.get("result_message_ids") or []
            if index < len(result_msg_ids):
                edit_message_reply_markup(
                    token,
                    chat_id=chat_id,
                    message_id=result_msg_ids[index],
                    reply_markup=_result_keyboard(sid, index, feedback_given=True),
                )
        return

    if action == "fb_noop":
        return

    if action == "more":
        _record_visible_feedback(
            auth_db=auth_db,
            session=session,
            username=str(user.get("username") or ""),
            chat_id=chat_id,
            value=-1,
            reason="more_variants",
        )
        old_visible = int(session.get("visible") or SEARCH_PAGE_SIZE)
        session["visible"] = old_visible + SEARCH_PAGE_SIZE
        results = _filter_session_results(session)
        new_batch = results[old_visible : session["visible"]]
        if new_batch:
            _send_result_messages(token, chat_id, sid, session, new_batch)
        _send_nav_message(token, chat_id, sid, session)
        return

    if action == "ft" and len(parts) >= 3:
        ft_value = parts[2]
        session["file_type"] = "" if ft_value == "all" else ft_value
        session["visible"] = SEARCH_PAGE_SIZE
        _reset_result_messages(token, chat_id, session)
        results = _filter_session_results(session)[:SEARCH_PAGE_SIZE]
        if results:
            _send_result_messages(token, chat_id, sid, session, results)
        _send_nav_message(token, chat_id, sid, session)
        return

    if action == "date" and len(parts) >= 3:
        date_value = parts[2]
        session["date_sort"] = "" if date_value == "none" else date_value
        session["visible"] = SEARCH_PAGE_SIZE
        _reset_result_messages(token, chat_id, session)
        results = _filter_session_results(session)[:SEARCH_PAGE_SIZE]
        if results:
            _send_result_messages(token, chat_id, sid, session, results)
        _send_nav_message(token, chat_id, sid, session)
        return

    if action == "refine":
        _record_visible_feedback(
            auth_db=auth_db,
            session=session,
            username=str(user.get("username") or ""),
            chat_id=chat_id,
            value=-1,
            reason="refine_query",
        )
        PENDING_REFINEMENTS[chat_id] = sid
        send_message(token, chat_id, "Напишите уточнение к запросу следующим сообщением.", _main_menu(user))
        return


def _portal_text(cfg: dict, user, action: str) -> str:
    shifts = action in {"смены", "/shifts", "путевой лист", "/waybill"}
    allowed = can_use_shifts(user) if shifts else can_use_catalog(user)
    if not allowed:
        return "Для вашей роли этот раздел недоступен."
    origin = employee_office_url(cfg)
    if not origin:
        return "Адрес рабочего кабинета пока не настроен. Обратитесь к администратору."
    if not shifts:
        return f"Рабочий кабинет\n{origin}/"
    if action in {"путевой лист", "/waybill"}:
        return f"Откройте нужную смену, чтобы посмотреть путевые листы.\n{origin}/shifts"
    title = "Журнал смен сотрудников" if can_manage_shifts(user) else "Мои смены"
    return f"{title}\n{origin}/shifts"


def process_message(
    *, searcher: RAGSearcher | None, auth_db: UserAuthDB, text: str, chat_id: str,
    allowed_chat_id: str, telegram_username: str = "", display_name: str = "", cfg: Dict[str, Any] | None = None,
) -> str:
    raw = (text or "").strip()
    low = raw.lower()
    parts = raw.split(maxsplit=1)
    if not is_allowed_chat(chat_id, allowed_chat_id):
        return "Доступ запрещён для этого chat_id."
    user = get_authorized_telegram_user(auth_db, chat_id)
    low = {"помощь": "/help", "кто я": "/whoami", "/menu": "/start"}.get(low, low)
    if low in {"офис", "/office", "смены", "/shifts", "путевой лист", "/waybill"}:
        return _portal_text(cfg or {}, user, low)
    if low in {"поиск", "/search", "каталог", "/catalog", "добавить файл", "/upload"}:
        if not can_use_catalog(user):
            return "Доступ к каталогу для этой роли закрыт."
        if low in {"добавить файл", "/upload"}:
            return "Отправьте сюда документ или фото для добавления в каталог."
        return "Напишите поисковый запрос или откройте каталог кнопкой «Каталог»."

    if low.startswith("/start "):
        payload = parts[1].strip()
        purpose, _, code = payload.partition("_")
        if purpose == "login":
            return "Подтверждайте вход, только если вы сами сейчас открыли кабинет в браузере."
        if purpose not in {"verify", "link"} or not code:
            return "Запросите новую ссылку."
        if purpose == "verify":
            out = auth_db.confirm_verification(telegram_chat_id=chat_id, code=code)
        else:
            out = auth_db.consume_telegram_start_token(token=code, telegram_chat_id=chat_id, expected_purpose="link",
                                                       telegram_username=telegram_username, display_name=display_name)
        if out.get("ok"):
            current = get_authorized_telegram_user(auth_db, chat_id)
            return f"Авторизация прошла успешно.\n\n{_user_help_text(current, chat_id)}"
        return "Ссылка недействительна. Запросите новую."

    if low.startswith("/verify"):
        if len(parts) < 2:
            return "Использование: /verify 123456"
        code = parts[1].strip()
        try:
            out = auth_db.confirm_verification(telegram_chat_id=chat_id, code=code)
        except Exception as exc:
            _log_tg_auth_event(
                auth_db,
                chat_id=chat_id,
                event_type="telegram_verify_failed",
                ok=False,
                error=type(exc).__name__,
            )
            return "Не удалось подтвердить код. Повторите позже."
        if out.get("ok"):
            username = str(out.get("username") or "")
            _log_tg_auth_event(
                auth_db,
                username=username,
                chat_id=chat_id,
                event_type="telegram_verify_success",
                ok=True,
            )
            return f"Пользователь '{out.get('username')}' подтверждён. Теперь можно войти в Web UI."
        _log_tg_auth_event(
            auth_db,
            chat_id=chat_id,
            event_type="telegram_verify_failed",
            ok=False,
            error=str(out.get("reason") or "not_found"),
        )
        if out.get("reason") == "expired":
            return "Код просрочен. Запросите новый код в Web UI."
        return "Код не найден. Проверьте код и chat_id."

    if low.startswith("/recover"):
        if len(parts) < 2:
            return "Использование: /recover 123456"
        code = parts[1].strip()
        try:
            out = auth_db.confirm_password_reset(telegram_chat_id=chat_id, code=code)
        except Exception as exc:
            _log_tg_auth_event(
                auth_db,
                chat_id=chat_id,
                event_type="telegram_recover_failed",
                ok=False,
                error=type(exc).__name__,
            )
            return "Не удалось проверить код восстановления. Повторите позже."
        if out.get("ok"):
            _log_tg_auth_event(
                auth_db,
                username=str(out.get("username") or ""),
                chat_id=chat_id,
                event_type="telegram_recover_success",
                ok=True,
            )
            return "Код восстановления подтверждён. Вернитесь в Web UI и задайте новый пароль."
        _log_tg_auth_event(
            auth_db,
            chat_id=chat_id,
            event_type="telegram_recover_failed",
            ok=False,
            error=str(out.get("reason") or "not_found"),
        )
        if out.get("reason") == "expired":
            return "Код восстановления просрочен. Запросите новый код в Web UI."
        return "Код восстановления не найден. Проверьте код и chat_id."

    if low in ("/start", "/help"):
        user = get_authorized_telegram_user(auth_db, chat_id)
        _log_tg_auth_event(
            auth_db,
            chat_id=chat_id,
            username=str((user or {}).get("username") or ""),
            event_type="telegram_help",
            ok=True,
        )
        return _user_help_text(user, chat_id)

    if low == "/whoami":
        user = get_authorized_telegram_user(auth_db, chat_id)
        if not user:
            _log_tg_auth_event(
                auth_db,
                chat_id=chat_id,
                event_type="telegram_whoami",
                ok=False,
                error="not_authorized",
            )
            return _user_help_text(None, chat_id)
        username = str(user.get("username") or "")
        _log_tg_auth_event(
            auth_db,
            username=username,
            chat_id=chat_id,
            event_type="telegram_whoami",
            ok=True,
        )
        roles = [ROLE_LABELS[r] for r in sorted(user_roles(user))]
        if _employee_admin(user) and "admin" not in user_roles(user):
            roles.append("Администратор организации")
        return f"Вы авторизованы как {username}. Роли: {', '.join(roles)}."

    if low == "/logout":
        try:
            username = auth_db.unlink_telegram_chat_id(chat_id)
        except Exception as exc:
            _log_tg_auth_event(
                auth_db,
                chat_id=chat_id,
                event_type="telegram_logout",
                ok=False,
                error=type(exc).__name__,
            )
            return "Не удалось отвязать Telegram. Повторите позже."
        if not username:
            _log_tg_auth_event(
                auth_db,
                chat_id=chat_id,
                event_type="telegram_logout",
                ok=False,
                error="not_authorized",
            )
            return "Telegram не был привязан к активному пользователю."
        _log_tg_auth_event(
            auth_db,
            username=username,
            chat_id=chat_id,
            event_type="telegram_logout",
            ok=True,
        )
        return f"Telegram отвязан от пользователя '{username}'."

    if not can_use_catalog(user):
        _log_tg_auth_event(auth_db, chat_id=chat_id, event_type="telegram_search_denied", ok=False, error="access_denied")
        return "Доступ к каталогу для этой роли закрыт." if user else "Доступ запрещён. Откройте /start для входа."
    if low.startswith("/"):
        return _user_help_text(user, chat_id)
    if searcher is None:
        return "Поиск временно недоступен."
    username = str(user.get("username") or "")
    _log_tg_auth_event(
        auth_db,
        username=username,
        chat_id=chat_id,
        event_type="telegram_search",
        ok=True,
    )
    return process_query(
        searcher,
        raw,
        source=f"telegram_bot:{chat_id}",
        username=username,
        cfg=cfg,
        user=user,
    )


def handle_update(token: str, auth_db: UserAuthDB, cfg: dict, update: dict) -> None:
    event_id = str(update.get("update_id", ""))
    if isinstance(update.get("callback_query"), dict):
        handle_callback_query(token=token, auth_db=auth_db, cfg=cfg, callback_query=update["callback_query"], event_id=event_id)
        return
    msg = update.get("message") or {}
    sender = msg.get("from") or {}
    allowed = str(cfg.get("telegram_allowed_chat_id") or "").strip()
    chat_id = _private_sender(msg, sender, allowed)
    if not chat_id:
        return
    user = get_authorized_telegram_user(auth_db, chat_id)
    _log_tg_auth_event(auth_db, chat_id=chat_id, event_type="telegram_message_received", ok=True)
    text = str(msg.get("text") or "").strip()
    if text.casefold() == "/start menu":
        text = "/menu"
    cmd = text.lower()
    if re.search(r"[?#&](?:token|start|verifier)=", cmd):
        send_message(token, chat_id, "Откройте личную ссылку на своём устройстве.", _main_menu(user))
        return
    if cmd.startswith("ob:"):
        send_message(token, chat_id, "Выберите действие кнопкой в боте.", _main_menu(user))
        return
    if cmd.startswith("/start login_"):
        challenge = text[len("/start login_"):]
        if not user or not re.fullmatch(r"[a-zA-Z0-9_-]{1,53}", challenge):
            send_message(token, chat_id, "Для входа привяжите Telegram к своему активному аккаунту.", _main_menu(user))
            return
        rows = [[{"text": "Подтвердить вход в браузере", "callback_data": "tglogin:" + challenge}]]
        send_message(token, chat_id, "Подтверждайте вход, только если вы сами сейчас открыли кабинет в браузере.",
                     {"inline_keyboard": rows})
        return
    contact = _contact_hint(msg)
    reserved = cmd.startswith(("/start link_", "/start verify_", "/verify", "/recover")) or cmd in {"/whoami", "кто я"}
    if not reserved and (text or contact):
        reply = _onboarding(auth_db, cfg, chat_id, user, text, sender, event_id=event_id, contact=contact)
        if reply:
            _deliver_onboarding(token, cfg, chat_id, reply, user)
            return
    if contact:
        send_message(token, chat_id, "Контакт можно использовать для приглашения сотрудника через /add_user.", _main_menu(user))
        return
    if _message_file_info(msg):
        if not can_use_catalog(user):
            answer = (_portal_text(cfg, user, "/waybill") if can_use_shifts(user)
                      else "Добавление документов доступно сотруднику офиса.")
        else:
            with chat_action(token, chat_id, "upload_document"):
                out = save_telegram_upload(token=token, cfg=cfg, auth_db=auth_db, chat_id=chat_id, msg=msg, user=user)
            answer = "Файл сохранён. Он появится в поиске после индексации." if out.get("ok") else "Не удалось сохранить файл."
        send_message(token, chat_id, answer, _main_menu(user))
        return
    if not text:
        return
    if cmd in {"каталог", "/catalog"} and can_use_catalog(user):
        with chat_action(token, chat_id):
            open_catalog(token, cfg, chat_id, user=user, auth_db=auth_db)
        return
    menu_labels = {button["text"].lower() for row in _main_menu(user)["keyboard"] for button in row}
    search_query = text[8:].strip() if cmd.startswith("/search ") else ""
    if can_use_catalog(user) and (search_query or (not cmd.startswith("/") and cmd not in menu_labels)):
        from rag_catalog.ui.helpers import _cached_searcher_if_ready

        with chat_action(token, chat_id):
            searcher = _cached_searcher_if_ready(cfg, connect_on_miss=True)
            if searcher is None:
                send_message(token, chat_id, "Поиск временно недоступен. Повторите запрос позже.", _main_menu(user))
            else:
                send_search_results(token, searcher, chat_id=chat_id, query=search_query or text, username=user["username"],
                                    previous_session_id=PENDING_REFINEMENTS.pop(chat_id, ""), cfg=cfg, user=user, auth_db=auth_db)
        return
    answer = process_message(searcher=None, auth_db=auth_db, text=text, chat_id=chat_id, allowed_chat_id=allowed,
                             telegram_username=str(sender.get("username") or ""), display_name=" ".join(
                                 str(sender[k]) for k in ("first_name", "last_name") if sender.get(k)), cfg=cfg)
    send_message(token, chat_id, answer, _main_menu(get_authorized_telegram_user(auth_db, chat_id)))


def main() -> int:
    cfg = load_config()
    token = (cfg.get("telegram_bot_token") or "").strip()
    allowed_chat_id = str(cfg.get("telegram_allowed_chat_id") or "").strip()

    if not token:
        logger.error("Не задан telegram_bot_token в config.json")
        return 1

    if not cfg.get("telegram_enabled", False):
        logger.error("telegram_enabled=false. Включите бота в настройках.")
        return 1

    auth_db = UserAuthDB(_users_db_path(cfg))

    try:
        set_bot_commands(token)
    except Exception as exc:
        logger.warning("Не удалось обновить меню команд Telegram: %s", exc)

    logger.info("Telegram бот запущен. Ограничение chat_id: %s", allowed_chat_id or "нет")
    offset = 0
    polling_failures = 0
    polling_started = False
    from rag_catalog.core.indexing.monitor import IndexHeartbeatMonitor
    index_monitor = IndexHeartbeatMonitor(cfg, auth_db, lambda chat, text: send_message(token, chat, text))
    while True:
        try:
            index_monitor.poll()
            updates = get_updates(token, offset)
            if polling_failures:
                logger.info("Telegram polling восстановлен после %s ошибок", polling_failures)
                polling_failures = 0
            elif not polling_started:
                logger.info("Telegram polling: первый запрос выполнен успешно")
            polling_started = True
            for upd in updates:
                offset = max(offset, int(upd.get("update_id", 0)) + 1)
                try:
                    handle_update(token, auth_db, cfg, upd)
                except Exception as exc:
                    logger.warning("Не удалось обработать событие Telegram %s (%s)", upd.get("update_id"), type(exc).__name__)
        except KeyboardInterrupt:
            logger.info("Остановлено пользователем.")
            return 0
        except TelegramAPIError as exc:
            polling_failures += 1
            delay = min(60.0, POLL_SLEEP_SEC * 2 ** min(polling_failures - 1, 6))
            logger.warning("Telegram polling: %s; повтор через %.0f с", exc, delay)
            time.sleep(delay)
        except Exception as exc:
            logger.exception("Ошибка цикла polling: %s", exc)
            time.sleep(POLL_SLEEP_SEC)


if __name__ == "__main__":
    raise SystemExit(main())
