"""
telegram_bot.py — Telegram бот для RAG-поиска по документам.

Запуск:
    python telegram_bot.py
"""

import logging
import os
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

from rag_catalog.core.rag_core import RAGSearcher, load_config
from rag_catalog.core.user_auth_db import UserAuthDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_TIMEOUT = 40
POLL_SLEEP_SEC = 1.0
SEARCH_PAGE_SIZE = 5
SEARCH_SESSION_TTL_SEC = 30 * 60
CHAT_ACTION_INTERVAL_SEC = 4.0
SEARCH_SESSIONS: Dict[str, Dict[str, Any]] = {}
PENDING_REFINEMENTS: Dict[str, str] = {}


def _api_url(token: str, method: str) -> str:
    return f"https://api.telegram.org/bot{token}/{method}"


def tg_call(token: str, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.post(_api_url(token, method), json=payload, timeout=API_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data


def send_message(token: str, chat_id: str, text: str, reply_markup: Dict[str, Any] | None = None) -> None:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_call(
        token,
        "sendMessage",
        payload,
    )


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
    with file_path.open("rb") as fh:
        resp = requests.post(
            _api_url(token, "sendDocument"),
            data={"chat_id": chat_id, "caption": caption[:1000]},
            files={"document": (file_path.name, fh)},
            timeout=API_TIMEOUT,
        )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")


def get_file_info(token: str, file_id: str) -> Dict[str, Any]:
    data = tg_call(token, "getFile", {"file_id": file_id})
    result = data.get("result") or {}
    if not result.get("file_path"):
        raise RuntimeError(f"Telegram getFile did not return file_path: {data}")
    return result


def download_file(token: str, file_path: str, destination: Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://api.telegram.org/file/bot{token}/{file_path}"
    with requests.get(url, stream=True, timeout=API_TIMEOUT) as resp:
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


def _telegram_deeplink(bot_link: str, purpose: str, token: str) -> str:
    base = str(bot_link or "").strip()
    value = str(token or "").strip()
    if not base or not value:
        return ""
    joiner = "&" if "?" in base else "?"
    return f"{base}{joiner}start={purpose}_{quote(value, safe='')}"


def format_fact_answer(result: Dict[str, Any]) -> str:
    src = result.get("source", {}) or {}
    filename = _clean_tg_text(src.get("filename", "неизвестный файл"), 200)
    full_path = str(src.get("full_path", "") or "")
    excerpt = _clean_tg_text(src.get("text_excerpt", ""), 300)
    answer = _clean_tg_text(result.get("answer", "Ответ не найден"), 800)
    lines = [answer, f"Файл: {filename}"]
    if excerpt:
        lines.append(f"Фрагмент: {excerpt}")
    if full_path:
        lines.append(f"Путь: {_clean_tg_text(full_path, 500)}")
        uri = _file_uri(full_path)
        if uri:
            lines.append(f"Ссылка: {uri}")
    return "\n".join(lines)


def is_allowed_chat(chat_id: str, allowed_chat_id: str) -> bool:
    if not allowed_chat_id:
        return True
    return str(chat_id).strip() == str(allowed_chat_id).strip()


def get_authorized_telegram_user(auth_db: UserAuthDB, chat_id: str) -> Dict[str, Any] | None:
    try:
        return auth_db.get_user_by_telegram_chat_id(str(chat_id or "").strip())
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


def _log_tg_message_event(
    auth_db: UserAuthDB,
    *,
    chat_id: str,
    event_type: str,
    ok: bool,
    text: str = "",
    username: str = "",
    error: str = "",
) -> None:
    details = _clean_tg_text(text, max_len=120)
    suffix = f"text={details}" if details else ""
    merged_error = " | ".join([part for part in [error, suffix] if part])
    _log_tg_auth_event(
        auth_db,
        username=username,
        chat_id=chat_id,
        event_type=event_type,
        ok=ok,
        error=merged_error,
    )


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
    info = _message_file_info(msg)
    if not info or not info.get("file_id"):
        return {"ok": False, "reason": "no_file"}
    username = str(user.get("username") or "").strip().lower()
    destination = _unique_destination(_upload_root(cfg, username), str(info.get("file_name") or "telegram_file"))
    tg_file = get_file_info(token, str(info["file_id"]))
    bytes_written = download_file(token, str(tg_file["file_path"]), destination)
    caption = _clean_tg_text(str(msg.get("caption") or ""), 500)
    _log_tg_auth_event(
        auth_db,
        username=username,
        chat_id=chat_id,
        event_type="telegram_file_uploaded",
        ok=True,
        error=f"path={destination} | bytes={bytes_written} | caption={caption}",
    )
    return {
        "ok": True,
        "path": str(destination),
        "bytes": bytes_written,
        "kind": str(info.get("kind") or "file"),
    }


def _main_menu(user: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if user:
        rows = [
            [{"text": "Поиск"}, {"text": "Добавить файл"}],
            [{"text": "Кто я"}, {"text": "Помощь"}],
        ]
    else:
        rows = [
            [{"text": "Заявка"}, {"text": "Помощь"}],
            [{"text": "Кто я"}],
        ]
    if user and str(user.get("role") or "") == "admin":
        rows.append([{"text": "Добавить пользователя"}, {"text": "Заявки"}])
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
                {"command": "upload", "description": "Добавить файл"},
                {"command": "register", "description": "Подать заявку"},
                {"command": "logout", "description": "Отвязать Telegram"},
            ],
        },
    )


def _admin_help_text() -> str:
    return (
        "Команды администратора:\n"
        "/add_user @telegram username — создать активного пользователя и ссылку приглашения\n"
        "/requests — показать заявки\n"
        "/approve ID — одобрить заявку\n"
        "/reject ID — отклонить заявку\n\n"
        "Можно также отправить контакт Telegram-пользователя."
    )


def _user_help_text(user: Dict[str, Any] | None, chat_id: str) -> str:
    if user:
        role = str(user.get("role") or "user")
        admin_block = f"\n\n{_admin_help_text()}" if role == "admin" else ""
        return (
            f"Вы авторизованы как {user.get('username')}.\n"
            f"Роль: {role}.\n\n"
            "Отправьте вопрос по документам обычным сообщением.\n"
            "Пример: Сколько весит PC300\n\n"
            "Чтобы добавить файл, отправьте документ или фото в этот чат.\n\n"
            "Команды:\n"
            "/whoami — текущий пользователь\n"
            "/upload — как добавить файл\n"
            "/logout — отвязать Telegram"
            f"{admin_block}"
        )
    return (
        "Вы пока не авторизованы.\n\n"
        "Откройте ссылку привязки или приглашения из Web UI.\n"
        "Если ссылки нет, отправьте /register ФИО, чтобы создать заявку.\n"
        f"Ваш chat_id: {chat_id}"
    )


def process_contact_message(
    *,
    auth_db: UserAuthDB,
    sender_chat_id: str,
    contact: Dict[str, Any],
    allowed_chat_id: str,
    app_auth_link: str,
) -> Dict[str, str]:
    if not is_allowed_chat(sender_chat_id, allowed_chat_id):
        _log_tg_auth_event(
            auth_db,
            chat_id=sender_chat_id,
            event_type="telegram_contact_denied_allowed_chat",
            ok=False,
            error="chat_not_allowed",
        )
        return {"reply": "Доступ запрещён для этого chat_id."}

    admin_user = get_authorized_telegram_user(auth_db, sender_chat_id)
    if not admin_user or str(admin_user.get("role") or "") != "admin":
        _log_tg_auth_event(
            auth_db,
            chat_id=sender_chat_id,
            username=str((admin_user or {}).get("username") or ""),
            event_type="telegram_contact_denied_not_admin",
            ok=False,
            error="not_admin",
        )
        return {"reply": "Добавлять пользователей через контакты может только администратор."}

    contact_chat_id = str(contact.get("user_id") or "").strip()
    if not contact_chat_id:
        _log_tg_auth_event(
            auth_db,
            username=str(admin_user.get("username") or ""),
            chat_id=sender_chat_id,
            event_type="telegram_contact_denied_no_user_id",
            ok=False,
            error="missing_user_id",
        )
        return {"reply": "В контакте нет Telegram user_id. Перешлите контакт Telegram-пользователя, а не телефонную карточку."}

    first_name = str(contact.get("first_name") or "").strip()
    last_name = str(contact.get("last_name") or "").strip()
    display_name = " ".join(part for part in [first_name, last_name] if part).strip()
    username_hint = str(contact.get("username") or f"tg_{contact_chat_id}")
    created = auth_db.upsert_user_from_telegram_contact(
        telegram_chat_id=contact_chat_id,
        username_hint=username_hint,
        display_name=display_name,
    )
    target_username = str(created.get("username") or "")
    invite_link = ""
    try:
        invite = auth_db.create_telegram_token(
            purpose="invite",
            username=target_username,
            telegram_chat_id=contact_chat_id,
            display_name=display_name,
            created_by=str(admin_user.get("username") or ""),
            ttl_minutes=7 * 24 * 60,
        )
        invite_link = _telegram_deeplink(app_auth_link, "invite", str(invite.get("token") or ""))
    except Exception:
        logger.debug("Не удалось создать invite-link для контакта", exc_info=True)

    _log_tg_auth_event(
        auth_db,
        username=target_username,
        chat_id=sender_chat_id,
        event_type="telegram_contact_upsert",
        ok=True,
    )

    intro = (
        f"Ваш Telegram привязан к пользователю '{target_username}'. "
        "Теперь можно сразу пользоваться ботом для поиска."
    )
    link_line = f"\nСсылка для активации бота: {invite_link or app_auth_link}" if (invite_link or app_auth_link) else ""
    temp_password = str(created.get("temp_password") or "")
    credentials_line = (
        f"\nЛогин: {target_username}\nВременный пароль: {temp_password}\n"
        "После входа смените пароль в настройках."
        if temp_password
        else ""
    )
    notify_text = f"{intro}{link_line}{credentials_line}"
    created_note = "создан" if created.get("created") else "обновлён"
    return {
        "reply": f"Пользователь '{target_username}' {created_note} и активирован для Telegram.",
        "notify_chat_id": contact_chat_id,
        "notify_text": notify_text,
        "invite_link": invite_link,
    }


def process_query(searcher: RAGSearcher, text: str, source: str = "telegram_bot", username: str = "") -> str:
    q = (text or "").strip()
    if not q:
        return "Пустой запрос."

    try:
        fact = searcher.answer_fact_question(q, limit=30)
    except (ConnectionError, RuntimeError) as exc:
        return f"Ошибка инфраструктуры поиска: {exc}"

    if fact.get("ok"):
        return format_fact_answer(fact)

    try:
        results = searcher.search(q, limit=3, content_only=False, source=source, username=username)
    except (ConnectionError, RuntimeError) as exc:
        return f"Ошибка инфраструктуры поиска: {exc}"
    if not results:
        return "Ничего не найдено."

    lines = ["Точный факт не извлечён. Ближайшие результаты:"]
    for i, r in enumerate(results, 1):
        lines.append(
            f"{i}. {r.get('filename', '')} | score={r.get('score', 0)} | путь={r.get('full_path', '')}"
        )
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


def _search_keyboard(session_id: str, session: Dict[str, Any], visible_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows: List[List[Dict[str, str]]] = []
    for index, result in enumerate(visible_results):
        number = index + 1
        rows.append([
            {"text": f"📄 Получить {number}", "callback_data": f"open:{session_id}:{index}"},
            {"text": f"👍 {number}", "callback_data": f"fb:{session_id}:{index}:pos"},
            {"text": f"👎 {number}", "callback_data": f"fb:{session_id}:{index}:neg"},
        ])
    rows.append([
        {"text": "Ещё варианты", "callback_data": f"more:{session_id}"},
        {"text": "Дополнить поиск", "callback_data": f"refine:{session_id}"},
    ])
    rows.append([
        {"text": "PDF", "callback_data": f"ft:{session_id}:pdf"},
        {"text": "Word", "callback_data": f"ft:{session_id}:doc"},
        {"text": "Excel", "callback_data": f"ft:{session_id}:xls"},
        {"text": "Фото", "callback_data": f"ft:{session_id}:img"},
        {"text": "Все", "callback_data": f"ft:{session_id}:all"},
    ])
    rows.append([
        {"text": "Новые", "callback_data": f"date:{session_id}:new"},
        {"text": "Старые", "callback_data": f"date:{session_id}:old"},
        {"text": "Без даты", "callback_data": f"date:{session_id}:none"},
    ])
    return {"inline_keyboard": rows}


def _format_search_page(session_id: str) -> tuple[str, Dict[str, Any] | None]:
    session = SEARCH_SESSIONS.get(session_id)
    if not session:
        return "Сессия поиска устарела. Повторите запрос.", None
    results = _filter_session_results(session)
    limit = int(session.get("visible") or SEARCH_PAGE_SIZE)
    shown = results[:limit]
    query = str(session.get("query") or "")
    if not shown:
        return f"По запросу «{query}» ничего не найдено с текущими фильтрами.", _search_keyboard(session_id, session, [])
    lines = [
        f"Варианты по запросу «{query}\":",
        "Кнопки ниже относятся к номерам результатов: 📄 получить файл, 👍 полезно, 👎 не то.",
    ]
    for index, result in enumerate(shown, 1):
        path = _clean_tg_text(_result_path(result), 220)
        lines.append(f"{index}. {_result_title(result)}\n{path}")
    filters = []
    if session.get("file_type"):
        filters.append(f"тип={session['file_type']}")
    if session.get("date_sort"):
        filters.append(f"дата={session['date_sort']}")
    if filters:
        lines.append("\nФильтр: " + ", ".join(filters))
    return "\n\n".join(lines), _search_keyboard(session_id, session, shown)


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


def build_interactive_search_response(
    searcher: RAGSearcher,
    *,
    chat_id: str,
    query: str,
    username: str,
    previous_session_id: str = "",
) -> Dict[str, Any]:
    q = (query or "").strip()
    if previous_session_id and previous_session_id in SEARCH_SESSIONS:
        old = SEARCH_SESSIONS[previous_session_id]
        q = f"{old.get('query', '')} {q}".strip()
    try:
        results = searcher.search(q, limit=30, content_only=False, source=f"telegram_bot:{chat_id}", username=username)
    except (ConnectionError, RuntimeError) as exc:
        return {"text": f"Ошибка инфраструктуры поиска: {exc}", "reply_markup": None}
    results = [item for item in (results or []) if isinstance(item, dict)]
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
    }
    text, markup = _format_search_page(sid)
    return {"text": text, "reply_markup": markup, "session_id": sid}


def handle_callback_query(
    *,
    token: str,
    auth_db: UserAuthDB,
    callback_query: Dict[str, Any],
) -> None:
    query_id = str(callback_query.get("id") or "")
    data = str(callback_query.get("data") or "")
    msg = callback_query.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = str(chat.get("id") or "")
    message_id = int(msg.get("message_id") or 0)
    user = get_authorized_telegram_user(auth_db, chat_id)
    if query_id:
        answer_callback_query(token, query_id)
    if not user:
        send_message(token, chat_id, "Telegram не привязан к активному пользователю.", _main_menu(None))
        return
    _cleanup_search_sessions()
    parts = data.split(":")
    if len(parts) < 2:
        return
    action, sid = parts[0], parts[1]
    session = SEARCH_SESSIONS.get(sid)
    if not session:
        send_message(token, chat_id, "Сессия поиска устарела. Повторите запрос.", _main_menu(user))
        return
    if str(session.get("chat_id") or "") != chat_id:
        send_message(token, chat_id, "Эти результаты принадлежат другому чату.", _main_menu(user))
        return

    if action == "open" and len(parts) >= 3:
        index = int(parts[2])
        results = _filter_session_results(session)
        if index >= len(results):
            send_message(token, chat_id, "Результат уже недоступен.", _main_menu(user))
            return
        result = results[index]
        path = _result_path(result)
        _record_result_feedback(
            auth_db=auth_db,
            session=session,
            result=result,
            username=str(user.get("username") or ""),
            chat_id=chat_id,
            value=2,
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
        if index >= len(results):
            send_message(token, chat_id, "Результат уже недоступен.", _main_menu(user))
            return
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
        send_message(
            token,
            chat_id,
            "Оценка сохранена. Следующие поиски будут учитывать этот результат.",
            _main_menu(user),
        )
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
        session["visible"] = int(session.get("visible") or SEARCH_PAGE_SIZE) + SEARCH_PAGE_SIZE
    elif action == "ft" and len(parts) >= 3:
        value = parts[2]
        session["file_type"] = "" if value == "all" else value
        session["visible"] = SEARCH_PAGE_SIZE
    elif action == "date" and len(parts) >= 3:
        value = parts[2]
        session["date_sort"] = "" if value == "none" else value
        session["visible"] = SEARCH_PAGE_SIZE
    elif action == "refine":
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
    text, markup = _format_search_page(sid)
    if message_id:
        edit_message_text(token, chat_id=chat_id, message_id=message_id, text=text, reply_markup=markup)
    else:
        send_message(token, chat_id, text, markup)


def _is_menu_or_command_text(text: str) -> bool:
    low = (text or "").strip().lower()
    if not low:
        return True
    if low.startswith("/"):
        return True
    return low in {
        "поиск",
        "добавить файл",
        "кто я",
        "помощь",
        "заявка",
        "заявки",
        "добавить пользователя",
    }


def process_message(
    *,
    searcher: RAGSearcher,
    auth_db: UserAuthDB,
    text: str,
    chat_id: str,
    allowed_chat_id: str,
    bot_link: str = "",
    telegram_username: str = "",
    display_name: str = "",
) -> str:
    raw = (text or "").strip()
    low = raw.lower()
    parts = raw.split(maxsplit=1)

    if low == "помощь":
        low = "/help"
        raw = "/help"
        parts = [raw]
    elif low == "кто я":
        low = "/whoami"
        raw = "/whoami"
        parts = [raw]
    elif low == "заявки":
        low = "/requests"
        raw = "/requests"
        parts = [raw]
    elif low == "поиск":
        return "Напишите поисковый запрос обычным сообщением. Я покажу варианты кнопками."
    elif low in {"добавить файл", "/upload"}:
        return "Отправьте сюда документ или фото. Я сохраню файл в каталог Telegram Uploads, после индексации он появится в поиске."
    elif low == "заявка":
        if get_authorized_telegram_user(auth_db, chat_id):
            return "Вы уже авторизованы. Заявка не нужна."
        return "Для заявки напишите: /register ФИО"
    elif low == "добавить пользователя":
        return "Для добавления пользователя напишите: /add_user @telegram username"

    if low.startswith("/start ") and len(parts) > 1:
        payload = parts[1].strip()
        if "_" in payload:
            purpose, token = payload.split("_", 1)
            purpose = purpose.strip().lower()
            token = token.strip()
        else:
            purpose, token = "", ""
        if purpose in {"link", "invite", "login", "register", "verify"}:
            if not token:
                return "Код в ссылке пустой. Запросите новую ссылку."
            try:
                if purpose == "verify":
                    out = auth_db.confirm_verification(telegram_chat_id=chat_id, code=token)
                    out = {**out, "purpose": "link" if out.get("ok") else "verify"}
                else:
                    out = auth_db.consume_telegram_start_token(
                        token=token,
                        telegram_chat_id=chat_id,
                        telegram_username=telegram_username,
                        display_name=display_name,
                    )
            except Exception as exc:
                _log_tg_auth_event(
                    auth_db,
                    chat_id=chat_id,
                    event_type=f"telegram_{purpose}_failed",
                    ok=False,
                    error=str(exc),
                )
                return f"Ошибка обработки ссылки: {exc}"
            if out.get("ok"):
                username = str(out.get("username") or "")
                _log_tg_auth_event(
                    auth_db,
                    username=username,
                    chat_id=chat_id,
                    event_type=f"telegram_{str(out.get('purpose') or purpose)}_success",
                    ok=True,
                )
                user = get_authorized_telegram_user(auth_db, chat_id)
                if str(out.get("purpose") or purpose) == "login":
                    return "Вход подтверждён. Вернитесь в приложение."
                if str(out.get("purpose") or purpose) == "register":
                    return "Заявка отправлена администратору. После одобрения бот сообщит, что доступ открыт."
                return f"Ок, вы авторизованы как {username}.\n\n{_user_help_text(user, chat_id)}"
            _log_tg_auth_event(
                auth_db,
                chat_id=chat_id,
                event_type=f"telegram_{purpose}_failed",
                ok=False,
                error=str(out.get("reason") or "not_found"),
            )
            if out.get("reason") == "expired":
                return "Ссылка просрочена. Запросите новую."
            if out.get("reason") == "telegram_not_linked":
                return "Этот Telegram не привязан к активному пользователю. Сначала используйте ссылку привязки или приглашения."
            if out.get("reason") == "telegram_username_mismatch":
                return "Ссылка выписана на другой Telegram username. Попросите администратора создать новую ссылку."
            return "Код из ссылки не найден. Запросите новую ссылку."

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
                error=str(exc),
            )
            return f"Ошибка подтверждения: {exc}"
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
                error=str(exc),
            )
            return f"Ошибка восстановления: {exc}"
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

    if not is_allowed_chat(chat_id, allowed_chat_id):
        _log_tg_auth_event(
            auth_db,
            chat_id=chat_id,
            event_type="telegram_denied_allowed_chat",
            ok=False,
            error="chat_not_allowed",
        )
        return "Доступ запрещён для этого chat_id."

    if low in ("/start", "/help"):
        user = get_authorized_telegram_user(auth_db, chat_id)
        _log_tg_message_event(
            auth_db,
            chat_id=chat_id,
            username=str((user or {}).get("username") or ""),
            event_type="telegram_help",
            ok=True,
            text=raw,
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
        return f"Вы авторизованы как {username}. Роль: {user.get('role', 'user')}."

    if low == "/logout":
        try:
            username = auth_db.unlink_telegram_chat_id(chat_id)
        except Exception as exc:
            _log_tg_auth_event(
                auth_db,
                chat_id=chat_id,
                event_type="telegram_logout",
                ok=False,
                error=str(exc),
            )
            return f"Не удалось отвязать Telegram: {exc}"
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

    if low.startswith("/register"):
        if get_authorized_telegram_user(auth_db, chat_id):
            return "Вы уже авторизованы. Новая заявка не нужна."
        body = parts[1].strip() if len(parts) > 1 else ""
        username_hint = ""
        display = body or display_name
        if body and " " not in body and re.match(r"^[a-zA-Z0-9_.-]{3,}$", body):
            username_hint = body
        req = auth_db.create_registration_request(
            username=username_hint,
            display_name=display,
            telegram_chat_id=chat_id,
            telegram_username=telegram_username,
            source="telegram",
            note=f"raw={raw}",
        )
        _log_tg_auth_event(
            auth_db,
            chat_id=chat_id,
            event_type="telegram_registration_requested",
            ok=bool(req.get("ok")),
            error=f"id={req.get('id', '')}",
        )
        return f"Заявка #{req.get('id')} отправлена администратору."

    if low.startswith("/requests"):
        user = get_authorized_telegram_user(auth_db, chat_id)
        if not user or str(user.get("role") or "") != "admin":
            return "Команда доступна только администратору."
        rows = auth_db.list_registration_requests(status="pending", limit=10)
        if not rows:
            return "Ожидающих заявок нет."
        lines = ["Ожидающие заявки:"]
        for row in rows:
            tg = f"@{row.get('telegram_username')}" if row.get("telegram_username") else str(row.get("telegram_chat_id") or "")
            lines.append(f"#{row.get('id')}: {row.get('username') or row.get('display_name') or 'без имени'} · {tg} · {row.get('source')}")
        lines.append("\nОдобрить: /approve ID\nОтклонить: /reject ID")
        return "\n".join(lines)

    if low.startswith("/approve") or low.startswith("/reject"):
        user = get_authorized_telegram_user(auth_db, chat_id)
        if not user or str(user.get("role") or "") != "admin":
            return "Команда доступна только администратору."
        if len(parts) < 2 or not parts[1].strip().split()[0].isdigit():
            return "Использование: /approve 12 или /reject 12"
        req_id = int(parts[1].strip().split()[0])
        decision = "approved" if low.startswith("/approve") else "rejected"
        out = auth_db.review_registration_request(
            request_id=req_id,
            reviewed_by=str(user.get("username") or ""),
            decision=decision,
        )
        if not out.get("ok"):
            return f"Не удалось обработать заявку: {out.get('reason')}"
        if decision == "rejected":
            return f"Заявка #{req_id} отклонена."
        return f"Заявка #{req_id} одобрена. Пользователь: {out.get('username')}."

    if low.startswith("/add_user"):
        admin = get_authorized_telegram_user(auth_db, chat_id)
        if not admin or str(admin.get("role") or "") != "admin":
            return "Команда доступна только администратору."
        args = parts[1].split() if len(parts) > 1 else []
        if not args:
            return "Использование: /add_user @telegram username"
        tg_name = args[0] if args[0].startswith("@") else ""
        username = args[1] if len(args) > 1 else (tg_name.lstrip("@") if tg_name else args[0])
        out = auth_db.create_admin_invite(
            created_by=str(admin.get("username") or ""),
            username=username,
            display_name=username,
            telegram_username=tg_name,
        )
        if not out.get("ok"):
            return f"Не удалось создать пользователя: {out.get('reason')}"
        link = _telegram_deeplink(bot_link, "invite", str(out.get("token") or ""))
        return (
            f"Пользователь '{out.get('username')}' создан и активирован.\n"
            f"Временный пароль: {out.get('temp_password')}\n"
            f"Ссылка для Telegram: {link}"
        )

    user = get_authorized_telegram_user(auth_db, chat_id)
    if not user:
        _log_tg_auth_event(
            auth_db,
            chat_id=chat_id,
            event_type="telegram_search_denied",
            ok=False,
            error="not_authorized",
        )
        return (
            "Доступ запрещён. Telegram chat_id не привязан к активному пользователю.\n"
            "Откройте ссылку привязки/приглашения или отправьте /register ФИО для заявки."
        )

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
    )


def main() -> int:
    cfg = load_config()
    token = (cfg.get("telegram_bot_token") or "").strip()
    allowed_chat_id = (cfg.get("telegram_allowed_chat_id") or "").strip()
    app_auth_link = _app_auth_link(cfg)

    if not token:
        logger.error("Не задан telegram_bot_token в config.json")
        return 1

    if not cfg.get("telegram_enabled", False):
        logger.error("telegram_enabled=false. Включите бота в настройках.")
        return 1

    searcher = RAGSearcher(cfg)
    auth_db = UserAuthDB(_users_db_path(cfg))
    if not searcher.connected:
        logger.error("Нет подключения к Qdrant")
        return 1

    try:
        set_bot_commands(token)
    except Exception as exc:
        logger.warning("Не удалось обновить меню команд Telegram: %s", exc)

    logger.info("Telegram бот запущен. Ограничение chat_id: %s", allowed_chat_id or "нет")
    offset = 0
    while True:
        try:
            updates = get_updates(token, offset)
            for upd in updates:
                offset = max(offset, int(upd.get("update_id", 0)) + 1)
                callback_query = upd.get("callback_query") if isinstance(upd.get("callback_query"), dict) else None
                if callback_query:
                    try:
                        handle_callback_query(
                            token=token,
                            auth_db=auth_db,
                            callback_query=callback_query,
                        )
                    except Exception as exc:
                        logger.exception("Ошибка callback_query: %s", exc)
                    continue

                msg = upd.get("message") or {}
                chat = msg.get("chat") or {}
                sender = msg.get("from") if isinstance(msg.get("from"), dict) else {}
                chat_id = str(chat.get("id", ""))
                telegram_username = str(sender.get("username") or "")
                display_name = " ".join(
                    part for part in [str(sender.get("first_name") or ""), str(sender.get("last_name") or "")] if part
                ).strip()
                contact = msg.get("contact") if isinstance(msg.get("contact"), dict) else None
                text = (msg.get("text") or "").strip()

                if contact:
                    _log_tg_message_event(
                        auth_db,
                        chat_id=chat_id,
                        event_type="telegram_contact_received",
                        ok=True,
                        text=f"contact_user_id={contact.get('user_id', '')}",
                    )
                    with chat_action(token, chat_id, "typing"):
                        out = process_contact_message(
                            auth_db=auth_db,
                            sender_chat_id=chat_id,
                            contact=contact,
                            allowed_chat_id=allowed_chat_id,
                            app_auth_link=app_auth_link,
                        )
                    admin_user = get_authorized_telegram_user(auth_db, chat_id)
                    send_message(token, chat_id, str(out.get("reply") or "Операция завершена."), _main_menu(admin_user))
                    _log_tg_message_event(
                        auth_db,
                        chat_id=chat_id,
                        event_type="telegram_message_replied",
                        ok=True,
                        text=str(out.get("reply") or ""),
                    )
                    notify_chat = str(out.get("notify_chat_id") or "").strip()
                    notify_text = str(out.get("notify_text") or "").strip()
                    if notify_chat and notify_text:
                        try:
                            notify_user = get_authorized_telegram_user(auth_db, notify_chat)
                            send_message(token, notify_chat, notify_text, _main_menu(notify_user))
                            _log_tg_message_event(
                                auth_db,
                                chat_id=notify_chat,
                                event_type="telegram_message_replied",
                                ok=True,
                                text=notify_text,
                            )
                        except Exception as exc:
                            _log_tg_message_event(
                                auth_db,
                                chat_id=notify_chat,
                                event_type="telegram_message_replied",
                                ok=False,
                                error=str(exc),
                                text=notify_text,
                            )
                            logger.warning("Не удалось отправить приветствие пользователю %s: %s", notify_chat, exc)
                    continue

                if _message_file_info(msg):
                    user = get_authorized_telegram_user(auth_db, chat_id)
                    if not user:
                        _log_tg_message_event(
                            auth_db,
                            chat_id=chat_id,
                            event_type="telegram_file_upload_denied",
                            ok=False,
                            error="not_authorized",
                        )
                        send_message(
                            token,
                            chat_id,
                            "Добавлять файлы может только авторизованный пользователь. Сначала привяжите Telegram.",
                            _main_menu(None),
                        )
                        continue
                    try:
                        with chat_action(token, chat_id, "upload_document"):
                            out = save_telegram_upload(
                                token=token,
                                cfg=cfg,
                                auth_db=auth_db,
                                chat_id=chat_id,
                                msg=msg,
                                user=user,
                            )
                        if out.get("ok"):
                            answer = (
                                "Файл сохранён.\n"
                                f"Путь: {out.get('path')}\n\n"
                                "В поиске он появится после очередной индексации."
                            )
                        else:
                            answer = f"Не удалось сохранить файл: {out.get('reason')}"
                        send_message(token, chat_id, answer, _main_menu(user))
                    except Exception as exc:
                        _log_tg_message_event(
                            auth_db,
                            chat_id=chat_id,
                            username=str(user.get("username") or ""),
                            event_type="telegram_file_uploaded",
                            ok=False,
                            error=str(exc),
                        )
                        send_message(token, chat_id, f"Не удалось сохранить файл: {exc}", _main_menu(user))
                    continue

                if not text:
                    _log_tg_message_event(
                        auth_db,
                        chat_id=chat_id,
                        event_type="telegram_message_ignored",
                        ok=True,
                        error="empty_text",
                    )
                    continue

                _log_tg_message_event(
                    auth_db,
                    chat_id=chat_id,
                    event_type="telegram_message_received",
                    ok=True,
                    text=text,
                )
                user = get_authorized_telegram_user(auth_db, chat_id)
                if user and not _is_menu_or_command_text(text):
                    pending_sid = PENDING_REFINEMENTS.pop(chat_id, "")
                    with chat_action(token, chat_id, "typing"):
                        response = build_interactive_search_response(
                            searcher,
                            chat_id=chat_id,
                            query=text,
                            username=str(user.get("username") or ""),
                            previous_session_id=pending_sid,
                        )
                    send_message(
                        token,
                        chat_id,
                        str(response.get("text") or ""),
                        response.get("reply_markup") or _main_menu(user),
                    )
                    _log_tg_message_event(
                        auth_db,
                        chat_id=chat_id,
                        username=str(user.get("username") or ""),
                        event_type="telegram_search_interactive",
                        ok=True,
                        text=text,
                    )
                    continue

                with chat_action(token, chat_id, "typing"):
                    answer = process_message(
                        searcher=searcher,
                        auth_db=auth_db,
                        text=text,
                        chat_id=chat_id,
                        allowed_chat_id=allowed_chat_id,
                        bot_link=app_auth_link,
                        telegram_username=telegram_username,
                        display_name=display_name,
                    )
                try:
                    user = get_authorized_telegram_user(auth_db, chat_id)
                    send_message(token, chat_id, answer, _main_menu(user))
                    _log_tg_message_event(
                        auth_db,
                        chat_id=chat_id,
                        event_type="telegram_message_replied",
                        ok=True,
                        text=answer,
                    )
                except Exception as exc:
                    _log_tg_message_event(
                        auth_db,
                        chat_id=chat_id,
                        event_type="telegram_message_replied",
                        ok=False,
                        error=str(exc),
                        text=answer,
                    )
                    raise
        except KeyboardInterrupt:
            logger.info("Остановлено пользователем.")
            return 0
        except Exception as exc:
            logger.exception("Ошибка цикла polling: %s", exc)
            time.sleep(POLL_SLEEP_SEC)


if __name__ == "__main__":
    raise SystemExit(main())
