"""
telegram_bot.py — Telegram бот для RAG-поиска по документам.

Запуск:
    python telegram_bot.py
"""

import logging
import time
from typing import Any, Dict, List

import requests

from rag_catalog.core.rag_core import RAGSearcher, load_config
from rag_catalog.core.user_auth_db import UserAuthDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_TIMEOUT = 40
POLL_SLEEP_SEC = 1.0


def _api_url(token: str, method: str) -> str:
    return f"https://api.telegram.org/bot{token}/{method}"


def tg_call(token: str, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.post(_api_url(token, method), json=payload, timeout=API_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data


def send_message(token: str, chat_id: str, text: str) -> None:
    tg_call(
        token,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
    )


def get_updates(token: str, offset: int) -> List[Dict[str, Any]]:
    data = tg_call(
        token,
        "getUpdates",
        {
            "offset": offset,
            "timeout": 25,
            "allowed_updates": ["message"],
        },
    )
    return data.get("result", [])


def format_fact_answer(result: Dict[str, Any]) -> str:
    src = result.get("source", {}) or {}
    filename = src.get("filename", "неизвестный файл")
    full_path = src.get("full_path", "")
    excerpt = src.get("text_excerpt", "")
    lines = [
        result.get("answer", "Ответ не найден"),
        f"Файл: {filename}",
    ]
    if excerpt:
        lines.append(f"Фрагмент: {excerpt}")
    if full_path:
        lines.append(f"Путь: {full_path}")
        lines.append(f"Ссылка: file:///{full_path.replace(chr(92), '/')}")
    return "\n".join(lines)


def is_allowed_chat(chat_id: str, allowed_chat_id: str) -> bool:
    if not allowed_chat_id:
        return True
    return str(chat_id).strip() == str(allowed_chat_id).strip()


def _users_db_path(cfg: Dict[str, Any]) -> str:
    value = str(cfg.get("users_db_path", "") or "").strip()
    if value:
        return value
    return f"{cfg.get('qdrant_db_path', '.')}/rag_users.db"


def process_query(searcher: RAGSearcher, text: str, source: str = "telegram_bot") -> str:
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
        results = searcher.search(q, limit=3, content_only=False, source=source)
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


def process_message(
    *,
    searcher: RAGSearcher,
    auth_db: UserAuthDB,
    text: str,
    chat_id: str,
    allowed_chat_id: str,
) -> str:
    raw = (text or "").strip()
    low = raw.lower()

    if low.startswith("/verify"):
        parts = raw.split(maxsplit=1)
        if len(parts) < 2:
            return "Использование: /verify 123456"
        code = parts[1].strip()
        try:
            out = auth_db.confirm_verification(telegram_chat_id=chat_id, code=code)
        except Exception as exc:
            return f"Ошибка подтверждения: {exc}"
        if out.get("ok"):
            return f"Пользователь '{out.get('username')}' подтверждён. Теперь можно войти в Web UI."
        if out.get("reason") == "expired":
            return "Код просрочен. Запросите новый код в Web UI."
        return "Код не найден. Проверьте код и chat_id."

    if low.startswith("/recover"):
        parts = raw.split(maxsplit=1)
        if len(parts) < 2:
            return "Использование: /recover 123456"
        code = parts[1].strip()
        try:
            out = auth_db.confirm_password_reset(telegram_chat_id=chat_id, code=code)
        except Exception as exc:
            return f"Ошибка восстановления: {exc}"
        if out.get("ok"):
            return "Код восстановления подтверждён. Вернитесь в Web UI и задайте новый пароль."
        if out.get("reason") == "expired":
            return "Код восстановления просрочен. Запросите новый код в Web UI."
        return "Код восстановления не найден. Проверьте код и chat_id."

    if not is_allowed_chat(chat_id, allowed_chat_id):
        return "Доступ запрещён для этого chat_id."

    if low in ("/start", "/help"):
        return (
            "Отправьте вопрос по документам, например:\n"
            "Сколько весит PC300\n\n"
            "Для подтверждения пользователя используйте:\n"
            "/verify 123456\n\n"
            "Для восстановления пароля используйте:\n"
            "/recover 123456"
        )

    return process_query(searcher, raw, source=f"telegram_bot:{chat_id}")


def main() -> int:
    cfg = load_config()
    token = (cfg.get("telegram_bot_token") or "").strip()
    allowed_chat_id = (cfg.get("telegram_allowed_chat_id") or "").strip()

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

    logger.info("Telegram бот запущен. Ограничение chat_id: %s", allowed_chat_id or "нет")
    offset = 0
    while True:
        try:
            updates = get_updates(token, offset)
            for upd in updates:
                offset = max(offset, int(upd.get("update_id", 0)) + 1)
                msg = upd.get("message") or {}
                chat = msg.get("chat") or {}
                chat_id = str(chat.get("id", ""))
                text = (msg.get("text") or "").strip()
                if not text:
                    continue

                answer = process_message(
                    searcher=searcher,
                    auth_db=auth_db,
                    text=text,
                    chat_id=chat_id,
                    allowed_chat_id=allowed_chat_id,
                )
                send_message(token, chat_id, answer)
        except KeyboardInterrupt:
            logger.info("Остановлено пользователем.")
            return 0
        except Exception as exc:
            logger.exception("Ошибка цикла polling: %s", exc)
            time.sleep(POLL_SLEEP_SEC)


if __name__ == "__main__":
    raise SystemExit(main())
