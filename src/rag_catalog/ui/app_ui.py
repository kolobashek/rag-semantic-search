"""
app_ui.py — Streamlit веб-интерфейс для RAG Каталога.

Запуск:
    streamlit run app_ui.py
"""

import html
import json
import logging
import re
import sqlite3
import time
import base64
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path, PureWindowsPath
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import streamlit as st

from rag_catalog.core.rag_core import RAGSearcher, load_config, save_config
from rag_catalog.core.user_auth_db import UserAuthDB

# ─────────────────────────── logging ───────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
APP_ICON_PATH = PROJECT_ROOT / "icon.ico"

# ─────────────────────────── page config ───────────────────────────────
st.set_page_config(
    page_title="RAG Каталог",
    page_icon=str(APP_ICON_PATH),
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS: поддержка светлой и тёмной тем ──────────────────────────────
st.markdown(
    """
<style>
    .hero {
        background: linear-gradient(120deg, #123b5d 0%, #176b87 55%, #24844f 100%);
        color: #fff;
        border-radius: 8px;
        padding: 1rem 1.2rem;
        margin-bottom: 0.8rem;
        box-shadow: 0 6px 18px rgba(31, 119, 180, 0.25);
    }
    .hero-title {
        font-size: 1.15rem;
        font-weight: 700;
        line-height: 1.3;
        display: flex;
        align-items: center;
        gap: 0.45rem;
    }
    .app-title-icon {
        width: 1.55rem;
        height: 1.55rem;
        object-fit: contain;
        flex: 0 0 auto;
    }
    .hero-sub {
        font-size: 0.88rem;
        opacity: 0.92;
        margin-top: 0.25rem;
    }
    .fact-box {
        border-left: 4px solid #2ca02c;
        background: rgba(44, 160, 44, 0.08);
        border-radius: 6px;
        padding: 0.7rem 0.9rem;
        margin-bottom: 0.8rem;
    }
    .fact-title {
        font-weight: 700;
        color: #1f4f1f;
        margin-bottom: 0.2rem;
    }
    .fact-link {
        font-size: 0.82rem;
        color: #0f4c81;
        text-decoration: none;
    }
    .fact-link:hover { text-decoration: underline; }
    .result-comment {
        margin-top: 0.55rem;
        padding: 0.55rem 0.7rem;
        border-radius: 6px;
        background: rgba(31, 119, 180, 0.08);
        border-left: 3px solid #1f77b4;
        font-size: 0.84rem;
        line-height: 1.45;
    }
    .result-comment .label {
        color: #24527a;
        font-weight: 700;
        margin-right: 0.35rem;
    }
    .result-card {
        background: #ffffff;
        border: 1px solid #dbe4f0;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        margin-bottom: 0.8rem;
        box-shadow: 0 2px 10px rgba(15, 23, 42, 0.06);
        transition: box-shadow 0.2s, border-color 0.2s;
    }
    .result-card:hover {
        box-shadow: 0 6px 16px rgba(15, 23, 42, 0.10);
        border-color: #60a5fa;
    }
    .result-group-title {
        display: flex;
        align-items: center;
        gap: 0.35rem;
        font-weight: 700;
        margin: 0.2rem 0 0.45rem;
    }
    .folder-result {
        border: 1px solid #ead7a4;
        border-left: 4px solid #b7791f;
        border-radius: 8px;
        padding: 0.75rem 0.9rem;
        margin-bottom: 0.7rem;
        background: #fff8e6;
        color: #3b2f12;
    }
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 4px;
        font-size: 0.78rem;
        font-weight: 600;
        margin-right: 6px;
    }
    .badge-score   { background: #dcfce7; color: #166534; }
    .badge-ext     { background: #dbeafe; color: #1d4ed8; }
    .badge-type    { background: #e2e8f0; color: #334155; }
    .text-preview  {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 6px;
        padding: 0.6rem 0.8rem;
        font-size: 0.88rem;
        color: #0f172a;
        line-height: 1.55;
        margin-top: 0.6rem;
    }
    .file-link {
        color: #1d4ed8;
        text-decoration: none;
        font-size: 0.82rem;
    }
    .file-link:hover {
        text-decoration: underline;
    }
    .ui-icon {
        display: inline-flex;
        width: 1.05em;
        height: 1.05em;
        vertical-align: -0.16em;
        margin-right: 0.25rem;
    }
    .file-link .ui-icon {
        width: 0.95em;
        height: 0.95em;
    }
    .ui-icon svg {
        width: 100%;
        height: 100%;
        stroke: currentColor;
        fill: none;
        stroke-width: 2;
        stroke-linecap: round;
        stroke-linejoin: round;
    }
    .icon-muted { color: #64748b; }
    .icon-doc { color: #1d4ed8; }
    .icon-sheet { color: #15803d; }
    .icon-pdf { color: #b91c1c; }
    .icon-folder { color: #b7791f; }
    .file-type-icon {
        display: inline-flex;
        width: 1.15rem;
        height: 1.15rem;
        vertical-align: -0.2rem;
        margin-right: 0.35rem;
    }
    /* Полоса релевантности */
    .score-bar-bg {
        display: inline-block;
        width: 60px;
        height: 8px;
        background: rgba(128,128,128,0.2);
        border-radius: 4px;
        vertical-align: middle;
        margin-right: 4px;
    }
    .score-bar-fill {
        height: 100%;
        border-radius: 4px;
        background: linear-gradient(90deg, #28a745, #1f77b4);
    }
    /* Лог-виджет */
    .log-container {
        background: #1e1e1e;
        color: #d4d4d4;
        font-family: 'Consolas', 'Monaco', monospace;
        font-size: 0.78rem;
        line-height: 1.5;
        padding: 0.8rem 1rem;
        border-radius: 6px;
        max-height: 420px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-all;
    }
    .log-line-error   { color: #f48771; }
    .log-line-warning { color: #dcdcaa; }
    .log-line-info    { color: #9cdcfe; }
    .log-line-debug   { color: #6a9955; }
    /* Прогресс-бар этапов */
    .stage-bar {
        height: 18px;
        border-radius: 4px;
        display: inline-block;
        vertical-align: middle;
    }
    /* Проводник */
    .explorer-breadcrumb {
        font-size: 0.88rem;
        padding: 0.4rem 0.6rem;
        background: var(--secondary-background-color, #f0f0f0);
        border-radius: 4px;
        margin-bottom: 0.6rem;
        word-break: break-all;
    }
    .explorer-dir-row {
        padding: 3px 6px;
        border-radius: 4px;
        cursor: pointer;
    }
    .explorer-dir-row:hover { background: rgba(31,119,180,0.1); }
    .ext-icon { font-size: 1rem; margin-right: 4px; }
</style>
""",
    unsafe_allow_html=True,
)

# ─────────────────────────── session state ─────────────────────────────
_STATE_KEYS = {
    "searcher": None,
    "auth_db": None,
    "auth_user": None,
    "auth_session_token": "",
    "auth_status_msg": "",
    "auth_status_level": "info",
    "qdrant_connected": False,
    "last_results": [],
    "last_query": "",
    "last_limit": 10,
    "last_file_type": None,
    "last_content_only": False,
    "trigger_search": False,
    "preset_query": "",
    "search_history": [],
    "active_screen": "Поиск",
    "stats_cache": None,
    "stats_cache_time": 0.0,
    "index_stats_cache": None,
    "index_stats_cache_time": 0.0,
    "last_fact_answer": None,
    # Проводник
    "explorer_path": None,        # текущая папка (str или None = корень каталога)
    "explorer_filter": "",        # фильтр по имени
    "explorer_page": 0,           # страница файлов
}


def _ensure_session_state() -> None:
    """Initialize Streamlit session keys for the current browser session."""
    for key, value in _STATE_KEYS.items():
        if key not in st.session_state:
            st.session_state[key] = value


_ensure_session_state()


_SEARCH_PRESETS = [
    ("Договоры", "договоры"),
    ("Паспорта", "паспорта"),
    ("Счета", "счета на оплату"),
    ("Служебные записки", "служебная записка"),
    ("Финансовые", "финансовый отчёт"),
    ("Юридические", "юридический"),
    ("Масса техники", "масса PC300"),
    ("Акты", "акты выполненных работ"),
]


_SVG_PATHS: Dict[str, str] = {
    "search": '<circle cx="11" cy="11" r="7"></circle><path d="M20 20l-3.5-3.5"></path>',
    "folder": '<path d="M3 7.5h6l2 2H21v8.5a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><path d="M3 7.5V6a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v1.5"></path>',
    "file": '<path d="M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><path d="M14 3v6h6"></path>',
    "sheet": '<path d="M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><path d="M14 3v6h6"></path><path d="M8 13h8M8 17h8M8 9h2"></path>',
    "pdf": '<path d="M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><path d="M14 3v6h6"></path><path d="M7 16h10M7 13h10"></path>',
    "bot": '<rect x="5" y="8" width="14" height="11" rx="3"></rect><path d="M12 4v4"></path><path d="M9 13h.01M15 13h.01"></path><path d="M10 17h4"></path>',
    "settings": '<circle cx="12" cy="12" r="3"></circle><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06-2 3.46-.08-.02a1.65 1.65 0 0 0-1.82.33 1.65 1.65 0 0 0-.5 1.7H8.61a1.65 1.65 0 0 0-.5-1.7 1.65 1.65 0 0 0-1.82-.33l-.08.02-2-3.46.06-.06A1.65 1.65 0 0 0 4.6 15 1.65 1.65 0 0 0 3 13.9V10.1A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06 2-3.46.08.02a1.65 1.65 0 0 0 1.82-.33 1.65 1.65 0 0 0 .5-1.7h6.78a1.65 1.65 0 0 0 .5 1.7 1.65 1.65 0 0 0 1.82.33l.08-.02 2 3.46-.06.06A1.65 1.65 0 0 0 19.4 9 1.65 1.65 0 0 0 21 10.1v3.8A1.65 1.65 0 0 0 19.4 15z"></path>',
    "chart": '<path d="M4 19V5"></path><path d="M4 19h16"></path><rect x="7" y="11" width="3" height="5"></rect><rect x="12" y="7" width="3" height="9"></rect><rect x="17" y="13" width="3" height="3"></rect>',
    "open": '<path d="M14 3h7v7"></path><path d="M10 14L21 3"></path><path d="M21 14v5a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5"></path>',
}


def _icon(name: str, class_name: str = "icon-muted") -> str:
    path = _SVG_PATHS.get(name, _SVG_PATHS["file"])
    return (
        f'<span class="ui-icon {class_name}" aria-hidden="true">'
        f'<svg viewBox="0 0 24 24">{path}</svg></span>'
    )


def _app_icon_img() -> str:
    icon_path = APP_ICON_PATH
    try:
        data = base64.b64encode(icon_path.read_bytes()).decode("ascii")
    except Exception:
        return _icon("search", "icon-doc")
    return f'<img class="app-title-icon" src="data:image/x-icon;base64,{data}" alt="">'


# ═══════════════════════════ helpers ═══════════════════════════════════

def _init_searcher(cfg: Dict[str, Any]) -> None:
    """Создать RAGSearcher и сохранить в session_state."""
    try:
        searcher = RAGSearcher(cfg)
        st.session_state.searcher = searcher
        st.session_state.qdrant_connected = searcher.connected
        st.session_state.stats_cache = None  # сбросить кэш
    except Exception as exc:
        st.error(f"Ошибка инициализации: {exc}")


def _get_searcher() -> Optional[RAGSearcher]:
    _ensure_session_state()
    return st.session_state.get("searcher")


def _users_db_path(cfg: Dict[str, Any]) -> str:
    path = str(cfg.get("users_db_path", "") or "").strip()
    if path:
        return path
    return str(Path(cfg.get("qdrant_db_path", ".")) / "rag_users.db")


def _get_auth_db(cfg: Dict[str, Any]) -> UserAuthDB:
    db = st.session_state.get("auth_db")
    current_path = _users_db_path(cfg)
    if db is None or str(getattr(db, "db_path", "")) != current_path:
        db = UserAuthDB(current_path)
        st.session_state.auth_db = db
    return db


def _auth_status() -> None:
    msg = st.session_state.get("auth_status_msg", "")
    if not msg:
        return
    lvl = st.session_state.get("auth_status_level", "info")
    if lvl == "success":
        st.success(msg)
    elif lvl == "warning":
        st.warning(msg)
    elif lvl == "error":
        st.error(msg)
    else:
        st.info(msg)


def _query_param(name: str) -> str:
    try:
        value = st.query_params.get(name, "")
    except Exception:
        return ""
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return str(value or "")


def _set_query_param(name: str, value: str) -> None:
    try:
        if value:
            st.query_params[name] = value
        elif name in st.query_params:
            del st.query_params[name]
    except Exception:
        logger.debug("Не удалось обновить query parameter %s", name, exc_info=True)


def _restore_auth_session(auth_db: UserAuthDB) -> Optional[Dict[str, Any]]:
    token = st.session_state.get("auth_session_token") or _query_param("session")
    if not token:
        return None
    user = auth_db.get_user_by_session(str(token))
    if not user:
        st.session_state.auth_session_token = ""
        _set_query_param("session", "")
        return None
    st.session_state.auth_session_token = str(token)
    st.session_state.auth_user = user
    return user


def _logout(auth_db: UserAuthDB) -> None:
    token = str(st.session_state.get("auth_session_token") or _query_param("session") or "")
    if token:
        auth_db.revoke_session(token)
    st.session_state.auth_session_token = ""
    st.session_state.auth_user = None
    _set_query_param("session", "")


def _bot_link(cfg: Dict[str, Any]) -> str:
    return str(cfg.get("telegram_bot_link") or "").strip()


def render_auth_gate(cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    auth_db = _get_auth_db(cfg)
    current = st.session_state.get("auth_user") or _restore_auth_session(auth_db)

    st.sidebar.subheader("Пользователь")
    if current:
        display = current.get("display_name") or current.get("username") or "user"
        st.sidebar.success(f"Вошли как: {display}")
        st.sidebar.caption(f"Роль: {current.get('role', 'user')}")
        with st.sidebar.expander("Настройки пользователя"):
            old_pass = st.text_input("Текущий пароль", type="password", key="profile_old_pass")
            new_pass = st.text_input("Новый пароль", type="password", key="profile_new_pass")
            new_pass2 = st.text_input("Повторите пароль", type="password", key="profile_new_pass2")
            if st.button("Сменить пароль", use_container_width=True, key="profile_change_pass"):
                if new_pass != new_pass2:
                    st.error("Новые пароли не совпадают.")
                elif auth_db.change_password(
                    username=str(current.get("username", "")),
                    old_password=old_pass,
                    new_password=new_pass,
                ):
                    st.session_state.auth_user["must_change_password"] = 0
                    st.success("Пароль изменён.")
                else:
                    st.error("Не удалось сменить пароль.")
        if st.sidebar.button("Выйти", use_container_width=True):
            _logout(auth_db)
            st.rerun()
        return current

    page = st.sidebar.radio(
        "Авторизация",
        ["Вход", "Регистрация", "Восстановление пароля"],
        key="auth_page",
    )
    st.subheader(page)

    if page == "Вход":
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("Логин", key="login_username")
            password = st.text_input("Пароль", type="password", key="login_password")
            submitted = st.form_submit_button("Войти", use_container_width=True)
        if submitted:
            user = auth_db.login(username=username, password=password)
            if user:
                token = auth_db.create_session(username=str(user.get("username") or username))
                st.session_state.auth_user = user
                st.session_state.auth_session_token = token
                st.session_state.auth_status_msg = ""
                _set_query_param("session", token)
                st.rerun()
            st.session_state.auth_status_level = "error"
            st.session_state.auth_status_msg = "Неверный логин/пароль или пользователь не подтверждён."
        _auth_status()
        return None

    if page == "Регистрация":
        link = _bot_link(cfg)
        if link:
            st.markdown(f"Бот для подтверждения: [{link}]({link})")
        else:
            st.info("Администратор должен указать ссылку на бота в настройках Telegram.")
        username = st.text_input("Логин", key="reg_username")
        display_name = st.text_input("Имя", key="reg_display_name")
        password = st.text_input("Пароль", type="password", key="reg_password")
        password2 = st.text_input("Повторите пароль", type="password", key="reg_password2")
        tg_chat_id = st.text_input(
            "Telegram chat_id",
            key="reg_tg_chat",
            help="Нужен для подтверждения через Telegram.",
        )
        if st.button("Зарегистрироваться", key="auth_request_code"):
            if password != password2:
                st.session_state.auth_status_level = "error"
                st.session_state.auth_status_msg = "Пароли не совпадают."
                _auth_status()
                return None
            try:
                req = auth_db.request_verification(
                    username=username,
                    display_name=display_name,
                    telegram_chat_id=tg_chat_id,
                    password=password,
                    ttl_minutes=30,
                )
                st.session_state.auth_status_level = "success"
                st.session_state.auth_status_msg = (
                    "Регистрация создана. В Telegram боту отправьте: "
                    f"`/verify {req['code']}` (действует 30 минут)."
                )
            except Exception as exc:
                st.session_state.auth_status_level = "error"
                st.session_state.auth_status_msg = f"Не удалось создать код: {exc}"
        _auth_status()
        return None

    username = st.text_input("Логин", key="reset_username")
    tg_chat_id = st.text_input("Telegram chat_id", key="reset_tg_chat")
    if st.button("Запросить восстановление", key="reset_request_btn"):
        out = auth_db.request_password_reset(username=username, telegram_chat_id=tg_chat_id)
        if out.get("ok"):
            st.session_state.auth_status_level = "success"
            st.session_state.auth_status_msg = (
                "Код восстановления создан. В Telegram боту отправьте: "
                f"`/recover {out['code']}`, затем вернитесь сюда."
            )
        else:
            st.session_state.auth_status_level = "error"
            st.session_state.auth_status_msg = "Пользователь с таким chat_id не найден."
    reset_code = st.text_input("Код восстановления", key="reset_code")
    new_password = st.text_input("Новый пароль", type="password", key="reset_new_pass")
    if st.button("Установить новый пароль", key="reset_complete_btn"):
        out = auth_db.complete_password_reset(
            username=username,
            code=reset_code,
            new_password=new_password,
        )
        if out.get("ok"):
            st.session_state.auth_status_level = "success"
            st.session_state.auth_status_msg = "Пароль изменён. Можно войти."
        else:
            st.session_state.auth_status_level = "error"
            st.session_state.auth_status_msg = "Код ещё не подтверждён через Telegram или неверен."
    _auth_status()
    return None


def _get_stats(searcher: RAGSearcher) -> Dict[str, Any]:
    """Получить статистику коллекции с кэшем на 10 секунд."""
    now = time.time()
    if st.session_state.stats_cache and now - st.session_state.stats_cache_time < 10:
        return st.session_state.stats_cache
    stats = searcher.get_collection_stats()
    st.session_state.stats_cache = stats
    st.session_state.stats_cache_time = now
    return stats


def _file_url(full_path: str) -> str:
    """
    Конвертирует Windows-путь в file:// URL.
    Пример: 'O:\\Обмен\\Договоры\\file.pdf' → 'file:///O:/Обмен/Договоры/file.pdf'

    PureWindowsPath.parts возвращает корень диска как 'O:\\' — его нельзя
    передавать в quote(), иначе получим 'O%3A%5C'. Обрабатываем диск отдельно:
    берём drive ('O:') и кодируем только компоненты пути без него.
    """
    try:
        p = PureWindowsPath(full_path)
        if not p.parts:
            return ""
        # drive = 'O:' (без слеша), остальные части кодируем через quote
        drive = p.drive  # 'O:'
        rest_parts = list(p.parts[1:])  # ['Обмен', 'Договоры', 'file.pdf']
        encoded = "/".join(quote(part, safe="") for part in rest_parts)
        return "file:///" + drive + "/" + encoded
    except Exception:
        return ""


def _folder_url(full_path: str) -> str:
    """file:// URL папки, содержащей файл."""
    try:
        p = PureWindowsPath(full_path).parent
        if not p.parts:
            return ""
        drive = p.drive
        rest_parts = list(p.parts[1:])
        encoded = "/".join(quote(part, safe="") for part in rest_parts)
        return "file:///" + drive + "/" + encoded
    except Exception:
        return ""


# ═══════════════════════════ indexing helpers ═══════════════════════════

def _get_index_stats(cfg: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
    """
    Читает state.json и возвращает статистику индексирования.
    Кэш на 15 секунд, чтобы не дёргать диск при каждом rerun.
    """
    now = time.time()
    if (
        not force
        and st.session_state.index_stats_cache
        and now - st.session_state.index_stats_cache_time < 15
    ):
        return st.session_state.index_stats_cache

    db_path = Path(cfg.get("qdrant_db_path", ""))
    state_file = db_path / "index_state.json"

    result: Dict[str, Any] = {
        "found": False,
        "state_file": str(state_file),
        "total": 0,
        "by_ext": {},
        "by_stage": {},
        "last_modified": None,
    }

    if not state_file.exists():
        st.session_state.index_stats_cache = result
        st.session_state.index_stats_cache_time = now
        return result

    try:
        with open(state_file, "r", encoding="utf-8") as fh:
            state = json.load(fh)
    except Exception as exc:
        result["error"] = str(exc)
        return result

    files = state.get("files", {})
    by_ext: Counter = Counter()
    by_stage: Counter = Counter()

    for key, meta in files.items():
        ext = Path(key).suffix.lower() or "(без расширения)"
        by_ext[ext] += 1
        stage = meta.get("stage", "content")
        by_stage[stage] += 1

    try:
        mtime = state_file.stat().st_mtime
        result["last_modified"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mtime))
    except Exception:
        pass

    result.update({
        "found": True,
        "total": len(files),
        "by_ext": dict(by_ext.most_common()),
        "by_stage": dict(by_stage),
    })

    st.session_state.index_stats_cache = result
    st.session_state.index_stats_cache_time = now
    return result


def _read_log_tail(cfg: Dict[str, Any], n_lines: int = 200) -> List[str]:
    """Читает последние n_lines строк лог-файла."""
    log_file = cfg.get("log_file", "")
    if not log_file:
        return ["⚠ Путь к лог-файлу не указан в config.json (поле 'log_file')."]

    try:
        p = Path(log_file)
        if not p.exists():
            return [f"⚠ Лог-файл не найден: {log_file}"]

        with open(p, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()

        return [line.rstrip() for line in lines[-n_lines:]]
    except Exception as exc:
        return [f"⚠ Ошибка чтения лога: {exc}"]


def _telemetry_db_path(cfg: Dict[str, Any]) -> Path:
    explicit = (cfg.get("telemetry_db_path") or "").strip()
    if explicit:
        return Path(explicit)
    return Path(cfg.get("qdrant_db_path", "")) / "rag_telemetry.db"


def _db_query_dicts(db_path: Path, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(query, params or ())
        return [dict(x) for x in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


def _dedupe_queries(values: List[str], limit: int = 12) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        q = re.sub(r"\s+", " ", str(value or "")).strip()
        key = q.lower()
        if not q or key in seen:
            continue
        seen.add(key)
        out.append(q)
        if len(out) >= limit:
            break
    return out


def _remember_search_query(query: str) -> None:
    q = re.sub(r"\s+", " ", str(query or "")).strip()
    if not q:
        return
    current = list(st.session_state.get("search_history") or [])
    st.session_state.search_history = _dedupe_queries([q, *current], limit=20)


def _submit_current_query() -> None:
    query = str(st.session_state.get("query_input") or "").strip()
    if query:
        st.session_state.trigger_search = True


def _choose_search_query(query: str) -> None:
    st.session_state.query_input = query
    st.session_state.preset_query = query
    st.session_state.trigger_search = True


def _recent_search_queries(cfg: Dict[str, Any], limit: int = 12) -> List[str]:
    db_path = _telemetry_db_path(cfg)
    rows = _db_query_dicts(
        db_path,
        """
        SELECT query
        FROM search_logs
        WHERE query <> ''
        ORDER BY id DESC
        LIMIT 80
        """,
    )
    return _dedupe_queries([str(row.get("query") or "") for row in rows], limit=limit)


def _search_suggestions(cfg: Dict[str, Any]) -> List[str]:
    session_history = list(st.session_state.get("search_history") or [])
    telemetry_history = _recent_search_queries(cfg, limit=12)
    presets = [query for _, query in _SEARCH_PRESETS]
    return _dedupe_queries([*session_history, *telemetry_history, *presets], limit=18)


def _open_in_explorer(path: str) -> None:
    value = str(path or "").strip()
    if not value:
        return
    p = Path(value)
    if p.is_file():
        p = p.parent
    st.session_state.explorer_path = str(p)
    st.session_state.explorer_page = 0
    st.session_state.explorer_filter = ""
    st.session_state.active_screen = "Проводник"


def _open_parent_in_explorer(path: str) -> None:
    value = str(path or "").strip()
    if not value:
        return
    p = Path(value)
    st.session_state.explorer_path = str(p.parent if p.suffix else p)
    st.session_state.explorer_page = 0
    st.session_state.explorer_filter = ""
    st.session_state.active_screen = "Проводник"


def _format_file_size(size_b: int) -> str:
    if size_b >= 1_048_576:
        return f"{size_b / 1_048_576:.1f} МБ"
    if size_b >= 1024:
        return f"{size_b / 1024:.1f} КБ"
    return f"{size_b} Б"


def _directory_children(path: str, limit: int = 40) -> Dict[str, Any]:
    p = Path(path)
    out: Dict[str, Any] = {"exists": p.exists(), "is_dir": p.is_dir(), "dirs": [], "files": []}
    if not p.exists() or not p.is_dir():
        return out
    try:
        entries = sorted(
            [x for x in p.iterdir() if not x.name.startswith(".") and not x.name.startswith("~$")],
            key=lambda x: (not x.is_dir(), x.name.lower()),
        )
    except Exception as exc:
        out["error"] = str(exc)
        return out
    for child in entries[:limit]:
        item = {"name": child.name, "path": str(child)}
        if child.is_dir():
            out["dirs"].append(item)
        elif child.is_file():
            try:
                item["size"] = _format_file_size(child.stat().st_size)
            except Exception:
                item["size"] = ""
            out["files"].append(item)
    out["truncated"] = len(entries) > limit
    return out


def _get_index_telemetry_snapshot(cfg: Dict[str, Any]) -> Dict[str, Any]:
    db_path = _telemetry_db_path(cfg)
    out: Dict[str, Any] = {
        "db_path": str(db_path),
        "exists": db_path.exists(),
        "latest_run": None,
        "stages": [],
        "windows": {},
        "search_recent": [],
    }
    if not db_path.exists():
        return out

    latest_runs = _db_query_dicts(
        db_path,
        """
        SELECT run_id, ts_started, ts_finished, status, total_files, added_files,
               updated_files, skipped_files, deleted_files, error_files, points_added, note
        FROM index_runs
        ORDER BY ts_started DESC
        LIMIT 1
        """,
    )
    if latest_runs:
        out["latest_run"] = latest_runs[0]
        run_id = latest_runs[0]["run_id"]
        out["stages"] = _db_query_dicts(
            db_path,
            """
            SELECT stage, status, total_files, processed_files, added_files, updated_files,
                   skipped_files, error_files, points_added, ts_started, ts_updated, ts_finished
            FROM index_stage_progress
            WHERE run_id=?
            ORDER BY id ASC
            """,
            (run_id,),
        )

    now = datetime.now(timezone.utc)
    for label, days in (("24ч", 1), ("7д", 7), ("30д", 30)):
        since = (now - timedelta(days=days)).isoformat()
        rows = _db_query_dicts(
            db_path,
            """
            SELECT
                COALESCE(SUM(added_files), 0) AS added_files,
                COALESCE(SUM(updated_files), 0) AS updated_files,
                COALESCE(SUM(deleted_files), 0) AS deleted_files,
                COALESCE(SUM(error_files), 0) AS error_files,
                COUNT(*) AS runs_count
            FROM index_runs
            WHERE ts_started >= ?
            """,
            (since,),
        )
        out["windows"][label] = rows[0] if rows else {
            "added_files": 0,
            "updated_files": 0,
            "deleted_files": 0,
            "error_files": 0,
            "runs_count": 0,
        }

    out["search_recent"] = _db_query_dicts(
        db_path,
        """
        SELECT ts, source, query, results_count, duration_ms, ok, error
        FROM search_logs
        ORDER BY id DESC
        LIMIT 20
        """,
    )
    return out


def _colorize_log_line(line: str) -> str:
    """Обернуть строку лога в span нужного цвета (HTML)."""
    escaped = html.escape(line)
    upper = line.upper()
    if " - ERROR" in upper or "ERROR" in upper[:20]:
        return f'<span class="log-line-error">{escaped}</span>'
    if " - WARNING" in upper or "WARNING" in upper[:20] or "WARN" in upper[:20]:
        return f'<span class="log-line-warning">{escaped}</span>'
    if " - DEBUG" in upper:
        return f'<span class="log-line-debug">{escaped}</span>'
    return f'<span class="log-line-info">{escaped}</span>'


# ═══════════════════════════ sidebar ═══════════════════════════════════

def render_sidebar(cfg: Dict[str, Any], user: Dict[str, Any]):
    """Боковая панель: статус, параметры поиска, быстрый поиск, настройки."""
    st.sidebar.title("RAG Каталог")

    # Статус подключения
    searcher = _get_searcher()
    if st.session_state.qdrant_connected and searcher:
        stats = _get_stats(searcher)
        pts = stats.get("points_count", "?")
        label = f"Qdrant подключён\n\nТочек: {pts:,}" if isinstance(pts, int) else f"Qdrant подключён\n\nТочек: {pts}"
        st.sidebar.success(label)
    else:
        st.sidebar.error("Нет подключения к Qdrant")

    if st.sidebar.button("Переподключить", use_container_width=True):
        _init_searcher(cfg)
        st.rerun()

    with st.sidebar.expander("Параметры поиска", expanded=True):
        limit = st.slider("Количество результатов", 5, 50, 10, step=5)
        file_type = st.selectbox(
            "Тип файла",
            options=["Все", ".docx", ".xlsx", ".xls", ".pdf"],
        )
        file_type_val: Optional[str] = None if file_type == "Все" else file_type
        content_only = st.checkbox("Только содержимое")

    # Быстрый поиск
    with st.sidebar.expander("Быстрый поиск", expanded=False):
        for label, query in _SEARCH_PRESETS[:6]:
            if st.button(label, use_container_width=True, key=f"quick_{label}"):
                _choose_search_query(query)
                st.session_state.active_screen = "Поиск"
                st.rerun()

    if str(user.get("role", "user")) != "admin":
        return limit, file_type_val, content_only

    # Настройки путей доступны только администратору.
    with st.sidebar.expander("Администрирование", expanded=False):
        new_catalog = st.text_input("Папка каталога", value=cfg.get("catalog_path", ""))
        new_qdrant  = st.text_input("База Qdrant",     value=cfg.get("qdrant_db_path", ""))
        new_log     = st.text_input("Лог файл",         value=cfg.get("log_file", ""))
        new_telemetry = st.text_input(
            "SQLite лог БД (опционально)",
            value=cfg.get("telemetry_db_path", ""),
            help="Если пусто, используется <qdrant_db_path>/rag_telemetry.db",
        )
        new_users_db = st.text_input(
            "SQLite БД пользователей (опционально)",
            value=cfg.get("users_db_path", ""),
            help="Если пусто, используется <qdrant_db_path>/rag_users.db",
        )
        st.markdown("**Индексация**")
        idx_chunk_size = st.number_input(
            "Chunk size",
            min_value=100,
            max_value=5000,
            value=int(cfg.get("chunk_size", 500)),
            step=50,
            help=(
                "Кратко: размер одного текстового чанка.\n\n"
                "Подробно: больше значение ускоряет индексацию и уменьшает число чанков, "
                "но может снизить точность поиска по узким фрагментам."
            ),
        )
        idx_chunk_overlap = st.number_input(
            "Chunk overlap",
            min_value=0,
            max_value=2000,
            value=int(cfg.get("chunk_overlap", 100)),
            step=10,
            help=(
                "Кратко: перекрытие между соседними чанками.\n\n"
                "Подробно: помогает не терять контекст на границах, но увеличивает размер индекса."
            ),
        )
        idx_batch = st.number_input(
            "Batch size",
            min_value=50,
            max_value=20000,
            value=int(cfg.get("batch_size", 1000)),
            step=50,
            help=(
                "Кратко: сколько векторов писать за один батч.\n\n"
                "Подробно: больше батч уменьшает overhead, но повышает пиковую память."
            ),
        )
        idx_workers = st.number_input(
            "Индекс. потоки",
            min_value=1,
            max_value=32,
            value=int(cfg.get("index_read_workers", 4)),
            step=1,
            help=(
                "Кратко: число потоков чтения документов.\n\n"
                "Подробно: ускоряет I/O, но при слишком большом значении может перегружать сеть/диск."
            ),
        )
        idx_max_chunks = st.number_input(
            "Макс. чанков/файл",
            min_value=0,
            max_value=20000,
            value=int(cfg.get("index_max_chunks", 2000)),
            step=100,
            help=(
                "Кратко: ограничение чанков на один файл.\n\n"
                "Подробно: 0 = без лимита; полезно для защиты от очень больших файлов."
            ),
        )
        idx_skip_ocr = st.checkbox(
            "Пропускать OCR для сканированных PDF",
            value=bool(cfg.get("index_skip_ocr", False)),
            help=(
                "Кратко: не запускать OCR, если в PDF нет текстового слоя.\n\n"
                "Подробно: сильно ускоряет индексацию, но текст сканов не будет найден."
            ),
        )
        idx_stage = st.selectbox(
            "Stage по умолчанию",
            options=["all", "metadata", "small", "large"],
            index=max(0, ["all", "metadata", "small", "large"].index(str(cfg.get("index_default_stage", "all")).lower()))
            if str(cfg.get("index_default_stage", "all")).lower() in {"all", "metadata", "small", "large"}
            else 0,
            help=(
                "Кратко: этап индексирования по умолчанию.\n\n"
                "Подробно: all — весь пайплайн; metadata — только имена/пути; "
                "small — быстрые файлы; large — тяжелые и крупные файлы."
            ),
        )
        st.markdown("**Telegram бот**")
        tg_enabled = st.checkbox("Включить Telegram-бота", value=bool(cfg.get("telegram_enabled", False)))
        tg_token = st.text_input(
            "Bot Token",
            value=cfg.get("telegram_bot_token", ""),
            type="password",
            help="Токен от @BotFather",
        )
        tg_chat = st.text_input(
            "Разрешённый chat_id (опционально)",
            value=cfg.get("telegram_allowed_chat_id", ""),
            help="Если указан — бот отвечает только этому чату",
        )
        tg_link = st.text_input(
            "Ссылка на бота",
            value=cfg.get("telegram_bot_link", ""),
            help="Например: https://t.me/my_company_rag_bot",
        )

        if st.button("Сохранить и перезапустить", use_container_width=True):
            cfg["catalog_path"]  = new_catalog
            cfg["qdrant_db_path"] = new_qdrant
            cfg["log_file"]       = new_log
            cfg["telemetry_db_path"] = new_telemetry.strip()
            cfg["users_db_path"] = new_users_db.strip()
            cfg["chunk_size"] = int(idx_chunk_size)
            cfg["chunk_overlap"] = int(idx_chunk_overlap)
            cfg["batch_size"] = int(idx_batch)
            cfg["index_read_workers"] = int(idx_workers)
            cfg["index_max_chunks"] = int(idx_max_chunks)
            cfg["index_skip_ocr"] = bool(idx_skip_ocr)
            cfg["index_default_stage"] = str(idx_stage).strip()
            cfg["telegram_enabled"] = bool(tg_enabled)
            cfg["telegram_bot_token"] = tg_token.strip()
            cfg["telegram_allowed_chat_id"] = tg_chat.strip()
            cfg["telegram_bot_link"] = tg_link.strip()
            save_config(cfg)
            st.session_state.searcher = None
            _init_searcher(cfg)
            st.success("Настройки сохранены")
            st.rerun()

    return limit, file_type_val, content_only


# ═══════════════════════════ result card ═══════════════════════════════

def _extract_context_bits(text: str) -> Dict[str, str]:
    """Вернуть короткие title/context из куска текста."""
    raw = (text or "").strip()
    if not raw:
        return {"title": "", "context": ""}

    lines = [x.strip() for x in re.split(r"[\r\n]+", raw) if x.strip()]
    title = ""
    for ln in lines[:6]:
        if 4 <= len(ln) <= 120:
            title = ln
            break

    sentence = re.split(r"(?<=[.!?])\s+", raw.replace("\n", " ").strip())[0]
    context = sentence if sentence else raw[:180]
    context = context[:220]
    return {"title": title, "context": context}


def _clean_display_text(text: str) -> str:
    """
    Очистить фрагмент для UI:
    - убрать HTML-теги вида <...>
    - декодировать HTML entities
    - схлопнуть лишние пробелы
    """
    raw = str(text or "")
    no_tags = re.sub(r"<[^>]{1,200}>", " ", raw)
    unescaped = html.unescape(no_tags)
    compact = re.sub(r"[ \t]+", " ", unescaped)
    compact = re.sub(r"\s*\n\s*", "\n", compact)
    return compact.strip()


def _where_found_comment(result: Dict[str, Any]) -> str:
    """Сформировать строку 'где найдено'."""
    type_raw = (result.get("type") or "").strip()
    chunk_index = result.get("chunk_index")
    parts: List[str] = []
    if type_raw:
        parts.append(type_raw)
    if chunk_index is not None:
        parts.append(f"фрагмент #{int(chunk_index) + 1}")
    ext = (result.get("extension") or "").strip()
    if ext:
        parts.append(ext)
    return " | ".join(parts) if parts else "в тексте документа"


def render_result_card(result: Dict[str, Any], index: int) -> None:
    """Отрисовать карточку одного результата нативными Streamlit-компонентами."""
    filename = str(result.get("filename") or "")
    path_str = str(result.get("path") or "")
    ext = str(result.get("extension") or "unknown")
    type_str = str(result.get("type") or "")
    full_path = result.get("full_path") or ""
    text_raw  = _clean_display_text(result.get("text") or "")
    text_preview = text_raw[:400] + ("…" if len(text_raw) > 400 else "")
    ctx = _extract_context_bits(text_raw)
    where_found = _where_found_comment(result)

    score    = result.get("score", 0)
    size_mb  = result.get("size_mb")
    modified = result.get("modified")
    score_text = f"{float(score or 0):.3f}"

    meta_parts = [f"score {score_text}", ext, type_str]
    if size_mb is not None:
        meta_parts.append(f"{size_mb} МБ")
    if modified:
        meta_parts.append(str(modified)[:10])

    furl = _file_url(full_path)
    durl = _folder_url(full_path)
    p = Path(full_path) if full_path else None

    with st.container(border=True):
        st.markdown(f"**[{index}] {filename}**")
        st.caption(" | ".join(x for x in meta_parts if x))
        if path_str:
            st.caption(path_str)

        if furl or durl or full_path:
            link_cols = st.columns([1, 1, 1, 1, 4])
            if furl:
                with link_cols[0]:
                    st.link_button("Открыть", furl, use_container_width=True)
            if durl:
                with link_cols[1]:
                    st.link_button("Папка ОС", durl, use_container_width=True)
            if full_path:
                with link_cols[2]:
                    if st.button("В проводник", key=f"goto_file_{index}_{full_path}", use_container_width=True):
                        _open_parent_in_explorer(full_path)
                        st.rerun()
            if p and p.exists() and p.is_file():
                try:
                    size_b = p.stat().st_size
                except Exception:
                    size_b = 0
                with link_cols[3]:
                    if size_b <= 50 * 1_048_576:
                        try:
                            st.download_button(
                                "Скачать",
                                data=p.read_bytes(),
                                file_name=p.name,
                                key=f"download_{index}_{full_path}",
                                use_container_width=True,
                            )
                        except Exception:
                            st.button("Скачать", disabled=True, key=f"download_disabled_{index}_{full_path}", use_container_width=True)
                    else:
                        st.button("Скачать", disabled=True, help="Файл больше 50 МБ", key=f"download_big_{index}_{full_path}", use_container_width=True)

        st.markdown(f"**Где найдено:** {where_found}")
        if ctx["title"]:
            st.markdown(f"**Заголовок:** {ctx['title']}")
        if ctx["context"]:
            st.markdown(f"**Контекст:** {ctx['context']}")
        with st.expander("Просмотреть фрагмент в приложении", expanded=False):
            if text_preview:
                st.text(text_preview)
            else:
                st.info("Для этого результата нет текстового фрагмента.")


def _classify_result(result: Dict[str, Any]) -> str:
    text = " ".join(
        str(result.get(k, "") or "").lower()
        for k in ("filename", "path", "type", "text", "extension")
    )
    if str(result.get("type") or "") == "folder_metadata":
        return "Каталоги"
    if any(x in text for x in ("птс", "псм", "стс", "техпаспорт", "техническ", "электронного паспорта", "документы на технику", "экскаватор")):
        return "Техпаспорта ТС (ПТС/ПСМ/СТС)"
    if any(x in text for x in ("паспорт", "паспорта", "удостоверен")):
        return "Личные паспорта и удостоверения"
    if any(x in text for x in ("договор", "соглашен")):
        return "Договоры и соглашения"
    if any(x in text for x in ("счет", "счёт", "оплат", "платеж")):
        return "Счета и платежные документы"
    if result.get("extension") in (".xlsx", ".xls"):
        return "Таблицы и реестры"
    if result.get("extension") == ".pdf":
        return "PDF документы"
    return "Прочие документы"


def _parent_catalog(path: str) -> str:
    parts = [p for p in re.split(r"[\\/]+", path or "") if p]
    if len(parts) >= 2:
        return parts[-2]
    if parts:
        return parts[0]
    return "Без каталога"


def render_folder_result(result: Dict[str, Any], index: int) -> None:
    full_path = result.get("full_path") or ""
    durl = _file_url(full_path)
    with st.container(border=True):
        st.markdown(f"**[{index}] {result.get('filename') or ''}**")
        st.caption(str(result.get("path") or ""))
        st.caption(f"score {float(result.get('score') or 0):.3f}")
        cols = st.columns([1, 1, 6])
        if durl:
            with cols[0]:
                st.link_button("Папка ОС", durl, use_container_width=True)
        if full_path:
            with cols[1]:
                if st.button("В проводник", key=f"goto_dir_{index}_{full_path}", use_container_width=True):
                    _open_in_explorer(full_path)
                    st.rerun()

        with st.expander("Раскрыть каталог", expanded=False):
            children = _directory_children(full_path)
            if not children["exists"]:
                st.info("Каталог недоступен на диске.")
            elif not children["is_dir"]:
                st.info("Путь результата не является каталогом.")
            elif children.get("error"):
                st.error(f"Не удалось прочитать каталог: {children['error']}")
            else:
                dirs = children["dirs"]
                files = children["files"]
                if not dirs and not files:
                    st.info("Каталог пуст.")
                if dirs:
                    st.markdown("**Папки**")
                    for item in dirs:
                        if st.button(
                            f":material/folder: {item['name']}",
                            key=f"result_child_dir_{index}_{item['path']}",
                            use_container_width=True,
                        ):
                            _open_in_explorer(item["path"])
                            st.rerun()
                if files:
                    st.markdown("**Файлы**")
                    for item in files:
                        st.caption(f"{item['name']}  {item.get('size', '')}")
                if children.get("truncated"):
                    st.caption("Показаны первые элементы. Откройте каталог в проводнике приложения для полного списка.")


def render_grouped_results(results: List[Dict[str, Any]]) -> None:
    order = [
        "Каталоги",
        "Техпаспорта ТС (ПТС/ПСМ/СТС)",
        "Личные паспорта и удостоверения",
        "Договоры и соглашения",
        "Счета и платежные документы",
        "Таблицы и реестры",
        "PDF документы",
        "Прочие документы",
    ]
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for result in results:
        group = _classify_result(result)
        catalog = _parent_catalog(str(result.get("path") or ""))
        grouped.setdefault(group, {}).setdefault(catalog, []).append(result)

    idx = 1
    for group_name in sorted(grouped, key=lambda g: order.index(g) if g in order else 999):
        total = sum(len(v) for v in grouped[group_name].values())
        with st.expander(f"{group_name} ({total})", expanded=(idx == 1)):
            for catalog, items in sorted(grouped[group_name].items()):
                st.markdown(f"**{catalog} ({len(items)})**")
                for item in sorted(items, key=lambda x: float(x.get("score") or 0), reverse=True):
                    if item.get("type") == "folder_metadata":
                        render_folder_result(item, idx)
                    else:
                        render_result_card(item, idx)
                    idx += 1


# ═══════════════════════════ fact answer ════════════════════════════════

def render_fact_answer(answer: Optional[Dict[str, Any]]) -> None:
    """Показать извлечённый факт-ответ с источником."""
    if not answer:
        return

    if not answer.get("ok"):
        st.info(answer.get("error", "Ответ не найден"))
        best = answer.get("best_source") or {}
        path = best.get("full_path", "")
        if path:
            url = _file_url(path)
            if url:
                st.markdown(f"[Открыть ближайший источник]({url})")
        return

    src = answer.get("source", {}) or {}
    path = src.get("full_path", "")
    fact_text = html.escape(answer.get("answer", ""))
    excerpt = html.escape(src.get("text_excerpt", ""))
    st.markdown(
        f"""
<div class="fact-box">
  <div class="fact-title">Ответ по документам</div>
  <div><strong>{fact_text}</strong></div>
  <div style="margin-top:0.25rem;font-size:0.84rem;">{excerpt}</div>
  <div style="margin-top:0.35rem;font-size:0.82rem;color:#666;">
    Источник: {html.escape(src.get("filename", ""))}
  </div>
</div>
""",
        unsafe_allow_html=True,
    )
    if path:
        url = _file_url(path)
        if url:
            st.markdown(
                f'<a class="fact-link" href="{html.escape(url)}" target="_blank">Открыть файл-источник</a>',
                unsafe_allow_html=True,
            )


# ═══════════════════════════ explorer tab ══════════════════════════════

_EXT_ICON: Dict[str, str] = {
    ".docx": _icon("file", "icon-doc"),
    ".doc":  _icon("file", "icon-doc"),
    ".xlsx": _icon("sheet", "icon-sheet"),
    ".xls":  _icon("sheet", "icon-sheet"),
    ".pdf":  _icon("pdf", "icon-pdf"),
    ".txt":  _icon("file", "icon-muted"),
    ".csv":  _icon("sheet", "icon-sheet"),
    ".zip":  _icon("file", "icon-muted"),
    ".rar":  _icon("file", "icon-muted"),
}
_DIR_ICON  = _icon("folder", "icon-folder")
_FILE_ICON = _icon("file", "icon-muted")
_PAGE_SIZE = 100  # файлов на страницу


def _ext_icon(ext: str) -> str:
    return _EXT_ICON.get(ext.lower(), _FILE_ICON)


def render_explorer_tab(cfg: Dict[str, Any]) -> None:
    """Вкладка 'Проводник': навигация по каталогу + таблица файлов."""
    catalog_root = Path(cfg.get("catalog_path", ""))

    # Инициализируем текущий путь при первом открытии
    if st.session_state.explorer_path is None:
        st.session_state.explorer_path = str(catalog_root)

    cur_path = Path(st.session_state.explorer_path)

    # Защита: если путь вышел за пределы каталога — сбрасываем в корень
    try:
        cur_path.relative_to(catalog_root)
    except ValueError:
        cur_path = catalog_root
        st.session_state.explorer_path = str(catalog_root)

    if not cur_path.exists():
        st.error(f"Папка каталога не найдена: `{catalog_root}`")
        st.info("Проверьте путь `catalog_path` в настройках (боковая панель).")
        return

    # ── Хлебные крошки ────────────────────────────────────────────────
    parts = []
    p = cur_path
    while True:
        parts.append(p)
        if p == catalog_root or p == p.parent:
            break
        p = p.parent
    parts.reverse()

    crumb_cols = st.columns([8, 2])
    with crumb_cols[0]:
        # Рендерим крошки как кнопки (inline через columns)
        breadcrumb_buttons = st.columns(len(parts))
        for i, part in enumerate(parts):
            label = ("🏠 Корень" if part == catalog_root else part.name)
            with breadcrumb_buttons[i]:
                if st.button(label, key=f"crumb_{i}_{part}", use_container_width=True):
                    st.session_state.explorer_path = str(part)
                    st.session_state.explorer_page = 0
                    st.session_state.explorer_filter = ""
                    st.rerun()

    with crumb_cols[1]:
        if st.button("⬆ На уровень выше", use_container_width=True,
                     disabled=(cur_path == catalog_root)):
            st.session_state.explorer_path = str(cur_path.parent)
            st.session_state.explorer_page = 0
            st.session_state.explorer_filter = ""
            st.rerun()

    # Текущий путь
    st.markdown(
        f'<div class="explorer-breadcrumb">{_icon("folder", "icon-folder")}<strong>{html.escape(str(cur_path))}</strong></div>',
        unsafe_allow_html=True,
    )

    # ── Фильтр + расширение ───────────────────────────────────────────
    fc1, fc2, fc3 = st.columns([4, 2, 2])
    with fc1:
        name_filter = st.text_input(
            "Фильтр по имени",
            value=st.session_state.explorer_filter,
            placeholder="часть имени файла или папки…",
            label_visibility="collapsed",
            key="explorer_filter_input",
        )
        if name_filter != st.session_state.explorer_filter:
            st.session_state.explorer_filter = name_filter
            st.session_state.explorer_page = 0
    with fc2:
        ext_filter = st.selectbox(
            "Расширение",
            options=["Все", ".docx", ".xlsx", ".xls", ".pdf"],
            label_visibility="collapsed",
            key="explorer_ext_filter",
        )
    with fc3:
        sort_by = st.selectbox(
            "Сортировка",
            options=["По имени ↑", "По имени ↓", "По размеру ↓", "По дате ↓"],
            label_visibility="collapsed",
            key="explorer_sort",
        )

    # ── Считываем содержимое папки ────────────────────────────────────
    try:
        entries = list(cur_path.iterdir())
    except PermissionError:
        st.error("Нет доступа к этой папке.")
        return
    except Exception as exc:
        st.error(f"Ошибка чтения папки: {exc}")
        return

    # Разделяем на директории и файлы
    dirs  = [e for e in entries if e.is_dir() and not e.name.startswith(".")]
    files = [e for e in entries if e.is_file()
             and not e.name.startswith("~$")   # пропускаем временные Office
             and not e.name.startswith(".")]

    # Фильтрация
    nf_lower = name_filter.strip().lower()
    if nf_lower:
        dirs  = [d for d in dirs  if nf_lower in d.name.lower()]
        files = [f for f in files if nf_lower in f.name.lower()]
    if ext_filter != "Все":
        files = [f for f in files if f.suffix.lower() == ext_filter]

    # Сортировка директорий (всегда по имени)
    dirs.sort(key=lambda d: d.name.lower())

    # Сортировка файлов
    if sort_by == "По имени ↑":
        files.sort(key=lambda f: f.name.lower())
    elif sort_by == "По имени ↓":
        files.sort(key=lambda f: f.name.lower(), reverse=True)
    elif sort_by == "По размеру ↓":
        files.sort(key=lambda f: f.stat().st_size if f.exists() else 0, reverse=True)
    elif sort_by == "По дате ↓":
        files.sort(key=lambda f: f.stat().st_mtime if f.exists() else 0, reverse=True)

    # ── Счётчик ───────────────────────────────────────────────────────
    total_files = len(files)
    st.caption(
        f"Папок: **{len(dirs)}** &nbsp;|&nbsp; Файлов: **{total_files}**"
        + (f" (фильтр: «{html.escape(name_filter)}»)" if name_filter else ""),
    )

    # ── Список директорий ─────────────────────────────────────────────
    if dirs:
        with st.expander(f":material/folder: Папки ({len(dirs)})", expanded=True):
            # Выводим в 3 колонки
            n_cols = 3
            for row_start in range(0, len(dirs), n_cols):
                row_dirs = dirs[row_start : row_start + n_cols]
                cols = st.columns(n_cols)
                for col, d in zip(cols, row_dirs):
                    with col:
                        if st.button(
                            f":material/folder: {d.name}",
                            key=f"dir_{d}",
                            use_container_width=True,
                        ):
                            st.session_state.explorer_path = str(d)
                            st.session_state.explorer_page = 0
                            st.session_state.explorer_filter = ""
                            st.rerun()

    st.divider()

    # ── Таблица файлов (с пагинацией) ────────────────────────────────
    if not files:
        st.info("Нет файлов, соответствующих фильтру.")
        return

    # Пагинация
    page = st.session_state.explorer_page
    n_pages = max(1, (total_files + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = max(0, min(page, n_pages - 1))

    page_files = files[page * _PAGE_SIZE : (page + 1) * _PAGE_SIZE]

    # Строим таблицу вручную (нужны кликабельные ссылки)
    rows_html = ""
    for f in page_files:
        try:
            stat = f.stat()
            size_b = stat.st_size
            size_str = (
                f"{size_b / 1_048_576:.1f} МБ" if size_b >= 1_048_576
                else f"{size_b / 1024:.1f} КБ" if size_b >= 1024
                else f"{size_b} Б"
            )
            mtime_str = time.strftime("%d.%m.%Y %H:%M", time.localtime(stat.st_mtime))
        except Exception:
            size_str = "—"
            mtime_str = "—"

        ext  = f.suffix.lower()
        icon = _ext_icon(ext)
        name_escaped = html.escape(f.name)
        ext_escaped  = html.escape(ext or "—")

        furl = _file_url(str(f))
        durl = _folder_url(str(f))
        link_file = (
            f'<a class="file-link" href="{html.escape(furl)}" target="_blank">{_icon("open", "icon-doc")}открыть</a>'
            if furl else ""
        )
        link_dir = (
            f'<a class="file-link" href="{html.escape(durl)}" target="_blank">{_icon("folder", "icon-folder")}папка</a>'
            if durl else ""
        )
        links = "&nbsp;&nbsp;".join(x for x in [link_file, link_dir] if x)

        rows_html += f"""
<tr>
  <td style="padding:4px 8px;">{icon} {name_escaped}</td>
  <td style="padding:4px 8px; color:#888; font-size:0.82rem;">{ext_escaped}</td>
  <td style="padding:4px 8px; color:#888; font-size:0.82rem; text-align:right;">{html.escape(size_str)}</td>
  <td style="padding:4px 8px; color:#888; font-size:0.82rem;">{html.escape(mtime_str)}</td>
  <td style="padding:4px 8px; font-size:0.82rem;">{links}</td>
</tr>"""

    table_html = f"""
<table style="width:100%; border-collapse:collapse;">
  <thead>
    <tr style="border-bottom:2px solid var(--border-color,#ddd); font-size:0.82rem; color:#666;">
      <th style="padding:4px 8px; text-align:left;">Имя файла</th>
      <th style="padding:4px 8px; text-align:left;">Тип</th>
      <th style="padding:4px 8px; text-align:right;">Размер</th>
      <th style="padding:4px 8px; text-align:left;">Изменён</th>
      <th style="padding:4px 8px; text-align:left;">Действия</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>
"""
    st.markdown(table_html, unsafe_allow_html=True)

    # Пагинация
    if n_pages > 1:
        pg_c1, pg_c2, pg_c3 = st.columns([2, 4, 2])
        with pg_c1:
            if st.button("◀ Пред.", disabled=(page == 0), key="exp_prev"):
                st.session_state.explorer_page = page - 1
                st.rerun()
        with pg_c2:
            st.markdown(
                f"<p style='text-align:center;color:#888;margin-top:0.4rem;'>"
                f"Страница {page + 1} из {n_pages} "
                f"(файлы {page * _PAGE_SIZE + 1}–{min((page + 1) * _PAGE_SIZE, total_files)})"
                f"</p>",
                unsafe_allow_html=True,
            )
        with pg_c3:
            if st.button("След. ▶", disabled=(page == n_pages - 1), key="exp_next"):
                st.session_state.explorer_page = page + 1
                st.rerun()


# ═══════════════════════════ indexing tab ══════════════════════════════

def render_indexing_tab(cfg: Dict[str, Any]) -> None:
    """Вкладка 'Индексирование': статистика state.json + просмотр логов."""

    # ── Заголовок + кнопка обновления ────────────────────────────────
    col_h, col_btn = st.columns([5, 1])
    with col_h:
        st.subheader("Статистика индексирования")
    with col_btn:
        if st.button("🔄 Обновить", use_container_width=True, key="idx_refresh"):
            st.session_state.index_stats_cache = None
            st.rerun()

    idx = _get_index_stats(cfg)

    if not idx.get("found"):
        if idx.get("error"):
            st.error(f"Ошибка чтения state.json: {idx['error']}")
        else:
            st.warning(
                f"Файл состояния не найден: `{idx['state_file']}`\n\n"
                "Запустите индексирование: `python index_rag.py`"
            )
    else:
        # ── Метрики ───────────────────────────────────────────────────
        total = idx["total"]
        by_stage = idx.get("by_stage", {})
        content_count = by_stage.get("content", 0)
        metadata_count = by_stage.get("metadata", 0)
        other_count = total - content_count - metadata_count

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Всего файлов в индексе", f"{total:,}")
        with c2:
            st.metric("Полностью проиндексировано", f"{content_count:,}",
                      help="Файлы с этапом 'content' — содержимое полностью проиндексировано")
        with c3:
            st.metric("Только метаданные", f"{metadata_count:,}",
                      help="Файлы с этапом 'metadata' — ожидают индексирования содержимого (этапы small/large)")
        with c4:
            st.metric("Обновлено", idx.get("last_modified", "—"),
                      help="Время последнего обновления state.json")

        # ── Прогресс-бар этапов ───────────────────────────────────────
        if total > 0:
            pct_content  = content_count / total * 100
            pct_metadata = metadata_count / total * 100
            pct_other    = max(0, 100 - pct_content - pct_metadata)

            st.markdown("**Прогресс по этапам:**")
            bar_html = f"""
<div style="display:flex; height:20px; border-radius:6px; overflow:hidden; width:100%; margin-bottom:0.6rem;">
  <div style="width:{pct_content:.1f}%; background:#28a745;" title="content: {content_count:,}"></div>
  <div style="width:{pct_metadata:.1f}%; background:#ffc107;" title="metadata: {metadata_count:,}"></div>
  <div style="width:{pct_other:.1f}%; background:#dee2e6;" title="прочее: {other_count:,}"></div>
</div>
<div style="font-size:0.82rem; color:#888;">
  <span style="color:#28a745">■</span> content ({content_count:,} &nbsp;·&nbsp; {pct_content:.1f}%)
  &nbsp;&nbsp;
  <span style="color:#ffc107">■</span> metadata ({metadata_count:,} &nbsp;·&nbsp; {pct_metadata:.1f}%)
  &nbsp;&nbsp;
  <span style="color:#adb5bd">■</span> прочее ({other_count:,} &nbsp;·&nbsp; {pct_other:.1f}%)
</div>
"""
            st.markdown(bar_html, unsafe_allow_html=True)

        st.divider()

        # ── Разбивка по расширениям ───────────────────────────────────
        by_ext = idx.get("by_ext", {})
        if by_ext:
            st.markdown("**Файлов по типу:**")
            ext_cols = st.columns(min(len(by_ext), 5))
            for i, (ext, count) in enumerate(by_ext.items()):
                with ext_cols[i % len(ext_cols)]:
                    pct = count / total * 100 if total else 0
                    st.metric(ext or "(нет расширения)", f"{count:,}", delta=f"{pct:.1f}%",
                              delta_color="off")

    telemetry = _get_index_telemetry_snapshot(cfg)
    st.divider()
    st.subheader("Этапы индексирования (из БД)")
    st.caption(f"SQLite: `{telemetry['db_path']}`")

    if not telemetry.get("exists"):
        st.info("БД телеметрии пока не создана. Она появится после первого запуска index_rag.py.")
    else:
        latest = telemetry.get("latest_run")
        if latest:
            st.markdown(
                f"**Последний запуск**: `{latest.get('status', '-')}` "
                f"(start: {latest.get('ts_started', '-')}, finish: {latest.get('ts_finished', '-')})"
            )
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.metric("Добавлено (run)", f"{int(latest.get('added_files', 0)):,}")
            with m2:
                st.metric("Изменено (run)", f"{int(latest.get('updated_files', 0)):,}")
            with m3:
                st.metric("Удалено (run)", f"{int(latest.get('deleted_files', 0)):,}")
            with m4:
                st.metric("Ошибок (run)", f"{int(latest.get('error_files', 0)):,}")
        else:
            st.info("Запусков индексирования в БД пока нет.")

        stage_rows = telemetry.get("stages", []) or []
        if stage_rows:
            st.markdown("**Прогресс по этапам:**")
            for row in stage_rows:
                stage = row.get("stage", "unknown")
                processed = int(row.get("processed_files", 0) or 0)
                total_files = int(row.get("total_files", 0) or 0)
                pct = 0.0
                if total_files > 0:
                    pct = min(100.0, processed / total_files * 100.0)
                st.write(f"`{stage}` — {processed:,}/{total_files:,} ({pct:.1f}%)")
                st.progress(min(1.0, pct / 100.0))
                st.caption(
                    f"added={int(row.get('added_files', 0)):,}, "
                    f"updated={int(row.get('updated_files', 0)):,}, "
                    f"skipped={int(row.get('skipped_files', 0)):,}, "
                    f"errors={int(row.get('error_files', 0)):,}, "
                    f"points={int(row.get('points_added', 0)):,}, "
                    f"status={row.get('status', '-')}"
                )

        st.markdown("**Изменения при повторных индексациях (по времени):**")
        wcols = st.columns(3)
        for idx_col, label in enumerate(("24ч", "7д", "30д")):
            row = telemetry.get("windows", {}).get(label, {})
            with wcols[idx_col]:
                st.markdown(f"**{label}**")
                st.metric("Запусков", f"{int(row.get('runs_count', 0)):,}")
                st.metric("Добавлено файлов", f"{int(row.get('added_files', 0)):,}")
                st.metric("Изменено файлов", f"{int(row.get('updated_files', 0)):,}")
                st.metric("Удалено файлов", f"{int(row.get('deleted_files', 0)):,}")

        st.markdown("**Логи поисковых запросов (последние 20):**")
        recent = telemetry.get("search_recent", []) or []
        if not recent:
            st.caption("Запросов в БД пока нет.")
        else:
            st.dataframe(
                recent,
                use_container_width=True,
                hide_index=True,
            )

    st.divider()

    # ── Просмотр лог-файла ────────────────────────────────────────────
    log_file = cfg.get("log_file", "")
    col_lh, col_ln = st.columns([4, 2])
    with col_lh:
        st.subheader("Лог индексирования")
        if log_file:
            st.caption(f"Файл: `{log_file}`")
    with col_ln:
        n_lines = st.selectbox(
            "Последних строк",
            options=[50, 100, 200, 500],
            index=1,
            key="log_n_lines",
            label_visibility="collapsed",
        )

    # Фильтр уровня
    log_level_filter = st.radio(
        "Фильтр уровня",
        options=["Все", "INFO+", "WARNING+", "ERROR"],
        index=0,
        horizontal=True,
        key="log_level_filter",
    )

    lines = _read_log_tail(cfg, n_lines=int(n_lines))

    # Применяем фильтр
    def _passes_filter(line: str) -> bool:
        upper = line.upper()
        if log_level_filter == "Все":
            return True
        if log_level_filter == "ERROR":
            return " - ERROR" in upper or upper.startswith("ERROR")
        if log_level_filter == "WARNING+":
            return (
                " - WARNING" in upper or " - ERROR" in upper
                or upper.startswith("WARNING") or upper.startswith("ERROR")
            )
        # INFO+
        return not (" - DEBUG" in upper or upper.startswith("DEBUG"))

    filtered = [l for l in lines if _passes_filter(l)]

    if not filtered:
        st.info("Нет строк, соответствующих фильтру.")
    else:
        colored_lines = "\n".join(_colorize_log_line(l) for l in filtered)
        st.markdown(
            f'<div class="log-container">{colored_lines}</div>',
            unsafe_allow_html=True,
        )
        st.caption(f"Показано {len(filtered)} строк из {len(lines)} последних в файле.")

    # ── Подсказка по командам ─────────────────────────────────────────
    with st.expander("Команды индексирования"):
        st.code(
            "# Быстрый старт: только метаданные (имена файлов, минуты)\n"
            "python index_rag.py --stage metadata\n\n"
            "# Полный контент: docx/xlsx + небольшие PDF\n"
            "python index_rag.py --stage small\n\n"
            "# Крупные и сканированные PDF (долго)\n"
            "python index_rag.py --stage large\n\n"
            "# Все этапы последовательно (по умолчанию)\n"
            "python index_rag.py\n\n"
            "# Удалить удалённые файлы из индекса\n"
            "python index_rag.py --cleanup\n\n"
            "# Пересоздать коллекцию с нуля\n"
            "python index_rag.py --recreate",
            language="bash",
        )


def render_telegram_tab(cfg: Dict[str, Any]) -> None:
    """Вкладка Telegram: статус интеграции и команды запуска."""
    st.subheader("Telegram-бот для поиска по документам")
    enabled = bool(cfg.get("telegram_enabled", False))
    token = (cfg.get("telegram_bot_token") or "").strip()
    chat_id = (cfg.get("telegram_allowed_chat_id") or "").strip()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Интеграция", "Включена" if enabled else "Выключена")
    with c2:
        st.metric("Токен", "Задан" if token else "Не задан")
    with c3:
        st.metric("chat_id", chat_id if chat_id else "Любой")

    if enabled and token:
        st.success("Конфигурация Telegram выглядит корректной.")
    else:
        st.warning(
            "Заполните в боковой панели: 'Пути и интеграции → Telegram бот'. "
            "Требуется включить интеграцию и задать Bot Token."
        )

    st.markdown("**Запуск бота:**")
    st.code("python telegram_bot.py", language="bash")
    st.markdown("**Пример вопроса в Telegram:** `Сколько весит PC300`")


# ═══════════════════════════ main ══════════════════════════════════════

def main() -> None:
    _ensure_session_state()
    cfg = load_config()

    # Инициализируем searcher один раз за сессию
    if st.session_state.get("searcher") is None:
        _init_searcher(cfg)

    # Заголовок
    col_title, col_status = st.columns([4, 1])
    with col_title:
        st.title("RAG Каталог — Семантический поиск")
    with col_status:
        if st.session_state.qdrant_connected:
            st.success("Подключено")
        else:
            st.error("Не подключено")

    st.info(
        "Поиск по документам и извлечение фактов. "
        "Задавайте вопросы естественным языком, система покажет найденные источники."
    )

    user = render_auth_gate(cfg)
    if not user:
        st.info("Войдите, зарегистрируйтесь или восстановите пароль через меню слева.")
        st.stop()

    if int(user.get("must_change_password") or 0):
        st.warning("Для дефолтного администратора требуется сменить пароль в меню 'Настройки пользователя'.")
        st.stop()

    # Боковая панель
    limit, file_type_val, content_only = render_sidebar(cfg, user)

    # ── Экраны приложения. Значение хранится в session_state, поэтому переходы
    # из результатов в проводник сохраняют состояние.
    screen = st.segmented_control(
        "Раздел",
        ["Поиск", "Проводник", "Индексирование", "Telegram"],
        key="active_screen",
        label_visibility="collapsed",
        width="stretch",
    )

    # ════════════════════ Вкладка: Поиск ═════════════════════════════
    if screen == "Поиск":
        st.divider()

        # Поисковая строка: обычный input сохраняет нормальное редактирование
        # текста, а история/подсказки вынесены в отдельный popover.
        initial_query = st.session_state.get("preset_query", "")
        if initial_query:
            st.session_state.query_input = initial_query

        suggestions = _search_suggestions(cfg)

        with st.container(border=True):
            q_col, s_col, b_col = st.columns([6, 1.4, 1])
            with q_col:
                query = st.text_input(
                    "Поисковый запрос",
                    placeholder="Введите что ищете: договоры, паспорта, счета, масса PC300...",
                    label_visibility="collapsed",
                    key="query_input",
                    on_change=_submit_current_query,
                )
            with s_col:
                with st.popover("История", use_container_width=True):
                    st.caption("История и подсказки")
                    if not suggestions:
                        st.info("История пока пуста.")
                    for idx, suggestion in enumerate(suggestions[:12]):
                        if st.button(
                            suggestion,
                            key=f"search_history_{idx}_{abs(hash(suggestion))}",
                            use_container_width=True,
                        ):
                            _choose_search_query(suggestion)
                            st.rerun()
            with b_col:
                submitted = st.button("Найти", use_container_width=True, type="primary")

        if initial_query:
            st.session_state.preset_query = ""

        should_search = (submitted or st.session_state.trigger_search) and bool(query.strip())
        st.session_state.trigger_search = False

        # Выполнить поиск
        if should_search:
            searcher = _get_searcher()
            if not searcher or not searcher.connected:
                st.error("Нет подключения к Qdrant. Запустите индексирование и обновите страницу.")
            else:
                with st.spinner("Поиск…"):
                    if searcher._embedder is None:
                        st.info("Первый запуск — загружается модель эмбеддинга (~5 сек)…")
                    try:
                        source_user = str(user.get("username") or "unknown")
                        results = searcher.search(
                            query.strip(),
                            limit=limit,
                            file_type=file_type_val,
                            content_only=content_only,
                            source=f"streamlit_ui:{source_user}",
                        )
                        fact = searcher.answer_fact_question(
                            query.strip(), limit=max(20, limit * 2)
                        )
                    except ConnectionError as exc:
                        st.error(f"Ошибка инфраструктуры: {exc}")
                        results = []
                        fact = {"ok": False, "error": str(exc)}
                    except RuntimeError as exc:
                        st.error(f"Ошибка выполнения поиска: {exc}")
                        results = []
                        fact = {"ok": False, "error": str(exc)}
                st.session_state.last_results = results
                st.session_state.last_query = query.strip()
                st.session_state.last_limit = limit
                st.session_state.last_file_type = file_type_val
                st.session_state.last_content_only = content_only
                st.session_state.last_fact_answer = fact
                _remember_search_query(query.strip())

        st.divider()

        # Показать результаты
        results = st.session_state.last_results
        render_fact_answer(st.session_state.get("last_fact_answer"))
        if results:
            params_changed = (
                limit != st.session_state.last_limit
                or file_type_val != st.session_state.last_file_type
                or content_only != st.session_state.last_content_only
            )
            if params_changed:
                st.warning("Параметры изменены. Нажмите «Найти» чтобы обновить результаты.")

            st.success(f"Найдено результатов: {len(results)}")
            render_grouped_results(results)
        elif should_search:
            st.info("По вашему запросу ничего не найдено.")
        else:
            c1, c2, c3 = st.columns(3)
            with c1:
                st.info("Введите запрос или используйте быстрый поиск в боковой панели")
            with c2:
                st.info("Например: договоры, паспорта, счета, акты")
            with c3:
                st.info("Фильтруйте по типу файла: .docx, .xlsx, .pdf")

        st.divider()

        # Статистика Qdrant
        searcher = _get_searcher()
        if searcher and st.session_state.qdrant_connected:
            st.subheader("Статистика коллекции")
            stats = _get_stats(searcher)
            if stats:
                c1, c2 = st.columns(2)
                with c1:
                    st.metric("Индексировано точек", f"{stats['points_count']:,}")
                with c2:
                    st.metric("Статус", stats["status"])

    # ════════════════════ Вкладка: Проводник ═════════════════════════
    elif screen == "Проводник":
        st.divider()
        render_explorer_tab(cfg)

    # ════════════════════ Вкладка: Индексирование ═════════════════════
    elif screen == "Индексирование":
        st.divider()
        render_indexing_tab(cfg)

    # ════════════════════ Вкладка: Telegram ═══════════════════════════
    elif screen == "Telegram":
        st.divider()
        render_telegram_tab(cfg)

    # Footer
    st.markdown(
        """
<div style="text-align:center;color:#aaa;font-size:0.8rem;padding:1rem 0;">
    RAG Semantic Search &nbsp;|&nbsp; DOCX · XLSX · PDF &nbsp;|&nbsp; Qdrant + all-MiniLM embeddings
</div>
""",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
