"""
state.py — PageState dataclass and per-user state helpers.

Depends on: .system (for _telemetry_db_path), core modules, nicegui.
Imported by: helpers.py, api.py, nice_app.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Collection, Dict, List, Optional

from nicegui import ui

from rag_catalog.core.rag_core import RAGSearcher, load_config, save_config
from rag_catalog.core.telemetry_db import TelemetryDB
from rag_catalog.core.user_auth_db import UserAuthDB

from .system import _telemetry_db_path

# ─────────────────────────── PageState ─────────────────────────────────────

@dataclass
class PageState:
    cfg: Dict[str, Any]
    searcher: Optional[RAGSearcher] = None
    searcher_error: str = ""
    screen: str = "search"
    query: str = ""
    file_type: Optional[str] = None
    limit: int = 50
    content_only: bool = False
    title_only: bool = False
    history: List[str] = field(default_factory=list)
    results: List[Dict[str, Any]] = field(default_factory=list)
    search_error: str = ""
    search_stats_hint: str = ""
    search_lazy_loading: bool = False
    search_request_id: int = 0
    searched_query: str = ""
    expanded_query: str = ""
    ai_search_expand: bool = True
    rag_answer_text: str = ""
    rag_answer_loading: bool = False
    rag_answer_ok: bool = True
    rag_answer_sources: List[Dict[str, Any]] = field(default_factory=list)
    doc_explain_path: str = ""
    doc_explain_text: str = ""
    doc_explain_loading: bool = False
    selected_result_paths: List[str] = field(default_factory=list)
    selection_summary_text: str = ""
    selection_summary_loading: bool = False
    settings_section: str = "profile"
    displayed_count: int = 10
    active_type_filter: Optional[str] = None
    explorer_path: Optional[str] = None
    explorer_filter: str = ""
    explorer_ext: str = "Все"
    explorer_sort: str = "По имени"
    explorer_desc: bool = False
    explorer_view: str = "Таблица"
    explorer_page: int = 0
    explorer_cd_path: str = ""
    explorer_tree_open: List[str] = field(default_factory=list)
    explorer_selected_paths: List[str] = field(default_factory=list)
    explorer_clipboard: Dict[str, Any] = field(default_factory=dict)
    explorer_hidden_paths: List[str] = field(default_factory=list)
    explorer_show_hidden: bool = False
    cloud_tab: str = "files"
    screen_scroll: Dict[str, int] = field(default_factory=dict)
    screen_cache: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    auth_db: Optional[UserAuthDB] = None
    current_user: Optional[Dict[str, Any]] = None
    auth_token: str = ""
    session_expired: bool = False
    theme: str = "light"
    favorites: List[Dict[str, Any]] = field(default_factory=list)
    saved_searches: List[Dict[str, Any]] = field(default_factory=list)
    header_explorer_actions: Optional[ui.row] = None
    header_breadcrumbs: Optional[ui.row] = None
    telemetry: Optional[TelemetryDB] = None
    index_progress_timer: Optional[Any] = None
    stage_status_timer: Optional[Any] = None
    tg_login_timer: Optional[Any] = None
    activity_timer: Optional[Any] = None
    scheduler_timer: Optional[Any] = None
    cloud_drive_timer: Optional[Any] = None
    # Cache for _read_index_telemetry() to avoid blocking event loop on every render
    _telemetry_nav_cache: Optional[Dict[str, Any]] = None
    _telemetry_nav_cache_ts: float = 0.0


def capture_screen_state(state: PageState, screen: Optional[str] = None) -> Dict[str, Any]:
    """Persist volatile per-screen state before rebuilding NiceGUI content."""
    current = screen or state.screen
    snapshot: Dict[str, Any] = {}
    if current == "search":
        snapshot = {
            "query": state.query,
            "file_type": state.file_type,
            "limit": state.limit,
            "content_only": state.content_only,
            "title_only": state.title_only,
            "history": list(state.history),
            "results": [dict(item) for item in state.results],
            "search_error": state.search_error,
            "search_stats_hint": state.search_stats_hint,
            "search_lazy_loading": state.search_lazy_loading,
            "searched_query": state.searched_query,
            "expanded_query": state.expanded_query,
            "rag_answer_text": state.rag_answer_text,
            "rag_answer_loading": state.rag_answer_loading,
            "rag_answer_ok": state.rag_answer_ok,
            "rag_answer_sources": [dict(item) for item in state.rag_answer_sources],
            "displayed_count": state.displayed_count,
            "active_type_filter": state.active_type_filter,
        }
    elif current == "explorer":
        snapshot = {
            "explorer_path": state.explorer_path,
            "explorer_filter": state.explorer_filter,
            "explorer_ext": state.explorer_ext,
            "explorer_sort": state.explorer_sort,
            "explorer_desc": state.explorer_desc,
            "explorer_view": state.explorer_view,
            "explorer_page": state.explorer_page,
            "explorer_cd_path": state.explorer_cd_path,
            "explorer_tree_open": list(state.explorer_tree_open),
            "explorer_selected_paths": list(state.explorer_selected_paths),
        }
    elif current == "cloud":
        snapshot = {"cloud_tab": state.cloud_tab}
    elif current == "settings":
        snapshot = {"settings_section": state.settings_section}
    if snapshot:
        state.screen_cache[current] = snapshot
    return snapshot


def restore_screen_state(state: PageState, screen: Optional[str] = None) -> bool:
    """Restore cached state for a screen without replacing the PageState object."""
    current = screen or state.screen
    snapshot = state.screen_cache.get(current)
    if not snapshot:
        return False

    if current == "search":
        state.query = str(snapshot.get("query") or "")
        state.file_type = snapshot.get("file_type") or None
        state.limit = int(snapshot.get("limit") or 50)
        state.content_only = bool(snapshot.get("content_only"))
        state.title_only = bool(snapshot.get("title_only"))
        state.history = list(snapshot.get("history") or [])
        state.results = [dict(item) for item in snapshot.get("results") or []]
        state.search_error = str(snapshot.get("search_error") or "")
        state.search_stats_hint = str(snapshot.get("search_stats_hint") or "")
        state.search_lazy_loading = bool(snapshot.get("search_lazy_loading"))
        state.searched_query = str(snapshot.get("searched_query") or "")
        state.expanded_query = str(snapshot.get("expanded_query") or "")
        state.rag_answer_text = str(snapshot.get("rag_answer_text") or "")
        state.rag_answer_loading = bool(snapshot.get("rag_answer_loading"))
        state.rag_answer_ok = bool(snapshot.get("rag_answer_ok", True))
        state.rag_answer_sources = [dict(item) for item in snapshot.get("rag_answer_sources") or []]
        state.displayed_count = int(snapshot.get("displayed_count") or 10)
        state.active_type_filter = snapshot.get("active_type_filter") or None
        return True

    if current == "explorer":
        state.explorer_path = snapshot.get("explorer_path") or None
        state.explorer_filter = str(snapshot.get("explorer_filter") or "")
        state.explorer_ext = str(snapshot.get("explorer_ext") or "Все")
        state.explorer_sort = str(snapshot.get("explorer_sort") or "По имени")
        state.explorer_desc = bool(snapshot.get("explorer_desc"))
        state.explorer_view = str(snapshot.get("explorer_view") or "Таблица")
        state.explorer_page = int(snapshot.get("explorer_page") or 0)
        state.explorer_cd_path = str(snapshot.get("explorer_cd_path") or "")
        state.explorer_tree_open = list(snapshot.get("explorer_tree_open") or [])
        state.explorer_selected_paths = list(snapshot.get("explorer_selected_paths") or [])
        return True

    if current == "cloud":
        state.cloud_tab = str(snapshot.get("cloud_tab") or "files")
        return True

    if current == "settings":
        state.settings_section = str(snapshot.get("settings_section") or "profile")
        return True

    return False


def should_rebuild_screen_container(
    screen: str,
    previous_screen: Optional[str],
    initialized_screens: Collection[str],
    dirty_screens: Collection[str],
) -> bool:
    """Return False only when a cached search DOM can be reused after navigation."""
    if screen not in initialized_screens:
        return True
    if screen in dirty_screens:
        return True
    if screen == "search" and previous_screen != screen:
        return False
    return True


# ─────────────────────────── config helpers ─────────────────────────────────

CONFIG_PATH_KEYS = {
    "catalog_path",
    "qdrant_db_path",
    "qdrant_url",
    "log_file",
    "collection_name",
    "telemetry_db_path",
    "embedding_model",
}


def _save_config_patch(values: Dict[str, Any]) -> Dict[str, Any]:
    clean = {key: str(values.get(key) or "").strip() for key in CONFIG_PATH_KEYS if key in values}
    cfg = load_config()
    cfg.update(clean)
    save_config(cfg)
    return cfg


# ─────────────────────────── auth / user helpers ────────────────────────────

def _users_db_path(cfg: Dict[str, Any]) -> Path:
    explicit = str(cfg.get("users_db_path") or "").strip()
    if explicit:
        return Path(explicit)
    return Path(str(cfg.get("qdrant_db_path") or ".")) / "rag_users.db"


def _get_auth_db(state: PageState) -> UserAuthDB:
    path = _users_db_path(state.cfg)
    if state.auth_db is None or Path(getattr(state.auth_db, "db_path", "")) != path:
        state.auth_db = UserAuthDB(str(path))
    return state.auth_db


def _refresh_current_user(state: PageState) -> None:
    if not state.current_user:
        return
    user = _get_auth_db(state).get_user(username=str(state.current_user.get("username") or ""))
    if user:
        state.current_user = user


def _username(state: PageState) -> str:
    return str((state.current_user or {}).get("username") or "").strip().lower()


# ─────────────────────────── telemetry helpers ──────────────────────────────

def _get_telemetry(state: PageState) -> TelemetryDB:
    path = _telemetry_db_path(state.cfg)
    if state.telemetry is None or Path(getattr(state.telemetry, "db_path", "")) != path:
        state.telemetry = TelemetryDB(str(path))
    return state.telemetry


def _log_app_event(
    state: PageState,
    feature: str,
    action: str,
    *,
    ok: bool = True,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        _get_telemetry(state).log_app_event(
            username=_username(state),
            screen=state.screen,
            feature=feature,
            action=action,
            ok=ok,
            details=details or {},
        )
    except Exception:
        pass


# ─────────────────────────── favorites helpers ──────────────────────────────

def _favorite_key(path: str) -> str:
    return str(path or "").strip().casefold()


def _is_favorite(state: PageState, path: str) -> bool:
    key = _favorite_key(path)
    return any(_favorite_key(str(item.get("path") or "")) == key for item in state.favorites)


def _favorite_type(path: Path) -> str:
    return "folder" if path.is_dir() else "file"


def _toggle_favorite(
    state: PageState,
    path: Path,
    *,
    item_type: Optional[str] = None,
    title: str = "",
) -> bool:
    username = _username(state)
    if not username:
        ui.notify("Войдите, чтобы сохранять избранное.", type="warning")
        return False
    auth_db = _get_auth_db(state)
    path_value = str(path)
    active = _is_favorite(state, path_value)
    if active:
        auth_db.remove_favorite(username=username, path=path_value)
        _log_app_event(state, "favorites", "remove", details={"path": path_value})
    else:
        auth_db.add_favorite(
            username=username,
            item_type=item_type or _favorite_type(path),
            path=path_value,
            title=title or path.name or path_value,
        )
        _log_app_event(state, "favorites", "add", details={"path": path_value, "item_type": item_type or _favorite_type(path)})
    state.favorites = auth_db.list_favorites(username=username)
    return not active


# ─────────────────────────── saved search helpers ───────────────────────────

def _is_saved_search(state: PageState, query: str) -> bool:
    key = (query or "").strip().lower()
    return any((item.get("query") or "").strip().lower() == key for item in state.saved_searches)


def _toggle_saved_search(state: PageState, query: str) -> bool:
    username = _username(state)
    if not username:
        ui.notify("Войдите, чтобы сохранять запросы.", type="warning")
        return False
    auth_db = _get_auth_db(state)
    q = (query or "").strip()
    if not q:
        return False
    active = _is_saved_search(state, q)
    if active:
        auth_db.remove_saved_search(username=username, query=q)
        _log_app_event(state, "saved_search", "remove", details={"query": q})
        ui.notify("Запрос удалён из сохранённых.", type="info")
    else:
        auth_db.add_saved_search(username=username, query=q)
        _log_app_event(state, "saved_search", "add", details={"query": q})
        ui.notify("Запрос сохранён.", type="positive")
    state.saved_searches = auth_db.list_saved_searches(username=username)
    return not active
