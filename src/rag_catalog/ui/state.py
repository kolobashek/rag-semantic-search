"""
state.py — PageState dataclass and per-user state helpers.

Depends on: .system (for _telemetry_db_path), core modules, nicegui.
Imported by: helpers.py, api.py, nice_app.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    auth_db: Optional[UserAuthDB] = None
    current_user: Optional[Dict[str, Any]] = None
    auth_token: str = ""
    session_expired: bool = False
    theme: str = "light"
    favorites: List[Dict[str, Any]] = field(default_factory=list)
    header_explorer_actions: Optional[ui.row] = None
    header_breadcrumbs: Optional[ui.row] = None
    telemetry: Optional[TelemetryDB] = None
    index_progress_timer: Optional[Any] = None
    stage_status_timer: Optional[Any] = None
    tg_login_timer: Optional[Any] = None
    activity_timer: Optional[Any] = None
    scheduler_timer: Optional[Any] = None
    cloud_drive_timer: Optional[Any] = None


# ─────────────────────────── config helpers ─────────────────────────────────

CONFIG_PATH_KEYS = {
    "catalog_path",
    "qdrant_db_path",
    "qdrant_url",
    "log_file",
    "collection_name",
    "telemetry_db_path",
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
