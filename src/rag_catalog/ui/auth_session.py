"""Session lifecycle helpers for the NiceGUI UI."""

from __future__ import annotations

from typing import Any, Callable, Dict

from nicegui import app

from rag_catalog.core.cloud_drive import CloudDriveService

from .state import PageState, _get_auth_db, _username


def ensure_cloud_drive_user_home(state: PageState, user: Dict[str, Any] | None = None) -> None:
    """Best-effort personal Cloud Drive folder check for login/session restore."""
    try:
        cfg = dict(state.cfg or {})
        if not bool(cfg.get("cloud_drive_enabled")) or not str(cfg.get("cloud_drive_db_path") or "").strip():
            return
        current = user or state.current_user or {}
        username = str(current.get("username") or "").strip().lower()
        if not username:
            return
        CloudDriveService.from_config(cfg).ensure_user_home_folder(username=username)
    except Exception:
        pass


def restore_session(state: PageState, *, on_restored: Callable[[PageState], None] | None = None) -> None:
    """Restore the current user from browser session storage if possible."""
    try:
        stored_token = str(app.storage.user.get("auth_token") or "")
        if not stored_token:
            return
        state.auth_token = stored_token
        state.current_user = _get_auth_db(state).get_user_by_session(stored_token)
        if state.current_user:
            ensure_cloud_drive_user_home(state)
            if on_restored is not None:
                on_restored(state)
            _get_auth_db(state).log_auth_event(username=_username(state), event_type="session_restore", ok=True)
            return
        state.session_expired = True
        state.auth_token = ""
        app.storage.user.pop("auth_token", None)
    except Exception:
        pass


def touch_session(state: PageState, *, min_interval_minutes: int = 60) -> None:
    """Refresh session activity with throttling in the auth DB."""
    if not state.auth_token or not state.current_user:
        return
    try:
        _get_auth_db(state).touch_session(state.auth_token, min_interval_minutes=min_interval_minutes)
    except Exception:
        pass


def complete_login_session(state: PageState, user: Dict[str, Any], *, event_type: str) -> None:
    """Set the current user, create a persistent session token, and audit login."""
    token = prepare_login_session(state, user, event_type=event_type)
    apply_login_session(state, user, token)


def prepare_login_session(state: PageState, user: Dict[str, Any], *, event_type: str) -> str:
    """Perform the blocking persistence work required for a login."""
    auth_db = _get_auth_db(state)
    username = str(user.get("username") or "").strip().lower()
    token = auth_db.create_session(username=username)
    ensure_cloud_drive_user_home(state, user)
    auth_db.log_auth_event(username=username, event_type=event_type, ok=True)
    return token


def apply_login_session(state: PageState, user: Dict[str, Any], token: str) -> None:
    """Apply a prepared login to UI-owned state and browser storage."""
    state.current_user = user
    state.auth_token = token
    state.session_expired = False
    try:
        app.storage.user["auth_token"] = state.auth_token
    except Exception:
        pass


def logout_session(state: PageState) -> None:
    """Revoke current session, clear user state, and remove browser token."""
    auth_db = _get_auth_db(state)
    if state.auth_token:
        auth_db.revoke_session(state.auth_token)
    auth_db.log_auth_event(username=_username(state), event_type="logout", ok=True)
    state.current_user = None
    state.auth_token = ""
    try:
        app.storage.user.pop("auth_token", None)
    except Exception:
        pass
