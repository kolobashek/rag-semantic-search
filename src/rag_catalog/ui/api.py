"""
api.py — FastAPI endpoint registrations for the NiceGUI app.

All routes are registered against NiceGUI's `app` (a Starlette/FastAPI instance)
at import time. This module must be imported before the server starts.

Depends on: .state, .system, .helpers, core modules.
"""

from __future__ import annotations

import hashlib
import json
import logging
import mimetypes
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Dict, List

from fastapi import File, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from nicegui import app

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.rag_core import load_config
from rag_catalog.core.telemetry_db import TelemetryDB
from rag_catalog.core.user_auth_db import UserAuthDB

from .helpers import _cd_registry_acl_allows, _resolve_catalog_file
from .state import _users_db_path
from .system import _read_cloud_bootstrap_status, _recover_cloud_drive_jobs, _safe_int, _telemetry_db_path

AuthHeader = Annotated[str, Header(alias="Authorization")]
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _share_token_fingerprint(token: str) -> str:
    clean_token = str(token or "").strip()
    return hashlib.sha256(clean_token.encode("utf-8")).hexdigest()[:12] if clean_token else ""


def _require_public_links_enabled(cfg: Dict[str, Any]) -> None:
    if not bool(cfg.get("cloud_drive_public_links_enabled")):
        raise HTTPException(status_code=403, detail="Публичные ссылки отключены политикой Cloud Drive.")


# ─────────────────────────── auth helpers (API-only) ───────────────────────


def _get_api_auth_db(cfg: Dict[str, Any]) -> UserAuthDB:
    return UserAuthDB(str(_users_db_path(cfg)))


def _require_cloud_drive_api_user(
    cfg: Dict[str, Any],
    *,
    authorization: str = "",
    write: bool = False,
    admin_only: bool = False,
) -> Dict[str, Any]:
    header = str(authorization or "").strip()
    token = ""
    if header.lower().startswith("bearer "):
        token = header[7:].strip()
    elif header:
        token = header
    if not token:
        try:
            token = str(app.storage.user.get("auth_token") or "").strip()
        except Exception:
            token = ""
    if not token:
        raise HTTPException(status_code=401, detail="Требуется авторизация.")
    user = _get_api_auth_db(cfg).get_user_by_session(token)
    if not user:
        raise HTTPException(status_code=401, detail="Сессия недействительна или истекла.")
    if str(user.get("status") or "") != "active":
        raise HTTPException(status_code=403, detail="Пользователь не активирован.")
    if admin_only and str(user.get("role") or "") != "admin":
        raise HTTPException(status_code=403, detail="Недостаточно прав.")
    try:
        if bool(cfg.get("cloud_drive_enabled")) and str(cfg.get("cloud_drive_db_path") or "").strip():
            CloudDriveService.from_config(cfg).ensure_user_home_folder(username=str(user.get("username") or ""))
    except Exception:
        pass
    return user


def _audit_cloud_drive_api_event(
    cfg: Dict[str, Any],
    user: Dict[str, Any],
    action: str,
    *,
    ok: bool = True,
    details: Dict[str, Any] | None = None,
) -> None:
    """Best-effort audit log for Cloud Drive API operations."""
    try:
        if not str(cfg.get("telemetry_db_path") or cfg.get("qdrant_db_path") or "").strip():
            return
        TelemetryDB(str(_telemetry_db_path(cfg))).log_app_event(
            username=str(user.get("username") or ""),
            screen="api",
            feature="cloud_drive",
            action=action,
            ok=ok,
            details=details or {},
        )
    except Exception:
        pass


def _cloud_drive_path_allowed(
    cfg: Dict[str, Any],
    user: Dict[str, Any],
    path: str,
    *,
    service: CloudDriveService | None = None,
    required_level: str = "viewer",
) -> bool:
    return _cd_registry_acl_allows(cfg, user, path, service=service, required_level=required_level)


def _require_cloud_drive_path_access(
    cfg: Dict[str, Any],
    user: Dict[str, Any],
    path: str,
    *,
    service: CloudDriveService | None = None,
    required_level: str = "viewer",
    audit_action: str = "path_access",
    audit_details: Dict[str, Any] | None = None,
) -> None:
    if not _cloud_drive_path_allowed(cfg, user, path, service=service, required_level=required_level):
        details = {
            "path": str(path or "").strip(),
            "required_level": required_level,
            "error": "acl_denied",
        }
        details.update(audit_details or {})
        _audit_cloud_drive_api_event(cfg, user, audit_action, ok=False, details=details)
        raise HTTPException(status_code=403, detail="Нет доступа к этому пути Cloud Drive.")


def _require_sync_client_access(
    service: CloudDriveService, user: Dict[str, Any], client_id: str, *, admin_ok: bool = True
) -> None:
    clean_client = str(client_id or "").strip()
    if not clean_client:
        raise HTTPException(status_code=400, detail="Не задан client_id.")
    if admin_ok and str(user.get("role") or "") == "admin":
        return
    client = service.registry.get_sync_client(clean_client)
    if client is None:
        raise HTTPException(status_code=404, detail=f"Sync-клиент не найден: {clean_client}")
    if str(client.username or "").lower() != str(user.get("username") or "").lower():
        raise HTTPException(status_code=403, detail="Нет доступа к этому sync-клиенту.")


# ─────────────────────────── job serializer ────────────────────────────────


def _serialize_cloud_drive_job(job: Any) -> Dict[str, Any]:
    return {
        "id": str(job.id),
        "job_type": str(job.job_type),
        "status": str(job.status),
        "file_id": str(job.file_id or ""),
        "version_id": str(job.version_id or ""),
        "payload": dict(getattr(job, "payload", {}) or {}),
        "progress": dict(getattr(job, "progress", {}) or {}),
        "attempts": _safe_int(getattr(job, "attempts", 0), 0),
        "last_error": str(getattr(job, "last_error", "") or ""),
        "created_at": str(getattr(job, "created_at", "") or ""),
        "updated_at": str(getattr(job, "updated_at", "") or ""),
        "started_at": str(getattr(job, "started_at", "") or ""),
        "finished_at": str(getattr(job, "finished_at", "") or ""),
        "lease_owner": str(getattr(job, "lease_owner", "") or ""),
        "lease_until": str(getattr(job, "lease_until", "") or ""),
        "next_run_at": str(getattr(job, "next_run_at", "") or ""),
    }


# ─────────────────────────── Device auth endpoints ───────────────────────────


@app.post("/api/auth/device/code")
def api_device_code(request: Request) -> Dict[str, Any]:
    """Create a device code pair for browser-based auth. No credentials required."""
    from . import device_auth as _da

    base = str(request.base_url).rstrip("/")
    return _da.create_code(base)


@app.get("/api/ping")
def api_ping() -> Dict[str, Any]:
    """Health check — no auth required. Used by sync clients to test connectivity."""
    return {"ok": True, "service": "rag-catalog"}


def _compact_ui_event_value(value: Any, *, depth: int = 0) -> Any:
    if depth > 3:
        return str(value)[:250]
    if isinstance(value, dict):
        return {str(key)[:80]: _compact_ui_event_value(val, depth=depth + 1) for key, val in list(value.items())[:40]}
    if isinstance(value, list):
        return [_compact_ui_event_value(item, depth=depth + 1) for item in value[:40]]
    if isinstance(value, str):
        return value[:1000]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)[:500]


@app.post("/api/ui-events")
async def api_ui_events(request: Request) -> Dict[str, Any]:
    """Best-effort browser diagnostics for page reloads, JS errors and websocket overlays."""
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {"payload": payload}
    cfg = load_config()
    action = str(payload.get("action") or "client_event").strip()[:80] or "client_event"
    details = _compact_ui_event_value(payload)
    if not isinstance(details, dict):
        details = {"payload": details}
    details["client_host"] = request.client.host if request.client else ""
    details["user_agent"] = str(request.headers.get("user-agent") or "")[:500]
    details["referer"] = str(request.headers.get("referer") or "")[:1000]
    username = ""
    try:
        token = str(app.storage.user.get("auth_token") or "").strip()
        if token:
            user = _get_api_auth_db(cfg).get_user_by_session(token)
            username = str((user or {}).get("username") or "")
    except Exception:
        username = ""
    try:
        TelemetryDB(str(_telemetry_db_path(cfg))).log_app_event(
            username=username,
            screen=str(payload.get("path") or payload.get("url") or "browser")[:120],
            feature="browser",
            action=action,
            ok=not action.endswith("_error") and "error" not in action,
            details=details,
        )
    except Exception:
        pass
    try:
        details_json = json.dumps(details, ensure_ascii=False, sort_keys=True, default=str)
        if len(details_json) > 4000:
            details_json = f"{details_json[:4000]}…"
        logger.info("browser_event action=%s username=%s details=%s", action, username, details_json)
    except Exception:
        pass
    return {"ok": True}


# Bump this whenever packaging/build.ps1 produces a new exe
_SYNC_CLIENT_VERSION = "1.1.0"


@app.get("/api/sync-client/version")
def api_sync_client_version() -> Dict[str, Any]:
    """
    Return the latest sync-client version available on this server.
    No auth required — clients check this before/after authentication.
    """
    root = Path(__file__).parents[3]
    packaging = root / "packaging" / "dist"
    has_exe = (packaging / "rag_sync_client.exe").is_file()
    has_msi = (packaging / "RAGSyncClient.msi").is_file()
    return {
        "version": _SYNC_CLIENT_VERSION,
        "has_exe": has_exe,
        "has_msi": has_msi,
        "download_url": "/api/cloud-drive/sync/client-download?format=exe",
    }


@app.get("/api/auth/device/token")
def api_device_token(device_code: str = "") -> Dict[str, Any]:
    """Poll for device auth result. Returns 428 while pending, 200 with token when approved."""
    from . import device_auth as _da

    if not str(device_code or "").strip():
        raise HTTPException(status_code=400, detail="Не задан device_code.")
    result = _da.poll_token(device_code)
    status = result["status"]
    if status == "not_found":
        raise HTTPException(status_code=404, detail="device_code не найден или истёк.")
    if status == "pending":
        raise HTTPException(status_code=428, detail="authorization_pending")
    if status in ("expired", "denied"):
        raise HTTPException(status_code=400, detail=status)
    # approved — include canonical server URL so client can save it
    return {
        "token": result["token"],
        "username": result["username"],
        "server": result.get("server"),
    }


# ─────────────────────────── API routes ─────────────────────────────────────


@app.get("/api/view-file")
def api_view_file(path: str, authorization: AuthHeader = "") -> FileResponse:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization)
    resolved = _resolve_catalog_file(cfg, path)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Файл не найден или недоступен")
    media_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
    return FileResponse(str(resolved), media_type=media_type, filename=resolved.name)


@app.get("/api/cloud-drive/bootstrap-status")
def api_cloud_drive_bootstrap_status(authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    return _read_cloud_bootstrap_status(cfg)


@app.post("/api/cloud-drive/bootstrap-recover")
def api_cloud_drive_bootstrap_recover(authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    result = _recover_cloud_drive_jobs(cfg)
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "bootstrap_recover",
        ok=bool(result.get("ok")),
        details={
            "recovered_jobs": result.get("recovered_jobs"),
            "legacy_state_recovered": result.get("legacy_state_recovered"),
            "error": result.get("error", ""),
        },
    )
    return result


@app.get("/api/cloud-drive/bootstrap-jobs")
def api_cloud_drive_bootstrap_jobs(limit: int = 20, authorization: AuthHeader = "") -> List[Dict[str, Any]]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    jobs = service.list_bootstrap_jobs(limit=max(1, min(int(limit), 100)))
    return [_serialize_cloud_drive_job(job) for job in jobs]


@app.get("/api/cloud-drive/jobs")
def api_cloud_drive_jobs(job_type: str = "", limit: int = 20, authorization: AuthHeader = "") -> List[Dict[str, Any]]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    jobs = service.list_jobs(job_type=str(job_type or "").strip(), limit=max(1, min(int(limit), 100)))
    return [_serialize_cloud_drive_job(job) for job in jobs]


@app.get("/api/cloud-drive/import-sources")
def api_cloud_drive_import_sources(
    enabled_only: bool = False,
    limit: int = 200,
    authorization: AuthHeader = "",
) -> List[Dict[str, Any]]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    return service.list_import_sources(enabled_only=bool(enabled_only), limit=max(1, min(int(limit or 200), 1000)))


@app.post("/api/cloud-drive/import-sources")
def api_cloud_drive_import_source_upsert(
    name: str = "",
    source_path: str = "",
    target_path: str = "",
    import_files: bool = True,
    enabled: bool = True,
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    try:
        source = service.upsert_import_source(
            name=name,
            source_path=source_path,
            target_path=target_path,
            import_files=bool(import_files),
            enabled=bool(enabled),
            created_by=str(user.get("username") or ""),
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "import_source_upsert",
            ok=False,
            details={"source_path": source_path, "target_path": target_path, "error": str(exc)},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "import_source_upsert",
        details={
            "source_id": source.get("id"),
            "source_path": source.get("source_path"),
            "target_path": source.get("target_path"),
        },
    )
    return source


@app.post("/api/cloud-drive/import-sources/enable")
def api_cloud_drive_import_source_enable(
    source_id: str = "",
    enabled: bool = True,
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    try:
        source = service.set_import_source_enabled(source_id, bool(enabled))
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "import_source_enable",
            ok=False,
            details={"source_id": source_id, "enabled": enabled, "error": str(exc)},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg, user, "import_source_enable", details={"source_id": source.get("id"), "enabled": source.get("enabled")}
    )
    return source


@app.post("/api/cloud-drive/import-sources/run")
def api_cloud_drive_import_source_run(
    source_id: str = "",
    max_files: int = 0,
    run_now: bool = False,
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    try:
        job = service.create_import_job(source_id=source_id, max_files=(int(max_files or 0) or None))
        result: Dict[str, Any] = {"job": _serialize_cloud_drive_job(job)}
        if run_now:
            stats = service.run_import_job(job.id)
            latest = service.get_job(job.id)
            result = {
                "job": _serialize_cloud_drive_job(latest or job),
                "stats": stats,
            }
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg, user, "import_source_run", ok=False, details={"source_id": source_id, "error": str(exc)}
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "import_source_run",
        details={"source_id": source_id, "job_id": result["job"].get("id"), "run_now": run_now},
    )
    return result


@app.get("/api/user-groups")
def api_user_groups(
    include_archived: bool = False,
    authorization: AuthHeader = "",
) -> List[Dict[str, Any]]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    is_admin = str(user.get("role") or "") == "admin"
    groups = _get_api_auth_db(cfg).list_groups(include_archived=bool(include_archived and is_admin))
    if is_admin:
        return groups
    return [
        {"id": group.get("id"), "name": group.get("name"), "description": group.get("description", "")}
        for group in groups
        if str(group.get("status") or "") == "active"
    ]


@app.post("/api/user-groups")
def api_user_group_create(
    name: str = "",
    description: str = "",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True, admin_only=True)
    try:
        group = _get_api_auth_db(cfg).create_group(
            name=name,
            description=description,
            created_by=str(user.get("username") or ""),
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "group_create", ok=False, details={"name": name, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg, user, "group_create", details={"group_id": group.get("id"), "name": group.get("name")}
    )
    return group


@app.patch("/api/user-groups")
def api_user_group_update(
    group_id: str = "",
    name: str = "",
    description: str = "",
    status: str = "active",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True, admin_only=True)
    try:
        group = _get_api_auth_db(cfg).update_group(
            group_id=group_id,
            name=name,
            description=description,
            status=status,
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg, user, "group_update", ok=False, details={"group_id": group_id, "error": str(exc)}
        )
        raise HTTPException(status_code=400, detail=str(exc))
    if group is None:
        raise HTTPException(status_code=404, detail="Группа не найдена.")
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "group_update",
        details={"group_id": group.get("id"), "name": group.get("name"), "status": group.get("status")},
    )
    return group


@app.post("/api/user-groups/members")
def api_user_group_member_add(
    group_id: str = "",
    username: str = "",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True, admin_only=True)
    try:
        added = _get_api_auth_db(cfg).add_group_member(
            group_id=group_id,
            username=username,
            added_by=str(user.get("username") or ""),
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "group_member_add",
            ok=False,
            details={"group_id": group_id, "username": username, "error": str(exc)},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg, user, "group_member_add", ok=added, details={"group_id": group_id, "username": username}
    )
    return {"ok": bool(added), "group_id": str(group_id or "").strip(), "username": str(username or "").strip().lower()}


@app.delete("/api/user-groups/members")
def api_user_group_member_remove(
    group_id: str = "",
    username: str = "",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True, admin_only=True)
    removed = _get_api_auth_db(cfg).remove_group_member(group_id=group_id, username=username)
    _audit_cloud_drive_api_event(
        cfg, user, "group_member_remove", ok=removed, details={"group_id": group_id, "username": username}
    )
    return {
        "ok": bool(removed),
        "group_id": str(group_id or "").strip(),
        "username": str(username or "").strip().lower(),
    }


@app.post("/api/cloud-drive/permissions")
def api_cloud_drive_permissions(
    subject_type: str = "",
    subject_id: str = "",
    path: str = "",
    resource_type: str = "",
    resource_id: str = "",
    access_level: str = "viewer",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    clean_subject_type = str(subject_type or "").strip().lower()
    if clean_subject_type == "group":
        group = _get_api_auth_db(cfg).get_group(subject_id)
        if group is None or str(group.get("status") or "") != "active":
            raise HTTPException(status_code=400, detail="Активная группа не найдена.")
        subject_id = str(group.get("id") or "")
    is_admin = str(user.get("role") or "") == "admin"
    if not is_admin:
        if not str(path or "").strip():
            raise HTTPException(status_code=400, detail="Для выдачи доступа пользователем нужен path.")
        if str(resource_type or "").strip() or str(resource_id or "").strip():
            raise HTTPException(status_code=403, detail="Только админ может выдавать доступ по resource_id.")
        if str(access_level or "viewer").strip().lower() not in {"viewer", "read", "editor", "write"}:
            raise HTTPException(status_code=403, detail="Пользователь может выдавать только чтение или редактирование.")
        if clean_subject_type not in {"user", "group", "*"}:
            raise HTTPException(
                status_code=403, detail="Пользователь может открыть доступ всем, пользователю или группе."
            )
        _require_cloud_drive_path_access(
            cfg, user, path, service=service, required_level="admin", audit_action="permissions_grant"
        )
    try:
        if str(path or "").strip() or not str(resource_type or "").strip():
            permission = service.grant_path_permission(
                subject_type=subject_type,
                subject_id=subject_id,
                path=path,
                access_level=access_level,
            )
        else:
            permission = service.grant_permission(
                subject_type=subject_type,
                subject_id=subject_id,
                resource_type=resource_type,
                resource_id=resource_id,
                access_level=access_level,
            )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "permissions_grant",
            ok=False,
            details={"subject_type": subject_type, "subject_id": subject_id, "path": path, "error": str(exc)},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(cfg, user, "permissions_grant", details=permission)
    return permission


@app.get("/api/cloud-drive/permissions")
def api_cloud_drive_permissions_list(
    path: str = "",
    authorization: AuthHeader = "",
) -> List[Dict[str, Any]]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    if str(user.get("role") or "") != "admin":
        if not str(path or "").strip():
            raise HTTPException(status_code=400, detail="Для пользователя нужен path.")
        _require_cloud_drive_path_access(
            cfg, user, path, service=service, required_level="admin", audit_action="permissions_list"
        )
    return service.list_permissions(path=path)


@app.delete("/api/cloud-drive/permissions")
def api_cloud_drive_permission_revoke(
    permission_id: str = "",
    path: str = "",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    if str(user.get("role") or "") != "admin":
        if not str(path or "").strip():
            raise HTTPException(status_code=400, detail="Для пользователя нужен path.")
        _require_cloud_drive_path_access(
            cfg, user, path, service=service, required_level="admin", audit_action="permissions_revoke"
        )
        allowed_ids = {str(item.get("id") or "") for item in service.list_permissions(path=path)}
        if str(permission_id or "").strip() not in allowed_ids:
            raise HTTPException(status_code=403, detail="Нет прав на отзыв этого доступа.")
    ok = service.revoke_permission(permission_id)
    _audit_cloud_drive_api_event(
        cfg, user, "permissions_revoke", ok=ok, details={"permission_id": permission_id, "path": path}
    )
    return {"ok": bool(ok), "permission_id": str(permission_id or "").strip()}


@app.post("/api/cloud-drive/share-links")
def api_cloud_drive_share_link_create(
    request: Request,
    path: str = "",
    expires_at: str = "",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    _require_public_links_enabled(cfg)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(
        cfg, user, path, service=service, required_level="admin", audit_action="share_link_create"
    )
    try:
        link = service.create_share_link(
            path=path,
            created_by=str(user.get("username") or ""),
            expires_at=expires_at,
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg, user, "share_link_create", ok=False, details={"path": path, "error": str(exc)}
        )
        raise HTTPException(status_code=400, detail=str(exc))
    base = str(request.base_url).rstrip("/")
    link["url"] = f"{base}{link.get('url_path')}"
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "share_link_create",
        details={"path": path, "token_fingerprint": _share_token_fingerprint(str(link.get("token") or ""))},
    )
    return link


@app.get("/api/cloud-drive/share-links")
def api_cloud_drive_share_links(
    path: str = "",
    include_inactive: bool = False,
    authorization: AuthHeader = "",
) -> List[Dict[str, Any]]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    clean_path = str(path or "").strip().replace("\\", "/").strip("/")
    if str(user.get("role") or "") != "admin":
        if not clean_path:
            raise HTTPException(status_code=400, detail="Для пользователя нужен path.")
        _require_cloud_drive_path_access(
            cfg, user, clean_path, service=service, required_level="admin", audit_action="share_links_list"
        )
    links = service.list_share_links(path=clean_path, include_inactive=bool(include_inactive))
    _audit_cloud_drive_api_event(cfg, user, "share_links_list", details={"path": clean_path, "count": len(links)})
    return links


@app.delete("/api/cloud-drive/share-links")
def api_cloud_drive_share_link_revoke(
    token: str = "",
    path: str = "",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    clean_path = str(path or "").strip().replace("\\", "/").strip("/")
    clean_token = str(token or "").strip()
    if not clean_path:
        raise HTTPException(status_code=400, detail="Для отзыва публичной ссылки нужен path.")
    _require_cloud_drive_path_access(
        cfg, user, clean_path, service=service, required_level="admin", audit_action="share_link_revoke"
    )
    allowed_tokens = {
        str(item.get("token") or "") for item in service.list_share_links(path=clean_path, include_inactive=True)
    }
    if not clean_token or clean_token not in allowed_tokens:
        raise HTTPException(status_code=404, detail="Публичная ссылка не найдена для указанного пути.")
    ok = service.revoke_share_link(clean_token)
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "share_link_revoke",
        ok=ok,
        details={"path": clean_path, "token_fingerprint": _share_token_fingerprint(clean_token)},
    )
    return {"ok": bool(ok), "path": clean_path}


def _require_public_share_access(
    cfg: Dict[str, Any], service: CloudDriveService, token: str, path: str = "", *, audit_action: str
) -> str:
    audit_user: Dict[str, Any] = {"username": "public-share"}
    audit_base = {
        "path": str(path or "").strip(),
        "token_fingerprint": _share_token_fingerprint(token),
    }
    link = service.registry.get_share_link(token)
    if link is None:
        _audit_cloud_drive_api_event(
            cfg, audit_user, audit_action, ok=False, details={**audit_base, "error": "link_missing_or_expired"}
        )
        raise HTTPException(status_code=404, detail="Публичная ссылка не найдена или истекла.")
    effective_path = str(path or link.get("path") or "").strip()
    if not service.registry.share_link_can_access(token=token, path=effective_path, required_level="viewer"):
        _audit_cloud_drive_api_event(
            cfg,
            audit_user,
            audit_action,
            ok=False,
            details={**audit_base, "path": effective_path, "error": "acl_denied"},
        )
        raise HTTPException(status_code=403, detail="Публичная ссылка не даёт доступ к этому пути.")
    return effective_path


@app.get("/api/cloud-drive/public/node")
def api_cloud_drive_public_node(token: str = "", path: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_public_links_enabled(cfg)
    service = CloudDriveService.from_config(cfg)
    effective_path = _require_public_share_access(cfg, service, token, path, audit_action="public_view_node")
    try:
        result = service.get_node(effective_path)
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            {"username": "public-share"},
            "public_view_node",
            ok=False,
            details={"path": effective_path, "token_fingerprint": _share_token_fingerprint(token), "error": str(exc)},
        )
        raise HTTPException(status_code=404, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg,
        {"username": "public-share"},
        "public_view_node",
        details={"path": effective_path, "token_fingerprint": _share_token_fingerprint(token)},
    )
    return result


@app.get("/api/cloud-drive/public/list")
def api_cloud_drive_public_list(token: str = "", path: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_public_links_enabled(cfg)
    service = CloudDriveService.from_config(cfg)
    effective_path = _require_public_share_access(cfg, service, token, path, audit_action="public_list_directory")
    try:
        result = service.list_directory(effective_path)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    result["folders"] = [
        item
        for item in result.get("folders", [])
        if service.registry.share_link_can_access(
            token=token, path=str(item.get("path") or ""), required_level="viewer"
        )
    ]
    result["files"] = [
        item
        for item in result.get("files", [])
        if service.registry.share_link_can_access(
            token=token, path=str(item.get("path") or ""), required_level="viewer"
        )
    ]
    _audit_cloud_drive_api_event(
        cfg,
        {"username": "public-share"},
        "public_list_directory",
        details={"path": effective_path, "token_fingerprint": _share_token_fingerprint(token)},
    )
    return result


@app.get("/api/cloud-drive/public/download")
def api_cloud_drive_public_download(token: str = "", path: str = ""):
    cfg = load_config()
    _require_public_links_enabled(cfg)
    service = CloudDriveService.from_config(cfg)
    effective_path = _require_public_share_access(cfg, service, token, path, audit_action="public_download")
    try:
        descriptor = service.get_download_descriptor(effective_path)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    if descriptor.get("mode") != "local_file":
        if descriptor.get("mode") == "redirect_url" and descriptor.get("url"):
            return RedirectResponse(str(descriptor["url"]))
        raise HTTPException(status_code=501, detail="Этот storage backend пока не поддерживает download.")
    _audit_cloud_drive_api_event(
        cfg,
        {"username": "public-share"},
        "public_download",
        details={"path": effective_path, "token_fingerprint": _share_token_fingerprint(token)},
    )
    return FileResponse(
        path=str(descriptor["file_path"]),
        media_type=str(descriptor["mime_type"]),
        filename=str(descriptor["filename"]),
    )


@app.get("/api/cloud-drive/job")
def api_cloud_drive_job(job_id: str, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job не найден: {job_id}")
    return _serialize_cloud_drive_job(job)


@app.post("/api/cloud-drive/jobs/recover-stale")
def api_cloud_drive_jobs_recover_stale(
    job_types: str = "reindex,cleanup",
    lease_timeout_seconds: int = 3600,
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    types = [part.strip() for part in str(job_types or "").split(",") if part.strip()]
    recovered = service.recover_stale_jobs(
        job_types=types or None,
        lease_timeout_seconds=max(1, int(lease_timeout_seconds or 3600)),
        limit=500,
    )
    _audit_cloud_drive_api_event(cfg, user, "jobs_recover_stale", details={"job_types": types, "recovered": recovered})
    return {"ok": True, "recovered": recovered, "job_types": types}


@app.get("/api/cloud-drive/file-statuses")
def api_cloud_drive_file_statuses(
    file_ids: str = "", paths: str = "", authorization: AuthHeader = ""
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    ids = [part.strip() for part in str(file_ids or "").split(",") if part.strip()]
    for path in [part.strip() for part in str(paths or "").split(",") if part.strip()]:
        _require_cloud_drive_path_access(cfg, user, path, service=service, audit_action="file_statuses")
        file_row = service.registry.get_file_by_path(path)
        if file_row is not None:
            ids.append(file_row.id)
    ids = list(dict.fromkeys(ids))
    if str(user.get("role") or "") != "admin":
        allowed_ids: list[str] = []
        for file_id in ids:
            file_row = service.registry.get_file_by_id(file_id)
            if file_row is not None and _cloud_drive_path_allowed(cfg, user, file_row.path, service=service):
                allowed_ids.append(file_id)
        ids = allowed_ids
    jobs = service.registry.list_latest_jobs_for_files(ids)
    return {file_id: _serialize_cloud_drive_job(job) for file_id, job in jobs.items()}


@app.get("/api/cloud-drive/job-latest")
def api_cloud_drive_job_latest(job_type: str, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    clean_type = str(job_type or "").strip()
    if not clean_type:
        raise HTTPException(status_code=400, detail="Не задан job_type.")
    service = CloudDriveService.from_config(cfg)
    job = service.get_latest_job(job_type=clean_type)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Jobs типа не найдены: {clean_type}")
    return _serialize_cloud_drive_job(job)


def _cloud_drive_backup_freshness(cfg: Dict[str, Any]) -> Dict[str, Any]:
    backup_dir = Path(str(cfg.get("cloud_drive_backup_dir") or PROJECT_ROOT / "runtime" / "backups")).expanduser()
    max_age_hours = max(1.0, float(cfg.get("cloud_drive_backup_max_age_hours") or 24.0))
    candidates = sorted(backup_dir.glob("*.zip"), key=lambda item: item.stat().st_mtime, reverse=True) if backup_dir.exists() else []
    if not candidates:
        return {
            "status": "missing",
            "ok": False,
            "backup_dir": str(backup_dir.resolve()),
            "max_age_hours": max_age_hours,
        }
    latest = candidates[0]
    age_hours = max(0.0, (datetime.now().timestamp() - latest.stat().st_mtime) / 3600.0)
    manifest_ok = False
    complete = False
    created_at = ""
    try:
        with zipfile.ZipFile(latest, "r") as zf:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
        manifest_ok = manifest.get("kind") == "rag-catalog-cloud-drive-backup"
        complete = (
            manifest_ok
            and int(manifest.get("version") or 1) >= 2
            and str(manifest.get("storage_backend") or "") == "local"
        )
        created_at = str(manifest.get("created_at") or "")
    except (OSError, KeyError, ValueError, zipfile.BadZipFile):
        pass
    drill_ok = False
    drill_completed_at = ""
    artifact_path = Path(f"{latest}.restore-drill.json")
    try:
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        drill_ok = (
            bool(artifact.get("ok"))
            and int(artifact.get("backup_size_bytes") or -1) == latest.stat().st_size
            and int(artifact.get("backup_mtime_ns") or -1) == latest.stat().st_mtime_ns
        )
        drill_completed_at = str(artifact.get("completed_at") or "") if drill_ok else ""
    except (OSError, ValueError):
        pass
    fresh = age_hours <= max_age_hours
    ok = manifest_ok and complete and fresh and drill_ok
    if not manifest_ok or not complete:
        status = "invalid"
    elif not fresh:
        status = "stale"
    elif not drill_ok:
        status = "unverified"
    else:
        status = "healthy"
    return {
        "status": status,
        "ok": ok,
        "backup_dir": str(backup_dir.resolve()),
        "latest_path": str(latest.resolve()),
        "created_at": created_at,
        "age_hours": round(age_hours, 2),
        "max_age_hours": max_age_hours,
        "manifest_ok": manifest_ok,
        "complete": complete,
        "restore_drill_ok": drill_ok,
        "restore_drill_completed_at": drill_completed_at,
    }


@app.get("/api/cloud-drive/storage-health")
def api_cloud_drive_storage_health(authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    health = service.get_storage_health()
    backup = _cloud_drive_backup_freshness(cfg)
    return {
        "backend": health.backend,
        "ok": health.ok,
        "writable": health.writable,
        "target": health.target,
        "error": health.error,
        "backup": backup,
    }


@app.get("/api/cloud-drive/index-coverage")
def api_cloud_drive_index_coverage(sample_limit: int = 25, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    index_state_path = Path(str(cfg.get("qdrant_db_path") or "")) / "index_state.db"
    return service.get_index_coverage(
        index_state_db_path=str(index_state_path),
        sample_limit=max(1, min(int(sample_limit or 25), 500)),
    )


@app.post("/api/cloud-drive/index-coverage/repair")
def api_cloud_drive_index_coverage_repair(
    scopes: str = "missing,stale,error",
    limit: int = 100,
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    index_state_path = Path(str(cfg.get("qdrant_db_path") or "")) / "index_state.db"
    try:
        result = service.enqueue_index_coverage_repair(
            index_state_db_path=str(index_state_path),
            scopes=scopes,
            limit=limit,
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "index_coverage_repair",
            ok=False,
            details={"scopes": scopes, "limit": limit, "error": str(exc)},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "index_coverage_repair",
        details={"scopes": scopes, "limit": limit, "queued": result.get("queued")},
    )
    return result


@app.post("/api/cloud-drive/index-coverage/quarantine-unavailable")
def api_cloud_drive_index_coverage_quarantine_unavailable(
    limit: int = 100,
    dry_run: bool = True,
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    index_state_path = Path(str(cfg.get("qdrant_db_path") or "")) / "index_state.db"
    try:
        result = service.quarantine_unavailable_index_coverage(
            index_state_db_path=str(index_state_path),
            limit=limit,
            dry_run=dry_run,
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "index_coverage_quarantine_unavailable",
            ok=False,
            details={"limit": limit, "dry_run": dry_run, "error": str(exc)},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "index_coverage_quarantine_unavailable",
        details={
            "limit": limit,
            "dry_run": dry_run,
            "candidates": result.get("candidates"),
            "quarantined": result.get("quarantined"),
        },
    )
    return result


@app.get("/api/cloud-drive/node")
def api_cloud_drive_node(path: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    _require_cloud_drive_path_access(cfg, user, path, audit_action="view_node")
    service = CloudDriveService.from_config(cfg)
    try:
        result = service.get_node(path)
        _audit_cloud_drive_api_event(cfg, user, "view_node", details={"path": path})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "view_node", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/api/cloud-drive/list")
def api_cloud_drive_list(path: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    _require_cloud_drive_path_access(cfg, user, path, audit_action="list_directory")
    service = CloudDriveService.from_config(cfg)
    try:
        result = service.list_directory(path)
        result["folders"] = [
            item
            for item in result.get("folders", [])
            if _cloud_drive_path_allowed(cfg, user, str(item.get("path") or ""), service=service)
        ]
        result["files"] = [
            item
            for item in result.get("files", [])
            if _cloud_drive_path_allowed(cfg, user, str(item.get("path") or ""), service=service)
        ]
        _audit_cloud_drive_api_event(cfg, user, "list_directory", details={"path": path})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "list_directory", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/api/cloud-drive/search")
def api_cloud_drive_search(
    query: str = "",
    path: str = "",
    limit: int = 50,
    offset: int = 0,
    node_type: str = "",
    extension: str = "",
    mime_type: str = "",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    _require_cloud_drive_path_access(cfg, user, path, audit_action="search_nodes")
    clean_query = str(query or "").strip()
    if not clean_query:
        raise HTTPException(status_code=400, detail="Не задан query.")
    service = CloudDriveService.from_config(cfg)
    clean_limit = max(1, min(int(limit or 50), 500))
    clean_offset = max(0, int(offset or 0))
    try:
        result = service.search_nodes(
            query=clean_query,
            path=path,
            limit=clean_limit,
            offset=clean_offset,
            node_type=node_type,
            extension=extension,
            mime_type=mime_type,
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg, user, "search_nodes", ok=False, details={"path": path, "query": clean_query, "error": str(exc)}
        )
        raise HTTPException(status_code=404, detail=str(exc))
    items = [
        item
        for item in result.get("items", [])
        if _cloud_drive_path_allowed(cfg, user, str(item.get("path") or ""), service=service)
    ]
    next_offset = result.get("next_offset")
    while len(items) < clean_limit and next_offset is not None:
        extra_offset = int(next_offset)
        try:
            extra = service.search_nodes(
                query=clean_query,
                path=path,
                limit=clean_limit,
                offset=extra_offset,
                node_type=node_type,
                extension=extension,
                mime_type=mime_type,
            )
        except RuntimeError:
            break
        consumed = 0
        for item in extra.get("items", []):
            consumed += 1
            if _cloud_drive_path_allowed(cfg, user, str(item.get("path") or ""), service=service):
                items.append(item)
                if len(items) >= clean_limit:
                    break
        if len(items) >= clean_limit:
            candidate_offset = extra_offset + consumed
            next_offset = candidate_offset if candidate_offset < int(extra.get("total") or 0) else None
        else:
            next_offset = extra.get("next_offset")
    result["items"] = items[:clean_limit]
    result["count"] = len(result["items"])
    result["next_offset"] = next_offset
    result["filters"] = {
        "node_type": str(node_type or "").strip().lower(),
        "extension": str(extension or "").strip().lower().lstrip("."),
        "mime_type": str(mime_type or "").strip().lower(),
    }
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "search_nodes",
        details={"path": path, "query": clean_query, "count": result["count"], "offset": clean_offset},
    )
    return result


@app.get("/api/cloud-drive/changes")
def api_cloud_drive_changes(since: str = "", limit: int = 500, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    result = service.list_changes(since=since, limit=max(1, min(int(limit or 500), 5000)))
    changes = [
        item
        for item in result.get("changes", [])
        if _cloud_drive_path_allowed(cfg, user, str(item.get("path") or ""), service=service)
    ]
    result["changes"] = changes
    result["count"] = len(changes)
    _audit_cloud_drive_api_event(cfg, user, "changes", details={"since": since, "count": len(changes)})
    return result


@app.get("/api/cloud-drive/sync/clients")
def api_cloud_drive_sync_clients(
    username: str = "", include_offline: bool = True, limit: int = 100, authorization: AuthHeader = ""
) -> List[Dict[str, Any]]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    requested_username = str(username or "").strip().lower()
    if str(user.get("role") or "") != "admin":
        requested_username = str(user.get("username") or "").strip().lower()
    clients = service.list_sync_clients(
        username=requested_username,
        include_offline=bool(include_offline),
        limit=max(1, min(int(limit or 100), 1000)),
    )
    _audit_cloud_drive_api_event(
        cfg, user, "sync_clients", details={"username": requested_username, "count": len(clients)}
    )
    return clients


@app.post("/api/cloud-drive/sync/clients")
def api_cloud_drive_sync_client_register(
    device_id: str = "",
    display_name: str = "",
    platform: str = "",
    status: str = "online",
    metadata_json: str = "{}",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    try:
        metadata = json.loads(str(metadata_json or "{}"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="metadata_json должен быть JSON-объектом.")
    if not isinstance(metadata, dict):
        raise HTTPException(status_code=400, detail="metadata_json должен быть JSON-объектом.")
    service = CloudDriveService.from_config(cfg)
    try:
        client = service.register_sync_client(
            username=str(user.get("username") or ""),
            device_id=device_id,
            display_name=display_name,
            platform=platform,
            status=status,
            metadata=metadata,
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg, user, "sync_client_register", ok=False, details={"device_id": device_id, "error": str(exc)}
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg, user, "sync_client_register", details={"client_id": client.get("id"), "device_id": device_id}
    )
    return client


@app.get("/api/cloud-drive/sync/pairs")
def api_cloud_drive_sync_pairs(
    client_id: str = "", enabled_only: bool = False, authorization: AuthHeader = ""
) -> List[Dict[str, Any]]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    if client_id:
        _require_sync_client_access(service, user, client_id)
    username = "" if str(user.get("role") or "") == "admin" else str(user.get("username") or "")
    pairs = service.list_sync_pairs(username=username, client_id=client_id, enabled_only=bool(enabled_only))
    _audit_cloud_drive_api_event(cfg, user, "sync_pairs", details={"client_id": client_id, "count": len(pairs)})
    return pairs


@app.get("/api/cloud-drive/sync/client-download")
def api_cloud_drive_sync_client_download(
    format: str = "auto",
    authorization: AuthHeader = "",
):
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization)
    root = Path(__file__).parents[3]
    packaging = root / "packaging" / "dist"
    fmt = str(format or "auto").strip().lower()
    if fmt == "py":
        candidates = [
            (root / "rag_sync_client.py", "text/x-python", "rag_sync_client.py"),
        ]
    elif fmt == "msi":
        candidates = [
            (packaging / "RAGSyncClient.msi", "application/x-msi", "RAGSyncClient.msi"),
        ]
    elif fmt == "exe":
        candidates = [
            (packaging / "RAGSyncClientSetup.exe", "application/octet-stream", "RAGSyncClientSetup.exe"),
            (packaging / "rag_sync_client.exe", "application/octet-stream", "rag_sync_client.exe"),
        ]
    else:  # auto: msi > exe installer > bare exe > py
        candidates = [
            (packaging / "RAGSyncClient.msi", "application/x-msi", "RAGSyncClient.msi"),
            (packaging / "RAGSyncClientSetup.exe", "application/octet-stream", "RAGSyncClientSetup.exe"),
            (packaging / "rag_sync_client.exe", "application/octet-stream", "rag_sync_client.exe"),
            (root / "rag_sync_client.py", "text/x-python", "rag_sync_client.py"),
        ]
    for path, mime, filename in candidates:
        if path.is_file():
            return FileResponse(
                path=str(path),
                media_type=mime,
                filename=filename,
                headers={"Content-Disposition": f"attachment; filename={filename}"},
            )
    raise HTTPException(
        status_code=404, detail="Sync-клиент не найден. Соберите установщик командой packaging/build.ps1."
    )


@app.post("/api/cloud-drive/sync/heartbeat")
def api_cloud_drive_sync_heartbeat(
    client_id: str = "",
    status: str = "online",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    _require_sync_client_access(service, user, client_id)
    ok = service.registry.update_sync_client_status(client_id, status)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Sync-клиент не найден: {client_id}")
    _audit_cloud_drive_api_event(cfg, user, "sync_heartbeat", details={"client_id": client_id, "status": status})
    return {"ok": True, "client_id": client_id, "status": status}


@app.post("/api/cloud-drive/sync/pairs")
def api_cloud_drive_sync_pair_upsert(
    client_id: str = "",
    local_path: str = "",
    cloud_path: str = "",
    conflict_policy: str = "ask",
    enabled: bool = True,
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_sync_client_access(service, user, client_id)
    _require_cloud_drive_path_access(
        cfg, user, cloud_path, service=service, required_level="editor", audit_action="sync_pair_upsert"
    )
    try:
        pair = service.upsert_sync_pair(
            client_id=client_id,
            local_path=local_path,
            cloud_path=cloud_path,
            conflict_policy=conflict_policy,
            enabled=bool(enabled),
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "sync_pair_upsert",
            ok=False,
            details={"client_id": client_id, "cloud_path": cloud_path, "error": str(exc)},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "sync_pair_upsert",
        details={"client_id": client_id, "pair_id": pair.get("id"), "cloud_path": cloud_path},
    )
    return pair


@app.post("/api/cloud-drive/sync/pairs/delete")
def api_cloud_drive_sync_pair_delete(
    pair_id: str = "", client_id: str = "", authorization: AuthHeader = ""
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_sync_client_access(service, user, client_id)
    result = service.delete_sync_pair(pair_id, client_id=client_id)
    _audit_cloud_drive_api_event(
        cfg, user, "sync_pair_delete", details={"pair_id": pair_id, "client_id": client_id, "ok": result.get("ok")}
    )
    return result


@app.get("/api/cloud-drive/sync/selective")
def api_cloud_drive_sync_selective(client_id: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    if client_id:
        _require_sync_client_access(service, user, client_id)
    username = "" if str(user.get("role") or "") == "admin" else str(user.get("username") or "")
    result = service.list_selective_sync_paths(username=username, client_id=client_id)
    _audit_cloud_drive_api_event(
        cfg, user, "sync_selective", details={"client_id": client_id, "count": result.get("count")}
    )
    return result


@app.post("/api/cloud-drive/sync/selective")
def api_cloud_drive_sync_selective_set(
    client_id: str = "",
    paths: str = "",
    mode: str = "exclude",
    replace: bool = True,
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_sync_client_access(service, user, client_id)
    path_values = [part.strip() for part in str(paths or "").split(",") if part.strip()]
    for path in path_values:
        _require_cloud_drive_path_access(
            cfg, user, path, service=service, required_level="editor", audit_action="sync_selective_set"
        )
    try:
        result = service.set_selective_sync_paths(
            client_id=client_id, paths=path_values, mode=mode, replace=bool(replace)
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg, user, "sync_selective_set", ok=False, details={"client_id": client_id, "error": str(exc)}
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg, user, "sync_selective_set", details={"client_id": client_id, "count": result.get("count")}
    )
    return result


@app.get("/api/cloud-drive/sync/conflicts")
def api_cloud_drive_sync_conflicts(
    status: str = "open", client_id: str = "", limit: int = 100, authorization: AuthHeader = ""
) -> List[Dict[str, Any]]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    if client_id:
        _require_sync_client_access(service, user, client_id)
    username = "" if str(user.get("role") or "") == "admin" else str(user.get("username") or "")
    conflicts = service.list_sync_conflicts(
        username=username,
        client_id=client_id,
        status=status,
        limit=max(1, min(int(limit or 100), 1000)),
    )
    _audit_cloud_drive_api_event(
        cfg, user, "sync_conflicts", details={"status": status, "client_id": client_id, "count": len(conflicts)}
    )
    return conflicts


@app.post("/api/cloud-drive/sync/conflicts")
def api_cloud_drive_sync_conflict_record(
    client_id: str = "",
    path: str = "",
    conflict_type: str = "",
    pair_id: str = "",
    local_path: str = "",
    cloud_path: str = "",
    local_version: str = "",
    cloud_version: str = "",
    details_json: str = "{}",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_sync_client_access(service, user, client_id)
    if cloud_path or path:
        _require_cloud_drive_path_access(
            cfg, user, cloud_path or path, service=service, required_level="editor", audit_action="sync_conflict_record"
        )
    try:
        details = json.loads(str(details_json or "{}"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="details_json должен быть JSON-объектом.")
    if not isinstance(details, dict):
        raise HTTPException(status_code=400, detail="details_json должен быть JSON-объектом.")
    try:
        conflict = service.record_sync_conflict(
            client_id=client_id,
            pair_id=pair_id,
            path=path,
            local_path=local_path,
            cloud_path=cloud_path,
            conflict_type=conflict_type,
            local_version=local_version,
            cloud_version=cloud_version,
            details=details,
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "sync_conflict_record",
            ok=False,
            details={"client_id": client_id, "path": path, "error": str(exc)},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg,
        user,
        "sync_conflict_record",
        details={"client_id": client_id, "conflict_id": conflict.get("id"), "path": path},
    )
    return conflict


@app.post("/api/cloud-drive/sync/conflicts/resolve")
def api_cloud_drive_sync_conflict_resolve(
    conflict_id: str = "",
    resolution: str = "",
    authorization: AuthHeader = "",
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    existing = service.registry.list_sync_conflicts(status="all", limit=1000)
    conflict = next((item for item in existing if item.id == str(conflict_id or "").strip()), None)
    if conflict is None:
        raise HTTPException(status_code=404, detail=f"Sync-конфликт не найден: {conflict_id}")
    _require_sync_client_access(service, user, conflict.client_id)
    try:
        result = service.resolve_sync_conflict(
            conflict_id,
            resolution=resolution,
            resolved_by=str(user.get("username") or ""),
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg, user, "sync_conflict_resolve", ok=False, details={"conflict_id": conflict_id, "error": str(exc)}
        )
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg, user, "sync_conflict_resolve", details={"conflict_id": conflict_id, "resolution": resolution}
    )
    return result


@app.post("/api/cloud-drive/folders")
def api_cloud_drive_create_folder(
    parent_path: str = "", name: str = "", authorization: AuthHeader = ""
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(
        cfg, user, parent_path, service=service, required_level="editor", audit_action="create_folder"
    )
    try:
        result = service.create_folder(parent_path=parent_path, name=name)
        _audit_cloud_drive_api_event(
            cfg, user, "create_folder", details={"parent_path": parent_path, "name": name, "path": result.get("path")}
        )
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg, user, "create_folder", ok=False, details={"parent_path": parent_path, "name": name, "error": str(exc)}
        )
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/cloud-drive/download")
def api_cloud_drive_download(path: str, authorization: AuthHeader = ""):
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    _require_cloud_drive_path_access(cfg, user, path, audit_action="download")
    service = CloudDriveService.from_config(cfg)
    try:
        descriptor = service.get_download_descriptor(path)
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "download", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=404, detail=str(exc))
    if descriptor.get("mode") != "local_file":
        if descriptor.get("mode") == "redirect_url" and descriptor.get("url"):
            _audit_cloud_drive_api_event(
                cfg,
                user,
                "download",
                details={"path": path, "filename": descriptor.get("filename"), "mode": "redirect_url"},
            )
            return RedirectResponse(str(descriptor["url"]))
        _audit_cloud_drive_api_event(
            cfg, user, "download", ok=False, details={"path": path, "mode": descriptor.get("mode")}
        )
        raise HTTPException(status_code=501, detail="Этот storage backend пока не поддерживает download.")
    _audit_cloud_drive_api_event(cfg, user, "download", details={"path": path, "filename": descriptor.get("filename")})
    return FileResponse(
        path=str(descriptor["file_path"]),
        media_type=str(descriptor["mime_type"]),
        filename=str(descriptor["filename"]),
    )


@app.get("/api/cloud-drive/preview")
def api_cloud_drive_preview(path: str, authorization: AuthHeader = ""):
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, path, service=service, audit_action="preview")
    try:
        descriptor = service.get_download_descriptor(path)
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "preview", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=404, detail=str(exc))
    if descriptor.get("mode") != "local_file":
        if descriptor.get("mode") == "redirect_url" and descriptor.get("url"):
            _audit_cloud_drive_api_event(
                cfg,
                user,
                "preview",
                details={"path": path, "filename": descriptor.get("filename"), "mode": "redirect_url"},
            )
            return RedirectResponse(str(descriptor["url"]))
        _audit_cloud_drive_api_event(
            cfg, user, "preview", ok=False, details={"path": path, "mode": descriptor.get("mode")}
        )
        raise HTTPException(status_code=501, detail="Этот storage backend пока не поддерживает preview.")
    _audit_cloud_drive_api_event(cfg, user, "preview", details={"path": path, "filename": descriptor.get("filename")})
    return FileResponse(
        path=str(descriptor["file_path"]),
        media_type=str(descriptor["mime_type"]),
        filename=str(descriptor["filename"]),
        content_disposition_type="inline",
    )


@app.post("/api/cloud-drive/upload")
async def api_cloud_drive_upload(
    parent_path: str = "", file: UploadFile = File(...), authorization: AuthHeader = ""
) -> Dict[str, Any]:
    if file is None or not str(file.filename or "").strip():
        raise HTTPException(status_code=400, detail="Не передан файл для загрузки.")
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(
        cfg, user, parent_path, service=service, required_level="editor", audit_action="upload"
    )
    suffix = Path(str(file.filename or "")).suffix
    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name
        result = service.upload_file(
            parent_path=parent_path,
            filename=str(file.filename or "").strip(),
            source_path=tmp_path,
            mime_type=str(file.content_type or "").strip(),
        )
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "upload",
            details={"parent_path": parent_path, "filename": result.get("name"), "path": result.get("path")},
        )
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "upload",
            ok=False,
            details={"parent_path": parent_path, "filename": str(file.filename or ""), "error": str(exc)},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        if tmp_path:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except OSError:
                pass


@app.get("/api/cloud-drive/versions")
def api_cloud_drive_versions(path: str, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    _require_cloud_drive_path_access(cfg, user, path, audit_action="versions")
    service = CloudDriveService.from_config(cfg)
    try:
        result = service.list_versions(path)
        _audit_cloud_drive_api_event(
            cfg, user, "versions", details={"path": path, "count": len(result.get("versions", []))}
        )
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "versions", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/api/cloud-drive/move")
def api_cloud_drive_move(
    source_path: str = "", dest_parent_path: str = "", new_name: str = "", authorization: AuthHeader = ""
) -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(
        cfg,
        user,
        source_path,
        service=service,
        required_level="editor",
        audit_action="move",
        audit_details={"path_role": "source"},
    )
    _require_cloud_drive_path_access(
        cfg,
        user,
        dest_parent_path,
        service=service,
        required_level="editor",
        audit_action="move",
        audit_details={"path_role": "destination"},
    )
    try:
        result = service.move_node(source_path=source_path, dest_parent_path=dest_parent_path, new_name=new_name)
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "move",
            details={
                "source_path": source_path,
                "dest_parent_path": dest_parent_path,
                "new_name": new_name,
                "result": result,
            },
        )
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg,
            user,
            "move",
            ok=False,
            details={
                "source_path": source_path,
                "dest_parent_path": dest_parent_path,
                "new_name": new_name,
                "error": str(exc),
            },
        )
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/cloud-drive/rename")
def api_cloud_drive_rename(path: str = "", new_name: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, path, service=service, required_level="editor", audit_action="rename")
    node = service.registry.get_node_by_path(path)
    if node is None:
        _audit_cloud_drive_api_event(
            cfg, user, "rename", ok=False, details={"path": path, "new_name": new_name, "error": "not_found"}
        )
        raise HTTPException(status_code=404, detail=f"Узел не найден: {path}")
    parent_path = node.path.rsplit("/", 1)[0] if "/" in node.path else ""
    try:
        result = service.move_node(source_path=path, dest_parent_path=parent_path, new_name=new_name)
        _audit_cloud_drive_api_event(
            cfg, user, "rename", details={"path": path, "new_name": new_name, "result": result}
        )
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(
            cfg, user, "rename", ok=False, details={"path": path, "new_name": new_name, "error": str(exc)}
        )
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/cloud-drive/delete")
def api_cloud_drive_delete(path: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, path, service=service, required_level="editor", audit_action="delete")
    try:
        result = service.delete_node(path)
        _audit_cloud_drive_api_event(cfg, user, "delete", details={"path": path, "result": result})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "delete", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/cloud-drive/trash")
def api_cloud_drive_trash(limit: int = 200, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    result = service.list_trash(limit=limit)
    items = [
        item
        for item in result.get("items", [])
        if _cloud_drive_path_allowed(cfg, user, str(item.get("path") or ""), service=service)
    ]
    payload = {"items": items, "count": len(items)}
    _audit_cloud_drive_api_event(cfg, user, "trash", details={"count": len(items)})
    return payload


@app.post("/api/cloud-drive/restore")
def api_cloud_drive_restore(path: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, path, service=service, required_level="editor", audit_action="restore")
    try:
        result = service.restore_node(path)
        _audit_cloud_drive_api_event(cfg, user, "restore", details={"path": path, "result": result})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "restore", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/cloud-drive/reindex")
def api_cloud_drive_reindex(path: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, path, service=service, required_level="editor", audit_action="reindex")
    try:
        job = service.enqueue_reindex(path)
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "reindex", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(cfg, user, "reindex", details={"path": path, "job_id": job.id})
    return _serialize_cloud_drive_job(job)


@app.post("/api/cloud-drive/job-run")
def api_cloud_drive_job_run(job_id: str, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    try:
        job = service.run_reindex_job(job_id, index_config=cfg)
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "job_run", ok=False, details={"job_id": job_id, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg, user, "job_run", details={"job_id": job_id, "status": job.status, "job_type": job.job_type}
    )
    return _serialize_cloud_drive_job(job)


@app.post("/api/cloud-drive/job-retry")
def api_cloud_drive_job_retry(job_id: str, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    try:
        job = service.retry_job(job_id)
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "job_retry", ok=False, details={"job_id": job_id, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(
        cfg, user, "job_retry", details={"job_id": job_id, "new_job_id": job.id, "job_type": job.job_type}
    )
    return _serialize_cloud_drive_job(job)
