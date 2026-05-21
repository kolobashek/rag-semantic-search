"""
api.py — FastAPI endpoint registrations for the NiceGUI app.

All routes are registered against NiceGUI's `app` (a Starlette/FastAPI instance)
at import time. This module must be imported before the server starts.

Depends on: .state, .system, .helpers, core modules.
"""

from __future__ import annotations

import json
import mimetypes
import tempfile
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
) -> None:
    if not _cloud_drive_path_allowed(cfg, user, path, service=service, required_level=required_level):
        raise HTTPException(status_code=403, detail="Нет доступа к этому пути Cloud Drive.")


def _require_sync_client_access(service: CloudDriveService, user: Dict[str, Any], client_id: str, *, admin_ok: bool = True) -> None:
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
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
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
        _audit_cloud_drive_api_event(cfg, user, "permissions_grant", ok=False, details={"subject_type": subject_type, "subject_id": subject_id, "path": path, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(cfg, user, "permissions_grant", details=permission)
    return permission


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
def api_cloud_drive_file_statuses(file_ids: str = "", paths: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    ids = [part.strip() for part in str(file_ids or "").split(",") if part.strip()]
    for path in [part.strip() for part in str(paths or "").split(",") if part.strip()]:
        _require_cloud_drive_path_access(cfg, user, path, service=service)
        file_row = service.registry.get_file_by_path(path)
        if file_row is not None:
            ids.append(file_row.id)
    ids = list(dict.fromkeys(ids))
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


@app.get("/api/cloud-drive/storage-health")
def api_cloud_drive_storage_health(authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, authorization=authorization, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    health = service.get_storage_health()
    return {
        "backend": health.backend,
        "ok": health.ok,
        "writable": health.writable,
        "target": health.target,
        "error": health.error,
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


@app.get("/api/cloud-drive/node")
def api_cloud_drive_node(path: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    _require_cloud_drive_path_access(cfg, user, path)
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
    _require_cloud_drive_path_access(cfg, user, path)
    service = CloudDriveService.from_config(cfg)
    try:
        result = service.list_directory(path)
        _audit_cloud_drive_api_event(cfg, user, "list_directory", details={"path": path})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "list_directory", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/api/cloud-drive/search")
def api_cloud_drive_search(query: str = "", path: str = "", limit: int = 50, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    _require_cloud_drive_path_access(cfg, user, path)
    clean_query = str(query or "").strip()
    if not clean_query:
        raise HTTPException(status_code=400, detail="Не задан query.")
    service = CloudDriveService.from_config(cfg)
    try:
        result = service.search_nodes(query=clean_query, path=path, limit=max(1, min(int(limit or 50), 500)))
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "search_nodes", ok=False, details={"path": path, "query": clean_query, "error": str(exc)})
        raise HTTPException(status_code=404, detail=str(exc))
    result["items"] = [
        item for item in result.get("items", [])
        if _cloud_drive_path_allowed(cfg, user, str(item.get("path") or ""), service=service)
    ]
    result["count"] = len(result["items"])
    _audit_cloud_drive_api_event(cfg, user, "search_nodes", details={"path": path, "query": clean_query, "count": result["count"]})
    return result


@app.get("/api/cloud-drive/changes")
def api_cloud_drive_changes(since: str = "", limit: int = 500, authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    service = CloudDriveService.from_config(cfg)
    result = service.list_changes(since=since, limit=max(1, min(int(limit or 500), 5000)))
    changes = [
        item for item in result.get("changes", [])
        if _cloud_drive_path_allowed(cfg, user, str(item.get("path") or ""), service=service)
    ]
    result["changes"] = changes
    result["count"] = len(changes)
    _audit_cloud_drive_api_event(cfg, user, "changes", details={"since": since, "count": len(changes)})
    return result


@app.get("/api/cloud-drive/sync/clients")
def api_cloud_drive_sync_clients(username: str = "", include_offline: bool = True, limit: int = 100, authorization: AuthHeader = "") -> List[Dict[str, Any]]:
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
    _audit_cloud_drive_api_event(cfg, user, "sync_clients", details={"username": requested_username, "count": len(clients)})
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
        _audit_cloud_drive_api_event(cfg, user, "sync_client_register", ok=False, details={"device_id": device_id, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(cfg, user, "sync_client_register", details={"client_id": client.get("id"), "device_id": device_id})
    return client


@app.get("/api/cloud-drive/sync/pairs")
def api_cloud_drive_sync_pairs(client_id: str = "", enabled_only: bool = False, authorization: AuthHeader = "") -> List[Dict[str, Any]]:
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
    raise HTTPException(status_code=404, detail="Sync-клиент не найден. Соберите установщик командой packaging/build.ps1.")


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
    _require_cloud_drive_path_access(cfg, user, cloud_path, service=service, required_level="editor")
    try:
        pair = service.upsert_sync_pair(
            client_id=client_id,
            local_path=local_path,
            cloud_path=cloud_path,
            conflict_policy=conflict_policy,
            enabled=bool(enabled),
        )
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "sync_pair_upsert", ok=False, details={"client_id": client_id, "cloud_path": cloud_path, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(cfg, user, "sync_pair_upsert", details={"client_id": client_id, "pair_id": pair.get("id"), "cloud_path": cloud_path})
    return pair


@app.post("/api/cloud-drive/sync/pairs/delete")
def api_cloud_drive_sync_pair_delete(pair_id: str = "", client_id: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_sync_client_access(service, user, client_id)
    result = service.delete_sync_pair(pair_id, client_id=client_id)
    _audit_cloud_drive_api_event(cfg, user, "sync_pair_delete", details={"pair_id": pair_id, "client_id": client_id, "ok": result.get("ok")})
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
    _audit_cloud_drive_api_event(cfg, user, "sync_selective", details={"client_id": client_id, "count": result.get("count")})
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
        _require_cloud_drive_path_access(cfg, user, path, service=service, required_level="editor")
    try:
        result = service.set_selective_sync_paths(client_id=client_id, paths=path_values, mode=mode, replace=bool(replace))
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "sync_selective_set", ok=False, details={"client_id": client_id, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(cfg, user, "sync_selective_set", details={"client_id": client_id, "count": result.get("count")})
    return result


@app.get("/api/cloud-drive/sync/conflicts")
def api_cloud_drive_sync_conflicts(status: str = "open", client_id: str = "", limit: int = 100, authorization: AuthHeader = "") -> List[Dict[str, Any]]:
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
    _audit_cloud_drive_api_event(cfg, user, "sync_conflicts", details={"status": status, "client_id": client_id, "count": len(conflicts)})
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
        _require_cloud_drive_path_access(cfg, user, cloud_path or path, service=service, required_level="editor")
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
        _audit_cloud_drive_api_event(cfg, user, "sync_conflict_record", ok=False, details={"client_id": client_id, "path": path, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(cfg, user, "sync_conflict_record", details={"client_id": client_id, "conflict_id": conflict.get("id"), "path": path})
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
        _audit_cloud_drive_api_event(cfg, user, "sync_conflict_resolve", ok=False, details={"conflict_id": conflict_id, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    _audit_cloud_drive_api_event(cfg, user, "sync_conflict_resolve", details={"conflict_id": conflict_id, "resolution": resolution})
    return result


@app.post("/api/cloud-drive/folders")
def api_cloud_drive_create_folder(parent_path: str = "", name: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, parent_path, service=service, required_level="editor")
    try:
        result = service.create_folder(parent_path=parent_path, name=name)
        _audit_cloud_drive_api_event(cfg, user, "create_folder", details={"parent_path": parent_path, "name": name, "path": result.get("path")})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "create_folder", ok=False, details={"parent_path": parent_path, "name": name, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/cloud-drive/download")
def api_cloud_drive_download(path: str, authorization: AuthHeader = ""):
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization)
    _require_cloud_drive_path_access(cfg, user, path)
    service = CloudDriveService.from_config(cfg)
    try:
        descriptor = service.get_download_descriptor(path)
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "download", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=404, detail=str(exc))
    if descriptor.get("mode") != "local_file":
        if descriptor.get("mode") == "redirect_url" and descriptor.get("url"):
            _audit_cloud_drive_api_event(cfg, user, "download", details={"path": path, "filename": descriptor.get("filename"), "mode": "redirect_url"})
            return RedirectResponse(str(descriptor["url"]))
        _audit_cloud_drive_api_event(cfg, user, "download", ok=False, details={"path": path, "mode": descriptor.get("mode")})
        raise HTTPException(status_code=501, detail="Этот storage backend пока не поддерживает download.")
    _audit_cloud_drive_api_event(cfg, user, "download", details={"path": path, "filename": descriptor.get("filename")})
    return FileResponse(
        path=str(descriptor["file_path"]),
        media_type=str(descriptor["mime_type"]),
        filename=str(descriptor["filename"]),
    )


@app.post("/api/cloud-drive/upload")
async def api_cloud_drive_upload(parent_path: str = "", file: UploadFile = File(...), authorization: AuthHeader = "") -> Dict[str, Any]:
    if file is None or not str(file.filename or "").strip():
        raise HTTPException(status_code=400, detail="Не передан файл для загрузки.")
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, parent_path, service=service, required_level="editor")
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
        _audit_cloud_drive_api_event(cfg, user, "upload", details={"parent_path": parent_path, "filename": result.get("name"), "path": result.get("path")})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "upload", ok=False, details={"parent_path": parent_path, "filename": str(file.filename or ""), "error": str(exc)})
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
    _require_cloud_drive_path_access(cfg, user, path)
    service = CloudDriveService.from_config(cfg)
    try:
        result = service.list_versions(path)
        _audit_cloud_drive_api_event(cfg, user, "versions", details={"path": path, "count": len(result.get("versions", []))})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "versions", ok=False, details={"path": path, "error": str(exc)})
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/api/cloud-drive/move")
def api_cloud_drive_move(source_path: str = "", dest_parent_path: str = "", new_name: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, source_path, service=service, required_level="editor")
    _require_cloud_drive_path_access(cfg, user, dest_parent_path, service=service, required_level="editor")
    try:
        result = service.move_node(source_path=source_path, dest_parent_path=dest_parent_path, new_name=new_name)
        _audit_cloud_drive_api_event(cfg, user, "move", details={"source_path": source_path, "dest_parent_path": dest_parent_path, "new_name": new_name, "result": result})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "move", ok=False, details={"source_path": source_path, "dest_parent_path": dest_parent_path, "new_name": new_name, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/cloud-drive/rename")
def api_cloud_drive_rename(path: str = "", new_name: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, path, service=service, required_level="editor")
    node = service.registry.get_node_by_path(path)
    if node is None:
        _audit_cloud_drive_api_event(cfg, user, "rename", ok=False, details={"path": path, "new_name": new_name, "error": "not_found"})
        raise HTTPException(status_code=404, detail=f"Узел не найден: {path}")
    parent_path = node.path.rsplit("/", 1)[0] if "/" in node.path else ""
    try:
        result = service.move_node(source_path=path, dest_parent_path=parent_path, new_name=new_name)
        _audit_cloud_drive_api_event(cfg, user, "rename", details={"path": path, "new_name": new_name, "result": result})
        return result
    except RuntimeError as exc:
        _audit_cloud_drive_api_event(cfg, user, "rename", ok=False, details={"path": path, "new_name": new_name, "error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/cloud-drive/delete")
def api_cloud_drive_delete(path: str = "", authorization: AuthHeader = "") -> Dict[str, Any]:
    cfg = load_config()
    user = _require_cloud_drive_api_user(cfg, authorization=authorization, write=True)
    service = CloudDriveService.from_config(cfg)
    _require_cloud_drive_path_access(cfg, user, path, service=service, required_level="editor")
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
    _require_cloud_drive_path_access(cfg, user, path, service=service, required_level="editor")
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
    _require_cloud_drive_path_access(cfg, user, path, service=service, required_level="editor")
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
    _audit_cloud_drive_api_event(cfg, user, "job_run", details={"job_id": job_id, "status": job.status, "job_type": job.job_type})
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
    _audit_cloud_drive_api_event(cfg, user, "job_retry", details={"job_id": job_id, "new_job_id": job.id, "job_type": job.job_type})
    return _serialize_cloud_drive_job(job)
