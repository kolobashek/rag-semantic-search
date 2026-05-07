"""
api.py — FastAPI endpoint registrations for the NiceGUI app.

All routes are registered against NiceGUI's `app` (a Starlette/FastAPI instance)
at import time. This module must be imported before the server starts.

Depends on: .state, .system, .helpers, core modules.
"""

from __future__ import annotations

import mimetypes
import tempfile
from pathlib import Path
from typing import Any, Dict, List

from fastapi import File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from nicegui import app

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.rag_core import load_config
from rag_catalog.core.user_auth_db import UserAuthDB

from .helpers import _resolve_catalog_file
from .state import _users_db_path
from .system import _read_cloud_bootstrap_status, _safe_int

# ─────────────────────────── auth helpers (API-only) ───────────────────────

def _get_api_auth_db(cfg: Dict[str, Any]) -> UserAuthDB:
    return UserAuthDB(str(_users_db_path(cfg)))


def _require_cloud_drive_api_user(
    cfg: Dict[str, Any],
    *,
    auth_token: str = "",
    write: bool = False,
    admin_only: bool = False,
) -> Dict[str, Any]:
    token = str(auth_token or "").strip()
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
    }


# ─────────────────────────── API routes ─────────────────────────────────────

@app.get("/api/view-file")
def api_view_file(path: str) -> FileResponse:
    resolved = _resolve_catalog_file(load_config(), path)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Файл не найден или недоступен")
    media_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
    return FileResponse(str(resolved), media_type=media_type, filename=resolved.name)


@app.get("/api/cloud-drive/bootstrap-status")
def api_cloud_drive_bootstrap_status(auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, admin_only=True)
    return _read_cloud_bootstrap_status(cfg)


@app.get("/api/cloud-drive/bootstrap-jobs")
def api_cloud_drive_bootstrap_jobs(limit: int = 20, auth_token: str = "") -> List[Dict[str, Any]]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    jobs = service.list_bootstrap_jobs(limit=max(1, min(int(limit), 100)))
    return [_serialize_cloud_drive_job(job) for job in jobs]


@app.get("/api/cloud-drive/jobs")
def api_cloud_drive_jobs(job_type: str = "", limit: int = 20, auth_token: str = "") -> List[Dict[str, Any]]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    jobs = service.list_jobs(job_type=str(job_type or "").strip(), limit=max(1, min(int(limit), 100)))
    return [_serialize_cloud_drive_job(job) for job in jobs]


@app.get("/api/cloud-drive/job")
def api_cloud_drive_job(job_id: str, auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job не найден: {job_id}")
    return _serialize_cloud_drive_job(job)


@app.get("/api/cloud-drive/job-latest")
def api_cloud_drive_job_latest(job_type: str, auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, admin_only=True)
    clean_type = str(job_type or "").strip()
    if not clean_type:
        raise HTTPException(status_code=400, detail="Не задан job_type.")
    service = CloudDriveService.from_config(cfg)
    job = service.get_latest_job(job_type=clean_type)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Jobs типа не найдены: {clean_type}")
    return _serialize_cloud_drive_job(job)


@app.get("/api/cloud-drive/storage-health")
def api_cloud_drive_storage_health(auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, admin_only=True)
    service = CloudDriveService.from_config(cfg)
    health = service.get_storage_health()
    return {
        "backend": health.backend,
        "ok": health.ok,
        "writable": health.writable,
        "target": health.target,
        "error": health.error,
    }


@app.get("/api/cloud-drive/node")
def api_cloud_drive_node(path: str = "", auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token)
    service = CloudDriveService.from_config(cfg)
    try:
        return service.get_node(path)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/api/cloud-drive/list")
def api_cloud_drive_list(path: str = "", auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token)
    service = CloudDriveService.from_config(cfg)
    try:
        return service.list_directory(path)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/api/cloud-drive/folders")
def api_cloud_drive_create_folder(parent_path: str = "", name: str = "", auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, write=True)
    service = CloudDriveService.from_config(cfg)
    try:
        return service.create_folder(parent_path=parent_path, name=name)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/cloud-drive/download")
def api_cloud_drive_download(path: str, auth_token: str = "") -> FileResponse:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token)
    service = CloudDriveService.from_config(cfg)
    try:
        descriptor = service.get_download_descriptor(path)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    if descriptor.get("mode") != "local_file":
        raise HTTPException(status_code=501, detail="Этот storage backend пока не поддерживает direct download.")
    return FileResponse(
        path=str(descriptor["file_path"]),
        media_type=str(descriptor["mime_type"]),
        filename=str(descriptor["filename"]),
    )


@app.post("/api/cloud-drive/upload")
async def api_cloud_drive_upload(parent_path: str = "", file: UploadFile = File(...), auth_token: str = "") -> Dict[str, Any]:
    if file is None or not str(file.filename or "").strip():
        raise HTTPException(status_code=400, detail="Не передан файл для загрузки.")
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, write=True)
    service = CloudDriveService.from_config(cfg)
    suffix = Path(str(file.filename or "")).suffix
    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name
        return service.upload_file(
            parent_path=parent_path,
            filename=str(file.filename or "").strip(),
            source_path=tmp_path,
            mime_type=str(file.content_type or "").strip(),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        if tmp_path:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except OSError:
                pass


@app.get("/api/cloud-drive/versions")
def api_cloud_drive_versions(path: str, auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token)
    service = CloudDriveService.from_config(cfg)
    try:
        return service.list_versions(path)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/api/cloud-drive/move")
def api_cloud_drive_move(source_path: str = "", dest_parent_path: str = "", new_name: str = "", auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, write=True)
    service = CloudDriveService.from_config(cfg)
    try:
        return service.move_node(source_path=source_path, dest_parent_path=dest_parent_path, new_name=new_name)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/cloud-drive/rename")
def api_cloud_drive_rename(path: str = "", new_name: str = "", auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, write=True)
    service = CloudDriveService.from_config(cfg)
    node = service.registry.get_node_by_path(path)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Узел не найден: {path}")
    parent_path = node.path.rsplit("/", 1)[0] if "/" in node.path else ""
    try:
        return service.move_node(source_path=path, dest_parent_path=parent_path, new_name=new_name)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/cloud-drive/delete")
def api_cloud_drive_delete(path: str = "", auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, write=True)
    service = CloudDriveService.from_config(cfg)
    try:
        return service.delete_node(path)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/cloud-drive/reindex")
def api_cloud_drive_reindex(path: str = "", auth_token: str = "") -> Dict[str, Any]:
    cfg = load_config()
    _require_cloud_drive_api_user(cfg, auth_token=auth_token, write=True)
    service = CloudDriveService.from_config(cfg)
    try:
        job = service.enqueue_reindex(path)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _serialize_cloud_drive_job(job)
