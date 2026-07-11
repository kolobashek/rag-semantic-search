from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.request
import zipfile
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .service import CloudDriveService

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def cloud_drive_backup_freshness(cfg: Dict[str, Any]) -> Dict[str, Any]:
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


def _sqlite_readiness(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"ok": False, "status": "missing", "path": str(path)}
    try:
        uri = f"file:{path.resolve().as_posix()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=2)) as conn:
            conn.execute("SELECT 1").fetchone()
        return {"ok": True, "status": "ready", "path": str(path.resolve())}
    except sqlite3.Error as exc:
        return {"ok": False, "status": "error", "path": str(path.resolve()), "error": str(exc)}


def _qdrant_readiness(cfg: Dict[str, Any]) -> Dict[str, Any]:
    url = str(cfg.get("qdrant_url") or "").strip().rstrip("/")
    if not url:
        return {"ok": False, "status": "not_configured", "url": ""}
    try:
        with urllib.request.urlopen(f"{url}/collections", timeout=1.0) as response:
            status_code = int(response.status)
        return {"ok": 200 <= status_code < 500, "status": "ready", "url": url, "status_code": status_code}
    except (OSError, urllib.error.URLError, urllib.error.HTTPError) as exc:
        return {"ok": False, "status": "error", "url": url, "error": str(exc)}


def _index_state_snapshot(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"ok": False, "status": "missing", "path": str(path)}
    try:
        uri = f"file:{path.resolve().as_posix()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=2)) as conn:
            entries = int(conn.execute("SELECT COUNT(*) FROM state_entries").fetchone()[0])
            failed = int(conn.execute("SELECT COUNT(*) FROM failed_paths").fetchone()[0])
            queue_rows = conn.execute("SELECT status, COUNT(*) FROM index_queue GROUP BY status").fetchall()
        queue = {str(status): int(count) for status, count in queue_rows}
        return {
            "ok": True,
            "status": "ready",
            "path": str(path.resolve()),
            "entries": entries,
            "failed_paths": failed,
            "queue": queue,
            "deep_coverage_url": "/api/cloud-drive/index-coverage",
        }
    except sqlite3.Error as exc:
        return {"ok": False, "status": "error", "path": str(path.resolve()), "error": str(exc)}


def cloud_drive_operations_health(cfg: Dict[str, Any]) -> Dict[str, Any]:
    service = CloudDriveService.from_config(cfg)
    storage = service.get_storage_health()
    registry_path = Path(str(cfg.get("cloud_drive_db_path") or service.registry.db_path))
    qdrant_base = Path(str(cfg.get("qdrant_db_path") or "."))
    registry = _sqlite_readiness(registry_path)
    telemetry_path = Path(str(cfg.get("telemetry_db_path") or qdrant_base / "rag_telemetry.db"))
    telemetry = _sqlite_readiness(telemetry_path)
    qdrant = _qdrant_readiness(cfg)
    backup = cloud_drive_backup_freshness(cfg)
    stats = service.registry.stats()
    index = _index_state_snapshot(qdrant_base / "index_state.db")
    storage_result = {
        "ok": bool(storage.ok and storage.writable),
        "status": "ready" if storage.ok and storage.writable else "error",
        "backend": storage.backend,
        "target": storage.target,
        "error": storage.error,
    }
    components = {
        "registry": registry,
        "telemetry": telemetry,
        "storage": storage_result,
        "qdrant": qdrant,
        "index": index,
        "backup": backup,
        "jobs": {
            "ok": True,
            "status": "pending" if int(stats.pending_jobs) else "idle",
            "pending": int(stats.pending_jobs),
        },
    }
    service_ok = all(bool(components[name].get("ok")) for name in ("registry", "telemetry", "storage", "qdrant"))
    pilot_ready = service_ok and bool(components["index"].get("ok")) and bool(backup.get("ok"))
    return {
        "ok": service_ok,
        "pilot_ready": pilot_ready,
        "status": "ready" if pilot_ready else ("degraded" if service_ok else "error"),
        "components": components,
    }
