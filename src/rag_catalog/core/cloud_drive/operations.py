from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import urllib.error
import urllib.request
import zipfile
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import psutil

from .storage import resolve_storage_adapter

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _provider_backup_freshness(backup_dir: Path, max_age_hours: float) -> Dict[str, Any]:
    candidates = sorted(
        [path.parent for path in backup_dir.glob("*/manifest.json") if path.is_file()],
        key=lambda path: (path / "manifest.json").stat().st_mtime,
        reverse=True,
    ) if backup_dir.exists() else []
    provider_snapshots: list[tuple[Path, Dict[str, Any]]] = []
    for candidate in candidates:
        try:
            manifest = json.loads((candidate / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if manifest.get("kind") == "rag-catalog-s3-provider-backup":
            provider_snapshots.append((candidate, manifest))
    if not provider_snapshots:
        return {
            "status": "missing",
            "ok": False,
            "backup_dir": str(backup_dir.resolve()),
            "max_age_hours": max_age_hours,
            "provider": "s3",
        }
    latest, manifest = provider_snapshots[0]
    created_at = str(manifest.get("created_at") or "")
    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        now = datetime.now(created.tzinfo) if created.tzinfo else datetime.now()
        age_hours = max(0.0, (now - created).total_seconds() / 3600.0)
    except ValueError:
        age_hours = max_age_hours + 1.0
    objects = list(manifest.get("objects") or [])
    state_files = list(manifest.get("state_files") or [])
    complete = (
        int(manifest.get("version") or 0) >= 1
        and int(manifest.get("object_count") or 0) == len(objects)
        and any(str(entry.get("name") or "") == "cloud_drive_db" for entry in state_files)
    )
    manifest_hash = hashlib.sha256((latest / "manifest.json").read_bytes()).hexdigest()
    drill_ok = False
    drill_completed_at = ""
    try:
        artifact = json.loads((latest / "restore-drill.json").read_text(encoding="utf-8"))
        drill_ok = bool(artifact.get("ok")) and str(artifact.get("manifest_sha256") or "") == manifest_hash
        drill_completed_at = str(artifact.get("completed_at") or "") if drill_ok else ""
    except (OSError, ValueError):
        pass
    fresh = age_hours <= max_age_hours
    ok = complete and fresh and drill_ok
    status = "healthy" if ok else ("invalid" if not complete else ("stale" if not fresh else "unverified"))
    return {
        "status": status,
        "ok": ok,
        "backup_dir": str(backup_dir.resolve()),
        "latest_path": str(latest.resolve()),
        "created_at": created_at,
        "age_hours": round(age_hours, 2),
        "max_age_hours": max_age_hours,
        "manifest_ok": True,
        "complete": complete,
        "restore_drill_ok": drill_ok,
        "restore_drill_completed_at": drill_completed_at,
        "provider": "s3",
        "object_count": len(objects),
        "total_object_bytes": int(manifest.get("total_object_bytes") or 0),
    }


def cloud_drive_backup_freshness(cfg: Dict[str, Any]) -> Dict[str, Any]:
    backup_dir = Path(str(cfg.get("cloud_drive_backup_dir") or PROJECT_ROOT / "runtime" / "backups")).expanduser()
    max_age_hours = max(1.0, float(cfg.get("cloud_drive_backup_max_age_hours") or 24.0))
    if str(cfg.get("cloud_drive_storage") or "local").strip().lower() == "s3":
        return _provider_backup_freshness(backup_dir, max_age_hours)
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
    last_error: sqlite3.Error | None = None
    for attempt in range(3):
        try:
            # A normal query-only connection can attach to an active WAL on Windows;
            # mode=ro intermittently fails when SQLite needs to refresh the SHM mapping.
            with closing(sqlite3.connect(str(path.resolve()), timeout=2)) as conn:
                conn.execute("PRAGMA query_only=ON")
                conn.execute("PRAGMA busy_timeout=2000")
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
            last_error = exc
            if attempt < 2:
                time.sleep(0.1 * (attempt + 1))
    return {
        "ok": False,
        "status": "error",
        "path": str(path.resolve()),
        "error": str(last_error or "unknown SQLite error"),
    }


def _module_processes(module: str) -> list[int]:
    pids: list[int] = []
    try:
        for process in psutil.process_iter(["pid", "cmdline"]):
            try:
                command = [str(part) for part in (process.info.get("cmdline") or [])]
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            if any(part == module for part in command):
                pids.append(int(process.info.get("pid") or 0))
    except (psutil.Error, OSError):
        return []
    return [pid for pid in pids if pid > 0]


def _queue_health(registry_path: Path, cfg: Dict[str, Any]) -> Dict[str, Any]:
    threshold = max(30, int(cfg.get("cloud_drive_queue_lag_warn_sec") or 300))
    lag_seconds = 0
    pending_count = 0
    oldest_created_at = ""
    try:
        uri = f"file:{registry_path.resolve().as_posix()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=2)) as conn:
            row = conn.execute(
                "SELECT COUNT(*), COALESCE(MIN(created_at), '') FROM cloud_jobs WHERE status='pending'"
            ).fetchone()
        pending_count = int(row[0] or 0) if row else 0
        oldest_created_at = str(row[1] or "") if row else ""
    except sqlite3.Error as exc:
        return {
            "ok": False,
            "status": "error",
            "pending": 0,
            "oldest_pending_age_sec": 0,
            "warn_after_sec": threshold,
            "error": str(exc),
        }
    if oldest_created_at:
        try:
            created_at = datetime.fromisoformat(oldest_created_at.replace("Z", "+00:00"))
            now = datetime.now(created_at.tzinfo) if created_at.tzinfo else datetime.now()
            lag_seconds = max(0, int((now - created_at).total_seconds()))
        except (TypeError, ValueError):
            lag_seconds = threshold
    ok = lag_seconds < threshold
    return {
        "ok": ok,
        "status": "lagging" if not ok else ("pending" if pending_count else "idle"),
        "pending": pending_count,
        "oldest_pending_age_sec": lag_seconds,
        "warn_after_sec": threshold,
    }


def cloud_drive_operations_health(cfg: Dict[str, Any]) -> Dict[str, Any]:
    qdrant_base = Path(str(cfg.get("qdrant_db_path") or "."))
    registry_path = Path(str(cfg.get("cloud_drive_db_path") or qdrant_base / "cloud_drive.db"))
    storage = dict(resolve_storage_adapter(cfg).healthcheck())
    registry = _sqlite_readiness(registry_path)
    telemetry_path = Path(str(cfg.get("telemetry_db_path") or qdrant_base / "rag_telemetry.db"))
    telemetry = _sqlite_readiness(telemetry_path)
    qdrant = _qdrant_readiness(cfg)
    backup = cloud_drive_backup_freshness(cfg)
    jobs = _queue_health(registry_path, cfg)
    workers = {
        "indexer": {"status": "running" if _module_processes("rag_catalog.core.index_rag") else "idle"},
        "ocr": {"status": "running" if _module_processes("rag_catalog.core.ocr_pdfs") else "idle"},
    }
    bot_required = bool(cfg.get("telegram_enabled") and str(cfg.get("telegram_bot_token") or "").strip())
    bot_running = bool(_module_processes("rag_catalog.integrations.telegram_bot"))
    workers["telegram_bot"] = {
        "status": "running" if bot_running else ("stopped" if bot_required else "disabled"),
        "ok": bot_running or not bot_required,
    }
    workers["ok"] = bool(workers["telegram_bot"]["ok"])
    index = _index_state_snapshot(qdrant_base / "index_state.db")
    storage_result = {
        "ok": bool(storage.get("ok") and storage.get("writable")),
        "status": "ready" if storage.get("ok") and storage.get("writable") else "error",
        "backend": str(storage.get("backend") or ""),
        "target": str(storage.get("target") or ""),
        "error": str(storage.get("error") or ""),
    }
    components = {
        "registry": registry,
        "telemetry": telemetry,
        "storage": storage_result,
        "qdrant": qdrant,
        "index": index,
        "backup": backup,
        "jobs": jobs,
        "workers": workers,
    }
    service_ok = all(
        bool(components[name].get("ok"))
        for name in ("registry", "telemetry", "storage", "qdrant", "jobs", "workers")
    )
    pilot_ready = service_ok and bool(components["index"].get("ok")) and bool(backup.get("ok"))
    return {
        "ok": service_ok,
        "pilot_ready": pilot_ready,
        "status": "ready" if pilot_ready else ("degraded" if service_ok else "error"),
        "components": components,
    }
