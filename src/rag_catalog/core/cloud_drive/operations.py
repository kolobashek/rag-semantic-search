from __future__ import annotations

import json
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

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
