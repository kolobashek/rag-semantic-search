"""Process management, scheduler, and recovery helpers for the NiceGUI app."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.log_history import open_run_log
from rag_catalog.core.telemetry_db import TelemetryDB

PROJECT_ROOT = Path(__file__).resolve().parents[3]

_STAGE_LABELS: Dict[str, str] = {
    "all": "Все этапы",
    "metadata": "metadata",
    "small": "small chunks",
    "large": "large chunks",
    "ocr": "OCR",
}

# ── Module-level globals ───────────────────────────────────────────────────
_RECOVERY_LOCK = threading.Lock()
_RECOVERY_WATCHDOG_STARTED = False
_RECOVERY_WATCHDOG_INTERVAL_SEC = 45
_FAILED_RUN_RECENCY_SEC = 10 * 60
_FAILED_RESTART_COOLDOWN_SEC = 90
_FAILED_RESTART_MAX_ATTEMPTS = 3
_FAILED_RESTART_WINDOW_SEC = 15 * 60
_FAILED_RESTART_HISTORY: Dict[str, List[float]] = {"index": [], "ocr": []}
_FAILED_RESTART_RESTARTED_IDS: Dict[str, set[str]] = {"index": set(), "ocr": set()}
_RUNTIME_DIR = PROJECT_ROOT / "runtime"
_GLOBAL_SCHEDULER_STARTED = False
_CLOUD_BOOTSTRAP_LOCK = threading.Lock()
_CLOUD_JOB_WORKER_STARTED = False
_CLOUD_JOB_WORKER_INTERVAL_SEC = 15


# ── Primitive helpers (no nicegui/state deps) ──────────────────────────────

def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _telemetry_db_path(cfg: Dict[str, Any]) -> Path:
    explicit = str(cfg.get("telemetry_db_path") or "").strip()
    if explicit:
        return Path(explicit)
    return Path(str(cfg.get("qdrant_db_path") or "")) / "rag_telemetry.db"


# ── Process utilities ──────────────────────────────────────────────────────

def _open_log(log_path: "Path", label: str) -> "Any":
    """Открыть новый run-сегмент лога и записать заголовок с временем."""
    return open_run_log(log_path, label)


def _windows_detached_creationflags() -> int:
    flags = 0
    for name in ("CREATE_NO_WINDOW", "DETACHED_PROCESS", "CREATE_NEW_PROCESS_GROUP", "CREATE_BREAKAWAY_FROM_JOB"):
        flags |= int(getattr(subprocess, name, 0) or 0)
    return flags


def _is_process_alive(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except PermissionError:
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return False
    return True


def _find_module_process_pids(module_name: str) -> List[int]:
    pids: List[int] = []
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        joined = " ".join(str(part) for part in cmdline)
        if module_name not in joined:
            continue
        pid = int(proc.info.get("pid") or 0)
        if pid > 0:
            pids.append(pid)
    return pids


# ── Runtime markers ────────────────────────────────────────────────────────

def _runtime_marker_path(kind: str) -> Path:
    _RUNTIME_DIR.mkdir(exist_ok=True)
    return _RUNTIME_DIR / f"{kind}_active.json"


def _read_cloud_bootstrap_status(cfg: Dict[str, Any]) -> Dict[str, Any]:
    try:
        service = CloudDriveService.from_config(cfg)
        job = service.get_latest_bootstrap_job()
        if job is not None:
            progress = dict(job.progress or {})
            progress.setdefault("status", job.status)
            progress["job_id"] = job.id
            progress["job_status"] = job.status
            progress["last_error"] = job.last_error
            progress["created_at"] = job.created_at
            progress["updated_at"] = job.updated_at
            return progress
        return {"status": "idle", "job_status": "idle"}
    except Exception as exc:
        return {"status": "unavailable", "job_status": "unavailable", "error": str(exc)}


def _recover_cloud_drive_jobs(cfg: Dict[str, Any]) -> None:
    try:
        service = CloudDriveService.from_config(cfg)
    except Exception:
        return
    try:
        service.recover_bootstrap_jobs()
    except Exception as exc:
        print(f"[nice_app] cloud drive recovery skipped: {exc}", file=sys.stderr)


def _start_cloud_drive_job_worker(cfg: Dict[str, Any]) -> None:
    global _CLOUD_JOB_WORKER_STARTED
    if _CLOUD_JOB_WORKER_STARTED:
        return
    if not bool(cfg.get("cloud_drive_enabled")):
        return
    _CLOUD_JOB_WORKER_STARTED = True

    def _loop() -> None:
        from rag_catalog.core.rag_core import load_config  # local import avoids a startup cycle

        while True:
            try:
                cfg_now = load_config()
                if bool(cfg_now.get("cloud_drive_enabled")):
                    service = CloudDriveService.from_config(cfg_now)
                    service.run_pending_reindex_jobs(index_config=cfg_now, limit=3)
            except Exception as exc:
                print(f"[nice_app] cloud drive job worker skipped cycle: {exc}", file=sys.stderr)
            time.sleep(_CLOUD_JOB_WORKER_INTERVAL_SEC)

    threading.Thread(target=_loop, name="cloud-drive-job-worker", daemon=True).start()


def _read_runtime_marker(kind: str) -> Optional[Dict[str, Any]]:
    path = _runtime_marker_path(kind)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    pid = _safe_int(data.get("pid"), 0)
    if not _is_process_alive(pid):
        try:
            path.unlink()
        except OSError:
            pass
        return None
    data["pid"] = pid
    return data


def _write_runtime_marker(kind: str, *, pid: int, stage: str = "", source: str = "nice_app") -> None:
    path = _runtime_marker_path(kind)
    payload = {
        "pid": int(pid),
        "stage": str(stage or ""),
        "source": source,
        "ts_started": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _clear_runtime_marker(kind: str, *, pid: int = 0) -> None:
    path = _runtime_marker_path(kind)
    if not path.exists():
        return
    if pid > 0:
        marker = _read_runtime_marker(kind)
        if marker and _safe_int(marker.get("pid"), 0) != int(pid):
            return
    try:
        path.unlink()
    except OSError:
        pass


def _find_live_running_index_run(telemetry: TelemetryDB) -> Optional[Dict[str, Any]]:
    marker = _read_runtime_marker("index")
    if marker:
        return {
            "run_id": "",
            "status": "running",
            "worker_pid": _safe_int(marker.get("pid"), 0),
            "note": f"stage={marker.get('stage') or 'all'} | runtime_marker",
            "_runtime_marker_only": True,
        }
    rows = telemetry.fetch_dicts(
        "SELECT * FROM index_runs WHERE status='running' ORDER BY ts_started DESC LIMIT 20"
    )
    for row in rows:
        pid = _safe_int(row.get("worker_pid"), 0)
        if _is_process_alive(pid):
            return row
    process_pids = _find_module_process_pids("rag_catalog.core.index_rag")
    if process_pids:
        return {
            "run_id": "",
            "status": "running",
            "worker_pid": process_pids[0],
            "note": "process_scan",
            "_process_scan_only": True,
        }
    return None


def _find_live_running_ocr_run(telemetry: TelemetryDB) -> Optional[Dict[str, Any]]:
    marker = _read_runtime_marker("ocr")
    if marker:
        return {
            "ocr_run_id": "",
            "status": "running",
            "worker_pid": _safe_int(marker.get("pid"), 0),
            "note": "runtime_marker",
            "_runtime_marker_only": True,
        }
    rows = telemetry.fetch_dicts(
        "SELECT * FROM ocr_runs WHERE status='running' ORDER BY ts_started DESC LIMIT 20"
    )
    for row in rows:
        pid = _safe_int(row.get("worker_pid"), 0)
        if _is_process_alive(pid):
            return row
    process_pids = _find_module_process_pids("rag_catalog.core.ocr_pdfs")
    if process_pids:
        return {
            "ocr_run_id": "",
            "status": "running",
            "worker_pid": process_pids[0],
            "note": "process_scan",
            "_process_scan_only": True,
        }
    return None


# ── Indexer/OCR launchers ──────────────────────────────────────────────────

def _effective_workers(configured: Any, *, stage: str = "all", mode: str = "index") -> int:
    """Рассчитать workers: фиксированное (>0) или auto (0/None)."""
    requested = _safe_int(configured, 0)
    if requested > 0:
        return max(1, min(32, requested))
    cpu = max(1, int(os.cpu_count() or 1))
    stage_key = str(stage or "all").strip().lower()
    if mode == "ocr":
        return max(1, min(4, max(1, cpu // 2)))
    if stage_key == "metadata":
        return max(2, min(16, cpu))
    if stage_key == "small":
        return max(2, min(8, cpu))
    if stage_key == "large":
        return max(1, min(6, max(2, cpu // 2)))
    return max(2, min(8, cpu))


def _launch_indexer(
    cfg: Dict[str, Any],
    *,
    stage: str = "all",
    recreate: bool = False,
    workers: Optional[int] = None,
    max_chunks: Optional[int] = None,
    skip_inline_ocr: bool = False,
) -> int:
    """Запустить index_rag как фоновый процесс. Возвращает PID."""
    telemetry = TelemetryDB(str(_telemetry_db_path(cfg)))
    active_run = _find_live_running_index_run(telemetry)
    if active_run:
        active_pid = _safe_int(active_run.get("worker_pid"), 0)
        raise RuntimeError(
            f"Индексация уже запущена (PID {active_pid}). Дождитесь завершения текущего процесса."
        )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    env["PYTHONIOENCODING"] = "utf-8"
    env["RAG_LOG_HISTORY_NAME"] = "indexer.log"
    env["RAG_LOG_LABEL"] = f"INDEXER stage={stage}"
    args = [
        sys.executable, "-m", "rag_catalog.core.index_rag",
        "--catalog", str(cfg.get("catalog_path") or ""),
        "--collection", str(cfg.get("collection_name") or ""),
        "--stage", stage,
        "--workers",
        str(
            _effective_workers(
                workers if workers is not None else cfg.get("index_read_workers"),
                stage=stage,
                mode="index",
            )
        ),
        "--max-chunks", str(int(max_chunks or cfg.get("index_max_chunks") or 2000)),
    ]
    qdrant_url = str(cfg.get("qdrant_url") or "")
    if qdrant_url:
        args += ["--url", qdrant_url]
    else:
        args += ["--db", str(cfg.get("qdrant_db_path") or "")]
    if recreate:
        args.append("--recreate")
    if skip_inline_ocr:
        args.append("--no-ocr")
    log_fh = _open_log(PROJECT_ROOT / "logs" / "indexer.log", f"INDEXER  stage={stage}")
    try:
        proc = subprocess.Popen(
            args,
            cwd=str(PROJECT_ROOT),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=log_fh,
            creationflags=_windows_detached_creationflags(),
        )
        _write_runtime_marker("index", pid=proc.pid, stage=stage)
    finally:
        log_fh.close()
    return proc.pid


def _launch_ocr(cfg: Dict[str, Any], *, min_text_len: int = 50, workers: Optional[int] = None) -> int:
    """Запустить ocr_pdfs как фоновый процесс. Возвращает PID."""
    telemetry = TelemetryDB(str(_telemetry_db_path(cfg)))
    live_index = _find_live_running_index_run(telemetry)
    if live_index:
        active_pid = _safe_int(live_index.get("worker_pid"), 0)
        raise RuntimeError(
            f"Индексация уже запущена (PID {active_pid}). "
            "Сначала дождитесь завершения текущей индексации."
        )
    active_run = _find_live_running_ocr_run(telemetry)
    if active_run:
        active_pid = _safe_int(active_run.get("worker_pid"), 0)
        raise RuntimeError(
            f"OCR уже запущен (PID {active_pid}). Дождитесь завершения текущего процесса."
        )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    env["PYTHONIOENCODING"] = "utf-8"
    env["RAG_LOG_HISTORY_NAME"] = "ocr.log"
    env["RAG_LOG_LABEL"] = "OCR"
    args = [
        sys.executable,
        "-m",
        "rag_catalog.core.ocr_pdfs",
        "--min-text-len",
        str(int(min_text_len)),
        "--workers",
        str(_effective_workers(workers if workers is not None else cfg.get("index_read_workers"), mode="ocr")),
    ]
    log_fh = _open_log(PROJECT_ROOT / "logs" / "ocr.log", "OCR")
    try:
        proc = subprocess.Popen(
            args,
            cwd=str(PROJECT_ROOT),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=log_fh,
            creationflags=_windows_detached_creationflags(),
        )
        _write_runtime_marker("ocr", pid=proc.pid)
    finally:
        log_fh.close()
    return proc.pid


# ── Scheduler ─────────────────────────────────────────────────────────────

def _schedules_due(schedules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Вернуть расписания, которые должны запуститься по локальному времени сервера."""
    now = datetime.now().astimezone()
    due = []
    for sched in schedules:
        if not int(sched.get("enabled") or 0):
            continue
        cadence = str(sched.get("cadence") or "daily")
        sched_time = str(sched.get("time") or "03:00")
        try:
            hh, mm = int(sched_time[:2]), int(sched_time[3:5])
        except (ValueError, IndexError):
            hh, mm = 3, 0

        # days_json column stores a JSON array string, e.g. '["Mon","Wed"]'
        raw_days = sched.get("days_json") or sched.get("days") or "[]"
        try:
            days: List[str] = json.loads(raw_days) if isinstance(raw_days, str) else list(raw_days or [])
        except (ValueError, TypeError):
            days = []

        day_name = now.strftime("%a")
        if cadence == "weekly" and day_name not in days:
            continue
        if cadence == "daily" and days and day_name not in days:
            continue
        if cadence == "hourly":
            if abs(now.minute - 0) > 1:
                continue
        else:
            if now.hour != hh or abs(now.minute - mm) > 1:
                continue

        # Dedup window: at least as wide as the trigger window (3 min) to prevent double-firing
        last_run = str(sched.get("last_run_at") or "")
        if last_run:
            try:
                lr = datetime.fromisoformat(last_run.replace("Z", "+00:00"))
                if (now - lr).total_seconds() < 300:
                    continue
            except ValueError:
                pass
        due.append(sched)
    return due


def _schedule_stage_priority(stage: str) -> int:
    """Lower priority value means this stage should claim a shared schedule slot first."""
    order = {
        "all": 0,
        "large": 10,
        "small": 20,
        "content": 30,
        "metadata": 40,
        "ocr": 50,
    }
    return order.get(str(stage or "all").lower(), 90)


def _schedule_stage_covers(launched_stage: str, candidate_stage: str) -> bool:
    launched = str(launched_stage or "").lower()
    candidate = str(candidate_stage or "").lower()
    if not launched or not candidate:
        return False
    if launched == candidate:
        return True
    return launched == "all" and candidate != "ocr"


def _run_scheduler_tick(cfg: Dict[str, Any]) -> None:
    # Reload config each tick so catalog_path and other settings are always current
    from rag_catalog.core.rag_core import load_config as _load_cfg
    try:
        live_cfg = _load_cfg()
    except Exception:
        live_cfg = cfg

    tdb = TelemetryDB(str(_telemetry_db_path(live_cfg)))
    if not hasattr(tdb, "list_index_schedules"):
        return
    schedules = tdb.list_index_schedules()
    due = _schedules_due(schedules)
    if not due:
        return
    due = sorted(
        due,
        key=lambda sched: (
            _schedule_stage_priority(str(sched.get("stage") or "all")),
            str(sched.get("created_at") or ""),
            str(sched.get("id") or ""),
        ),
    )
    cfg_settings = tdb.get_index_settings() if hasattr(tdb, "get_index_settings") else {}
    workers = int(cfg_settings.get("workers") or live_cfg.get("index_read_workers") or 4)
    launched_index_stage = ""
    for sched in due:
        stage = str(sched.get("stage") or "all")
        sched_id = str(sched["id"])
        if stage != "ocr" and launched_index_stage:
            if _schedule_stage_covers(launched_index_stage, stage):
                tdb.touch_index_schedule(id=sched_id)
            continue
        try:
            if stage == "ocr":
                if launched_index_stage:
                    continue
                _launch_ocr(live_cfg, workers=workers)
            else:
                _launch_indexer(
                    live_cfg,
                    stage=stage,
                    workers=workers,
                    max_chunks=int(cfg_settings.get("max_chunks") or live_cfg.get("index_max_chunks") or 2000),
                    skip_inline_ocr=bool(cfg_settings.get("skip_inline_ocr")),
                )
        except RuntimeError:
            continue
        tdb.touch_index_schedule(id=sched_id)
        if stage != "ocr":
            launched_index_stage = stage


def _start_global_scheduler(cfg: Dict[str, Any]) -> None:
    global _GLOBAL_SCHEDULER_STARTED
    if _GLOBAL_SCHEDULER_STARTED:
        return
    _GLOBAL_SCHEDULER_STARTED = True

    def _loop() -> None:
        while True:
            try:
                _run_scheduler_tick(cfg)
            except Exception as exc:
                print(f"[nice_app] scheduler loop skipped: {exc}", file=sys.stderr)
            time.sleep(60.0)

    thread = threading.Thread(target=_loop, name="rag-scheduler", daemon=True)
    thread.start()


# ── Timer and worker helpers ───────────────────────────────────────────────

def _stop_managed_timer(timer_obj: Any) -> None:
    if timer_obj is None:
        return
    try:
        timer_obj.active = False
    except Exception:
        pass
    try:
        timer_obj.delete()
    except Exception:
        pass


# ── Recovery ──────────────────────────────────────────────────────────────

def _resolve_index_recovery_stage(telemetry: TelemetryDB, active_run: Dict[str, Any]) -> str:
    run_id = str(active_run.get("run_id") or "")
    if run_id:
        stage_rows = telemetry.fetch_dicts(
            """
            SELECT stage
            FROM index_stage_progress
            WHERE run_id=? AND status='running'
            ORDER BY ts_updated DESC, ts_started DESC
            LIMIT 1
            """,
            [run_id],
        )
        if stage_rows:
            candidate = str(stage_rows[0].get("stage") or "").strip().lower()
            if candidate in _STAGE_LABELS:
                return candidate
    note = str(active_run.get("note") or "")
    match = re.search(r"stage=(all|metadata|small|large)", note.lower())
    if match:
        return match.group(1)
    return "all"


def _parse_utc_iso(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_recent_failure(run: Dict[str, Any], *, now: datetime) -> bool:
    finished = _parse_utc_iso(run.get("ts_finished"))
    started = _parse_utc_iso(run.get("ts_started"))
    ts = finished or started
    if ts is None:
        return False
    return (now - ts).total_seconds() <= _FAILED_RUN_RECENCY_SEC


def _trim_failed_restart_history(task_name: str, now_ts: float) -> List[float]:
    history = list(_FAILED_RESTART_HISTORY.get(task_name, []))
    history = [item for item in history if now_ts - item <= _FAILED_RESTART_WINDOW_SEC]
    _FAILED_RESTART_HISTORY[task_name] = history
    return history


def _can_attempt_failed_restart(task_name: str, run_id: str, now_ts: float) -> bool:
    if not run_id:
        return False
    if run_id in _FAILED_RESTART_RESTARTED_IDS.get(task_name, set()):
        return False
    history = _trim_failed_restart_history(task_name, now_ts)
    if history and now_ts - history[-1] < _FAILED_RESTART_COOLDOWN_SEC:
        return False
    if len(history) >= _FAILED_RESTART_MAX_ATTEMPTS:
        return False
    return True


def _register_failed_restart(task_name: str, run_id: str, now_ts: float) -> None:
    history = _trim_failed_restart_history(task_name, now_ts)
    history.append(now_ts)
    _FAILED_RESTART_HISTORY[task_name] = history
    _FAILED_RESTART_RESTARTED_IDS.setdefault(task_name, set()).add(run_id)


def _recover_background_tasks(
    cfg: Dict[str, Any],
    *,
    recovery_note: str = "server_restart_recovery",
    allow_failed_restart: bool = False,
) -> None:
    now = datetime.now(timezone.utc)
    now_ts = time.time()
    telemetry = TelemetryDB(str(_telemetry_db_path(cfg)))
    settings = telemetry.get_index_settings() if hasattr(telemetry, "get_index_settings") else {}
    workers = _safe_int(settings.get("workers") or cfg.get("index_read_workers") or 4, 4)
    max_chunks = _safe_int(settings.get("max_chunks") or cfg.get("index_max_chunks") or 2000, 2000)
    skip_inline_ocr = bool(settings.get("skip_inline_ocr"))
    ocr_min_text_len = _safe_int(settings.get("ocr_min_text_len") or 50, 50)

    recovered_index_now = False
    live_index = _find_live_running_index_run(telemetry)
    active_index = telemetry.get_active_index_run() if hasattr(telemetry, "get_active_index_run") else None
    active_index_pid = _safe_int((active_index or {}).get("worker_pid"), 0)
    active_index_pid_dead = bool(active_index and active_index_pid > 0 and not _is_process_alive(active_index_pid))
    if active_index and (not live_index or (active_index_pid_dead and live_index.get("_process_scan_only"))):
        recovery_stage = _resolve_index_recovery_stage(telemetry, active_index)
        telemetry.finalize_running_index_runs(status="cancelled", note=recovery_note)
        _launch_indexer(cfg, stage=recovery_stage, workers=workers, max_chunks=max_chunks, skip_inline_ocr=skip_inline_ocr)
        recovered_index_now = True
    elif allow_failed_restart and not live_index:
        failed_rows = telemetry.fetch_dicts(
            "SELECT * FROM index_runs WHERE status='failed' ORDER BY COALESCE(ts_finished, ts_started) DESC LIMIT 1"
        )
        if failed_rows:
            failed_run = failed_rows[0]
            failed_run_id = str(failed_run.get("run_id") or "")
            if _is_recent_failure(failed_run, now=now) and _can_attempt_failed_restart("index", failed_run_id, now_ts):
                recovery_stage = _resolve_index_recovery_stage(telemetry, failed_run)
                _launch_indexer(cfg, stage=recovery_stage, workers=workers, max_chunks=max_chunks, skip_inline_ocr=skip_inline_ocr)
                _register_failed_restart("index", failed_run_id, now_ts)
                recovered_index_now = True

    live_ocr = _find_live_running_ocr_run(telemetry)
    active_ocr = telemetry.get_active_ocr_run() if hasattr(telemetry, "get_active_ocr_run") else None
    active_ocr_pid = _safe_int((active_ocr or {}).get("worker_pid"), 0)
    active_ocr_pid_dead = bool(active_ocr and active_ocr_pid > 0 and not _is_process_alive(active_ocr_pid))
    if recovered_index_now:
        if active_ocr and hasattr(telemetry, "finalize_running_ocr_runs"):
            telemetry.finalize_running_ocr_runs(status="cancelled", note=recovery_note)
        return
    if active_ocr and (not live_ocr or (active_ocr_pid_dead and live_ocr.get("_process_scan_only"))):
        if hasattr(telemetry, "finalize_running_ocr_runs"):
            telemetry.finalize_running_ocr_runs(status="cancelled", note=recovery_note)
        _launch_ocr(cfg, min_text_len=ocr_min_text_len, workers=workers)
        return
    if allow_failed_restart and not live_ocr and not _find_live_running_index_run(telemetry):
        failed_ocr_rows = telemetry.fetch_dicts(
            "SELECT * FROM ocr_runs WHERE status='failed' ORDER BY COALESCE(ts_finished, ts_updated, ts_started) DESC LIMIT 1"
        )
        if not failed_ocr_rows:
            return
        failed_ocr = failed_ocr_rows[0]
        failed_ocr_id = str(failed_ocr.get("ocr_run_id") or "")
        if _is_recent_failure(failed_ocr, now=now) and _can_attempt_failed_restart("ocr", failed_ocr_id, now_ts):
            _launch_ocr(cfg, min_text_len=ocr_min_text_len, workers=workers)
            _register_failed_restart("ocr", failed_ocr_id, now_ts)


def _run_recovery_cycle(
    cfg: Dict[str, Any],
    *,
    recovery_note: str,
    allow_failed_restart: bool,
) -> None:
    with _RECOVERY_LOCK:
        _recover_background_tasks(cfg, recovery_note=recovery_note, allow_failed_restart=allow_failed_restart)


def _start_recovery_watchdog(cfg: Dict[str, Any]) -> None:
    global _RECOVERY_WATCHDOG_STARTED
    if _RECOVERY_WATCHDOG_STARTED:
        return
    _RECOVERY_WATCHDOG_STARTED = True

    def _loop() -> None:
        while True:
            try:
                _run_recovery_cycle(cfg, recovery_note="watchdog_recovery", allow_failed_restart=True)
            except Exception as exc:
                print(f"[nice_app] recovery watchdog skipped: {exc}", file=sys.stderr)
            time.sleep(_RECOVERY_WATCHDOG_INTERVAL_SEC)

    thread = threading.Thread(target=_loop, name="rag-recovery-watchdog", daemon=True)
    thread.start()
