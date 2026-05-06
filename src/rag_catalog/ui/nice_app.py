"""NiceGUI web frontend for RAG Catalog."""

from __future__ import annotations

import argparse
import html
import json
import mimetypes
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import HTTPException
from fastapi.responses import FileResponse
from nicegui import app, events, run, ui

from rag_catalog.core.rag_core import RAGSearcher, load_config, save_config
from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.telemetry_db import TelemetryDB
from rag_catalog.core.user_auth_db import UserAuthDB


PROJECT_ROOT = Path(__file__).resolve().parents[3]
APP_ICON_PATH = PROJECT_ROOT / "assets" / "brand" / "ico" / "favicon.ico"
LOGO_PATH = PROJECT_ROOT / "assets" / "brand" / "svg" / "rag-search-mark.svg"

_STAGE_LABELS: Dict[str, str] = {
    "all": "Все этапы",
    "metadata": "metadata",
    "small": "small chunks",
    "large": "large chunks",
}
_CADENCE_LABELS: Dict[str, str] = {
    "hourly": "Каждый час",
    "daily": "Ежедневно",
    "weekly": "Еженедельно",
}
_DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_DAY_RU = {"Mon": "Пн", "Tue": "Вт", "Wed": "Ср", "Thu": "Чт", "Fri": "Пт", "Sat": "Сб", "Sun": "Вс"}

_RECOVERY_LOCK = threading.Lock()
_RECOVERY_WATCHDOG_STARTED = False
_RECOVERY_WATCHDOG_INTERVAL_SEC = 45
_FAILED_RUN_RECENCY_SEC = 10 * 60
_FAILED_RESTART_COOLDOWN_SEC = 90
_FAILED_RESTART_MAX_ATTEMPTS = 3
_FAILED_RESTART_WINDOW_SEC = 15 * 60
_FAILED_RESTART_HISTORY: Dict[str, List[float]] = {"index": [], "ocr": []}
_FAILED_RESTART_RESTARTED_IDS: Dict[str, set[str]] = {"index": set(), "ocr": set()}


def _open_log(log_path: "Path", label: str) -> "Any":
    """Открыть лог-файл на дозапись, записать заголовок с временем."""
    log_path.parent.mkdir(exist_ok=True)
    fh = open(log_path, "a", encoding="utf-8", errors="replace")  # noqa: WPS515
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fh.write(f"\n{'='*60}\n{label}  {ts}\n{'='*60}\n")
    fh.flush()
    return fh


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


def _find_live_running_index_run(telemetry: TelemetryDB) -> Optional[Dict[str, Any]]:
    rows = telemetry.fetch_dicts(
        "SELECT * FROM index_runs WHERE status='running' ORDER BY ts_started DESC LIMIT 20"
    )
    for row in rows:
        pid = _safe_int(row.get("worker_pid"), 0)
        if _is_process_alive(pid):
            return row
    return None


def _find_live_running_ocr_run(telemetry: TelemetryDB) -> Optional[Dict[str, Any]]:
    rows = telemetry.fetch_dicts(
        "SELECT * FROM ocr_runs WHERE status='running' ORDER BY ts_started DESC LIMIT 20"
    )
    for row in rows:
        pid = _safe_int(row.get("worker_pid"), 0)
        if _is_process_alive(pid):
            return row
    return None


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
            stdout=log_fh,
            stderr=log_fh,
            creationflags=_windows_detached_creationflags(),
        )
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
            stdout=log_fh,
            stderr=log_fh,
            creationflags=_windows_detached_creationflags(),
        )
    finally:
        log_fh.close()
    return proc.pid


def _schedules_due(schedules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Вернуть расписания, которые должны запуститься прямо сейчас (±1 мин)."""
    now = datetime.now(timezone.utc)
    due = []
    for sched in schedules:
        if not int(sched.get("enabled") or 0):
            continue
        sched_time = str(sched.get("time") or "03:00")
        try:
            hh, mm = int(sched_time[:2]), int(sched_time[3:5])
        except (ValueError, IndexError):
            continue
        cadence = str(sched.get("cadence") or "daily")
        days = sched.get("days") or []
        day_name = now.strftime("%a")  # "Mon", "Tue", ...
        if cadence == "weekly" and day_name not in days:
            continue
        if cadence == "daily" and days and day_name not in days:
            continue
        # Проверяем совпадение часа и минуты (±1 мин)
        if now.hour != hh or abs(now.minute - mm) > 1:
            continue
        # Не запускать дважды в одну минуту
        last_run = str(sched.get("last_run_at") or "")
        if last_run:
            try:
                lr = datetime.fromisoformat(last_run.replace("Z", "+00:00"))
                if (now - lr).total_seconds() < 90:
                    continue
            except ValueError:
                pass
        due.append(sched)
    return due

SEARCH_PRESETS = [
    ("Договоры", "договор поставки"),
    ("Счета", "счет на оплату"),
    ("Паспорта", "паспорт техника"),
    ("PDF", "pdf скан"),
    ("Таблицы", "реестр xlsx"),
]

FILE_PREVIEW_EXTENSIONS = {".txt", ".log", ".csv", ".json", ".md", ".py", ".ps1", ".xml", ".html", ".css"}
INLINE_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".svg"}
OFFICE_PREVIEW_EXTENSIONS = {".docx", ".xlsx", ".xls", ".rtf"}
PAGE_SIZE = 80
SYSTEM_FILE_EXTENSIONS = {
    ".dll",
    ".exe",
    ".msi",
    ".sys",
    ".inf",
    ".cat",
    ".pnf",
    ".bak",
    ".tmp",
    ".temp",
    ".dat",
    ".db",
    ".db3",
    ".sqlite",
    ".sqlite-journal",
    ".db-shm",
    ".db-wal",
    ".lock",
    ".cfg",
    ".ini",
    ".config",
    ".lnk",
    ".chm",
    ".toc",
    ".cldbin",
}

if LOGO_PATH.exists():
    app.add_static_file(local_file=LOGO_PATH, url_path="/rag-logo.png")


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
    expanded_query: str = ""       # расширенный запрос от LLM (пусто если не менялся)
    ai_search_expand: bool = True  # включать AI-дополнение поискового запроса
    rag_answer_text: str = ""      # RAG Q&A ответ (пусто если LLM отключён)
    rag_answer_loading: bool = False
    settings_section: str = "profile"  # активная секция в настройках (IDE-навигация)
    displayed_count: int = 10
    active_type_filter: Optional[str] = None
    explorer_path: Optional[str] = None
    explorer_filter: str = ""
    explorer_ext: str = "Все"
    explorer_sort: str = "По имени"
    explorer_desc: bool = False
    explorer_view: str = "Таблица"
    explorer_page: int = 0
    auth_db: Optional[UserAuthDB] = None
    current_user: Optional[Dict[str, Any]] = None
    auth_token: str = ""
    theme: str = "light"
    favorites: List[Dict[str, Any]] = field(default_factory=list)
    header_explorer_actions: Optional[ui.row] = None
    header_breadcrumbs: Optional[ui.row] = None
    telemetry: Optional[TelemetryDB] = None


def _file_url(full_path: str) -> str:
    try:
        p = PureWindowsPath(full_path)
        if not p.parts:
            return ""
        drive = p.drive
        encoded = "/".join(quote(part, safe="") for part in p.parts[1:])
        return "file:///" + drive + "/" + encoded
    except Exception:
        return ""


def _folder_url(full_path: str) -> str:
    try:
        p = PureWindowsPath(full_path).parent
        if not p.parts:
            return ""
        drive = p.drive
        encoded = "/".join(quote(part, safe="") for part in p.parts[1:])
        return "file:///" + drive + "/" + encoded
    except Exception:
        return ""


def _resolve_catalog_file(cfg: Dict[str, Any], raw_path: str) -> Optional[Path]:
    try:
        catalog = Path(str(cfg.get("catalog_path") or "")).resolve()
        if not catalog.exists() or not catalog.is_dir():
            return None
        candidate = Path(str(raw_path or "")).resolve()
        candidate.relative_to(catalog)
    except Exception:
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    return candidate


def _viewer_file_url(full_path: str) -> str:
    value = str(full_path or "").strip()
    if not value:
        return ""
    return f"/api/view-file?path={quote(value, safe='')}"


def _preview_office_file(path: Path, limit: int = 12000) -> str:
    ext = path.suffix.lower()
    if ext == ".docx":
        try:
            from docx import Document  # noqa: PLC0415

            doc = Document(path)
            parts: List[str] = [p.text for p in doc.paragraphs if p.text]
            for table in doc.tables:
                for row in table.rows:
                    row_cells = [str(cell.text or "").strip() for cell in row.cells]
                    line = " | ".join(item for item in row_cells if item)
                    if line:
                        parts.append(line)
            return "\n".join(parts)[:limit] or "Документ пуст."
        except Exception as exc:
            return f"Не удалось прочитать DOCX: {exc}"
    if ext == ".xlsx":
        try:
            from openpyxl import load_workbook  # noqa: PLC0415

            wb = load_workbook(path, read_only=True, data_only=True)
            parts: List[str] = []
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                parts.append(f"Лист: {sheet_name}")
                for row in ws.iter_rows(values_only=True):
                    row_text = " | ".join(str(c) if c is not None else "" for c in row)
                    if row_text.strip():
                        parts.append(row_text)
                    if len("\n".join(parts)) >= limit:
                        return "\n".join(parts)[:limit]
            return "\n".join(parts)[:limit] or "Таблица пуста."
        except Exception as exc:
            return f"Не удалось прочитать XLSX: {exc}"
    if ext == ".xls":
        try:
            import xlrd  # type: ignore  # noqa: PLC0415

            book = xlrd.open_workbook(str(path))
            parts = []
            for sheet in book.sheets():
                parts.append(f"Лист: {sheet.name}")
                for row_idx in range(sheet.nrows):
                    row = sheet.row_values(row_idx)
                    row_text = " | ".join(str(v) if v not in ("", None) else "" for v in row)
                    if row_text.strip():
                        parts.append(row_text)
                    if len("\n".join(parts)) >= limit:
                        return "\n".join(parts)[:limit]
            return "\n".join(parts)[:limit] or "Таблица пуста."
        except Exception as exc:
            return f"Не удалось прочитать XLS: {exc}"
    if ext == ".rtf":
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
            no_ctrl = re.sub(r"\\[a-z]+\d* ?", " ", raw)
            no_braces = re.sub(r"[{}]", "", no_ctrl)
            clean = re.sub(r"\s+", " ", no_braces).strip()
            return clean[:limit] or "Документ пуст."
        except Exception as exc:
            return f"Не удалось прочитать RTF: {exc}"
    return "Для этого типа файла доступно открытие или скачивание."


@app.get("/api/view-file")
def api_view_file(path: str) -> FileResponse:
    resolved = _resolve_catalog_file(load_config(), path)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Файл не найден или недоступен")
    media_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
    return FileResponse(str(resolved), media_type=media_type, filename=resolved.name)


def _telemetry_db_path(cfg: Dict[str, Any]) -> Path:
    explicit = str(cfg.get("telemetry_db_path") or "").strip()
    if explicit:
        return Path(explicit)
    return Path(str(cfg.get("qdrant_db_path") or "")) / "rag_telemetry.db"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


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
    if not live_index and active_index:
        recovery_stage = _resolve_index_recovery_stage(telemetry, active_index)
        telemetry.finalize_running_index_runs(
            status="cancelled",
            note=recovery_note,
        )
        _launch_indexer(
            cfg,
            stage=recovery_stage,
            workers=workers,
            max_chunks=max_chunks,
            skip_inline_ocr=skip_inline_ocr,
        )
        recovered_index_now = True
    elif allow_failed_restart and not live_index:
        failed_rows = telemetry.fetch_dicts(
            """
            SELECT * FROM index_runs
            WHERE status='failed'
            ORDER BY COALESCE(ts_finished, ts_started) DESC
            LIMIT 1
            """
        )
        if failed_rows:
            failed_run = failed_rows[0]
            failed_run_id = str(failed_run.get("run_id") or "")
            if _is_recent_failure(failed_run, now=now) and _can_attempt_failed_restart("index", failed_run_id, now_ts):
                recovery_stage = _resolve_index_recovery_stage(telemetry, failed_run)
                _launch_indexer(
                    cfg,
                    stage=recovery_stage,
                    workers=workers,
                    max_chunks=max_chunks,
                    skip_inline_ocr=skip_inline_ocr,
                )
                _register_failed_restart("index", failed_run_id, now_ts)
                recovered_index_now = True

    live_ocr = _find_live_running_ocr_run(telemetry)
    active_ocr = telemetry.get_active_ocr_run() if hasattr(telemetry, "get_active_ocr_run") else None
    if recovered_index_now:
        # OCR-проход внутри себя запускает index_rag(stage=large), поэтому
        # в тот же recovery-цикл после старта indexer его не перезапускаем.
        # При этом зависший OCR-ран закрываем, чтобы не оставлять "running".
        if active_ocr and hasattr(telemetry, "finalize_running_ocr_runs"):
            telemetry.finalize_running_ocr_runs(
                status="cancelled",
                note=recovery_note,
            )
        return
    if not live_ocr and active_ocr:
        if hasattr(telemetry, "finalize_running_ocr_runs"):
            telemetry.finalize_running_ocr_runs(
                status="cancelled",
                note=recovery_note,
            )
        _launch_ocr(
            cfg,
            min_text_len=ocr_min_text_len,
            workers=workers,
        )
        return
    if allow_failed_restart and not live_ocr and not _find_live_running_index_run(telemetry):
        failed_ocr_rows = telemetry.fetch_dicts(
            """
            SELECT * FROM ocr_runs
            WHERE status='failed'
            ORDER BY COALESCE(ts_finished, ts_updated, ts_started) DESC
            LIMIT 1
            """
        )
        if not failed_ocr_rows:
            return
        failed_ocr = failed_ocr_rows[0]
        failed_ocr_id = str(failed_ocr.get("ocr_run_id") or "")
        if _is_recent_failure(failed_ocr, now=now) and _can_attempt_failed_restart("ocr", failed_ocr_id, now_ts):
            _launch_ocr(
                cfg,
                min_text_len=ocr_min_text_len,
                workers=workers,
            )
            _register_failed_restart("ocr", failed_ocr_id, now_ts)


def _run_recovery_cycle(
    cfg: Dict[str, Any],
    *,
    recovery_note: str,
    allow_failed_restart: bool,
) -> None:
    with _RECOVERY_LOCK:
        _recover_background_tasks(
            cfg,
            recovery_note=recovery_note,
            allow_failed_restart=allow_failed_restart,
        )


def _start_recovery_watchdog(cfg: Dict[str, Any]) -> None:
    global _RECOVERY_WATCHDOG_STARTED
    if _RECOVERY_WATCHDOG_STARTED:
        return
    _RECOVERY_WATCHDOG_STARTED = True

    def _loop() -> None:
        while True:
            try:
                _run_recovery_cycle(
                    cfg,
                    recovery_note="watchdog_recovery",
                    allow_failed_restart=True,
                )
            except Exception as exc:
                print(f"[nice_app] recovery watchdog skipped: {exc}", file=sys.stderr)
            time.sleep(_RECOVERY_WATCHDOG_INTERVAL_SEC)

    thread = threading.Thread(target=_loop, name="rag-recovery-watchdog", daemon=True)
    thread.start()


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


def _load_user_state(state: PageState) -> None:
    username = _username(state)
    if not username:
        return
    auth_db = _get_auth_db(state)
    settings = auth_db.get_user_settings(username=username)
    explorer = settings.get("explorer") if isinstance(settings.get("explorer"), dict) else {}
    if explorer:
        state.explorer_view = str(explorer.get("view") or state.explorer_view)
        state.explorer_sort = str(explorer.get("sort") or state.explorer_sort)
        state.explorer_desc = bool(explorer.get("desc", state.explorer_desc))
        state.explorer_ext = str(explorer.get("ext") or state.explorer_ext)
    ui_settings = settings.get("ui") if isinstance(settings.get("ui"), dict) else {}
    if ui_settings:
        theme = str(ui_settings.get("theme") or "").strip().lower()
        if theme in {"light", "dark"}:
            state.theme = theme
        if "ai_search_expand" in ui_settings:
            state.ai_search_expand = bool(ui_settings.get("ai_search_expand"))
    state.favorites = auth_db.list_favorites(username=username)
    # Восстановить личную историю поиска из БД (персистентна между сессиями)
    if not state.history:
        try:
            _get_telemetry(state)
        except Exception:
            pass
        state.history = _my_recent_queries(state.cfg, username, limit=24)


def _save_ui_settings(state: PageState) -> None:
    username = _username(state)
    if not username:
        return
    auth_db = _get_auth_db(state)
    settings = auth_db.get_user_settings(username=username)
    ui_settings = settings.get("ui") if isinstance(settings.get("ui"), dict) else {}
    ui_settings["theme"] = state.theme if state.theme in {"light", "dark"} else "light"
    ui_settings["ai_search_expand"] = bool(state.ai_search_expand)
    settings["ui"] = ui_settings
    auth_db.save_user_settings(username=username, settings=settings)


def _save_explorer_settings(state: PageState) -> None:
    username = _username(state)
    if not username:
        return
    auth_db = _get_auth_db(state)
    settings = auth_db.get_user_settings(username=username)
    settings["explorer"] = {
        "view": state.explorer_view,
        "sort": state.explorer_sort,
        "desc": state.explorer_desc,
        "ext": state.explorer_ext,
    }
    auth_db.save_user_settings(username=username, settings=settings)
    _log_app_event(state, "explorer", "save_settings", details=settings["explorer"])


def _favorite_key(path: str) -> str:
    return str(path or "").strip().casefold()


def _is_favorite(state: PageState, path: str) -> bool:
    key = _favorite_key(path)
    return any(_favorite_key(str(item.get("path") or "")) == key for item in state.favorites)


def _favorite_type(path: Path) -> str:
    return "folder" if path.is_dir() else "file"


def _toggle_favorite(state: PageState, path: Path, *, item_type: Optional[str] = None, title: str = "") -> bool:
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


def _db_query_dicts(db_path: Path, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(query, params or ())
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


def _read_log_tail(path: Path, *, max_chars: int = 12000) -> str:
    try:
        if not path.exists():
            return "Лог-файл не найден."
        text = path.read_text(encoding="utf-8", errors="replace")
        if not text:
            return "Лог-файл пуст."
        return text[-max_chars:]
    except Exception as exc:
        return f"Не удалось прочитать лог: {exc}"


def _read_log_tail_lines(path: Path, *, max_lines: int = 200, max_chars: int = 200_000) -> str:
    try:
        if not path.exists():
            return "Лог-файл не найден."
        text = path.read_text(encoding="utf-8", errors="replace")
        if not text:
            return "Лог-файл пуст."
        lines = text.splitlines()
        tail = "\n".join(lines[-max(1, int(max_lines)):])
        if len(tail) > max_chars:
            tail = tail[-max_chars:]
        return tail
    except Exception as exc:
        return f"Не удалось прочитать лог: {exc}"


def _filter_log_text(text: str, level: str) -> str:
    level_key = str(level or "all").strip().lower()
    if level_key in {"", "all"}:
        return text
    token = f" - {level_key.upper()} - "
    lines = [line for line in str(text or "").splitlines() if token in line.upper()]
    return "\n".join(lines) if lines else "Записей для выбранного уровня не найдено."


def _dedupe_queries(values: List[str], limit: int = 12) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        query = re.sub(r"\s+", " ", str(value or "")).strip()
        key = query.lower()
        if not query or key in seen:
            continue
        seen.add(key)
        out.append(query)
        if len(out) >= limit:
            break
    return out


def _my_recent_queries(cfg: Dict[str, Any], username: str = "", limit: int = 12) -> List[str]:
    """Последние запросы текущего пользователя из БД."""
    rows = _db_query_dicts(
        _telemetry_db_path(cfg),
        """
        SELECT COALESCE(NULLIF(query_original, ''), query) AS query
        FROM search_logs
        WHERE query <> '' AND lower(username) = ?
        ORDER BY id DESC
        LIMIT 80
        """,
        (username.strip().lower(),),
    )
    return _dedupe_queries([str(row.get("query") or "") for row in rows], limit=limit)


def _popular_queries(cfg: Dict[str, Any], exclude_username: str = "", limit: int = 10) -> List[str]:
    """Топ запросов по частоте среди всех пользователей (кроме текущего)."""
    rows = _db_query_dicts(
        _telemetry_db_path(cfg),
        """
        SELECT COALESCE(NULLIF(query_original, ''), query) AS query, COUNT(*) AS cnt
        FROM search_logs
        WHERE query <> '' AND lower(username) != ?
        GROUP BY lower(COALESCE(NULLIF(query_original, ''), query))
        ORDER BY cnt DESC
        LIMIT 40
        """,
        (exclude_username.strip().lower(),),
    )
    return _dedupe_queries([str(row.get("query") or "") for row in rows], limit=limit)


def _search_suggestions(state: PageState, typed: str = "") -> List[str]:
    username = _username(state)
    personal = _dedupe_queries([*state.history, *_my_recent_queries(state.cfg, username, limit=12)], limit=24)
    needle = typed.strip().lower()
    if not needle:
        return personal[:12]
    starts = [item for item in personal if item.lower().startswith(needle)]
    contains = [item for item in personal if needle in item.lower() and item not in starts]
    return [*starts, *contains][:12]


def _telegram_deeplink(bot_link: str, purpose: str, token: str) -> str:
    base = str(bot_link or "").strip()
    value = str(token or "").strip()
    if not base or not value:
        return ""
    joiner = "&" if "?" in base else "?"
    return f"{base}{joiner}start={purpose}_{quote(value, safe='')}"


def _remember_query(state: PageState, query: str) -> None:
    clean = re.sub(r"\s+", " ", str(query or "")).strip()
    if clean:
        state.history = _dedupe_queries([clean, *state.history], limit=24)


def _normalize_search_results(results: Any) -> List[Dict[str, Any]]:
    if results is None:
        return []
    if not isinstance(results, list):
        return []
    return [item for item in results if isinstance(item, dict)]


def _run_catalog_search(
    searcher: RAGSearcher,
    *,
    query: str,
    query_original: str,
    query_used: str,
    limit: int,
    file_type: Optional[str],
    content_only: bool,
    title_only: bool,
    username: str = "",
) -> List[Dict[str, Any]]:
    results = _normalize_search_results(
        searcher.search(
            query,
            limit=limit,
            file_type=file_type,
            content_only=content_only,
            title_only=title_only,
            source="nicegui",
            username=username,
            query_original=query_original,
        )
    )
    if results or content_only or title_only:
        return results

    # If the vector path returns an empty/invalid value, keep name/path search usable.
    try:
        fallback = searcher._lexical_catalog_search(  # noqa: SLF001 - UI fallback for catalog metadata search
            query=query_used,
            limit=max(limit, 10),
            file_type=file_type,
            content_only=False,
            title_only=title_only,
        )
    except Exception:
        return results
    return _normalize_search_results(fallback)[:limit]


def _run_quick_name_search(
    searcher: RAGSearcher,
    *,
    query: str,
    limit: int,
    file_type: Optional[str],
) -> List[Dict[str, Any]]:
    """Быстрый поиск только по именам/пути (metadata), без LLM и контентных чанков."""
    quick = _normalize_search_results(
        searcher._lexical_catalog_search(  # noqa: SLF001
            query=query,
            limit=max(limit * 3, 60),
            file_type=file_type,
            content_only=False,
            title_only=True,
        )
    )
    needle = re.sub(r"\s+", " ", str(query or "")).strip().lower().replace("ё", "е")

    def sort_key(item: Dict[str, Any]) -> tuple:
        name = str(item.get("filename") or "").lower().replace("ё", "е")
        path = str(item.get("path") or "").lower().replace("ё", "е")
        exact_name = 1 if needle and needle in name else 0
        exact_path = 1 if needle and needle in path else 0
        score = float(item.get("score") or 0.0)
        return (exact_name, exact_path, score, -len(path))

    quick.sort(key=sort_key, reverse=True)
    return quick[:limit]


def _count_exact_name_matches(query: str, results: List[Dict[str, Any]]) -> int:
    needle = re.sub(r"\s+", " ", str(query or "")).strip().lower().replace("ё", "е")
    if not needle:
        return 0
    count = 0
    for item in results:
        name = str(item.get("filename") or "").lower().replace("ё", "е")
        path = str(item.get("path") or "").lower().replace("ё", "е")
        if needle in name or needle in path:
            count += 1
    return count


def _merge_search_results(
    primary: List[Dict[str, Any]],
    secondary: List[Dict[str, Any]],
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    merged_by_key: Dict[str, Dict[str, Any]] = {}

    def key_of(item: Dict[str, Any]) -> str:
        return f"{item.get('full_path')}::{item.get('chunk_index')}::{item.get('type')}"

    def rank_key(item: Dict[str, Any]) -> tuple:
        rank_score = float(item.get("rank_score", item.get("score") or 0) or 0)
        score = float(item.get("score") or 0)
        path = str(item.get("path") or item.get("full_path") or "")
        return (rank_score, score, -len(path))

    for item in [*primary, *secondary]:
        key = key_of(item)
        existing = merged_by_key.get(key)
        if existing is None or rank_key(item) > rank_key(existing):
            merged_by_key[key] = item

    ranked = sorted(merged_by_key.values(), key=rank_key, reverse=True)
    return ranked[:limit]


def _format_file_size(size_b: int) -> str:
    if size_b >= 1_048_576:
        return f"{size_b / 1_048_576:.1f} МБ"
    if size_b >= 1024:
        return f"{size_b / 1024:.1f} КБ"
    return f"{size_b} Б"


def _clean_text(value: Any) -> str:
    raw = str(value or "")
    raw = re.sub(r"<[^>]{1,200}>", " ", raw)
    raw = html.unescape(raw)
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\s*\n\s*", "\n", raw)
    return raw.strip()


def _directory_children(path: str, limit: int = 60) -> Dict[str, Any]:
    p = Path(path)
    out: Dict[str, Any] = {"exists": p.exists(), "is_dir": p.is_dir(), "dirs": [], "files": []}
    if not p.exists() or not p.is_dir():
        return out
    try:
        entries = sorted(
            [x for x in p.iterdir() if not x.name.startswith(".") and not x.name.startswith("~$")],
            key=lambda x: (not x.is_dir(), x.name.lower()),
        )
    except Exception as exc:
        out["error"] = str(exc)
        return out
    for child in entries[:limit]:
        item = {"name": child.name, "path": str(child)}
        if child.is_dir():
            out["dirs"].append(item)
        elif child.is_file():
            try:
                item["size"] = _format_file_size(child.stat().st_size)
            except Exception:
                item["size"] = ""
            out["files"].append(item)
    out["truncated"] = len(entries) > limit
    return out


def _preview_file(path: Path, limit: int = 6000) -> str:
    if not path.exists() or not path.is_file():
        return "Файл недоступен."
    if path.suffix.lower() not in FILE_PREVIEW_EXTENSIONS:
        return "Для этого типа файла доступно открытие или скачивание."
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:limit]
    except Exception as exc:
        return f"Не удалось прочитать файл: {exc}"


def _open_os_path(path: str) -> None:
    value = str(path or "").strip()
    if not value:
        return
    try:
        subprocess.Popen(["explorer", value])
    except Exception as exc:
        ui.notify(f"Не удалось открыть проводник ОС: {exc}", type="negative")


def _within_catalog(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _safe_explorer_path(state: PageState) -> Path:
    root = Path(str(state.cfg.get("catalog_path") or ""))
    if state.explorer_path:
        candidate = Path(state.explorer_path)
        if candidate.exists() and _within_catalog(root, candidate):
            return candidate
    state.explorer_path = str(root)
    return root


def _read_index_stats(cfg: Dict[str, Any]) -> Dict[str, Any]:
    state_file = Path(str(cfg.get("qdrant_db_path") or "")) / "index_state.db"
    out: Dict[str, Any] = {
        "found": False,
        "state_file": str(state_file),
        "total": 0,
        "total_size_bytes": 0,
        "by_ext": {},
        "by_ext_size": {},
        "by_stage": {},
    }
    if not state_file.exists():
        return out
    try:
        db = IndexStateDB(str(state_file))
        stats = db.stats()
    except Exception as exc:
        out["error"] = str(exc)
        return out
    out.update({
        "found": True,
        "total": int(stats.get("total") or 0),
        "total_size_bytes": int(stats.get("total_size_bytes") or 0),
        "by_ext": dict(stats.get("by_ext") or {}),
        "by_ext_size": dict(stats.get("by_ext_size") or {}),
        "by_stage": dict(stats.get("by_stage") or {}),
    })
    try:
        out["last_modified"] = time.strftime("%d.%m.%Y %H:%M", time.localtime(state_file.stat().st_mtime))
    except Exception:
        pass
    return out


def _format_bytes(value: Any) -> str:
    try:
        size = float(value or 0)
    except (TypeError, ValueError):
        size = 0.0
    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.1f} {units[idx]}"


def _format_duration_seconds(value: Any) -> str:
    try:
        seconds = int(float(value or 0))
    except (TypeError, ValueError):
        seconds = 0
    if seconds <= 0:
        return "0 сек"
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours} ч {minutes:02d} мин"
    if minutes:
        return f"{minutes} мин {secs:02d} сек"
    return f"{secs} сек"


def _format_relative_time(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "не запускался"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return raw[:16]
    if dt.tzinfo is None:
        now = datetime.now()
    else:
        now = datetime.now(dt.tzinfo)
    seconds = max(0, int((now - dt).total_seconds()))
    if seconds < 60:
        return "только что"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} ч назад"
    days = hours // 24
    if days < 30:
        return f"{days} дн назад"
    months = days // 30
    if months < 12:
        return f"{months} мес назад"
    years = months // 12
    return f"{years} г назад"


def _duration_between(start: Any, finish: Any = None) -> int:
    start_s = str(start or "")
    finish_s = str(finish or "")
    if not start_s:
        return 0
    try:
        start_dt = datetime.fromisoformat(start_s)
        finish_dt = datetime.fromisoformat(finish_s) if finish_s else datetime.now(start_dt.tzinfo)
        return max(0, int((finish_dt - start_dt).total_seconds()))
    except ValueError:
        return 0


def _read_index_telemetry(cfg: Dict[str, Any]) -> Dict[str, Any]:
    db_path = _telemetry_db_path(cfg)
    last_run = _db_query_dicts(
        db_path,
        """
        SELECT *,
               CAST((julianday(COALESCE(ts_finished, CURRENT_TIMESTAMP)) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM index_runs
        ORDER BY ts_started DESC
        LIMIT 1
        """,
    )
    active_runs = _db_query_dicts(
        db_path,
        """
        SELECT *,
               CAST((julianday(CURRENT_TIMESTAMP) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM index_runs
        WHERE status='running'
        ORDER BY ts_started DESC
        LIMIT 3
        """,
    )
    active_run_ids = [str(row.get("run_id") or "") for row in active_runs if str(row.get("run_id") or "")]
    active_stages: List[Dict[str, Any]] = []
    if active_run_ids:
        placeholders = ",".join("?" for _ in active_run_ids)
        active_stages = _db_query_dicts(
            db_path,
            f"""
            SELECT isp.*,
                   ir.status AS run_status,
                   ir.note AS run_note,
                   CAST((julianday(COALESCE(isp.ts_finished, CURRENT_TIMESTAMP)) - julianday(isp.ts_started)) * 86400 AS INTEGER) AS duration_sec
            FROM index_stage_progress AS isp
            LEFT JOIN index_runs AS ir ON ir.run_id = isp.run_id
            WHERE isp.run_id IN ({placeholders})
            ORDER BY isp.ts_started, isp.stage
            """,
            tuple(active_run_ids),
        )
    latest_stages = _db_query_dicts(
        db_path,
        """
        SELECT isp.*,
               ir.status AS run_status,
               ir.note AS run_note,
               CAST((julianday(COALESCE(isp.ts_finished, CURRENT_TIMESTAMP)) - julianday(isp.ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM index_stage_progress AS isp
        LEFT JOIN index_runs AS ir ON ir.run_id = isp.run_id
        WHERE isp.run_id=(SELECT run_id FROM index_runs ORDER BY ts_started DESC LIMIT 1)
        ORDER BY CASE isp.stage WHEN 'metadata' THEN 1 WHEN 'small' THEN 2 WHEN 'large' THEN 3 WHEN 'content' THEN 4 ELSE 9 END
        """,
    )
    stage_summary = _db_query_dicts(
        db_path,
        """
        WITH finished AS (
            SELECT stage,
                   status,
                   total_files,
                   processed_files,
                   added_files,
                   updated_files,
                   skipped_files,
                   error_files,
                   points_added,
                   ts_started,
                   ts_finished,
                   CAST((julianday(ts_finished) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
            FROM index_stage_progress
            WHERE ts_finished IS NOT NULL
        ),
        latest AS (
            SELECT f.*
            FROM finished f
            JOIN (
                SELECT stage, MAX(ts_started) AS ts_started
                FROM finished
                GROUP BY stage
            ) m ON m.stage=f.stage AND m.ts_started=f.ts_started
        ),
        avg_by_stage AS (
            SELECT stage,
                   COUNT(*) AS runs_count,
                   AVG(duration_sec) AS avg_duration_sec,
                   AVG(processed_files) AS avg_processed_files
            FROM finished
            GROUP BY stage
        )
        SELECT latest.stage,
               latest.status,
               latest.total_files,
               latest.processed_files,
               latest.added_files,
               latest.updated_files,
               latest.skipped_files,
               latest.error_files,
               latest.points_added,
               latest.ts_started,
               latest.ts_finished,
               latest.duration_sec AS last_duration_sec,
               avg_by_stage.runs_count,
               CAST(avg_by_stage.avg_duration_sec AS INTEGER) AS avg_duration_sec,
               CAST(avg_by_stage.avg_processed_files AS INTEGER) AS avg_processed_files
        FROM latest
        JOIN avg_by_stage ON avg_by_stage.stage=latest.stage
        ORDER BY CASE latest.stage WHEN 'metadata' THEN 1 WHEN 'small' THEN 2 WHEN 'large' THEN 3 WHEN 'content' THEN 4 ELSE 9 END
        """,
    )
    overall = _db_query_dicts(
        db_path,
        """
        SELECT COUNT(*) AS runs_count,
               CAST(AVG((julianday(ts_finished) - julianday(ts_started)) * 86400) AS INTEGER) AS avg_duration_sec,
               MAX(total_files) AS max_total_files,
               AVG(total_files) AS avg_total_files
        FROM index_runs
        WHERE ts_finished IS NOT NULL
        """,
    )
    active_ocr = _db_query_dicts(
        db_path,
        """
        SELECT *,
               CAST((julianday(CURRENT_TIMESTAMP) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM ocr_runs
        WHERE status='running'
        ORDER BY ts_started DESC
        LIMIT 1
        """,
    )
    last_ocr = _db_query_dicts(
        db_path,
        """
        SELECT *,
               CAST((julianday(COALESCE(ts_finished, CURRENT_TIMESTAMP)) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM ocr_runs
        ORDER BY ts_started DESC
        LIMIT 1
        """,
    )
    ocr_summary = _db_query_dicts(
        db_path,
        """
        SELECT COUNT(*) AS runs_count,
               CAST(AVG((julianday(ts_finished) - julianday(ts_started)) * 86400) AS INTEGER) AS avg_duration_sec,
               AVG(found_scanned) AS avg_found_scanned,
               AVG(processed_pdfs) AS avg_processed_pdfs
        FROM ocr_runs
        WHERE ts_finished IS NOT NULL
        """,
    )
    return {
        "last_run": last_run[0] if last_run else None,
        "active_runs": active_runs,
        "active_stages": active_stages,
        "latest_stages": latest_stages,
        "stage_summary": stage_summary,
        "overall": overall[0] if overall else {},
        "active_ocr": active_ocr[0] if active_ocr else None,
        "last_ocr": last_ocr[0] if last_ocr else None,
        "ocr_summary": ocr_summary[0] if ocr_summary else {},
    }


def _ensure_searcher(state: PageState) -> Optional[RAGSearcher]:
    if state.searcher is not None:
        return state.searcher
    try:
        state.searcher = RAGSearcher(state.cfg)
    except Exception as exc:
        state.searcher_error = str(exc)
        return None
    if not state.searcher.connected:
        state.searcher_error = "Нет подключения к Qdrant."
    return state.searcher


def _is_admin(state: PageState) -> bool:
    return str((state.current_user or {}).get("role") or "") == "admin"


def _show_system_files(state: PageState) -> bool:
    if not _is_admin(state):
        return False
    try:
        auth = state.auth_db if state.auth_db is not None else _get_auth_db(state)
        return bool(auth.get_show_system_files_for_admin())
    except Exception:
        return False


def _is_system_file(path_or_ext: str | Path) -> bool:
    value = str(path_or_ext or "")
    ext = Path(value).suffix.lower() or value.lower()
    name = Path(value).name.lower()
    if not ext:
        return False
    if ext in SYSTEM_FILE_EXTENSIONS:
        return True
    if ext.endswith("_"):
        return True
    if ext in {".sample", ".backup", ".sav", ".asd"}:
        return True
    return name.startswith("~$") or name.endswith(".tmp")


def _result_kind(result: Dict[str, Any]) -> str:
    if result.get("type") == "folder_metadata":
        return "Каталог"
    ext = str(result.get("extension") or "").lower()
    if ext in {".xlsx", ".xls", ".csv"}:
        return "Таблица"
    if ext == ".pdf":
        return "PDF"
    if ext in {".docx", ".doc"}:
        return "Документ"
    return "Файл"


def _result_group(result: Dict[str, Any]) -> str:
    text = " ".join(
        str(result.get(key, "") or "").lower()
        for key in ("filename", "path", "type", "text", "extension")
    )
    if str(result.get("type") or "") == "folder_metadata":
        return "Каталоги"
    if any(word in text for word in ("птс", "псм", "стс", "техпаспорт", "техническ", "паспорт транспорт", "экскаватор")):
        return "Техпаспорта ТС"
    if any(word in text for word in ("паспорт", "удостоверен")):
        return "Паспорта и удостоверения"
    if any(word in text for word in ("договор", "соглашен", "контракт")):
        return "Договоры"
    if any(word in text for word in ("счет", "счёт", "оплат", "платеж")):
        return "Счета и платежи"
    if str(result.get("extension") or "").lower() in {".xlsx", ".xls", ".csv"}:
        return "Таблицы"
    if str(result.get("extension") or "").lower() == ".pdf":
        return "PDF"
    return "Другие файлы"


def _grouped_results(results: List[Dict[str, Any]]) -> List[tuple[str, List[Dict[str, Any]]]]:
    order = [
        "Каталоги",
        "Техпаспорта ТС",
        "Паспорта и удостоверения",
        "Договоры",
        "Счета и платежи",
        "Таблицы",
        "PDF",
        "Другие файлы",
    ]
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for result in results:
        grouped.setdefault(_result_group(result), []).append(result)
    return [
        (group, sorted(grouped[group], key=lambda item: float(item.get("score") or 0), reverse=True))
        for group in order
        if group in grouped
    ]


def _file_icon_svg(path_or_ext: str, kind: str = "Файл") -> str:
    ext = Path(str(path_or_ext or "")).suffix.lower() or str(path_or_ext or "").lower()
    if kind == "Каталог":
        color, label = "#f2b84b", ""
    elif ext in {".doc", ".docx"}:
        color, label = "#2b579a", "W"
    elif ext in {".xls", ".xlsx", ".csv"}:
        color, label = "#217346", "X"
    elif ext in {".ppt", ".pptx"}:
        color, label = "#d24726", "P"
    elif ext == ".pdf":
        color, label = "#d32f2f", "PDF"
    elif ext in {".zip", ".rar", ".7z"}:
        color, label = "#d39a18", "ZIP"
    elif ext in {".jpg", ".jpeg", ".png", ".gif", ".webp"}:
        color, label = "#7b4ab8", "IMG"
    elif ext in {".txt", ".log", ".md"}:
        color, label = "#64748b", "TXT"
    elif ext in {".mp3", ".wav", ".flac"}:
        color, label = "#0e7490", "AUD"
    elif ext in {".mp4", ".avi", ".mov", ".mkv"}:
        color, label = "#7c3aed", "VID"
    elif ext in {".json", ".xml", ".html", ".htm", ".css", ".js", ".py", ".ps1", ".cmd", ".bat"}:
        color, label = "#475569", "DEV"
    elif ext in {".dwg", ".svg", ".psd", ".ico"}:
        color, label = "#0891b2", "ART"
    elif _is_system_file(path_or_ext):
        color, label = "#94a3b8", (ext.replace(".", "").upper()[:3] or "SYS")
    else:
        color, label = "#0f766e", (ext.replace(".", "").upper()[:3] or "FILE")

    if kind == "Каталог":
        svg = f"""
        <svg viewBox="0 0 56 56" aria-hidden="true">
          <path fill="#d99d2b" d="M5 15a6 6 0 0 1 6-6h12l5 6h17a6 6 0 0 1 6 6v4H5z"/>
          <path fill="{color}" d="M5 20h46v21a6 6 0 0 1-6 6H11a6 6 0 0 1-6-6z"/>
          <path fill="#ffd56d" opacity=".75" d="M8 23h40v4H8z"/>
        </svg>
        """
    else:
        font_size = "12" if len(label) <= 1 else "9"
        svg = f"""
        <svg viewBox="0 0 56 56" aria-hidden="true">
          <path fill="#ffffff" d="M12 4h22l10 10v36a4 4 0 0 1-4 4H12a4 4 0 0 1-4-4V8a4 4 0 0 1 4-4z"/>
          <path fill="#dbe4ef" d="M34 4v10h10z"/>
          <path fill="{color}" d="M6 29h37a4 4 0 0 1 4 4v12a4 4 0 0 1-4 4H6z"/>
          <text x="25" y="42" fill="#fff" text-anchor="middle" font-family="Arial, sans-serif" font-size="{font_size}" font-weight="700">{html.escape(label)}</text>
          <path fill="none" stroke="#cbd5e1" d="M12.5 4.5h21.3L43.5 14v35.5a4 4 0 0 1-4 4h-27a4 4 0 0 1-4-4v-41a4 4 0 0 1 4-4z"/>
        </svg>
        """
    icon_class = "rag-file-icon system" if kind != "Каталог" and _is_system_file(path_or_ext) else "rag-file-icon"
    return f'<span class="{icon_class}">{svg}</span>'


def _path_sort_key(path: Path, sort_by: str) -> Any:
    if sort_by == "По размеру":
        name = path.name.lower()
        if path.is_file():
            try:
                return (path.stat().st_size, name)
            except Exception:
                return (0, name)
        return (-1, name)
    if sort_by == "По дате":
        name = path.name.lower()
        try:
            return (path.stat().st_mtime, name)
        except Exception:
            return (0, name)
    return path.name.lower()


def _file_rows(path: Path, state: PageState) -> tuple[List[Path], List[Path], int]:
    try:
        entries = [x for x in path.iterdir() if not x.name.startswith(".") and not x.name.startswith("~$")]
    except Exception:
        return [], [], 0

    dirs = [x for x in entries if x.is_dir()]
    files = [x for x in entries if x.is_file()]
    if not _show_system_files(state):
        files = [x for x in files if not _is_system_file(x)]
    needle = state.explorer_filter.strip().lower()
    if needle:
        dirs = [x for x in dirs if needle in x.name.lower()]
        files = [x for x in files if needle in x.name.lower()]
    if state.explorer_ext != "Все":
        files = [x for x in files if x.suffix.lower() == state.explorer_ext.lower()]

    reverse = bool(state.explorer_desc)
    dirs.sort(key=lambda x: _path_sort_key(x, state.explorer_sort), reverse=reverse)
    files.sort(key=lambda x: _path_sort_key(x, state.explorer_sort), reverse=reverse)
    return dirs, files, len(files)


def _event_input_value(event: Any, fallback: Any = "") -> str:
    value = getattr(event, "value", None)
    if value is not None:
        return str(value)
    args = getattr(event, "args", None)
    if isinstance(args, dict):
        for key in ("value", "modelValue"):
            if args.get(key) is not None:
                return str(args[key])
        target = args.get("target")
        if isinstance(target, dict) and target.get("value") is not None:
            return str(target["value"])
    return str(fallback or "")


def _apply_explorer_filter_input(state: PageState, event: Any, fallback: Any = "") -> None:
    state.explorer_filter = _event_input_value(event, fallback)
    state.explorer_page = 0


def _install_css() -> None:
    ui.add_head_html('<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">')
    ui.add_head_html('<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">')
    ui.add_css(
        """
        :root {
          --rag-font-display: 'Manrope', system-ui, -apple-system, sans-serif;
          --rag-font-text: 'Inter', system-ui, sans-serif;
          --rag-font-mono: 'JetBrains Mono', ui-monospace, monospace;
          --rag-bg: #fafaf7;
          --rag-surface: #ffffff;
          --rag-surface-strong: #ffffff;
          --rag-sunken: #f4f3ee;
          --rag-border: #e6e3da;
          --rag-border-strong: #d8d3c4;
          --rag-text: #14141a;
          --rag-muted: #6c6c78;
          --rag-muted-2: #9a9aa2;
          --rag-accent: #3d63ff;
          --rag-accent-hover: #2949e6;
          --rag-accent-2: #10b981;
          --rag-danger: #dc2626;
          --rag-warn: #f59e0b;
          --rag-header-bg: rgba(250, 250, 247, 0.88);
          --rag-drawer-bg: #ffffff;
          --rag-search-bg: #ffffff;
          --rag-suggest-bg: #ffffff;
          --rag-chip-bg: #ffffff;
          --rag-group-bg: #ffffff;
          --rag-bookmark-bg: #ffffff;
          --rag-bookmark-hover-bg: #f4f3ee;
          --rag-bookmark-remove-bg: #ffffff;
          --rag-bookmark-remove-hover-bg: #fff1f1;
          --rag-context-bg: #ffffff;
          --rag-code-bg: #f4f3ee;
          --rag-shadow: 0 8px 24px -18px rgba(20, 20, 26, 0.34);
        }
        body.body--dark {
          --rag-bg: #0c0c0f;
          --rag-surface: #15151a;
          --rag-surface-strong: #1b1b22;
          --rag-sunken: #08080a;
          --rag-border: #23232b;
          --rag-border-strong: #2e2e38;
          --rag-text: #f4f4f7;
          --rag-muted: #8a8a96;
          --rag-muted-2: #5a5a64;
          --rag-accent: #6385ff;
          --rag-accent-hover: #4f6dff;
          --rag-header-bg: rgba(12, 12, 15, 0.9);
          --rag-drawer-bg: #15151a;
          --rag-search-bg: #15151a;
          --rag-suggest-bg: #15151a;
          --rag-chip-bg: #1b1b22;
          --rag-group-bg: #15151a;
          --rag-bookmark-bg: #15151a;
          --rag-bookmark-hover-bg: #23232b;
          --rag-bookmark-remove-bg: #15151a;
          --rag-bookmark-remove-hover-bg: rgba(127, 29, 29, 0.42);
          --rag-context-bg: #15151a;
          --rag-code-bg: #08080a;
          --rag-shadow: 0 12px 32px -20px rgba(0, 0, 0, 0.8);
          background-image: none;
        }
        body {
          background: var(--rag-bg);
          color: var(--rag-text);
          font-family: var(--rag-font-text);
          font-size: 87.5%;
          letter-spacing: 0;
          background-image: none;
        }
        .material-icons,
        .q-icon.material-icons,
        i.q-icon.notranslate {
          font-family: 'Material Icons' !important;
          font-weight: normal;
          font-style: normal;
          font-size: 24px;
          line-height: 1;
          letter-spacing: normal;
          text-transform: none;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          white-space: nowrap;
          word-wrap: normal;
          direction: ltr;
          -webkit-font-feature-settings: 'liga';
          -webkit-font-smoothing: antialiased;
          font-feature-settings: 'liga';
        }
        .q-layout,
        .q-page-container,
        .q-page,
        .q-drawer,
        .q-drawer__content {
          background: var(--rag-bg);
          color: var(--rag-text);
        }
        .q-field__control,
        .q-menu,
        .q-list,
        .q-table__container,
        .q-table__top,
        .q-table__bottom,
        .q-card {
          background: var(--rag-surface);
          color: var(--rag-text);
        }
        .q-field__native,
        .q-field__input,
        .q-field__label,
        .q-item,
        .q-table,
        .q-table th,
        .q-table td {
          color: var(--rag-text);
        }
        .q-field--outlined .q-field__control:before {
          border-color: var(--rag-border);
        }
        .q-separator {
          background: var(--rag-border);
        }
        .rag-header {
          height: 48px !important;
          min-height: 48px !important;
          max-height: 48px !important;
          background: var(--rag-header-bg);
          color: var(--rag-text);
          border-bottom: 1px solid var(--rag-border);
          backdrop-filter: blur(16px) saturate(140%);
          -webkit-backdrop-filter: blur(16px) saturate(140%);
          display: flex;
          align-items: center;
          overflow: hidden;
        }
        .rag-header > .q-toolbar,
        .rag-header .q-toolbar,
        .rag-header .nicegui-content {
          height: 48px !important;
          min-height: 48px !important;
          align-items: center;
        }
        .rag-header .q-btn,
        .rag-header-button,
        .rag-drawer .q-btn {
          color: var(--rag-text) !important;
        }
        .rag-header .q-btn {
          height: 32px !important;
          min-height: 32px !important;
          max-height: 32px !important;
          padding-top: 0 !important;
          padding-bottom: 0 !important;
          align-self: center;
        }
        .rag-header .q-btn--round {
          width: 32px !important;
          min-width: 32px !important;
        }
        .rag-header .q-btn__content {
          min-height: 0 !important;
          height: 32px;
          line-height: 1;
          align-items: center;
          justify-content: center;
          flex-wrap: nowrap;
        }
        .rag-header .q-icon {
          line-height: 1;
          font-size: 20px;
        }
        .rag-header .q-img,
        .rag-header img {
          display: block;
          flex: 0 0 auto;
        }
        .rag-header-breadcrumbs,
        .rag-header-actions {
          height: 32px;
          align-items: center;
        }
        .rag-header-breadcrumbs .q-btn { min-height: 32px; padding: 0 6px; }
        .rag-header-actions .q-btn { min-width: 32px; min-height: 32px; }
        .rag-drawer {
          background: var(--rag-drawer-bg);
          border-right: 1px solid var(--rag-border);
        }
        .rag-drawer-body {
          min-height: calc(100vh - 92px);
          display: flex;
          flex-direction: column;
        }
        .rag-drawer-bottom {
          margin-top: auto;
          padding-top: 12px;
          border-top: 1px solid var(--rag-border);
        }
        .rag-page {
          width: min(1440px, calc(100vw - 24px));
          margin: 0 auto;
          padding: 10px 0 32px;
        }
        .rag-page.search { padding-top: 4px; }
        .rag-title, h1, h2, h3, .text-2xl, .text-xl {
          font-family: var(--rag-font-display);
          letter-spacing: 0;
        }
        .rag-title { font-size: clamp(22px, 3.5vw, 34px); font-weight: 760; line-height: 1.05; letter-spacing: 0; }
        .rag-subtitle { color: var(--rag-muted); font-size: 13px; max-width: 820px; }
        .rag-card {
          background: var(--rag-surface);
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          box-shadow: var(--rag-shadow);
          backdrop-filter: none;
          transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        .rag-card:hover {
          transform: translateY(-2px);
          box-shadow: 0 14px 30px -18px rgba(20, 20, 26, 0.36);
        }
        .rag-search-shell { position: relative; z-index: 5; }
        .rag-search-box {
          background: var(--rag-search-bg);
          border: 1px solid var(--rag-border);
          border-radius: 16px;
          box-shadow: var(--rag-shadow);
          backdrop-filter: blur(12px);
          transition: box-shadow 0.2s ease;
        }
        .rag-search-box:focus-within {
          box-shadow: 0 0 0 2px var(--rag-accent), var(--rag-shadow);
        }
        .rag-ai-expand {
          flex: 0 0 auto;
          min-width: 30px;
          height: 32px;
          padding: 0 2px;
          border: 0;
          border-radius: 0;
          background: transparent;
          opacity: .34;
          text-decoration: line-through;
          text-decoration-thickness: 2px;
          text-decoration-color: currentColor;
          transition: opacity .14s ease, transform .14s ease, outline-color .14s ease;
          transform-origin: center;
        }
        .rag-ai-expand:hover {
          opacity: .72;
          transform: scale(1.06);
        }
        .rag-ai-expand:active {
          transform: scale(.96);
        }
        .rag-ai-expand:focus-within {
          outline: 2px solid color-mix(in srgb, var(--rag-accent) 70%, transparent);
          outline-offset: 3px;
          border-radius: 6px;
        }
        .rag-ai-expand .q-checkbox__label {
          font-weight: 700;
          color: var(--rag-text);
          line-height: 1;
        }
        .rag-ai-expand .q-checkbox__inner {
          display: none;
        }
        .rag-ai-expand[aria-checked="true"],
        .rag-ai-expand.q-checkbox--truthy {
          opacity: 1;
          text-decoration: none;
        }
        .rag-ai-expand[aria-checked="true"]:hover,
        .rag-ai-expand.q-checkbox--truthy:hover {
          opacity: 1;
          transform: scale(1.06);
        }
        .rag-suggest {
          position: absolute;
          left: 0;
          right: 0;
          top: calc(100% + 8px);
          background: var(--rag-suggest-bg);
          border: 1px solid var(--rag-border);
          border-radius: 16px;
          box-shadow: 0 24px 48px -12px rgba(0, 0, 0, 0.18);
          backdrop-filter: blur(16px);
          overflow: hidden;
          z-index: 30;
        }
        .rag-result {
          background: var(--rag-surface);
          border: 1px solid var(--rag-border);
          border-radius: 10px;
          padding: 12px;
          box-shadow: 0 4px 14px rgba(0, 0, 0, 0.02);
          width: 100%;
          box-sizing: border-box;
          backdrop-filter: blur(8px);
          transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .rag-result:hover {
          background: var(--rag-surface-strong);
          transform: translateY(-2px) scale(1.005);
          box-shadow: 0 12px 24px -8px rgba(0, 0, 0, 0.1);
          border-color: rgba(59, 130, 246, 0.3);
        }
        .rag-meta { color: var(--rag-muted); font-size: 12px; }
        .rag-chip {
          display: inline-flex;
          align-items: center;
          min-height: 28px;
          padding: 0 12px;
          border: 1px solid var(--rag-border);
          border-radius: 14px;
          color: var(--rag-muted);
          background: var(--rag-chip-bg);
          backdrop-filter: blur(4px);
          font-size: 12px;
          font-weight: 500;
          cursor: pointer;
          user-select: none;
          transition: all 0.2s ease;
        }
        .rag-chip:hover {
          background: var(--rag-surface-strong);
          color: var(--rag-accent);
          border-color: var(--rag-accent);
          transform: translateY(-1px);
          box-shadow: 0 4px 6px -1px rgba(59, 130, 246, 0.1);
        }
        .rag-chip-active {
          background: linear-gradient(135deg, var(--rag-accent), #2563eb);
          color: #ffffff;
          border-color: transparent;
          font-weight: 600;
          box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
        }
        .rag-chip-active:hover {
          transform: translateY(-1px);
          box-shadow: 0 6px 14px rgba(59, 130, 246, 0.4);
        }
        .rag-search-toolbar {
          position: sticky;
          top: 56px;
          z-index: 4;
          padding: 8px;
          border: 1px solid var(--rag-border);
          border-radius: 12px;
          background: color-mix(in srgb, var(--rag-surface) 92%, transparent);
          box-shadow: var(--rag-shadow);
          backdrop-filter: blur(14px);
        }
        .rag-section-label {
          color: var(--rag-muted);
          font-size: 11px;
          font-weight: 700;
          letter-spacing: .08em;
        }
        .rag-explorer-v2-layout {
          display: grid;
          grid-template-columns: minmax(190px, 240px) minmax(0, 1fr) minmax(210px, 270px);
        }
        .rag-explorer-tree,
        .rag-explorer-details {
          position: sticky;
          top: 66px;
          max-height: calc(100vh - 84px);
          overflow: auto;
        }
        .rag-explorer-files {
          min-width: 0;
        }
        .rag-index-phase {
          padding: 10px;
          border: 1px solid var(--rag-border);
          border-radius: 10px;
          background: var(--rag-sunken);
        }
        .rag-index-phase.running {
          border-color: color-mix(in srgb, var(--rag-accent) 48%, var(--rag-border));
        }
        .rag-index-phase.failed,
        .rag-index-phase.cancelled {
          border-color: var(--rag-border);
        }
        .rag-phase-status {
          color: var(--rag-accent);
        }
        .rag-phase-status.completed { color: #16a34a; }
        .rag-phase-status.failed { color: #dc2626; }
        .rag-phase-status.cancelled { color: #f59e0b; }
        .rag-phase-status.idle { color: var(--rag-muted); }
        .rag-index-layout {
          display: grid;
          grid-template-columns: minmax(260px, 360px) minmax(0, 1fr);
          gap: 14px;
          align-items: start;
        }
        .rag-index-control-panel {
          border: 1px solid var(--rag-border);
          border-radius: 10px;
          background: var(--rag-sunken);
          padding: 12px;
        }
        .rag-pipeline-row {
          display: grid;
          grid-template-columns: 190px minmax(280px, 1fr) minmax(300px, 420px) 150px;
          gap: 12px;
          align-items: center;
          padding: 12px;
          border: 1px solid var(--rag-border);
          border-radius: 10px;
          background: var(--rag-sunken);
        }
        .rag-pipeline-row.running {
          border-color: color-mix(in srgb, var(--rag-accent) 55%, var(--rag-border));
        }
        .rag-pipeline-head {
          padding: 0 12px;
          border: 0;
          background: transparent;
          box-shadow: none;
        }
        .rag-pipeline-row.failed,
        .rag-pipeline-row.cancelled {
          border-color: var(--rag-border);
        }
        .rag-status-chip.completed {
          color: #16a34a;
          border-color: color-mix(in srgb, #16a34a 45%, var(--rag-border));
        }
        .rag-status-chip.running {
          color: var(--rag-accent);
          border-color: color-mix(in srgb, var(--rag-accent) 45%, var(--rag-border));
        }
        .rag-status-chip.failed {
          color: #dc2626;
          border-color: color-mix(in srgb, #dc2626 45%, var(--rag-border));
        }
        .rag-status-chip.cancelled {
          color: #f59e0b;
          border-color: color-mix(in srgb, #f59e0b 45%, var(--rag-border));
        }
        .rag-content-coverage {
          border: 1px dashed var(--rag-border);
          border-radius: 10px;
          background: var(--rag-sunken);
          padding: 10px 12px;
        }
        .rag-pipeline-actions {
          display: flex;
          gap: 4px;
          justify-content: flex-end;
          flex-wrap: nowrap;
          width: 150px;
          min-width: 150px;
        }
        .rag-pipeline-row > * {
          min-width: 0;
        }
        .rag-index-config-layout {
          display: grid;
          grid-template-columns: minmax(0, 1.15fr) minmax(280px, .85fr);
          gap: 14px;
          align-items: start;
        }
        .rag-analytics-tabs .q-btn {
          border-radius: 999px;
        }
        .rag-kpi {
          min-width: 180px;
          flex: 1 1 180px;
        }
        .rag-kpi-value {
          font-family: var(--rag-font-display);
          font-size: 24px;
          font-weight: 800;
        }
        .rag-mini-bar {
          height: 5px;
          width: 100%;
          border-radius: 999px;
          background: linear-gradient(90deg, var(--rag-accent), var(--rag-accent-2));
        }
        .rag-path {
          word-break: break-word;
          overflow-wrap: anywhere;
          color: var(--rag-muted);
          font-size: 12px;
        }
        .rag-actions { display: flex; flex-wrap: wrap; gap: 6px; }
        .rag-feedback-btn {
          width: 30px;
          height: 30px;
          min-width: 30px;
        }
        .rag-nav-button { justify-content: flex-start; border-radius: 8px; text-align: left; }
        .rag-nav-button .q-btn__content { justify-content: flex-start; width: 100%; text-align: left; }
        .rag-nav-button .q-icon { margin-right: 10px; }
        .rag-tree-button {
          min-height: 34px;
          height: auto;
          padding-top: 4px;
          padding-bottom: 4px;
        }
        .rag-tree-button .q-btn__content {
          display: grid;
          grid-template-columns: 22px minmax(0, 1fr);
          column-gap: 8px;
          align-items: start;
          flex-wrap: nowrap;
          min-width: 0;
          width: 100%;
        }
        .rag-tree-button .q-icon {
          margin-right: 0;
          width: 22px;
          min-width: 22px;
          line-height: 1.25;
        }
        .rag-tree-button .block {
          min-width: 0;
          white-space: normal;
          overflow-wrap: anywhere;
          word-break: normal;
          line-height: 1.25;
        }
        .rag-tree-button.active {
          background: color-mix(in srgb, var(--rag-accent) 14%, transparent);
          color: var(--rag-accent) !important;
          font-weight: 700;
        }
        .rag-tree-button.ancestor {
          color: var(--rag-text) !important;
          font-weight: 650;
        }
        .rag-breadcrumbs {
          min-width: 0;
          overflow: hidden;
        }
        .rag-breadcrumbs .q-btn {
          min-width: 0;
          max-width: 220px;
        }
        .rag-breadcrumbs .block {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .rag-filter-chip.active {
          border-color: var(--rag-accent);
          color: var(--rag-accent);
          background: color-mix(in srgb, var(--rag-accent) 10%, var(--rag-surface));
          font-weight: 700;
        }
        .rag-dirty-actions {
          position: sticky;
          bottom: 10px;
          z-index: 25;
          display: flex;
          justify-content: center;
          width: 100%;
          pointer-events: none;
        }
        .rag-dirty-actions > * {
          pointer-events: auto;
        }
        .rag-dirty-actions-inner {
          display: inline-flex;
          align-items: center;
          gap: 8px;
          padding: 8px 10px;
          border: 1px solid var(--rag-border);
          border-radius: 999px;
          background: var(--rag-surface-strong);
          box-shadow: var(--rag-shadow);
          backdrop-filter: blur(12px);
        }
        .rag-suggest-item {
          min-width: 0;
          overflow: hidden;
        }
        .rag-suggest-item .q-btn__content {
          width: 100%;
          min-width: 0;
          display: flex;
          align-items: center;
          flex-wrap: nowrap;
          gap: 8px;
          overflow: hidden;
        }
        .rag-suggest-item .q-icon {
          flex: 0 0 20px;
          width: 20px;
          min-width: 20px;
        }
        .rag-suggest-item .block {
          display: block;
          flex: 1 1 auto;
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          line-height: 1.25;
          text-align: left;
        }
        .rag-group-panel {
          width: 100%;
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          background: var(--rag-group-bg);
          overflow: hidden;
        }
        .rag-file-icon {
          display: inline-flex;
          width: 34px;
          height: 34px;
          flex: 0 0 34px;
        }
        .rag-file-icon svg { width: 34px; height: 34px; display: block; }
        .rag-file-icon.system {
          opacity: .42;
          filter: grayscale(1);
        }
        .rag-file-icon svg { width: 42px; height: 42px; display: block; }
        .rag-explorer-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
          gap: 10px;
        }
        .rag-explorer-grid.medium { grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); }
        .rag-explorer-grid.small {
          grid-template-columns: repeat(auto-fill, minmax(82px, 92px));
          gap: 8px;
        }
        .rag-explorer-item {
          width: 100%;
          min-width: 0;
          background: var(--rag-surface);
          border: 1px solid var(--rag-border);
          border-radius: 12px;
          color: var(--rag-text);
          backdrop-filter: blur(8px);
          transition: all 0.2s ease;
        }
        .rag-explorer-item:hover {
          background: var(--rag-surface-strong);
          border-color: rgba(59, 130, 246, 0.3);
          transform: translateY(-2px);
          box-shadow: 0 10px 20px -10px rgba(59, 130, 246, 0.15);
        }
        .rag-explorer-item.system {
          opacity: .55;
          color: #64748b;
        }
        .rag-explorer-item.system:hover {
          opacity: .78;
          background: #f1f5f9;
          border-color: #cbd5e1;
        }
        .rag-explorer-item { position: relative; }
        .rag-explorer-grid.small .rag-explorer-item {
          min-height: 96px;
          max-height: 106px;
          padding: 6px;
          overflow: hidden;
        }
        .rag-explorer-grid.small .rag-file-icon,
        .rag-explorer-grid.small .rag-file-icon svg {
          width: 34px;
          height: 34px;
          flex-basis: 34px;
        }
        .rag-explorer-grid.small .rag-favorite-star {
          position: absolute;
          top: 2px;
          right: 2px;
          z-index: 2;
          background: var(--rag-surface);
        }
        .rag-explorer-grid.small .rag-explorer-opener {
          width: 100%;
          min-width: 0;
          overflow: hidden;
        }
        .rag-favorite-star {
          opacity: 0;
          color: rgba(0, 0, 0, 0.45);
          transition: opacity .12s ease, color .12s ease, transform .12s ease;
        }
        .rag-explorer-item:hover .rag-favorite-star,
        .rag-favorite-star.active {
          opacity: 1;
        }
        .rag-favorite-star:hover {
          color: #d89b00;
          transform: scale(1.08);
        }
        .rag-favorite-star.active {
          color: #f6b700;
        }
        .rag-tile-star-wrap {
          position: absolute;
          top: 4px;
          right: 4px;
          z-index: 2;
        }
        .rag-favorite-star.header {
          opacity: .65;
        }
        .rag-favorite-star.header:hover {
          opacity: 1;
          color: #d89b00;
        }
        .rag-bookmarks {
          display: flex;
          width: 100%;
          gap: 8px;
          overflow-x: auto;
          padding: 8px 0;
          align-items: center;
          flex-wrap: nowrap;
        }
        .rag-bookmark {
          position: relative;
          flex: 0 0 auto;
          width: 220px;
          min-width: 160px;
          height: 42px;
          border: 1px solid var(--rag-border);
          background: var(--rag-bookmark-bg);
          border-radius: 8px;
          overflow: hidden;
          transition: background .12s ease, border-color .12s ease, box-shadow .12s ease;
        }
        .rag-bookmark:hover {
          background: var(--rag-bookmark-hover-bg);
          border-color: #bdd7e9;
        }
        .rag-bookmark-main {
          position: absolute;
          inset: 0 36px 0 0;
          display: flex;
          align-items: center;
          min-width: 0;
        }
        .rag-bookmark:hover .rag-bookmark-main {
          box-shadow: 18px 0 24px rgba(23, 32, 44, 0.12);
        }
        .rag-bookmark-remove {
          position: absolute;
          right: 0;
          top: 0;
          width: 36px;
          height: 100%;
          display: flex;
          align-items: center;
          justify-content: center;
          opacity: 0;
          background: var(--rag-bookmark-remove-bg);
          border-left: 1px solid var(--rag-border);
          color: #7b8794;
          transition: opacity .12s ease, color .12s ease, background .12s ease;
        }
        .rag-bookmark:hover .rag-bookmark-remove {
          opacity: 1;
        }
        .rag-bookmark-remove:hover {
          background: var(--rag-bookmark-remove-hover-bg);
          color: #b42318;
        }
        .rag-bookmark .q-btn {
          min-width: 0;
          width: 100%;
          height: 100%;
          padding-right: 4px;
        }
        .rag-bookmark .q-btn__content {
          min-width: 0;
          flex-wrap: nowrap;
          overflow: hidden;
        }
        .rag-bookmark .block {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .rag-bookmark-more {
          flex: 0 0 auto;
          width: 42px;
          height: 42px;
        }
        .rag-context-menu {
          position: fixed;
          z-index: 10000;
          min-width: 220px;
          background: var(--rag-context-bg);
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          box-shadow: 0 18px 48px rgba(23, 32, 44, 0.18);
          padding: 6px;
          display: none;
        }
        .rag-context-menu button {
          display: block;
          width: 100%;
          padding: 8px 10px;
          border: 0;
          background: transparent;
          text-align: left;
          border-radius: 8px;
          color: var(--rag-text);
          cursor: pointer;
        }
        .rag-context-menu button:hover { background: #eef6fb; }
        .rag-favorites-dialog-row {
          display: grid;
          grid-template-columns: auto minmax(0, 1fr) auto;
          gap: 8px;
          align-items: center;
          width: 100%;
        }
        .rag-explorer-name {
          width: 100%;
          min-width: 0;
          overflow-wrap: anywhere;
          word-break: break-word;
          line-height: 1.2;
        }
        .rag-explorer-grid.small .rag-explorer-name {
          display: -webkit-box;
          width: 100%;
          max-width: 100%;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
          overflow: hidden;
          text-overflow: ellipsis;
          overflow-wrap: anywhere;
          word-break: break-word;
          font-size: 12px;
          line-height: 1.15;
        }
        .rag-explorer-list {
          display: grid;
          grid-template-columns: 1fr;
          gap: 4px;
        }
        .rag-code {
          white-space: pre-wrap;
          word-break: break-word;
          font-family: var(--rag-font-mono);
          font-size: 12px;
          background: var(--rag-code-bg);
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          padding: 12px;
        }
        body.body--dark .rag-context-menu button:hover { background: rgba(30, 64, 175, 0.25); }
        body.body--dark .rag-explorer-item.system:hover {
          background: var(--rag-surface-strong);
          border-color: var(--rag-border-strong);
        }
        @media (max-width: 760px) {
          .rag-page { width: calc(100vw - 20px); padding-top: 18px; }
          .rag-title { font-size: 28px; }
          .rag-actions .q-btn { width: auto; }
          .rag-search-box { box-shadow: 0 4px 12px rgba(23, 32, 44, 0.06); }
          .rag-search-toolbar { top: 50px; }
          .rag-index-layout,
          .rag-index-config-layout { display: flex; flex-direction: column; }
          .rag-pipeline-row { display: flex; flex-direction: column; align-items: stretch; }
          .rag-pipeline-actions { justify-content: flex-start; flex-wrap: wrap; }
          .rag-explorer-v2-layout { display: flex; flex-direction: column; }
          .rag-explorer-tree,
          .rag-explorer-details {
            position: static;
            max-height: none;
            width: 100%;
          }
        }
        """
    )
    ui.add_body_html(
        """
        <div id="rag-global-context-menu" class="rag-context-menu" role="menu"></div>
        <script>
        (() => {
          if (window.__ragContextMenuInstalled) return;
          window.__ragContextMenuInstalled = true;
          const menu = () => document.getElementById('rag-global-context-menu');
          const hide = () => { const m = menu(); if (m) m.style.display = 'none'; };
          const addButton = (m, label, action) => {
            const b = document.createElement('button');
            b.textContent = label;
            b.onclick = () => { hide(); action(); };
            m.appendChild(b);
          };
          const show = (event) => {
            const root = event.target.closest('.q-layout');
            if (!root) return;
            event.preventDefault();
            const m = menu();
            if (!m) return;
            const item = event.target.closest('[data-rag-context="explorer-item"]');
            m.innerHTML = '';
            if (item) {
              const itemType = item.dataset.ragType || 'file';
              const itemPath = decodeURIComponent(item.dataset.ragPath || '');
              const itemUrl = item.dataset.ragUrl || '';
              addButton(m, 'Открыть', () => {
                item.querySelector('[data-rag-open]')?.click();
              });
              if (itemType === 'file' && itemUrl) {
                addButton(m, 'Скачать', () => {
                  const a = document.createElement('a');
                  a.href = itemUrl;
                  a.download = '';
                  document.body.appendChild(a);
                  a.click();
                  a.remove();
                });
              }
              addButton(m, 'Показать в ОС', () => item.querySelector('[data-rag-os]')?.click());
              addButton(m, item.dataset.ragFavorite === 'true' ? 'Убрать из избранного' : 'Добавить в избранное', () => item.querySelector('[data-rag-favorite-button]')?.click());
              addButton(m, 'Поделиться путем', () => navigator.clipboard && navigator.clipboard.writeText(itemPath));
            } else {
              addButton(m, 'Обновить экран', () => location.reload());
              addButton(m, 'Скопировать адрес экрана', () => navigator.clipboard && navigator.clipboard.writeText(location.href));
              addButton(m, 'Настройки', () => { location.href = '/settings'; });
            }
            m.style.left = Math.min(event.clientX, window.innerWidth - 240) + 'px';
            m.style.top = Math.min(event.clientY, window.innerHeight - 160) + 'px';
            m.style.display = 'block';
          };
          document.addEventListener('contextmenu', show);
          document.addEventListener('click', hide);
          document.addEventListener('scroll', hide, true);
          document.addEventListener('keydown', (e) => { if (e.key === 'Escape') hide(); });
        })();
        </script>
        """
    )


def _build_page(initial_screen: str = "search") -> None:
    state = PageState(cfg=load_config())
    state.screen = initial_screen
    state.explorer_path = str(Path(str(state.cfg.get("catalog_path") or "")))
    _install_css()
    try:
        stored_token = str(app.storage.user.get("auth_token") or "")
        if stored_token:
            state.auth_token = stored_token
            state.current_user = _get_auth_db(state).get_user_by_session(stored_token)
            if state.current_user:
                _load_user_state(state)
                _get_auth_db(state).log_auth_event(username=_username(state), event_type="session_restore", ok=True)
    except Exception:
        pass

    dark_mode = ui.dark_mode(state.theme == "dark")

    with ui.header(fixed=True, elevated=False).classes("rag-header px-3 md:px-4 items-center no-wrap"):
        menu_button = ui.button(icon="menu", on_click=lambda: drawer.toggle(), color=None).props("flat round dense").classes("rag-header-button")
        ui.image("/rag-logo.png").classes("w-6 h-6 rounded self-center") if LOGO_PATH.exists() else ui.icon("manage_search").classes("text-2xl self-center")
        ui.label("RAG Каталог").classes("font-semibold text-base self-center leading-none")
        # header_title убран — активный экран видно по подсветке в сайдбаре.
        # Оставляем ссылочное поле на None для совместимости с render().
        header_title = ui.label("").classes("hidden")
        header_breadcrumbs = ui.row().classes("rag-header-breadcrumbs items-center gap-1 hidden md:flex")
        header_actions = ui.row().classes("rag-header-actions items-center gap-1")
        state.header_breadcrumbs = header_breadcrumbs
        state.header_explorer_actions = header_actions
        ui.space()
        theme_button = ui.button(
            icon="light_mode" if state.theme == "dark" else "dark_mode",
            on_click=lambda: toggle_theme(),
            color=None,
        ).props("flat round dense").classes("rag-header-button")
        status_text = "Qdrant готов" if _ensure_searcher(state) and state.searcher and state.searcher.connected else "Qdrant недоступен"
        ui.label(status_text).classes("hidden sm:block rag-chip")

    with ui.left_drawer(value=False, fixed=True, bordered=True).props("show-if-above breakpoint=1024").classes("rag-drawer w-80 p-4") as drawer:
        with ui.column().classes("rag-drawer-body w-full"):
            ui.label("Меню").classes("text-xl font-semibold mb-2")
            nav_area = ui.column().classes("w-full gap-2")
            settings_area = ui.column().classes("w-full gap-3 mt-4")
            bottom_nav_area = ui.column().classes("rag-drawer-bottom w-full gap-2")

    page_root = ui.column().classes("rag-page gap-5")
    with page_root:
        content = ui.column().classes("w-full gap-5")

    def touch_activity() -> None:
        if not state.auth_token or not state.current_user:
            return
        try:
            _get_auth_db(state).touch_session(state.auth_token, min_interval_minutes=60)
        except Exception:
            pass

    ui.timer(3600.0, touch_activity)

    # ── Встроенный планировщик ────────────────────────────────────────
    def _tick_scheduler() -> None:
        """Каждую минуту проверяет расписание и запускает индексатор при совпадении."""
        if not _is_admin(state):
            return
        tdb = _get_telemetry(state)
        if not hasattr(tdb, "list_index_schedules"):
            return
        try:
            schedules = tdb.list_index_schedules()
            due = _schedules_due(schedules)
            cfg_settings = tdb.get_index_settings() if hasattr(tdb, "get_index_settings") else {}
            for sched in due:
                try:
                    pid = _launch_indexer(
                        state.cfg,
                        stage=str(sched.get("stage") or "all"),
                        workers=int(cfg_settings.get("workers") or state.cfg.get("index_read_workers") or 4),
                        max_chunks=int(cfg_settings.get("max_chunks") or state.cfg.get("index_max_chunks") or 2000),
                        skip_inline_ocr=bool(cfg_settings.get("skip_inline_ocr")),
                    )
                except RuntimeError as exc:
                    _log_app_event(
                        state,
                        "index",
                        "schedule_skip",
                        ok=False,
                        details={"sched_id": sched.get("id"), "reason": str(exc)},
                    )
                    continue
                tdb.touch_index_schedule(id=str(sched["id"]))
                _log_app_event(state, "index", "schedule_run",
                               details={"sched_id": sched["id"], "stage": sched.get("stage"), "pid": pid})
        except Exception:
            pass

    ui.timer(60.0, _tick_scheduler)

    def do_logout() -> None:
        auth_db = _get_auth_db(state)
        if state.auth_token:
            auth_db.revoke_session(state.auth_token)
        auth_db.log_auth_event(username=_username(state), event_type="logout", ok=True)
        state.current_user = None
        state.auth_token = ""
        state.theme = "light"
        dark_mode.set_value(False)
        try:
            app.storage.user.pop("auth_token", None)
        except Exception:
            pass
        render()

    def toggle_theme() -> None:
        if state.current_user is None:
            return
        state.theme = "dark" if state.theme == "light" else "light"
        dark_mode.set_value(state.theme == "dark")
        theme_button.set_icon("light_mode" if state.theme == "dark" else "dark_mode")
        _save_ui_settings(state)
        _log_app_event(state, "ui", "theme_toggle", details={"theme": state.theme})

    def set_screen(screen: str, *, close_drawer: bool = False) -> None:
        touch_activity()
        if close_drawer:
            try:
                drawer.set_value(False)
            except Exception:
                pass
        state.screen = screen
        ui.run_javascript(f"history.pushState(null, '', '/{screen}')")
        _log_app_event(state, "navigation", "open_screen", details={"screen": screen})
        render()

    def go_explorer(path: str) -> None:
        value = str(path or "").strip()
        if value:
            p = Path(value)
            state.explorer_path = str(p.parent if p.is_file() else p)
            state.explorer_page = 0
        set_screen("explorer")

    def update_nav() -> None:
        nav_area.clear()
        with nav_area:
            for screen, label, icon in [
                ("search", "Поиск", "search"),
                ("explorer", "Проводник", "folder"),
            ]:
                color = "primary" if state.screen == screen else None
                ui.button(label, icon=icon, on_click=lambda s=screen: set_screen(s, close_drawer=True), color=color).props("flat align=left no-caps").classes("rag-nav-button w-full")
            if str((state.current_user or {}).get("role") or "") == "admin":
                color = "primary" if state.screen == "index" else None
                ui.button("Индекс", icon="analytics", on_click=lambda: set_screen("index", close_drawer=True), color=color).props("flat align=left no-caps").classes("rag-nav-button w-full")
                color = "primary" if state.screen == "stats" else None
                ui.button("Аналитика", icon="query_stats", on_click=lambda: set_screen("stats", close_drawer=True), color=color).props("flat align=left no-caps").classes("rag-nav-button w-full")

        settings_area.clear()

        bottom_nav_area.clear()
        with bottom_nav_area:
            color = "primary" if state.screen == "settings" else None
            user_label = "Настройки"
            if state.current_user:
                user_label = f"Настройки · {state.current_user.get('username')}"
            ui.button(user_label, icon="settings", on_click=lambda: set_screen("settings", close_drawer=True), color=color).props("flat align=left no-caps").classes("rag-nav-button w-full")
            if state.current_user:
                ui.button("Выйти", icon="logout", on_click=do_logout, color=None).props("flat align=left no-caps").classes("rag-nav-button w-full")

    async def run_search(explicit_query: Optional[str] = None) -> None:
        touch_activity()
        raw = explicit_query if explicit_query is not None else state.query
        query = re.sub(r"\s+", " ", str(raw or "")).strip()
        if not query:
            ui.notify("Введите запрос.", type="warning")
            return
        request_id = state.search_request_id + 1
        state.search_request_id = request_id
        state.query = query
        state.search_error = ""
        state.search_stats_hint = ""
        state.search_lazy_loading = False
        state.results = []
        state.searched_query = query
        state.expanded_query = ""
        state.rag_answer_text = ""
        state.rag_answer_loading = False
        state.displayed_count = 10
        state.active_type_filter = None
        _remember_query(state, query)
        render_results_loading()
        searcher = _ensure_searcher(state)
        if searcher is None or not searcher.connected:
            state.search_error = state.searcher_error or "Нет подключения к Qdrant."
            render()
            return

        llm_enabled = bool(state.cfg.get("llm_enabled"))
        llm_expand_enabled = llm_enabled and bool(state.ai_search_expand)
        ollama_url = str(state.cfg.get("ollama_url") or "http://localhost:11434")
        expand_model = str(state.cfg.get("llm_expand_model") or "phi3:mini")
        rag_model = str(state.cfg.get("llm_rag_model") or "qwen3:8b")
        try:
            quick_results = await run.io_bound(
                _run_quick_name_search,
                searcher,
                query=query,
                limit=state.limit,
                file_type=state.file_type,
            )
            if state.search_request_id != request_id:
                return
            state.results = quick_results
            exact_count = _count_exact_name_matches(query, quick_results)
            state.search_stats_hint = f"Быстро найдено: {len(quick_results)} · точных совпадений: {exact_count}"
            state.search_lazy_loading = True
            render()
            _log_app_event(
                state,
                "search",
                "run_quick",
                details={
                    "query": query,
                    "results": len(quick_results),
                    "exact_matches": exact_count,
                },
            )
        except Exception as exc:
            state.search_error = str(exc)
            state.search_lazy_loading = False
            _log_app_event(
                state,
                "search",
                "run_quick",
                ok=False,
                details={
                    "query": query,
                    "error": str(exc),
                },
            )
            render()
            return

        # Ленивая догрузка: сначала, при необходимости, расширяем запрос через LLM.
        search_query = query
        if llm_expand_enabled:
            try:
                from rag_catalog.core.llm import expand_query  # noqa: PLC0415
                expanded = await run.io_bound(
                    expand_query, query, model=expand_model, ollama_url=ollama_url
                )
                if state.search_request_id != request_id:
                    return
                if expanded and expanded.lower() != query.lower():
                    state.expanded_query = expanded
                    search_query = expanded
            except Exception:
                pass

        try:
            full_results = await run.io_bound(
                _run_catalog_search,
                searcher,
                limit=state.limit,
                file_type=state.file_type,
                content_only=state.content_only,
                title_only=state.title_only,
                username=_username(state),
                query=search_query,
                query_original=query,
                query_used=search_query,
            )
            if state.search_request_id != request_id:
                return
            state.results = _merge_search_results(state.results, full_results, limit=state.limit)
            state.search_stats_hint = (
                f"{state.search_stats_hint} · после догрузки: {len(state.results)}"
                if state.search_stats_hint else f"После догрузки: {len(state.results)}"
            )
            _log_app_event(
                state,
                "search",
                "run_full",
                details={
                    "query": query,
                    "query_used": search_query,
                    "results": len(state.results),
                    "content_only": bool(state.content_only),
                    "title_only": bool(state.title_only),
                },
            )
        except Exception as exc:
            if state.search_request_id != request_id:
                return
            _log_app_event(
                state,
                "search",
                "run_full",
                ok=False,
                details={
                    "query": query,
                    "query_used": search_query,
                    "error": str(exc),
                    "content_only": bool(state.content_only),
                    "title_only": bool(state.title_only),
                },
            )
            if not state.results:
                state.search_error = str(exc)

        # RAG Q&A — только после полной догрузки
        if llm_enabled and state.results and not state.search_error and state.search_request_id == request_id:
            state.rag_answer_loading = True
            render()
            try:
                from rag_catalog.core.llm import rag_answer  # noqa: PLC0415
                answer = await run.io_bound(
                    rag_answer, query, state.results, model=rag_model, ollama_url=ollama_url
                )
                if state.search_request_id != request_id:
                    return
                state.rag_answer_text = answer or ""
            except Exception as exc:
                if state.search_request_id != request_id:
                    return
                state.rag_answer_text = f"Ошибка LLM: {exc}"
            finally:
                if state.search_request_id == request_id:
                    state.rag_answer_loading = False

        if state.search_request_id == request_id:
            state.search_lazy_loading = False
            render()

    async def choose_query(query: str) -> None:
        # Прямой async-обработчик: пресеты больше не зависят от ui.timer и гонок с перерисовкой.
        await run_search(query)

    def choose_query_handler(query: str) -> Any:
        async def handler() -> None:
            await choose_query(query)

        return handler

    def render_suggestions(area: ui.column, typed: str) -> None:
        area.clear()
        username = _username(state)
        personal = _dedupe_queries([*state.history, *_my_recent_queries(state.cfg, username, limit=12)], limit=12)
        popular = _popular_queries(state.cfg, exclude_username=username, limit=10)

        needle = typed.strip().lower()
        if needle:
            personal = [q for q in personal if needle in q.lower()]
            popular = [q for q in popular if needle in q.lower()]
        else:
            personal = personal[:8]
            popular = popular[:8]

        if not personal and not popular:
            return

        with area:
            with ui.row().classes("rag-suggest p-3 gap-0 w-full"):
                # Левая колонка — личная история
                if personal:
                    col_cls = "flex-1 gap-1 min-w-0" + (" pr-3 border-r border-gray-200" if popular else "")
                    with ui.column().classes(col_cls):
                        ui.label("Моя история").classes("rag-meta px-2 py-1 font-semibold text-xs uppercase tracking-wide")
                        for item in personal:
                            btn = ui.button(item, icon="history", on_click=choose_query_handler(item), color=None).props("flat align=left no-caps").classes("rag-nav-button rag-suggest-item w-full")
                            btn.tooltip(item)
                # Правая колонка — часто ищут
                if popular:
                    col_cls = "flex-1 gap-1 min-w-0" + (" pl-3" if personal else "")
                    with ui.column().classes(col_cls):
                        ui.label("Часто ищут").classes("rag-meta px-2 py-1 font-semibold text-xs uppercase tracking-wide")
                        for item in popular:
                            btn = ui.button(item, icon="trending_up", on_click=choose_query_handler(item), color=None).props("flat align=left no-caps").classes("rag-nav-button rag-suggest-item w-full")
                            btn.tooltip(item)

    def render_search_box() -> None:
        with ui.column().classes("rag-search-shell w-full max-w-5xl"):
            suggest_area = ui.column().classes("w-full")
            with ui.row().classes("rag-search-box w-full items-center gap-2 p-2"):
                search_input = ui.input(
                    placeholder="Введите название, номер, контрагента или фразу из документа",
                    value=state.query,
                    autocomplete=_search_suggestions(state),
                ).props("borderless dense clearable input-class=text-base").classes("flex-1")
                ai_expand_checkbox = ui.checkbox("AI", value=bool(state.ai_search_expand)).props("dense").classes("rag-ai-expand")
                ai_expand_checkbox.tooltip("AI-дополнение запроса")
                if not bool(state.cfg.get("llm_enabled")):
                    ai_expand_checkbox.disable()

                def update_ai_expand(event: events.ValueChangeEventArguments) -> None:
                    state.ai_search_expand = bool(event.value)
                    _save_ui_settings(state)
                    _log_app_event(
                        state,
                        "search",
                        "toggle_ai_expand",
                        details={"enabled": state.ai_search_expand},
                    )

                ai_expand_checkbox.on_value_change(update_ai_expand)

                async def submit_click() -> None:
                    await run_search(str(search_input.value or ""))

                ui.button(icon="search", on_click=submit_click, color="primary").props("unelevated round")

            def handle_input(_: events.GenericEventArguments | None = None) -> None:
                state.query = str(search_input.value or "")
                render_suggestions(suggest_area, state.query)

            async def submit_from_input(_: events.GenericEventArguments | None = None) -> None:
                typed = str(search_input.value or "")
                suggest_area.clear()
                await run_search(typed)

            search_input.on("focus", handle_input)
            search_input.on("input", handle_input)
            search_input.on("keyup.enter", submit_from_input)

    def render_results_loading() -> None:
        content.clear()
        with content:
            render_search_header()
            ui.spinner(size="lg").classes("mt-4")
            ui.label("Ищу совпадения...").classes("rag-meta")

    def render_search_header() -> None:
        with ui.column().classes("w-full gap-2"):
            render_search_box()
            render_search_filters_bar()

    def render_search_filters_bar() -> None:
        initial = {
            "file_type": state.file_type or "Все",
            "limit": int(state.limit or 50),
            "content_only": bool(state.content_only),
            "title_only": bool(state.title_only),
        }
        with ui.column().classes("rag-search-toolbar w-full max-w-5xl gap-2"):
            with ui.row().classes("w-full items-end gap-2 flex-wrap"):
                file_type_input = ui.select(
                    ["Все", ".docx", ".xlsx", ".xls", ".pdf"],
                    label="Тип файла",
                    value=initial["file_type"],
                ).props("dense outlined").classes("w-36")
                limit_input = ui.number(
                    "Лимит",
                    value=initial["limit"],
                    min=1,
                    max=50,
                    step=1,
                ).props("dense outlined").classes("w-28")
                content_only_input = ui.checkbox(
                    "Только содержимое",
                    value=initial["content_only"],
                ).classes("min-w-44")
                title_only_input = ui.checkbox(
                    "Только названия",
                    value=initial["title_only"],
                ).classes("min-w-40")
                action_row = ui.row().classes("rag-dirty-actions")
                action_row.set_visibility(False)
                dirty_ready = [False]

            def current_values() -> Dict[str, Any]:
                return {
                    "file_type": str(file_type_input.value or "Все"),
                    "limit": int(limit_input.value or 50),
                    "content_only": bool(content_only_input.value),
                    "title_only": bool(title_only_input.value),
                }

            def refresh_dirty() -> None:
                if dirty_ready[0]:
                    action_row.set_visibility(current_values() != initial)

            def sync_toggle(source: str, value: bool) -> None:
                if source == "content" and value and bool(title_only_input.value):
                    title_only_input.set_value(False)
                if source == "title" and value and bool(content_only_input.value):
                    content_only_input.set_value(False)
                refresh_dirty()

            def reset_changes() -> None:
                file_type_input.set_value(initial["file_type"])
                limit_input.set_value(initial["limit"])
                content_only_input.set_value(initial["content_only"])
                title_only_input.set_value(initial["title_only"])
                action_row.set_visibility(False)

            def apply_changes() -> None:
                values = current_values()
                state.file_type = None if values["file_type"] == "Все" else values["file_type"]
                state.limit = values["limit"]
                state.content_only = values["content_only"]
                state.title_only = values["title_only"]
                initial.update(values)
                action_row.set_visibility(False)
                _log_app_event(
                    state,
                    "search",
                    "save_filters",
                    details={
                        "file_type": state.file_type or "Все",
                        "limit": state.limit,
                        "content_only": state.content_only,
                        "title_only": state.title_only,
                    },
                )
                ui.notify("Параметры поиска применены.", type="positive")

            file_type_input.on_value_change(lambda _: refresh_dirty())
            limit_input.on_value_change(lambda _: refresh_dirty())
            content_only_input.on_value_change(lambda e: sync_toggle("content", bool(e.value)))
            title_only_input.on_value_change(lambda e: sync_toggle("title", bool(e.value)))
            dirty_ready[0] = True
            with action_row:
                with ui.row().classes("rag-dirty-actions-inner"):
                    ui.button("Отменить", icon="close", on_click=reset_changes).props("flat dense")
                    ui.button("Применить", icon="done", on_click=apply_changes).props("unelevated dense")

    def open_file_viewer(path_value: Path | str) -> None:
        candidate = _resolve_catalog_file(state.cfg, str(path_value or ""))
        if candidate is None:
            ui.notify("Файл недоступен для просмотра.", type="warning")
            return
        viewer_url = _viewer_file_url(str(candidate))
        ext = candidate.suffix.lower()

        with ui.dialog() as dialog, ui.card().classes("w-[min(1100px,96vw)] max-h-[90vh] overflow-auto gap-3"):
            with ui.row().classes("w-full items-center gap-2"):
                with ui.column().classes("min-w-0 flex-1 gap-0"):
                    ui.label(candidate.name).classes("text-lg font-semibold truncate")
                    ui.label(str(candidate)).classes("rag-path")
                ui.button("Скачать", icon="download", on_click=lambda p=candidate: ui.download(p, filename=p.name)).props("outline dense")
                ui.button("Открыть в ОС", icon="open_in_new", on_click=lambda p=candidate: _open_os_path(str(p.parent))).props("outline dense")
                ui.button(icon="close", on_click=dialog.close, color=None).props("flat round dense")

            if ext == ".pdf":
                ui.html(
                    f'<iframe src="{html.escape(viewer_url, quote=True)}" '
                    'style="width:100%; height:72vh; border:1px solid rgba(148,163,184,.45); border-radius:10px;"></iframe>',
                    sanitize=False,
                )
            elif ext in INLINE_IMAGE_EXTENSIONS:
                ui.image(viewer_url).classes("max-w-full max-h-[72vh] object-contain mx-auto")
            elif ext in FILE_PREVIEW_EXTENSIONS:
                ui.label(_preview_file(candidate, limit=32000)).classes("rag-code")
            elif ext in OFFICE_PREVIEW_EXTENSIONS:
                ui.label(_preview_office_file(candidate, limit=32000)).classes("rag-code")
                ui.label("Для офисных форматов показывается текстовый извлеченный фрагмент.").classes("rag-meta")
            else:
                ui.label("Встроенный просмотр для этого формата не поддерживается. Используйте скачивание или открытие в ОС.").classes("rag-meta")
        dialog.open()

    def render_result(result: Dict[str, Any], index: int) -> None:
        name = str(result.get("filename") or "Без имени")
        path = str(result.get("path") or "")
        full_path = str(result.get("full_path") or "")
        score = float(result.get("score") or 0)
        kind = _result_kind(result)
        text = _clean_text(result.get("text") or "")
        preview = text[:280] + ("..." if len(text) > 280 else "")
        p = Path(full_path) if full_path else None

        def rate_result(value: int, result: Dict[str, Any] = result, index: int = index) -> None:
            result_path = str(result.get("full_path") or result.get("path") or "")
            _get_telemetry(state).log_search_feedback(
                username=_username(state),
                source="nicegui",
                query=state.searched_query,
                result_path=result_path,
                result_title=str(result.get("filename") or result_path),
                feedback=value,
                result_rank=index,
                result_score=float(result.get("score") or 0),
                details={"screen": "search", "reason": "explicit"},
            )
            _log_app_event(
                state,
                "search",
                "feedback",
                details={"value": value, "path": result_path, "query": state.searched_query},
            )
            ui.notify("Оценка сохранена.", type="positive")

        def track_result_use(reason: str, result: Dict[str, Any] = result, index: int = index) -> None:
            result_path = str(result.get("full_path") or result.get("path") or "")
            try:
                _get_telemetry(state).log_search_feedback(
                    username=_username(state),
                    source="nicegui",
                    query=state.searched_query,
                    result_path=result_path,
                    result_title=str(result.get("filename") or result_path),
                    feedback=2,
                    result_rank=index,
                    result_score=float(result.get("score") or 0),
                    details={"screen": "search", "reason": reason},
                )
            except Exception:
                pass

        def open_primary() -> None:
            if kind == "Каталог":
                track_result_use("open_folder")
                go_explorer(full_path)
                return
            if p and p.exists() and p.is_file():
                track_result_use("open_viewer")
                open_file_viewer(p)

        with ui.column().classes("rag-result gap-2"):
            with ui.row().classes("w-full items-start gap-2"):
                opener = ui.row().classes("flex-1 min-w-0 items-start gap-2 cursor-pointer").on("click", open_primary)
                with opener:
                    ui.html(_file_icon_svg(full_path or path, kind), sanitize=False)
                    with ui.column().classes("flex-1 min-w-0 gap-0"):
                        title = ui.label(f"{index}. {name}").classes("text-base font-semibold truncate")
                        title.tooltip(name)
                        path_label = ui.label(path or full_path).classes("rag-path truncate")
                        path_label.tooltip(path or full_path)
                ui.label(f"{kind} · {score:.3f}").classes("rag-chip")

            with ui.row().classes("w-full items-center justify-between gap-2"):
                with ui.row().classes("rag-actions items-center"):
                    if full_path:
                        if kind == "Каталог":
                            ui.button("В проводник приложения", icon="folder_open", on_click=lambda p=full_path: go_explorer(p)).props("outline dense")
                        else:
                            def open_in_app_explorer(pth: str = full_path) -> None:
                                track_result_use("open_in_app_explorer")
                                go_explorer(pth)

                            ui.button("В проводник приложения", icon="folder", on_click=open_in_app_explorer).props("outline dense")
                            if p and p.exists() and p.is_file():
                                ui.button("Скачать", icon="download", on_click=lambda pth=p: ui.download(pth, filename=pth.name)).props("outline dense")
                        ui.button("Открыть в ОС", icon="open_in_new", on_click=lambda pth=full_path: _open_os_path(str(Path(pth).parent if kind != "Каталог" else pth))).props("outline dense")
                with ui.row().classes("items-center justify-end gap-1"):
                    bad = ui.button(icon="thumb_down", on_click=lambda: rate_result(-3), color=None).props("flat round dense")
                    bad.classes("rag-feedback-btn")
                    bad.tooltip("Не то")
                    good = ui.button(icon="thumb_up", on_click=lambda: rate_result(3), color=None).props("flat round dense")
                    good.classes("rag-feedback-btn")
                    good.tooltip("Полезно")

            if kind == "Каталог":
                with ui.expansion("Раскрыть каталог", icon="account_tree").classes("w-full"):
                    children = _directory_children(full_path)
                    if not children["exists"]:
                        ui.label("Каталог недоступен на диске.").classes("rag-meta")
                    elif children.get("error"):
                        ui.label(f"Не удалось прочитать каталог: {children['error']}").classes("text-red-700")
                    else:
                        if children["dirs"]:
                            ui.label("Папки").classes("font-semibold")
                            with ui.column().classes("w-full gap-1"):
                                for item in children["dirs"]:
                                    btn = ui.button(item["name"], icon="folder", on_click=lambda pth=item["path"]: go_explorer(pth), color=None).props("flat align=left no-caps").classes("rag-nav-button w-full")
                                    btn.tooltip(str(item["path"]))
                        if children["files"]:
                            ui.label("Файлы").classes("font-semibold mt-2")
                            with ui.column().classes("w-full gap-1"):
                                for item in children["files"]:
                                    item_path = Path(str(item["path"]))
                                    with ui.row().classes("w-full items-center gap-2"):
                                        ui.html(_file_icon_svg(str(item_path), "Файл"), sanitize=False)
                                        file_btn = ui.button(
                                            f"{item['name']} · {item.get('size', '')}",
                                            on_click=lambda pth=item_path: open_file_viewer(pth),
                                            color=None,
                                        ).props("flat align=left no-caps dense").classes("rag-nav-button flex-1")
                                        file_btn.tooltip(str(item_path))
                        if children.get("truncated"):
                            ui.label("Показаны первые элементы. Полный список доступен в проводнике приложения.").classes("rag-meta")
            elif preview:
                ui.label(preview).classes("rag-meta")

    def render_search_screen() -> None:
        render_search_header()
        if state.search_error:
            ui.label(state.search_error).classes("text-red-700 rag-card p-4")
        if not state.searched_query:
            with ui.row().classes("w-full gap-3"):
                for label, query in SEARCH_PRESETS:
                    ui.button(label, on_click=choose_query_handler(query)).props("outline")
            return
        # Заголовок с опциональной подсказкой о расширении запроса
        with ui.row().classes("w-full items-center gap-2 mt-2"):
            ui.label(f"Результаты по запросу: {state.searched_query}").classes("text-xl font-semibold")
            if state.expanded_query:
                ui.label(f"→ расширен: {state.expanded_query}").classes("rag-meta text-sm italic")
        if state.search_stats_hint:
            ui.label(state.search_stats_hint).classes("rag-meta")
        if state.search_lazy_loading:
            with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
                ui.spinner(size="sm")
                ui.label("Догружаю дополнительные совпадения…").classes("rag-meta")

        # RAG Q&A карточка
        if state.rag_answer_loading:
            with ui.row().classes("rag-card w-full p-3 gap-2 items-center"):
                ui.spinner(size="sm")
                ui.label("Анализирую документы…").classes("rag-meta")
        elif state.rag_answer_text:
            with ui.column().classes("rag-card w-full p-3 gap-1"):
                with ui.row().classes("items-center gap-1"):
                    ui.icon("smart_toy", size="18px").classes("text-indigo-500")
                    ui.label("Ответ ИИ").classes("font-semibold text-sm text-indigo-700")
                ui.label(state.rag_answer_text).classes("text-sm whitespace-pre-wrap")

        if not state.results:
            ui.label("Совпадений не найдено.").classes("rag-card p-4 rag-meta")
            return

        # Все результаты — плоский список, отсортированный по релевантности
        sorted_results = sorted(
            state.results,
            key=lambda r: float(r.get("rank_score", r.get("score") or 0) or 0),
            reverse=True,
        )

        # Считаем кол-во по группам для чипов
        group_counts: Dict[str, int] = {}
        for r in sorted_results:
            group_counts.setdefault(_result_group(r), 0)
            group_counts[_result_group(r)] += 1

        # Порядок групп как был в _grouped_results
        group_order = [
            "Каталоги", "Техпаспорта ТС", "Паспорта и удостоверения",
            "Договоры", "Счета и платежи", "Таблицы", "PDF", "Другие файлы",
        ]

        def set_filter(gname: Optional[str]) -> None:
            state.active_type_filter = gname
            state.displayed_count = 10
            render()

        # Чипы-фильтры
        with ui.row().classes("w-full gap-2 flex-wrap"):
            # «Все»
            all_active = state.active_type_filter is None
            all_chip = ui.label(f"Все: {len(sorted_results)}").classes(
                "rag-chip" + (" rag-chip-active" if all_active else "")
            )
            all_chip.on("click", lambda: set_filter(None))
            # По типам
            for gname in group_order:
                cnt = group_counts.get(gname, 0)
                if cnt == 0:
                    continue
                is_active = state.active_type_filter == gname
                chip = ui.label(f"{gname}: {cnt}").classes(
                    "rag-chip" + (" rag-chip-active" if is_active else "")
                )
                chip.on("click", lambda g=gname: set_filter(g))

        # Применяем фильтр
        if state.active_type_filter:
            visible = [r for r in sorted_results if _result_group(r) == state.active_type_filter]
        else:
            visible = sorted_results

        # Показываем первые displayed_count штук
        to_show = visible[: state.displayed_count]
        with ui.column().classes("w-full gap-3"):
            for idx, result in enumerate(to_show, 1):
                render_result(result, idx)

        # Кнопка «Загрузить ещё»
        remaining = len(visible) - state.displayed_count
        if remaining > 0:
            def load_more() -> None:
                state.displayed_count += 10
                render()

            ui.button(
                f"Загрузить ещё  ({remaining})",
                on_click=load_more,
                icon="expand_more",
            ).props("outline no-caps").classes("w-full mt-1")

    def render_explorer_screen() -> None:
        root = Path(str(state.cfg.get("catalog_path") or ""))
        if not root.exists():
            ui.label(f"Каталог не найден: {root}").classes("text-red-700 rag-card p-4")
            return

        toolbar = ui.column().classes("w-full gap-3")
        with ui.row().classes("rag-explorer-v2-layout w-full gap-3 items-start"):
            tree_area = ui.column().classes("rag-explorer-tree rag-card p-3 gap-2")
            entries_area = ui.column().classes("rag-explorer-files rag-card p-3 gap-3")
            details_area = ui.column().classes("rag-explorer-details rag-card p-3 gap-3")

        def open_folder(path: Path) -> None:
            state.explorer_path = str(path)
            state.explorer_page = 0
            _get_auth_db(state).touch_favorite(username=_username(state), path=str(path))
            _log_app_event(state, "explorer", "open_folder", details={"path": str(path)})
            render()

        def open_file(path: Path) -> None:
            if path.exists() and path.is_file():
                _get_auth_db(state).touch_favorite(username=_username(state), path=str(path))
                _log_app_event(state, "explorer", "open_file", details={"path": str(path)})
                open_file_viewer(path)

        def copy_path(path: Path) -> None:
            ui.run_javascript(f"navigator.clipboard.writeText({json.dumps(str(path))})")
            ui.notify("Путь скопирован.", type="positive")

        def explorer_context_props(path: Path, *, is_dir: bool) -> str:
            item_type = "folder" if is_dir else "file"
            item_url = "" if is_dir else _viewer_file_url(str(path))
            favorite = "true" if _is_favorite(state, str(path)) else "false"
            attrs = {
                "data-rag-context": "explorer-item",
                "data-rag-type": item_type,
                "data-rag-path": quote(str(path), safe=""),
                "data-rag-url": item_url,
                "data-rag-favorite": favorite,
            }
            return " ".join(f'{key}="{html.escape(value, quote=True)}"' for key, value in attrs.items())

        def render_star(path: Path, *, item_type: Optional[str] = None) -> None:
            active = _is_favorite(state, str(path))
            icon = "star" if active else "star_border"
            star = ui.button(icon=icon, color=None).props("flat round dense data-rag-favorite-button")
            star.classes("rag-favorite-star active" if active else "rag-favorite-star")
            star.tooltip("Убрать из избранного" if active else "Добавить в избранное")

            def toggle() -> None:
                _toggle_favorite(state, path, item_type=item_type)
                render()

            star.on("click.stop", toggle)

        def open_favorites_dialog() -> None:
            with ui.dialog() as dialog, ui.card().classes("w-[min(900px,92vw)] max-h-[80vh] overflow-auto gap-3"):
                ui.label("Избранное").classes("text-xl font-semibold")
                if not state.favorites:
                    ui.label("Закладок пока нет.").classes("rag-meta")
                for fav in state.favorites:
                    fav_path = Path(str(fav.get("path") or ""))
                    item_type = str(fav.get("item_type") or "file")
                    label = str(fav.get("title") or fav_path.name or fav_path)
                    with ui.element("div").classes("rag-favorites-dialog-row"):
                        ui.icon("folder" if item_type == "folder" else "description")
                        with ui.column().classes("min-w-0 gap-0"):
                            ui.label(label).classes("font-medium truncate")
                            ui.label(str(fav_path)).classes("rag-path truncate")
                        action = (lambda p=fav_path: (dialog.close(), open_folder(p))) if item_type == "folder" else (lambda p=fav_path: (dialog.close(), open_file(p)))
                        ui.button("Открыть", on_click=action).props("outline dense")
                        ui.button(icon="close", on_click=lambda p=fav_path: (_toggle_favorite(state, p), dialog.close(), render())).props("flat round dense").tooltip("Убрать из избранного")
                ui.button("Закрыть", on_click=dialog.close).props("flat")
            dialog.open()

        def render_tile(path: Path, is_dir: bool, size_class: str) -> None:
            icon = _file_icon_svg(str(path), "Каталог" if is_dir else "Файл")
            click = (lambda p=path: open_folder(p)) if is_dir else (lambda p=path: open_file(p))
            system_class = " system" if not is_dir and _is_system_file(path) else ""
            tile = ui.column().classes(f"rag-explorer-item items-center gap-1 p-2 {size_class}{system_class}")
            tile.props(explorer_context_props(path, is_dir=is_dir))
            with tile:
                with ui.element("div").classes("rag-tile-star-wrap"):
                    render_star(path, item_type="folder" if is_dir else "file")
                opener = ui.column().classes("rag-explorer-opener items-center gap-1 cursor-pointer").on("click", click)
                opener.props("data-rag-open")
                with opener:
                    ui.html(icon, sanitize=False)
                    name_label = ui.label(path.name).classes("rag-explorer-name text-center text-sm")
                    name_label.tooltip(str(path.name))
                os_button = ui.button(on_click=lambda p=path: _open_os_path(str(p.parent if p.is_file() else p))).props("data-rag-os")
                os_button.classes("hidden")

        def render_row(path: Path, is_dir: bool, compact: bool = False) -> None:
            try:
                stat = path.stat()
                size = "" if is_dir else _format_file_size(stat.st_size)
                modified = time.strftime("%d.%m.%Y %H:%M", time.localtime(stat.st_mtime))
            except Exception:
                size, modified = "", ""
            system_class = " system" if not is_dir and _is_system_file(path) else ""
            row = ui.row().classes(f"rag-explorer-item w-full p-2 items-center gap-3{system_class}")
            row.props(explorer_context_props(path, is_dir=is_dir))
            with row:
                ui.html(_file_icon_svg(str(path), "Каталог" if is_dir else "Файл"), sanitize=False)
                action = (lambda p=path: open_folder(p)) if is_dir else (lambda p=path: open_file(p))
                with ui.column().classes("flex-1 gap-0"):
                    open_btn = ui.button(path.name, on_click=action, color=None).props("flat align=left no-caps dense data-rag-open").classes("rag-nav-button w-full")
                    open_btn.tooltip(str(path.name))
                    if not compact:
                        ui.label(f"{'Папка' if is_dir else path.suffix or 'без расширения'} · {size} · {modified}").classes("rag-meta")
                if not compact:
                    if not is_dir:
                        ui.button("Скачать", icon="download", on_click=lambda p=path: (_log_app_event(state, "explorer", "download", details={"path": str(p)}), ui.download(p, filename=p.name))).props("outline dense")
                    ui.button("ОС", icon="open_in_new", on_click=lambda p=path: _open_os_path(str(p.parent if p.is_file() else p))).props("flat dense data-rag-os")
                else:
                    os_button = ui.button(on_click=lambda p=path: _open_os_path(str(p.parent if p.is_file() else p))).props("data-rag-os")
                    os_button.classes("hidden")
                render_star(path, item_type="folder" if is_dir else "file")

        def _explorer_path_parts(root_path: Path, current_path: Path) -> List[Path]:
            parts: List[Path] = []
            p = current_path
            while True:
                parts.append(p)
                if p == root_path or p == p.parent:
                    break
                p = p.parent
            parts.reverse()
            return parts

        def _child_dirs(path: Path) -> List[Path]:
            try:
                return sorted(
                    [p for p in path.iterdir() if p.is_dir() and not p.name.startswith(".")],
                    key=lambda p: p.name.lower(),
                )
            except Exception:
                return []

        def render_breadcrumbs(root_path: Path, current_path: Path) -> None:
            parts = _explorer_path_parts(root_path, current_path)
            with ui.row().classes("rag-breadcrumbs flex-1 min-w-0 items-center gap-1 no-wrap"):
                for idx, part in enumerate(parts):
                    label = "Обмен" if part == root_path else part.name
                    btn = ui.button(
                        label,
                        on_click=lambda p=part: (_log_app_event(state, "explorer", "breadcrumb", details={"path": str(p)}), open_folder(p)),
                        color=None,
                    ).props("flat dense no-caps")
                    btn.tooltip(str(part))
                    if idx < len(parts) - 1:
                        ui.icon("chevron_right").classes("text-slate-400")

        def render_tree_node(path: Path, depth: int, current_path: Path, ancestors: set[str]) -> None:
            path_key = str(path)
            is_current = path_key == str(current_path)
            is_ancestor = path_key in ancestors and not is_current
            class_bits = ["rag-nav-button", "rag-tree-button", "w-full"]
            if is_current:
                class_bits.append("active")
            elif is_ancestor:
                class_bits.append("ancestor")
            icon = "folder_open" if is_current or is_ancestor else "folder"
            label = "Обмен" if path == root else path.name
            btn = ui.button(
                label,
                icon=icon,
                on_click=lambda p=path: open_folder(p),
                color=None,
            ).props("flat align=left no-caps dense").classes(" ".join(class_bits)).style(f"padding-left: {depth * 12}px")
            btn.tooltip(str(path))
            if is_current or is_ancestor:
                for child in _child_dirs(path):
                    render_tree_node(child, depth + 1, current_path, ancestors)

        def render_explorer_details() -> None:
            details_area.clear()
            with details_area:
                ui.label("Фильтры").classes("text-lg font-semibold")
                type_active = state.explorer_ext != "Все"
                sort_active = state.explorer_sort != "По имени" or state.explorer_desc
                view_active = state.explorer_view != "Таблица"
                name_active = bool(state.explorer_filter.strip())
                with ui.row().classes("w-full gap-2 flex-wrap"):
                    ui.label(f"Тип: {state.explorer_ext}").classes("rag-chip rag-filter-chip" + (" active" if type_active else ""))
                    ui.label(f"Вид: {state.explorer_view}").classes("rag-chip rag-filter-chip" + (" active" if view_active else ""))
                    ui.label(
                        f"{state.explorer_sort} · {'убывание' if state.explorer_desc else 'возрастание'}"
                    ).classes("rag-chip rag-filter-chip" + (" active" if sort_active else ""))
                    if name_active:
                        ui.label(f"Имя: {state.explorer_filter}").classes("rag-chip rag-filter-chip active")
                ui.separator()
                ui.label("Свойства").classes("text-lg font-semibold")
                current_details = _safe_explorer_path(state)
                ui.label(current_details.name or str(current_details)).classes("font-semibold truncate")
                ui.label(str(current_details)).classes("rag-path")

        def render_entries() -> None:
            entries_area.clear()
            current = _safe_explorer_path(state)
            if not current.exists():
                state.explorer_path = str(root)
                current = root

            parts = _explorer_path_parts(root, current)

            if state.header_breadcrumbs is not None:
                state.header_breadcrumbs.clear()
                with state.header_breadcrumbs:
                    for idx, part in enumerate(parts):
                        label = "Корень" if part == root else part.name
                        ui.button(label, on_click=lambda p=part: (_log_app_event(state, "explorer", "breadcrumb", details={"path": str(p)}), open_folder(p)), color=None).props("flat dense no-caps")
                        if idx < len(parts) - 1:
                            ui.icon("chevron_right").classes("text-slate-400")

            if state.header_explorer_actions is not None:
                state.header_explorer_actions.clear()
                with state.header_explorer_actions:
                    active = _is_favorite(state, str(current))
                    fav = ui.button(icon="star" if active else "star_border", color=None).props("flat round dense")
                    fav.classes("rag-favorite-star header active" if active else "rag-favorite-star header")
                    fav.tooltip("Убрать текущую папку из избранного" if active else "Добавить текущую папку в избранное")
                    fav.on("click", lambda p=current: (_toggle_favorite(state, p, item_type="folder"), render()))

            dirs, files, total_files = _file_rows(current, state)
            state.explorer_page = max(0, min(state.explorer_page, max(0, (len(files) - 1) // PAGE_SIZE)))
            page_files = files[state.explorer_page * PAGE_SIZE : (state.explorer_page + 1) * PAGE_SIZE]

            with entries_area:
                with ui.row().classes("w-full items-center gap-2"):
                    up_button = ui.button(icon="arrow_upward", on_click=lambda: (_log_app_event(state, "explorer", "up", details={"path": str(current.parent)}), open_folder(current.parent)), color=None).props("outline round dense")
                    up_button.tooltip("На уровень выше")
                    if current == root:
                        up_button.disable()
                    ui.label(f"папок {len(dirs)} · файлов {total_files}").classes("rag-path")

                if state.favorites:
                    with ui.row().classes("rag-bookmarks"):
                        for fav in state.favorites:
                            fav_path = Path(str(fav.get("path") or ""))
                            item_type = str(fav.get("item_type") or "file")
                            label = str(fav.get("title") or fav_path.name or fav_path)
                            icon = "folder" if item_type == "folder" else "description"
                            action = (lambda p=fav_path: open_folder(p)) if item_type == "folder" else (lambda p=fav_path: open_file(p))
                            with ui.element("div").classes("rag-bookmark"):
                                with ui.element("div").classes("rag-bookmark-main"):
                                    button = ui.button(label, icon=icon, on_click=action, color=None).props("flat dense no-caps").classes("rag-nav-button")
                                    button.tooltip(label)
                                with ui.element("div").classes("rag-bookmark-remove"):
                                    remove_button = ui.button(icon="close", color=None).props("flat round dense")
                                    remove_button.tooltip("Убрать из избранного")
                                    remove_button.on("click.stop", lambda p=fav_path: (_toggle_favorite(state, p), render()))
                        ui.button(icon="more_horiz", on_click=open_favorites_dialog, color=None).props("outline round dense").classes("rag-bookmark-more").tooltip("Показать все избранное")

                if not dirs and not files:
                    ui.label("Нет элементов, соответствующих фильтру.").classes("rag-card p-4 rag-meta")
                    return

                if state.explorer_view in {"Крупные значки", "Средние значки", "Мелкие значки"}:
                    grid_class = {
                        "Крупные значки": "",
                        "Средние значки": "medium",
                        "Мелкие значки": "small",
                    }[state.explorer_view]
                    with ui.element("div").classes(f"rag-explorer-grid {grid_class} w-full"):
                        for path in [*dirs, *page_files]:
                            render_tile(path, path.is_dir(), grid_class)
                elif state.explorer_view == "Список":
                    with ui.column().classes("rag-explorer-list w-full"):
                        for path in [*dirs, *page_files]:
                            render_row(path, path.is_dir(), compact=True)
                else:
                    with ui.column().classes("w-full gap-2"):
                        for path in [*dirs, *page_files]:
                            render_row(path, path.is_dir(), compact=False)

                if total_files > PAGE_SIZE:
                    with ui.row().classes("items-center gap-2"):
                        ui.button("Назад", on_click=lambda: (setattr(state, "explorer_page", max(0, state.explorer_page - 1)), render_entries())).props("outline")
                        ui.label(f"Страница {state.explorer_page + 1} из {(total_files + PAGE_SIZE - 1) // PAGE_SIZE}").classes("rag-meta")
                        ui.button("Вперед", on_click=lambda: (setattr(state, "explorer_page", state.explorer_page + 1), render_entries())).props("outline")

        with tree_area:
            ui.input(placeholder="Фильтр по дереву").props("dense outlined clearable").classes("w-full")
            ui.label("ИЗБРАННОЕ").classes("rag-section-label")
            if state.favorites:
                for fav in state.favorites[:5]:
                    fav_path = Path(str(fav.get("path") or ""))
                    ui.button(
                        str(fav.get("title") or fav_path.name or fav_path),
                        icon="folder" if str(fav.get("item_type") or "") == "folder" else "description",
                        on_click=lambda p=fav_path: go_explorer(str(p)),
                        color=None,
                    ).props("flat align=left no-caps dense").classes("rag-nav-button rag-tree-button w-full")
            else:
                ui.label("Нет закреплённых элементов").classes("rag-meta")
            ui.label("ДЕРЕВО").classes("rag-section-label")
            current_tree_path = _safe_explorer_path(state)
            current_ancestors = {str(part) for part in _explorer_path_parts(root, current_tree_path)}
            render_tree_node(root, 0, current_tree_path, current_ancestors)

        render_explorer_details()

        with toolbar:
            current_for_toolbar = _safe_explorer_path(state)
            with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
                ui.button(icon="arrow_back", color=None).props("flat round dense").tooltip("Назад")
                ui.button(icon="arrow_forward", color=None).props("flat round dense").tooltip("Вперёд")
                up_btn = ui.button(icon="arrow_upward", on_click=lambda: open_folder(current_for_toolbar.parent), color=None).props("flat round dense")
                if current_for_toolbar == root:
                    up_btn.disable()
                render_breadcrumbs(root, current_for_toolbar)
                ui.button(icon="refresh", on_click=lambda: render(), color=None).props("flat round dense").tooltip("Обновить")
                render_star(current_for_toolbar, item_type="folder")
            with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
                ui.icon("search").classes("text-lg")
                ui.input(placeholder="Семантический поиск только в этой папке").props("borderless dense").classes("flex-1")
                ui.checkbox("Включая подпапки", value=True)
                ui.checkbox("AI", value=bool(state.cfg.get("llm_enabled")))
            with ui.row().classes("rag-card w-full p-3 gap-3 items-center"):
                filter_input = ui.input(placeholder="Фильтр по имени", value=state.explorer_filter).props("dense outlined clearable debounce=0").classes("min-w-64 flex-1")

                def update_explorer_setting(attr: str, value: Any) -> None:
                    setattr(state, attr, value)
                    state.explorer_page = 0
                    _save_explorer_settings(state)
                    _log_app_event(state, "explorer", "change_setting", details={attr: value})
                    render()

                ui.select(["Все", ".docx", ".xlsx", ".xls", ".pdf"], value=state.explorer_ext, on_change=lambda e: update_explorer_setting("explorer_ext", e.value)).props("dense outlined").classes("w-36")
                ui.select(["Крупные значки", "Средние значки", "Мелкие значки", "Список", "Таблица"], value=state.explorer_view, on_change=lambda e: update_explorer_setting("explorer_view", e.value)).props("dense outlined").classes("w-44")
                ui.select(["По имени", "По размеру", "По дате"], value=state.explorer_sort, on_change=lambda e: update_explorer_setting("explorer_sort", e.value)).props("dense outlined").classes("w-40")
                ui.select(["По возрастанию", "По убыванию"], value="По убыванию" if state.explorer_desc else "По возрастанию", on_change=lambda e: update_explorer_setting("explorer_desc", e.value == "По убыванию")).props("dense outlined").classes("w-44")

                def apply_filter(event: events.ValueChangeEventArguments | events.GenericEventArguments | None = None) -> None:
                    _apply_explorer_filter_input(state, event, filter_input.value)
                    render()

                filter_input.on_value_change(apply_filter)

        render_entries()

    def render_index_screen() -> None:  # noqa: C901
        if not _is_admin(state):
            ui.label("Раздел индексирования доступен только администратору.").classes("rag-card p-4 text-red-700")
            return
        stats = _read_index_stats(state.cfg)
        telemetry = _read_index_telemetry(state.cfg)
        settings_db = _get_telemetry(state)
        settings = settings_db.get_index_settings() if hasattr(settings_db, "get_index_settings") else {}

        with ui.row().classes("w-full items-center gap-2"):
            ui.label("Индексация").classes("text-2xl font-semibold")
            active_label = "running" if (telemetry.get("active_stages") or telemetry.get("active_ocr")) else "idle"
            ui.label(f"● {active_label}").classes("rag-chip")
            ui.space()
            ui.button("Пауза", icon="pause", on_click=lambda: ui.notify("Пауза будет доступна после добавления cooperative-cancel в worker.", type="warning")).props("outline dense")
            ui.button("Отмена", icon="close", on_click=lambda: ui.notify("Отмена будет доступна после добавления cooperative-cancel в worker.", type="warning")).props("outline dense color=negative")
        ui.label("Этапы, OCR, расписание и параметры индексирования.").classes("rag-meta")

        # ── Метрики ──────────────────────────────────────────────────────
        format_tooltip = "\n".join(
            f"{ext}: {count:,} · {_format_bytes((stats.get('by_ext_size') or {}).get(ext, 0))}".replace(",", " ")
            for ext, count in list((stats.get("by_ext") or {}).items())[:30]
        )

        def render_metric(label: str, value: str, icon: str = "analytics", tooltip_text: str = "") -> None:
            card = ui.column().classes("rag-card p-4 gap-1 min-w-52 flex-1")
            if tooltip_text:
                card.tooltip(tooltip_text)
            with card:
                with ui.row().classes("items-center gap-2"):
                    ui.icon(icon).classes("text-xl")
                    ui.label(label).classes("rag-meta")
                ui.label(value).classes("text-xl font-semibold")

        with ui.row().classes("w-full gap-3"):
            render_metric("Файлов в state", f"{stats['total']:,}".replace(",", " "), "description", format_tooltip)
            render_metric("Размер файлов", _format_bytes(stats.get("total_size_bytes")), "storage")
            render_metric("State обновлен", str(stats.get("last_modified") or "не найден"), "schedule")
            overall = telemetry.get("overall") or {}
            render_metric("Средняя длительность", _format_duration_seconds(overall.get("avg_duration_sec")), "timer")

        # ── Pipeline: запуск, прогресс и OCR ─────────────────────────────
        workers_now = int(settings.get("workers") or state.cfg.get("index_read_workers") or 4)
        chunks_now = int(settings.get("max_chunks") or state.cfg.get("index_max_chunks") or 2000)
        skip_ocr_now = bool(settings.get("skip_inline_ocr"))
        ocr_min_len_now = int(settings.get("ocr_min_text_len") or 50)
        active_ocr = telemetry.get("active_ocr")
        last_ocr = telemetry.get("last_ocr")
        ocr_summary = telemetry.get("ocr_summary") or {}

        def make_run_handler(stage_key: str) -> Any:
            def handler() -> None:
                try:
                    pid = _launch_indexer(
                        state.cfg,
                        stage=stage_key,
                        workers=workers_now,
                        max_chunks=chunks_now,
                        skip_inline_ocr=skip_ocr_now,
                    )
                except RuntimeError as exc:
                    ui.notify(str(exc), type="warning")
                    return
                _log_app_event(state, "index", "run_now", details={"stage": stage_key, "pid": pid})
                ui.notify(f"Индексация «{_STAGE_LABELS.get(stage_key, stage_key)}» запущена (PID {pid}).", type="positive")

            return handler

        def run_ocr_now() -> None:
            try:
                pid = _launch_ocr(
                    state.cfg,
                    min_text_len=ocr_min_len_now,
                    workers=workers_now,
                )
            except RuntimeError as exc:
                ui.notify(str(exc), type="warning")
                return
            _log_app_event(state, "index", "run_ocr_now", details={"pid": pid})
            ui.notify(f"OCR-проход запущен (PID {pid}).", type="positive")

        stage_status_ctx: Dict[str, Any] = {
            "row": {},
            "log_path": PROJECT_ROOT / "logs" / "indexer.log",
        }
        with ui.dialog() as stage_status_dialog, ui.card().classes("w-[min(1200px,96vw)] max-h-[90vh] overflow-auto p-4 gap-3"):
            stage_status_title = ui.label("Статус этапа").classes("text-lg font-semibold")
            stage_status_run = ui.label("Run ID: -").classes("rag-meta")
            stage_status_note_title = ui.label("Сообщение рана").classes("font-semibold")
            stage_status_note_value = ui.label("").classes("rag-meta")
            stage_status_log_path = ui.label("").classes("rag-meta text-xs")
            with ui.row().classes("w-full items-end gap-3 flex-wrap"):
                stage_status_lines = ui.number("Записей", value=200, min=20, max=5000, step=20).props("dense outlined").classes("w-32")
                stage_status_level = ui.select(
                    options={
                        "all": "Все",
                        "info": "INFO",
                        "warning": "WARNING",
                        "error": "ERROR",
                        "debug": "DEBUG",
                    },
                    value="all",
                    label="Уровень",
                ).props("dense outlined").classes("w-40")
                stage_status_autorefresh = ui.checkbox("Автообновление", value=True)
            stage_status_log = ui.textarea().props("readonly outlined autogrow").classes("w-full text-xs font-mono")
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Обновить", icon="refresh", on_click=lambda: _refresh_stage_status_log(force=True)).props("outline")
                ui.button("Копировать", icon="content_copy", on_click=lambda: _copy_stage_status_log()).props("outline")
                ui.button("Скачать", icon="download", on_click=lambda: _download_stage_status_log()).props("outline")
                ui.button("Закрыть", on_click=stage_status_dialog.close).props("unelevated")

        def _refresh_stage_status_log(*, force: bool = False) -> None:
            is_open = bool(getattr(stage_status_dialog, "value", False))
            if not is_open:
                return
            if not force and not bool(stage_status_autorefresh.value):
                return
            log_path = Path(stage_status_ctx.get("log_path") or PROJECT_ROOT / "logs" / "indexer.log")
            line_count = max(20, _safe_int(stage_status_lines.value, 200))
            raw_tail = _read_log_tail_lines(log_path, max_lines=line_count)
            filtered = _filter_log_text(raw_tail, str(stage_status_level.value or "all"))
            stage_status_log.set_value(filtered)
            stage_status_log_path.set_text(f"Лог: {log_path}")

        def _copy_stage_status_log() -> None:
            text = str(stage_status_log.value or "")
            if not text.strip():
                ui.notify("Нечего копировать.", type="warning")
                return
            ui.run_javascript(f"navigator.clipboard && navigator.clipboard.writeText({json.dumps(text)})")
            ui.notify("Лог скопирован в буфер.", type="positive")

        def _download_stage_status_log() -> None:
            text = str(stage_status_log.value or "")
            if not text.strip():
                ui.notify("Нечего скачивать.", type="warning")
                return
            export_dir = PROJECT_ROOT / "logs" / "exports"
            export_dir.mkdir(parents=True, exist_ok=True)
            run_id = str((stage_status_ctx.get("row") or {}).get("run_id") or "unknown")
            stage_name = str((stage_status_ctx.get("row") or {}).get("stage") or "stage")
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = export_dir / f"indexer_{stage_name}_{run_id}_{stamp}.log"
            file_path.write_text(text, encoding="utf-8")
            ui.download(file_path, filename=file_path.name)

        ui.timer(1.5, lambda: _refresh_stage_status_log())

        def show_stage_status_details(row: Dict[str, Any]) -> None:
            stage = str(row.get("stage") or "-")
            status = str(row.get("status") or "-")
            run_id = str(row.get("run_id") or row.get("ocr_run_id") or "-")
            run_note = str(row.get("run_note") or "").strip()
            stage_status_ctx["row"] = dict(row)
            stage_status_ctx["log_path"] = row.get("_log_path") or PROJECT_ROOT / "logs" / "indexer.log"
            stage_status_title.set_text(f"Статус этапа: {stage} / {status}")
            stage_status_run.set_text(f"Run ID: {run_id}")
            if run_note:
                stage_status_note_title.set_visibility(True)
                stage_status_note_value.set_visibility(True)
                stage_status_note_value.set_text(run_note)
            else:
                stage_status_note_title.set_visibility(False)
                stage_status_note_value.set_visibility(False)
                stage_status_note_value.set_text("")
            stage_status_dialog.open()
            _refresh_stage_status_log(force=True)

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            with ui.row().classes("w-full items-center gap-2"):
                ui.icon("account_tree").classes("text-2xl text-indigo-500")
                ui.label("Pipeline индексации").classes("text-xl font-semibold")
                ui.label(active_label).classes("rag-chip")
                ui.space()
                refresh_btn = ui.button(icon="refresh", on_click=lambda: _refresh_progress()).props("flat dense round").tooltip("Обновить")
            ui.label(
                "Запускаемые фазы: metadata, small chunks, large chunks и OCR. "
                "Покрытие содержимым показано отдельно, это агрегат state DB, а не отдельная команда запуска."
            ).classes("rag-meta")

            coverage_area = ui.column().classes("w-full gap-1")
            progress_area = ui.column().classes("w-full gap-2")
            with progress_area:
                with ui.element("div").classes("rag-pipeline-row rag-pipeline-head"):
                    ui.label("Этап").classes("rag-meta font-semibold")
                    ui.label("Прогресс").classes("rag-meta font-semibold")
                    ui.label("Статистика").classes("rag-meta font-semibold")
                    ui.label("Действия").classes("rag-meta font-semibold text-right")

            def pause_phase(label: str) -> None:
                ui.notify(f"Пауза для «{label}» будет доступна после cooperative-pause в worker.", type="warning")

            def stop_phase(label: str) -> None:
                ui.notify(f"Остановка для «{label}» будет доступна после cooperative-cancel в worker.", type="warning")

            def render_phase_row(
                *,
                key: str,
                label: str,
                row: Dict[str, Any],
                is_ocr: bool = False,
            ) -> None:
                status_str = str(row.get("status") or "idle")
                is_running = status_str == "running"
                processed = int(row.get("processed_files") or row.get("processed_pdfs") or 0)
                total_f = int(row.get("total_files") or row.get("found_scanned") or 0)
                pct = min(1.0, processed / total_f) if total_f > 0 else (1.0 if status_str not in {"running", "idle"} else 0.0)
                duration_value = row.get("duration_sec", row.get("last_duration_sec"))
                last_ts = row.get("ts_finished") or row.get("ts_updated") or row.get("ts_started")
                status_cls = "running" if is_running else status_str
                stats_text = (
                    f"найдено {int(row.get('found_scanned') or 0):,} · обработано {int(row.get('processed_pdfs') or 0):,}"
                    if is_ocr else
                    f"добавлено {int(row.get('added_files') or 0):,} · обновлено {int(row.get('updated_files') or 0):,} · "
                    f"пропущено {int(row.get('skipped_files') or 0):,} · ошибок {int(row.get('error_files') or 0):,} · "
                    f"точек {int(row.get('points_added') or 0):,}"
                ).replace(",", " ")
                status_icon = (
                    "sync" if is_running else
                    "check_circle" if status_str == "completed" else
                    "error" if status_str == "failed" else
                    "cancel" if status_str == "cancelled" else
                    "radio_button_unchecked"
                )
                status_title = {
                    "running": "Запущено",
                    "completed": "Завершено",
                    "failed": "Ошибка",
                    "cancelled": "Отменено",
                    "idle": "Не запускалось",
                }.get(status_str, status_str)
                row_for_dialog = dict(row)
                row_for_dialog["stage"] = label
                if is_ocr:
                    row_for_dialog["_log_path"] = PROJECT_ROOT / "logs" / "ocr.log"
                with ui.element("div").classes(f"rag-pipeline-row {status_cls}"):
                    with ui.row().classes("items-center gap-2 min-w-0"):
                        ui.icon(status_icon).classes(f"rag-phase-status {status_cls}")
                        with ui.column().classes("gap-0 min-w-0"):
                            ui.label(label).classes("font-semibold truncate")
                            ui.label(_format_relative_time(last_ts)).classes("rag-meta")
                    with ui.column().classes("gap-1 min-w-0"):
                        with ui.row().classes("w-full items-center gap-2"):
                            ui.button(status_title, on_click=lambda r=row_for_dialog: show_stage_status_details(r), color=None).props("flat dense no-caps").classes(f"rag-chip rag-status-chip {status_cls}")
                            ui.label(f"{processed:,} / {total_f:,}".replace(",", " ")).classes("rag-meta")
                            ui.space()
                            ui.label(_format_duration_seconds(duration_value)).classes("rag-meta")
                        ui.linear_progress(value=pct).props("color=indigo-5" if is_running else "").classes("w-full")
                    ui.label(stats_text).classes("rag-meta")
                    with ui.row().classes("rag-pipeline-actions"):
                        if is_ocr:
                            ui.button(icon="play_arrow", on_click=run_ocr_now).props("flat dense round").tooltip("Запустить")
                            ui.button(icon="restart_alt", on_click=run_ocr_now).props("flat dense round").tooltip("Рестарт")
                        else:
                            ui.button(icon="play_arrow", on_click=make_run_handler(key)).props("flat dense round").tooltip("Запустить")
                            ui.button(icon="restart_alt", on_click=make_run_handler(key)).props("flat dense round").tooltip("Рестарт")
                        ui.button(icon="pause", on_click=lambda l=label: pause_phase(l)).props("flat dense round").tooltip("Пауза")
                        ui.button(icon="stop", on_click=lambda l=label: stop_phase(l)).props("flat dense round").tooltip("Остановить")

            def _refresh_progress() -> None:
                fresh = _read_index_telemetry(state.cfg)
                stats = _read_index_stats(state.cfg)
                by_stage = dict(stats.get("by_stage") or {})
                total_files = int(stats.get("total") or 0)
                content_files = int(by_stage.get("content") or 0)
                coverage_pct = min(1.0, content_files / total_files) if total_files > 0 else 0.0
                coverage_area.clear()
                with coverage_area:
                    with ui.element("div").classes("rag-content-coverage w-full"):
                        with ui.row().classes("w-full items-center gap-2"):
                            ui.icon("article").classes("text-indigo-500")
                            ui.label("Покрытие содержимым").classes("font-semibold")
                            ui.label(f"{content_files:,} / {total_files:,}".replace(",", " ")).classes("rag-meta")
                            ui.space()
                            ui.label(f"{coverage_pct * 100:.0f}%").classes("rag-meta")
                        ui.linear_progress(value=coverage_pct).props("color=indigo-5").classes("w-full mt-1")
                        ui.label(
                            "Файлы со stage=content уже имеют проиндексированное содержимое. "
                            "Остальные пока представлены метаданными или ждут фаз small/large/OCR."
                        ).classes("rag-meta")
                active_by_stage = {str(row.get("stage") or ""): row for row in (fresh.get("active_stages") or [])}
                latest_by_stage = {str(row.get("stage") or ""): row for row in (fresh.get("latest_stages") or [])}
                summary_by_stage = {str(row.get("stage") or ""): row for row in (fresh.get("stage_summary") or [])}
                progress_area.clear()
                with progress_area:
                    with ui.element("div").classes("rag-pipeline-row rag-pipeline-head"):
                        ui.label("Этап").classes("rag-meta font-semibold")
                        ui.label("Прогресс").classes("rag-meta font-semibold")
                        ui.label("Статистика").classes("rag-meta font-semibold")
                        ui.label("Действия").classes("rag-meta font-semibold text-right")
                    for stage_key in ["metadata", "small", "large"]:
                        row = dict(summary_by_stage.get(stage_key) or {})
                        row.update(latest_by_stage.get(stage_key) or {})
                        row.update(active_by_stage.get(stage_key) or {})
                        render_phase_row(
                            key=stage_key,
                            label=_STAGE_LABELS.get(stage_key, stage_key),
                            row=row,
                        )
                    ocr_row = dict(fresh.get("last_ocr") or {})
                    if fresh.get("active_ocr"):
                        ocr_row.update(fresh.get("active_ocr") or {})
                    render_phase_row(
                        key="ocr",
                        label="OCR",
                        row=ocr_row,
                        is_ocr=True,
                    )

            # Initial render
            _refresh_progress()
            # Auto-refresh every 5 seconds while indexing may be running
            ui.timer(5.0, _refresh_progress)

        # ── Расписание и параметры ───────────────────────────────────────
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            with ui.row().classes("w-full items-center gap-2"):
                ui.icon("settings_suggest").classes("text-2xl text-indigo-500")
                ui.label("Расписание и параметры").classes("text-xl font-semibold")
            ui.label(
                "Здесь задаются автоматические запуски и рабочие параметры pipeline. "
                "Ручной запуск и прогресс вынесены выше, чтобы не дублировать действия."
            ).classes("rag-meta")

            with ui.element("div").classes("rag-index-config-layout w-full"):
                with ui.column().classes("gap-3 min-w-0"):
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.icon("event_repeat").classes("text-xl text-indigo-500")
                        ui.label("Расписание").classes("font-semibold")
                    ui.label(
                        "Планировщик проверяет расписание каждую минуту. Несколько записей можно использовать для разных этапов."
                    ).classes("rag-meta")

                    sched_area = ui.column().classes("w-full gap-2")

                    def render_schedules() -> None:
                        sched_area.clear()
                        current = settings_db.list_index_schedules() if hasattr(settings_db, "list_index_schedules") else []
                        with sched_area:
                            if not current:
                                ui.label("Расписаний пока нет. Нажмите «Добавить расписание» чтобы создать.").classes("rag-meta")
                            for sched in current:
                                days_str = " ".join(_DAY_RU.get(d, d) for d in (sched.get("days") or []))
                                cadence_str = _CADENCE_LABELS.get(str(sched.get("cadence") or "daily"), "")
                                stage_str = _STAGE_LABELS.get(str(sched.get("stage") or "all"), str(sched.get("stage") or ""))
                                last_run = str(sched.get("last_run_at") or "—")
                                enabled_val = bool(int(sched.get("enabled") or 0))
                                color_cls = "" if enabled_val else "opacity-50"
                                with ui.row().classes(f"w-full items-center gap-2 p-2 border border-gray-200 rounded {color_cls}"):
                                    ui.icon("check_circle" if enabled_val else "radio_button_unchecked").classes(
                                        "text-xl " + ("text-green-500" if enabled_val else "text-gray-400")
                                    )
                                    ui.label(str(sched.get("label") or "Без названия")).classes("font-semibold min-w-32")
                                    ui.label(stage_str).classes("rag-chip")
                                    ui.label(f"{cadence_str} в {sched.get('time') or '?'}").classes("rag-meta")
                                    if days_str:
                                        ui.label(days_str).classes("rag-meta min-w-20")
                                    ui.space()
                                    ui.label(f"Последний: {last_run[:16] if last_run != '—' else '—'}").classes("rag-meta text-xs")
                                    ui.button(icon="edit", on_click=lambda s=sched: open_sched_dialog(s), color=None).props("flat dense round")
                                    ui.button(icon="delete", on_click=lambda s=sched: delete_sched(str(s.get("id") or "")), color=None).props("flat dense round color=red-5")

                    def delete_sched(sched_id: str) -> None:
                        settings_db.delete_index_schedule(id=sched_id)
                        render_schedules()
                        ui.notify("Расписание удалено.", type="warning")

                    def open_sched_dialog(existing: Optional[Dict[str, Any]] = None) -> None:
                        with ui.dialog() as dlg, ui.card().classes("w-full max-w-lg p-4 gap-3"):
                            ui.label("Изменить расписание" if existing else "Новое расписание").classes("text-lg font-semibold")
                            dlg_label = ui.input("Название", value=str((existing or {}).get("label") or "")).props("dense outlined").classes("w-full")
                            with ui.row().classes("w-full gap-3"):
                                dlg_enabled = ui.checkbox("Включено", value=bool(int((existing or {}).get("enabled", 1))))
                                dlg_stage = ui.select(
                                    _STAGE_LABELS, value=str((existing or {}).get("stage") or "all"), label="Этап"
                                ).props("dense outlined").classes("flex-1")
                            with ui.row().classes("w-full gap-3"):
                                dlg_cadence = ui.select(
                                    _CADENCE_LABELS, value=str((existing or {}).get("cadence") or "daily"), label="Период"
                                ).props("dense outlined").classes("flex-1")
                                dlg_time = ui.input(
                                    "Время (ЧЧ:ММ)", value=str((existing or {}).get("time") or "03:00")
                                ).props("dense outlined mask='##:##'").classes("w-32")
                            ui.label("Дни недели (для ежедневного/еженедельного):").classes("rag-meta")
                            existing_days = (existing or {}).get("days") or _DAY_LABELS[:5]
                            day_checks = {d: ui.checkbox(_DAY_RU.get(d, d), value=(d in existing_days)) for d in _DAY_LABELS}
                            with ui.row().classes("w-full gap-2 justify-end"):
                                ui.button("Отмена", on_click=dlg.close).props("flat")

                                def save_sched() -> None:
                                    days_sel = [d for d, cb in day_checks.items() if cb.value]
                                    settings_db.save_index_schedule(
                                        id=(existing or {}).get("id"),
                                        label=str(dlg_label.value or ""),
                                        enabled=bool(dlg_enabled.value),
                                        cadence=str(dlg_cadence.value or "daily"),
                                        time=str(dlg_time.value or "03:00"),
                                        days=days_sel,
                                        stage=str(dlg_stage.value or "all"),
                                    )
                                    dlg.close()
                                    render_schedules()
                                    ui.notify("Расписание сохранено.", type="positive")

                                ui.button("Сохранить", icon="save", on_click=save_sched).props("unelevated")
                        dlg.open()

                    render_schedules()
                    ui.button("Добавить расписание", icon="add", on_click=lambda: open_sched_dialog()).props("outline")

                with ui.column().classes("gap-3 min-w-0"):
                    initial_index_settings = {
                        "workers": int(settings.get("workers") or state.cfg.get("index_read_workers") or 4),
                        "max_chunks": int(settings.get("max_chunks") or state.cfg.get("index_max_chunks") or 2000),
                        "recreate": bool(settings.get("recreate")),
                        "skip_inline_ocr": bool(settings.get("skip_inline_ocr")),
                        "ocr_enabled": bool(settings.get("ocr_enabled")),
                        "ocr_min_text_len": int(settings.get("ocr_min_text_len") or 50),
                    }
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.icon("tune").classes("text-xl text-indigo-500")
                        ui.label("Параметры").classes("font-semibold")
                    with ui.row().classes("w-full gap-3 flex-wrap"):
                        workers_input = ui.number("Потоки чтения (0 = авто)", value=initial_index_settings["workers"], min=0, max=32, step=1).props("dense outlined").classes("w-40")
                        max_chunks_input = ui.number("Макс. чанков на файл", value=initial_index_settings["max_chunks"], min=0, max=100000, step=100).props("dense outlined").classes("w-52")
                        recreate_input = ui.checkbox("Пересоздавать коллекцию", value=initial_index_settings["recreate"])
                        skip_inline_ocr_input = ui.checkbox("Пропускать OCR внутри индекса", value=initial_index_settings["skip_inline_ocr"])

                    ui.separator()
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.icon("document_scanner").classes("text-xl text-orange-500")
                        ui.label("OCR настройки").classes("font-semibold")
                    with ui.row().classes("w-full gap-3 items-end flex-wrap"):
                        ocr_enabled_input = ui.checkbox("Запускать OCR после индексации", value=initial_index_settings["ocr_enabled"])
                        with ui.column().classes("gap-0"):
                            ocr_min_text_input = ui.number(
                                "Порог текста для скана (символов)",
                                value=initial_index_settings["ocr_min_text_len"],
                                min=1, max=100000, step=10,
                            ).props("dense outlined").classes("w-64")
                            ui.label("Если в PDF меньше указанного числа символов — файл считается сканом.").classes("rag-meta text-xs")

                    action_row = ui.row().classes("rag-dirty-actions")
                    action_row.set_visibility(False)
                    dirty_ready = [False]

                    def current_index_settings() -> Dict[str, Any]:
                        return {
                            "workers": int(workers_input.value if workers_input.value is not None else 4),
                            "max_chunks": int(max_chunks_input.value or 0),
                            "recreate": bool(recreate_input.value),
                            "skip_inline_ocr": bool(skip_inline_ocr_input.value),
                            "ocr_enabled": bool(ocr_enabled_input.value),
                            "ocr_min_text_len": int(ocr_min_text_input.value or 50),
                        }

                    def refresh_index_dirty() -> None:
                        if not dirty_ready[0]:
                            return
                        action_row.set_visibility(current_index_settings() != initial_index_settings)

                    def reset_index_settings() -> None:
                        workers_input.set_value(initial_index_settings["workers"])
                        max_chunks_input.set_value(initial_index_settings["max_chunks"])
                        recreate_input.set_value(initial_index_settings["recreate"])
                        skip_inline_ocr_input.set_value(initial_index_settings["skip_inline_ocr"])
                        ocr_enabled_input.set_value(initial_index_settings["ocr_enabled"])
                        ocr_min_text_input.set_value(initial_index_settings["ocr_min_text_len"])
                        action_row.set_visibility(False)

                    def save_index_settings() -> None:
                        values = current_index_settings()
                        saved = settings_db.save_index_settings(values)
                        initial_index_settings.update(values)
                        action_row.set_visibility(False)
                        _log_app_event(state, "index", "save_settings", details=saved)
                        ui.notify("Настройки индексирования сохранены.", type="positive")
                        render()

                    workers_input.on_value_change(lambda _: refresh_index_dirty())
                    max_chunks_input.on_value_change(lambda _: refresh_index_dirty())
                    recreate_input.on_value_change(lambda _: refresh_index_dirty())
                    skip_inline_ocr_input.on_value_change(lambda _: refresh_index_dirty())
                    ocr_enabled_input.on_value_change(lambda _: refresh_index_dirty())
                    ocr_min_text_input.on_value_change(lambda _: refresh_index_dirty())
                    dirty_ready[0] = True

                    with action_row:
                        with ui.row().classes("rag-dirty-actions-inner"):
                            ui.button("Отменить", icon="close", on_click=reset_index_settings).props("flat dense")
                            ui.button("Сохранить настройки", icon="save", on_click=save_index_settings).props("unelevated dense")

        # ── Статистика по этапам + график ────────────────────────────────
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            with ui.row().classes("w-full items-center gap-2"):
                ui.label("Статистика по этапам").classes("text-xl font-semibold")
            rows = telemetry.get("stage_summary") or []
            if rows:
                ui.table(
                    rows=rows,
                    columns=[
                        {"name": "stage", "label": "Этап", "field": "stage"},
                        {"name": "status", "label": "Статус", "field": "status"},
                        {"name": "processed_files", "label": "Файлов", "field": "processed_files"},
                        {"name": "added_files", "label": "Добавлено", "field": "added_files"},
                        {"name": "updated_files", "label": "Обновлено", "field": "updated_files"},
                        {"name": "error_files", "label": "Ошибок", "field": "error_files"},
                        {"name": "points_added", "label": "Точек", "field": "points_added"},
                        {"name": "last_duration_sec", "label": "Последний, сек", "field": "last_duration_sec"},
                        {"name": "avg_duration_sec", "label": "Среднее, сек", "field": "avg_duration_sec"},
                    ],
                    pagination=10,
                ).classes("w-full")
            else:
                ui.label("История этапов пока пустая.").classes("rag-meta")

            # ── График по дням ──────────────────────────────────────────
            ui.separator()
            ui.label("График индексации по дням").classes("font-semibold")
            with ui.row().classes("w-full gap-3 items-center"):
                chart_metric = ui.select(
                    {"files": "Файлов обработано", "added": "Файлов добавлено", "points": "Точек (чанков)"},
                    value="files", label="Метрика"
                ).props("dense outlined").classes("w-56")
                chart_period = ui.select(
                    {"7": "7 дней", "30": "30 дней", "90": "90 дней"},
                    value="30", label="Период"
                ).props("dense outlined").classes("w-36")
                chart_area = ui.column().classes("w-full")

            def rebuild_chart() -> None:
                chart_area.clear()
                period_days = int(chart_period.value or 30)
                metric_key = str(chart_metric.value or "files")
                daily = settings_db.get_daily_index_stats(days=period_days) if hasattr(settings_db, "get_daily_index_stats") else []
                if not daily:
                    with chart_area:
                        ui.label("Нет данных за выбранный период.").classes("rag-meta")
                    return
                # Группируем по дням, суммируем метрику по всем этапам
                from collections import defaultdict
                by_day: Dict[str, int] = defaultdict(int)
                for d in daily:
                    by_day[str(d.get("day") or "")] += int(d.get(metric_key) or 0)
                days_sorted = sorted(by_day.keys())
                values = [by_day[d] for d in days_sorted]
                metric_label = {"files": "Файлов", "added": "Добавлено", "points": "Точек"}[metric_key]
                chart_option = {
                    "tooltip": {"trigger": "axis"},
                    "xAxis": {"type": "category", "data": days_sorted, "axisLabel": {"rotate": 30}},
                    "yAxis": {"type": "value", "name": metric_label},
                    "series": [{"name": metric_label, "type": "bar", "data": values,
                                "itemStyle": {"color": "#6366f1"}}],
                    "grid": {"left": "60px", "right": "20px", "bottom": "60px"},
                }
                with chart_area:
                    ui.echart(chart_option).classes("w-full h-64")

            chart_metric.on_value_change(lambda _: rebuild_chart())
            chart_period.on_value_change(lambda _: rebuild_chart())
            rebuild_chart()

    def render_index_dashboard() -> None:
        if not _is_admin(state):
            return
        stats = _read_index_stats(state.cfg)
        telemetry = _read_index_telemetry(state.cfg)
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Дашборд индексирования").classes("text-xl font-semibold")
            if not stats["found"]:
                ui.label(f"Состояние индекса не найдено: {stats['state_file']}").classes("rag-meta")
                return
            with ui.row().classes("w-full gap-3"):
                ui.label(f"Файлов: {stats['total']:,}".replace(",", " ")).classes("rag-chip")
                ui.label(f"Размер: {_format_bytes(stats.get('total_size_bytes'))}").classes("rag-chip")
                ui.label(f"Обновлен: {stats.get('last_modified', 'неизвестно')}").classes("rag-chip")
                last_run = telemetry.get("last_run") or {}
                if last_run:
                    ui.label(f"Последний запуск: {_format_duration_seconds(last_run.get('duration_sec'))}").classes("rag-chip")
            if stats.get("by_ext"):
                for ext, count in list(stats["by_ext"].items())[:12]:
                    ui.label(f"{ext}: {count}").classes("rag-meta")

    def render_login_screen() -> None:
        auth_db = _get_auth_db(state)
        with ui.column().classes("w-full min-h-[70vh] items-center justify-center"):
            with ui.column().classes("rag-card w-full max-w-xl p-5 gap-3"):
                ui.label("Вход в RAG Каталог").classes("text-2xl font-semibold")
                ui.label("Войдите в аккаунт или отправьте заявку на доступ.").classes("rag-meta")

                tg_login_token = {"value": ""}

                def _complete_login(user: Dict[str, Any], *, event_type: str) -> None:
                    state.current_user = user
                    state.auth_token = auth_db.create_session(username=str(user.get("username") or ""))
                    auth_db.log_auth_event(username=_username(state), event_type=event_type, ok=True)
                    _load_user_state(state)
                    try:
                        app.storage.user["auth_token"] = state.auth_token
                    except Exception:
                        pass
                    ui.notify("Вход выполнен.", type="positive")
                    render()

                def login() -> None:
                    username = str(username_input.value or "")
                    user = auth_db.login(username=username, password=str(password_input.value or ""))
                    if not user:
                        auth_db.log_auth_event(username=username, event_type="login_failed", ok=False, error="bad_credentials")
                        ui.notify("Неверный логин или пароль.", type="negative")
                        return
                    _complete_login(user, event_type="login")

                def request_tg_login() -> None:
                    bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
                    if not bot_link:
                        ui.notify("Telegram-вход не настроен: задайте telegram_bot_link в config.json.", type="warning")
                        return
                    out = auth_db.create_telegram_login_challenge(target="web")
                    token = str(out.get("token") or "")
                    link = _telegram_deeplink(bot_link, "login", token)
                    if not token or not link:
                        ui.notify("Не удалось создать Telegram-ссылку входа.", type="negative")
                        return
                    tg_login_token["value"] = token
                    ui.run_javascript(
                        "(() => {"
                        f"const url = {json.dumps(link)};"
                        "const w = window.open(url, '_blank', 'noopener,noreferrer');"
                        "if (!w) { window.location.href = url; }"
                        "})();"
                    )
                    ui.notify("Подтвердите вход в Telegram, затем вернитесь в браузер.", type="positive")

                def poll_tg_login() -> None:
                    token = tg_login_token["value"]
                    if not token or state.current_user is not None:
                        return
                    out = auth_db.consume_confirmed_telegram_login(token=token)
                    if not out.get("ok"):
                        return
                    tg_login_token["value"] = ""
                    user = out.get("user") or auth_db.get_user(username=str(out.get("username") or ""))
                    if not user:
                        return
                    _complete_login(user, event_type="telegram_web_login")

                def register_request() -> None:
                    username = str(reg_username_input.value or "").strip().lower()
                    display_name = str(reg_display_input.value or "").strip()
                    tg_username = str(reg_tg_user_input.value or "").strip().lstrip("@")
                    if len(username) < 3:
                        ui.notify("Укажите логин (минимум 3 символа).", type="warning")
                        return
                    if auth_db.get_user(username=username):
                        ui.notify("Пользователь с таким логином уже существует. Используйте вход.", type="warning")
                        return
                    out = auth_db.create_registration_request(
                        username=username,
                        display_name=display_name or username,
                        telegram_username=tg_username,
                        source="web",
                        note="requested from web login form",
                    )
                    if not out.get("ok"):
                        ui.notify("Не удалось отправить заявку. Попробуйте позже.", type="negative")
                        return
                    ui.notify("Заявка отправлена администратору.", type="positive")
                    reg_username_input.value = ""
                    reg_display_input.value = ""
                    reg_tg_user_input.value = ""
                    reg_username_input.update()
                    reg_display_input.update()
                    reg_tg_user_input.update()

                tabs = ui.tabs().classes("w-full")
                with tabs:
                    tab_login = ui.tab("Войти", icon="login")
                    tab_register = ui.tab("Зарегистрироваться", icon="person_add")

                with ui.tab_panels(tabs, value=tab_login).classes("w-full"):
                    with ui.tab_panel(tab_login).classes("w-full gap-3"):
                        username_input = ui.input("Логин").props("dense outlined").classes("w-full")
                        password_input = ui.input("Пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                        password_input.on("keyup.enter", lambda _: login())
                        ui.button("Войти", icon="login", on_click=login).props("unelevated")
                        ui.separator()
                        ui.button("Войти через Telegram", icon="send", on_click=request_tg_login).props("outline").classes("w-full")
                        ui.label("Стандартный сценарий: как у OAuth-входа — нажали кнопку, подтвердили в Telegram, вернулись в приложение.").classes("rag-meta")

                    with ui.tab_panel(tab_register).classes("w-full gap-3"):
                        reg_username_input = ui.input("Логин").props("dense outlined").classes("w-full")
                        reg_display_input = ui.input("Имя").props("dense outlined").classes("w-full")
                        reg_tg_user_input = ui.input("Telegram username (необязательно)").props("dense outlined").classes("w-full")
                        ui.button("Отправить заявку", icon="how_to_reg", on_click=register_request).props("unelevated")
                        ui.label("После одобрения администратором вы получите доступ к аккаунту.").classes("rag-meta")

                ui.timer(2.0, poll_tg_login)

    def render_admin_users(auth_db: UserAuthDB) -> None:
        with ui.column().classes("rag-card w-full p-4 gap-4"):
            ui.label("Админ-панель пользователей").classes("text-xl font-semibold")
            with ui.expansion("Создать пользователя", icon="person_add").classes("w-full"):
                new_username = ui.input("Логин").props("dense outlined").classes("w-full")
                new_display = ui.input("Имя").props("dense outlined").classes("w-full")
                new_telegram = ui.input("Telegram chat id").props("dense outlined").classes("w-full")
                new_telegram_username = ui.input("Telegram username").props("dense outlined prefix=@").classes("w-full")
                new_password = ui.input("Временный пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                new_role = ui.select(["user", "admin"], value="user", label="Роль").props("dense outlined").classes("w-full")
                new_status = ui.select(["active", "pending", "blocked"], value="active", label="Статус").props("dense outlined").classes("w-full")
                new_must_change = ui.checkbox("Потребовать смену пароля", value=True)

                def create_user() -> None:
                    ok = auth_db.admin_create_user(
                        username=str(new_username.value or ""),
                        display_name=str(new_display.value or ""),
                        telegram_chat_id=str(new_telegram.value or ""),
                        telegram_username=str(new_telegram_username.value or ""),
                        password=str(new_password.value or ""),
                        role=str(new_role.value or "user"),
                        status=str(new_status.value or "active"),
                        must_change_password=bool(new_must_change.value),
                    )
                    ui.notify("Пользователь создан." if ok else "Не удалось создать пользователя.", type="positive" if ok else "negative")
                    render()

                ui.button("Создать", icon="person_add", on_click=create_user).props("unelevated")

            users = auth_db.list_users()
            for user in users:
                username = str(user.get("username") or "")
                role = str(user.get("role") or "user")
                status = str(user.get("status") or "")
                with ui.expansion(f"{username} · {role} · {status}", icon="person").classes("w-full"):
                    initial_user = {
                        "display_name": str(user.get("display_name") or ""),
                        "telegram_chat_id": str(user.get("telegram_chat_id") or ""),
                        "telegram_username": str(user.get("telegram_username") or ""),
                        "role": role,
                        "status": status or "active",
                        "must_change_password": bool(int(user.get("must_change_password") or 0)),
                    }
                    display_input = ui.input("Имя", value=str(user.get("display_name") or "")).props("dense outlined").classes("w-full")
                    telegram_input = ui.input("Telegram chat id", value=str(user.get("telegram_chat_id") or "")).props("dense outlined").classes("w-full")
                    telegram_username_input = ui.input("Telegram username", value=str(user.get("telegram_username") or "")).props("dense outlined prefix=@").classes("w-full")
                    role_input = ui.select(["user", "admin"], value=role, label="Роль").props("dense outlined").classes("w-full")
                    status_input = ui.select(["active", "pending", "blocked"], value=status or "active", label="Статус").props("dense outlined").classes("w-full")
                    must_input = ui.checkbox("Потребовать смену пароля", value=bool(int(user.get("must_change_password") or 0)))
                    reset_password = ui.input("Новый временный пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")

                    def save_user(
                        username: str = username,
                        display_input: Any = display_input,
                        telegram_input: Any = telegram_input,
                        telegram_username_input: Any = telegram_username_input,
                        role_input: Any = role_input,
                        status_input: Any = status_input,
                        must_input: Any = must_input,
                    ) -> None:
                        ok = auth_db.admin_update_user(
                            username=username,
                            display_name=str(display_input.value or ""),
                            telegram_chat_id=str(telegram_input.value or ""),
                            telegram_username=str(telegram_username_input.value or ""),
                            role=str(role_input.value or "user"),
                            status=str(status_input.value or "active"),
                            must_change_password=bool(must_input.value),
                        )
                        initial_user.update({
                            "display_name": str(display_input.value or ""),
                            "telegram_chat_id": str(telegram_input.value or ""),
                            "telegram_username": str(telegram_username_input.value or ""),
                            "role": str(role_input.value or "user"),
                            "status": str(status_input.value or "active"),
                            "must_change_password": bool(must_input.value),
                        })
                        user_actions.set_visibility(False)
                        ui.notify("Пользователь обновлен." if ok else "Не удалось обновить пользователя.", type="positive" if ok else "negative")
                        _refresh_current_user(state)
                        render()

                    def set_password(
                        username: str = username,
                        reset_password: Any = reset_password,
                    ) -> None:
                        ok = auth_db.admin_set_password(
                            username=username,
                            new_password=str(reset_password.value or ""),
                            must_change_password=True,
                        )
                        ui.notify("Пароль обновлен." if ok else "Введите новый пароль.", type="positive" if ok else "warning")
                        render()

                    user_actions = ui.row().classes("rag-dirty-actions")
                    user_actions.set_visibility(False)

                    def current_user_values() -> Dict[str, Any]:
                        return {
                            "display_name": str(display_input.value or ""),
                            "telegram_chat_id": str(telegram_input.value or ""),
                            "telegram_username": str(telegram_username_input.value or ""),
                            "role": str(role_input.value or "user"),
                            "status": str(status_input.value or "active"),
                            "must_change_password": bool(must_input.value),
                        }

                    def refresh_user_dirty() -> None:
                        user_actions.set_visibility(current_user_values() != initial_user)

                    def reset_user_fields() -> None:
                        display_input.set_value(initial_user["display_name"])
                        telegram_input.set_value(initial_user["telegram_chat_id"])
                        telegram_username_input.set_value(initial_user["telegram_username"])
                        role_input.set_value(initial_user["role"])
                        status_input.set_value(initial_user["status"])
                        must_input.set_value(initial_user["must_change_password"])
                        user_actions.set_visibility(False)

                    display_input.on_value_change(lambda _: refresh_user_dirty())
                    telegram_input.on_value_change(lambda _: refresh_user_dirty())
                    telegram_username_input.on_value_change(lambda _: refresh_user_dirty())
                    role_input.on_value_change(lambda _: refresh_user_dirty())
                    status_input.on_value_change(lambda _: refresh_user_dirty())
                    must_input.on_value_change(lambda _: refresh_user_dirty())

                    with ui.row().classes("gap-2"):
                        ui.button("Сбросить пароль", icon="key", on_click=set_password).props("outline")
                        def make_invite(
                            username: str = username,
                            display_input: Any = display_input,
                            telegram_username_input: Any = telegram_username_input,
                        ) -> None:
                            bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
                            if not bot_link:
                                ui.notify("В config.json не задан telegram_bot_link.", type="warning")
                                return
                            out = auth_db.create_telegram_token(
                                purpose="invite",
                                username=username,
                                display_name=str(display_input.value or ""),
                                telegram_username=str(telegram_username_input.value or ""),
                                created_by=_username(state),
                                ttl_minutes=7 * 24 * 60,
                            )
                            link = _telegram_deeplink(bot_link, "invite", str(out.get("token") or ""))
                            ui.notify(f"Invite-link: {link}", type="positive", timeout=12000)

                        ui.button("Invite Telegram", icon="link", on_click=make_invite).props("outline")
                    with user_actions:
                        with ui.row().classes("rag-dirty-actions-inner"):
                            ui.button("Отменить", icon="close", on_click=reset_user_fields).props("flat dense")
                            ui.button("Сохранить", icon="save", on_click=save_user).props("outline dense")

    def render_admin_telegram_chats(auth_db: UserAuthDB) -> None:
        rows = auth_db.list_telegram_chats()
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Telegram чаты").classes("text-xl font-semibold")
            if not rows:
                ui.label("Привязанных Telegram chat_id пока нет.").classes("rag-meta")
                return
            ui.table(
                rows=rows,
                columns=[
                    {"name": "username", "label": "Пользователь", "field": "username"},
                    {"name": "display_name", "label": "Имя", "field": "display_name"},
                    {"name": "role", "label": "Роль", "field": "role"},
                    {"name": "status", "label": "Статус", "field": "status"},
                    {"name": "telegram_chat_id", "label": "Chat ID", "field": "telegram_chat_id"},
                    {"name": "last_telegram_event_at", "label": "Последнее Telegram-событие", "field": "last_telegram_event_at"},
                    {"name": "last_login_at", "label": "Последний web-вход", "field": "last_login_at"},
                ],
                pagination=10,
            ).classes("w-full")

    def render_admin_registration_requests(auth_db: UserAuthDB) -> None:
        rows = auth_db.list_registration_requests(status="pending", limit=50)
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Заявки на регистрацию").classes("text-xl font-semibold")
            if not rows:
                ui.label("Ожидающих заявок нет.").classes("rag-meta")
                return
            for row in rows:
                req_id = int(row.get("id") or 0)
                title = str(row.get("username") or row.get("display_name") or f"заявка {req_id}")
                tg = str(row.get("telegram_username") or row.get("telegram_chat_id") or "")
                with ui.row().classes("w-full items-center gap-2"):
                    ui.label(f"#{req_id}").classes("rag-chip")
                    ui.label(title).classes("font-medium")
                    ui.label(f"Telegram: {tg or '-'}").classes("rag-meta flex-1")
                    ui.label(str(row.get("source") or "")).classes("rag-meta")

                    def approve(req_id: int = req_id) -> None:
                        out = auth_db.review_registration_request(
                            request_id=req_id,
                            reviewed_by=_username(state),
                            decision="approved",
                        )
                        ui.notify(
                            f"Заявка одобрена: {out.get('username')}" if out.get("ok") else f"Не удалось одобрить: {out.get('reason')}",
                            type="positive" if out.get("ok") else "negative",
                        )
                        render()

                    def reject(req_id: int = req_id) -> None:
                        out = auth_db.review_registration_request(
                            request_id=req_id,
                            reviewed_by=_username(state),
                            decision="rejected",
                        )
                        ui.notify(
                            "Заявка отклонена." if out.get("ok") else f"Не удалось отклонить: {out.get('reason')}",
                            type="positive" if out.get("ok") else "negative",
                        )
                        render()

                    ui.button("Одобрить", icon="check", on_click=approve).props("outline dense")
                    ui.button("Отклонить", icon="close", on_click=reject).props("flat dense")

    def render_admin_security_settings(auth_db: UserAuthDB) -> None:
        current_ttl = auth_db.get_session_ttl_days()
        current_show_system = auth_db.get_show_system_files_for_admin()
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            initial_security = {
                "ttl": int(current_ttl),
                "show_system": bool(current_show_system),
            }
            ui.label("Безопасность").classes("text-xl font-semibold")
            ui.label("Максимальная длительность новой сессии пользователя. Допустимый диапазон: 1-7 дней.").classes("rag-meta")
            ttl_input = ui.number(
                "Срок сессии, дней",
                value=current_ttl,
                min=1,
                max=7,
                step=1,
            ).props("dense outlined").classes("w-full max-w-xs")
            show_system_input = ui.checkbox(
                "Показывать служебные файлы администратору",
                value=current_show_system,
            )
            ui.label("Обычные пользователи служебные файлы не видят независимо от этой настройки.").classes("rag-meta")
            action_row = ui.row().classes("rag-dirty-actions")
            action_row.set_visibility(False)

            def current_security() -> Dict[str, Any]:
                return {
                    "ttl": int(ttl_input.value or current_ttl),
                    "show_system": bool(show_system_input.value),
                }

            def refresh_security_dirty() -> None:
                action_row.set_visibility(current_security() != initial_security)

            def reset_security() -> None:
                ttl_input.set_value(initial_security["ttl"])
                show_system_input.set_value(initial_security["show_system"])
                action_row.set_visibility(False)

            def save_session_ttl() -> None:
                saved = auth_db.set_session_ttl_days(int(ttl_input.value or current_ttl))
                show_system = auth_db.set_show_system_files_for_admin(bool(show_system_input.value))
                initial_security.update({"ttl": int(saved), "show_system": bool(show_system)})
                action_row.set_visibility(False)
                _log_app_event(
                    state,
                    "settings",
                    "security",
                    details={"session_ttl_days": saved, "show_system_files_for_admin": show_system},
                )
                ui.notify(f"Сохранено: сессии {saved} дн., служебные файлы {'видны админу' if show_system else 'скрыты'}.", type="positive")
                render()

            ttl_input.on_value_change(lambda _: refresh_security_dirty())
            show_system_input.on_value_change(lambda _: refresh_security_dirty())
            with action_row:
                with ui.row().classes("rag-dirty-actions-inner"):
                    ui.button("Отменить", icon="close", on_click=reset_security).props("flat dense")
                    ui.button("Сохранить настройки безопасности", icon="save", on_click=save_session_ttl).props("outline dense")

    def render_admin_path_settings() -> None:
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            initial_paths = {
                "catalog_path": str(state.cfg.get("catalog_path") or "").strip(),
                "qdrant_url": str(state.cfg.get("qdrant_url") or "").strip(),
                "qdrant_db_path": str(state.cfg.get("qdrant_db_path") or "").strip(),
                "collection_name": str(state.cfg.get("collection_name") or "catalog").strip() or "catalog",
                "telemetry_db_path": str(state.cfg.get("telemetry_db_path") or "").strip(),
                "log_file": str(state.cfg.get("log_file") or "").strip(),
            }
            ui.label("Пути и подключение").classes("text-xl font-semibold")
            ui.label("Эти настройки видны только администратору. После сохранения поиск переподключается к Qdrant с новыми значениями.").classes("rag-meta")
            catalog_input = ui.input("Каталог документов", value=str(state.cfg.get("catalog_path") or "")).props("dense outlined").classes("w-full")
            qdrant_url_input = ui.input("Qdrant URL", value=str(state.cfg.get("qdrant_url") or "")).props("dense outlined").classes("w-full")
            qdrant_db_input = ui.input("Локальный путь Qdrant", value=str(state.cfg.get("qdrant_db_path") or "")).props("dense outlined").classes("w-full")
            collection_input = ui.input("Коллекция", value=str(state.cfg.get("collection_name") or "catalog")).props("dense outlined").classes("w-full")
            telemetry_input = ui.input("БД телеметрии", value=str(state.cfg.get("telemetry_db_path") or "")).props("dense outlined").classes("w-full")
            log_input = ui.input("Лог автоматизации", value=str(state.cfg.get("log_file") or "")).props("dense outlined").classes("w-full")

            with ui.row().classes("w-full gap-2"):
                ui.label(f"Текущий каталог: {state.cfg.get('catalog_path') or '-'}").classes("rag-path")
                ui.label(f"Текущий Qdrant: {state.cfg.get('qdrant_url') or state.cfg.get('qdrant_db_path') or '-'}").classes("rag-path")
            action_row = ui.row().classes("rag-dirty-actions")
            action_row.set_visibility(False)

            def current_paths() -> Dict[str, Any]:
                return {
                    "catalog_path": str(catalog_input.value or "").strip(),
                    "qdrant_url": str(qdrant_url_input.value or "").strip(),
                    "qdrant_db_path": str(qdrant_db_input.value or "").strip(),
                    "collection_name": str(collection_input.value or "catalog").strip() or "catalog",
                    "telemetry_db_path": str(telemetry_input.value or "").strip(),
                    "log_file": str(log_input.value or "").strip(),
                }

            def refresh_paths_dirty() -> None:
                action_row.set_visibility(current_paths() != initial_paths)

            def reset_paths() -> None:
                catalog_input.set_value(initial_paths["catalog_path"])
                qdrant_url_input.set_value(initial_paths["qdrant_url"])
                qdrant_db_input.set_value(initial_paths["qdrant_db_path"])
                collection_input.set_value(initial_paths["collection_name"])
                telemetry_input.set_value(initial_paths["telemetry_db_path"])
                log_input.set_value(initial_paths["log_file"])
                action_row.set_visibility(False)

            def save_paths() -> None:
                values = current_paths()
                new_catalog = values["catalog_path"]
                if new_catalog and not Path(new_catalog).exists():
                    ui.notify("Каталог документов не найден. Проверьте путь.", type="negative")
                    return
                new_qdrant_url = values["qdrant_url"]
                new_qdrant_db = values["qdrant_db_path"]
                if not new_qdrant_url and not new_qdrant_db:
                    ui.notify("Укажите Qdrant URL или локальный путь Qdrant.", type="warning")
                    return
                try:
                    state.cfg = _save_config_patch(values)
                    initial_paths.update(values)
                    action_row.set_visibility(False)
                    state.searcher = None
                    state.searcher_error = ""
                    state.telemetry = None
                    _log_app_event(state, "settings", "save_paths", details={key: state.cfg.get(key) for key in CONFIG_PATH_KEYS})
                    ui.notify("Пути сохранены.", type="positive")
                    render()
                except Exception as exc:
                    ui.notify(f"Не удалось сохранить пути: {exc}", type="negative")

            catalog_input.on_value_change(lambda _: refresh_paths_dirty())
            qdrant_url_input.on_value_change(lambda _: refresh_paths_dirty())
            qdrant_db_input.on_value_change(lambda _: refresh_paths_dirty())
            collection_input.on_value_change(lambda _: refresh_paths_dirty())
            telemetry_input.on_value_change(lambda _: refresh_paths_dirty())
            log_input.on_value_change(lambda _: refresh_paths_dirty())
            with action_row:
                with ui.row().classes("rag-dirty-actions-inner"):
                    ui.button("Отменить", icon="close", on_click=reset_paths).props("flat dense")
                    ui.button("Сохранить пути", icon="save", on_click=save_paths).props("outline dense")

    def render_admin_llm_settings() -> None:
        def _fetch_ollama_models(ollama_url: str) -> List[str]:
            """Запросить список моделей из Ollama /api/tags. Возвращает [] при ошибке."""
            try:
                import urllib.request as _ur  # noqa: PLC0415
                import json as _json  # noqa: PLC0415
                req = _ur.Request(f"{ollama_url.rstrip('/')}/api/tags", method="GET")
                with _ur.urlopen(req, timeout=4) as resp:
                    data = _json.loads(resp.read().decode())
                return sorted(m["name"] for m in (data.get("models") or []) if m.get("name"))
            except Exception:
                return []

        current_url = str(state.cfg.get("ollama_url") or "http://localhost:11434")
        current_expand = str(state.cfg.get("llm_expand_model") or "phi3:mini")
        current_rag = str(state.cfg.get("llm_rag_model") or "qwen3:8b")

        # Подтягиваем модели сразу при рендере
        available_models = _fetch_ollama_models(current_url)
        # Гарантируем, что текущие значения есть в списке даже если Ollama недоступен
        for m in [current_expand, current_rag]:
            if m and m not in available_models:
                available_models.insert(0, m)
        if not available_models:
            available_models = [current_expand, current_rag]

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            initial_llm = {
                "llm_enabled": bool(state.cfg.get("llm_enabled")),
                "ollama_url": current_url.strip(),
                "llm_expand_model": current_expand.strip(),
                "llm_rag_model": current_rag.strip(),
            }
            ui.label("Нейросеть (LLM)").classes("text-xl font-semibold")
            ui.label(
                "Используется Ollama, запущенный локально. "
                "Включите, чтобы получать ответ ИИ по документам и автоматически расширять запросы."
            ).classes("rag-meta")

            llm_toggle = ui.switch(
                "Включить ИИ-ответ и расширение запроса",
                value=bool(state.cfg.get("llm_enabled")),
            )
            ollama_url_input = ui.input(
                "Ollama URL",
                value=current_url,
            ).props("dense outlined").classes("w-full")

            status_label = ui.label(
                f"Найдено моделей: {len(available_models)}" if available_models else "Ollama недоступен — список пуст"
            ).classes("rag-meta text-sm")

            expand_select = ui.select(
                label="Модель расширения запроса (быстрая, лёгкая)",
                options=available_models,
                value=current_expand,
                with_input=True,
            ).props("dense outlined").classes("w-full")

            rag_select = ui.select(
                label="Модель RAG Q&A (умная, для анализа документов)",
                options=available_models,
                value=current_rag,
                with_input=True,
            ).props("dense outlined").classes("w-full")

            async def refresh_models() -> None:
                url = str(ollama_url_input.value or "http://localhost:11434").strip()
                models = await run.io_bound(_fetch_ollama_models, url)
                for m in [str(expand_select.value or ""), str(rag_select.value or "")]:
                    if m and m not in models:
                        models.insert(0, m)
                if not models:
                    status_label.set_text("Ollama недоступен или нет установленных моделей")
                    ui.notify("Ollama не отвечает по адресу: " + url, type="warning")
                    return
                expand_select.options = models
                rag_select.options = models
                expand_select.update()
                rag_select.update()
                status_label.set_text(f"Найдено моделей: {len(models)}")
                ui.notify(f"Обновлено: {len(models)} моделей", type="positive")

            ui.button("Обновить список моделей", icon="refresh", on_click=refresh_models).props("flat dense")

            action_row = ui.row().classes("rag-dirty-actions")
            action_row.set_visibility(False)

            def current_llm_settings() -> Dict[str, Any]:
                return {
                    "llm_enabled": bool(llm_toggle.value),
                    "ollama_url": str(ollama_url_input.value or "http://localhost:11434").strip(),
                    "llm_expand_model": str(expand_select.value or "phi3:mini").strip(),
                    "llm_rag_model": str(rag_select.value or "qwen3:8b").strip(),
                }

            def refresh_llm_dirty() -> None:
                action_row.set_visibility(current_llm_settings() != initial_llm)

            def reset_llm_settings() -> None:
                llm_toggle.set_value(initial_llm["llm_enabled"])
                ollama_url_input.set_value(initial_llm["ollama_url"])
                expand_select.set_value(initial_llm["llm_expand_model"])
                rag_select.set_value(initial_llm["llm_rag_model"])
                action_row.set_visibility(False)

            def save_llm_settings() -> None:
                try:
                    values = current_llm_settings()
                    cfg = load_config()
                    cfg["llm_enabled"] = values["llm_enabled"]
                    cfg["ollama_url"] = values["ollama_url"]
                    cfg["llm_expand_model"] = values["llm_expand_model"]
                    cfg["llm_rag_model"] = values["llm_rag_model"]
                    save_config(cfg)
                    state.cfg = cfg
                    initial_llm.update(values)
                    action_row.set_visibility(False)
                    _log_app_event(state, "settings", "save_llm", details={
                        "llm_enabled": cfg["llm_enabled"],
                        "ollama_url": cfg["ollama_url"],
                    })
                    ui.notify("Настройки нейросети сохранены.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось сохранить: {exc}", type="negative")

            llm_toggle.on_value_change(lambda _: refresh_llm_dirty())
            ollama_url_input.on_value_change(lambda _: refresh_llm_dirty())
            expand_select.on_value_change(lambda _: refresh_llm_dirty())
            rag_select.on_value_change(lambda _: refresh_llm_dirty())
            with action_row:
                with ui.row().classes("rag-dirty-actions-inner"):
                    ui.button("Отменить", icon="close", on_click=reset_llm_settings).props("flat dense")
                    ui.button("Сохранить настройки нейросети", icon="save", on_click=save_llm_settings).props("outline dense")

    def render_admin_search_aliases() -> None:
        telemetry = _get_telemetry(state)
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Синонимы поиска").classes("text-xl font-semibold")
            ui.label(
                "Группы расширяют запросы без переиндексации: например, «реквизиты» ищет карточки предприятия и расчетные счета."
            ).classes("rag-meta")

            groups = telemetry.list_search_alias_groups() if hasattr(telemetry, "list_search_alias_groups") else []
            with ui.expansion("Добавить группу", icon="add", value=False).classes("w-full"):
                new_key = ui.input("Ключ группы", placeholder="company_card").props("dense outlined").classes("w-full")
                new_label = ui.input("Название", placeholder="Карточка предприятия").props("dense outlined").classes("w-full")
                new_aliases = ui.textarea("Синонимы, по одному на строку").props("dense outlined autogrow").classes("w-full")
                new_negative = ui.textarea("Исключения, по одному на строку").props("dense outlined autogrow").classes("w-full")

                def add_group() -> None:
                    label = str(new_label.value or "").strip()
                    key = str(new_key.value or label).strip()
                    aliases = [x.strip() for x in str(new_aliases.value or "").splitlines() if x.strip()]
                    negatives = [x.strip() for x in str(new_negative.value or "").splitlines() if x.strip()]
                    try:
                        telemetry.save_search_alias_group(key=key, label=label or key, aliases=aliases, negative_aliases=negatives)
                        _log_app_event(state, "settings", "search_alias_add", details={"key": key, "label": label})
                        ui.notify("Группа синонимов добавлена.", type="positive")
                        render()
                    except Exception as exc:
                        ui.notify(f"Не удалось сохранить: {exc}", type="negative")

                ui.button("Добавить группу", icon="save", on_click=add_group).props("outline")

            for group in groups:
                group_key = str(group.get("key") or "")
                alias_text = "\n".join(str(a.get("alias") or "") for a in group.get("aliases") or [])
                negative_text = "\n".join(str(x) for x in group.get("negative_aliases") or [])
                with ui.expansion(str(group.get("label") or group_key), icon="travel_explore", value=False).classes("w-full"):
                    initial_group = {
                        "label": str(group.get("label") or ""),
                        "aliases": alias_text,
                        "negative": negative_text,
                    }
                    label_input = ui.input("Название", value=str(group.get("label") or "")).props("dense outlined").classes("w-full")
                    aliases_input = ui.textarea("Синонимы", value=alias_text).props("dense outlined autogrow").classes("w-full")
                    negative_input = ui.textarea("Исключения", value=negative_text).props("dense outlined autogrow").classes("w-full")
                    ui.label(f"Ключ: {group_key} · обновлено: {group.get('updated_at') or '-'}").classes("rag-meta")

                    def save_group(
                        key: str = group_key,
                        label_ref: Any = label_input,
                        aliases_ref: Any = aliases_input,
                        negative_ref: Any = negative_input,
                    ) -> None:
                        aliases = [x.strip() for x in str(aliases_ref.value or "").splitlines() if x.strip()]
                        negatives = [x.strip() for x in str(negative_ref.value or "").splitlines() if x.strip()]
                        telemetry.save_search_alias_group(
                            key=key,
                            label=str(label_ref.value or key),
                            aliases=aliases,
                            negative_aliases=negatives,
                        )
                        initial_group.update({
                            "label": str(label_ref.value or key),
                            "aliases": str(aliases_ref.value or ""),
                            "negative": str(negative_ref.value or ""),
                        })
                        group_actions.set_visibility(False)
                        _log_app_event(state, "settings", "search_alias_save", details={"key": key})
                        ui.notify("Синонимы сохранены.", type="positive")
                        render()

                    def delete_group(key: str = group_key) -> None:
                        telemetry.delete_search_alias_group(key=key)
                        _log_app_event(state, "settings", "search_alias_delete", details={"key": key})
                        ui.notify("Группа удалена.", type="positive")
                        render()

                    group_actions = ui.row().classes("rag-dirty-actions")
                    group_actions.set_visibility(False)

                    def current_group_values() -> Dict[str, Any]:
                        return {
                            "label": str(label_input.value or group_key),
                            "aliases": str(aliases_input.value or ""),
                            "negative": str(negative_input.value or ""),
                        }

                    def refresh_group_dirty() -> None:
                        group_actions.set_visibility(current_group_values() != initial_group)

                    def reset_group_fields() -> None:
                        label_input.set_value(initial_group["label"])
                        aliases_input.set_value(initial_group["aliases"])
                        negative_input.set_value(initial_group["negative"])
                        group_actions.set_visibility(False)

                    label_input.on_value_change(lambda _: refresh_group_dirty())
                    aliases_input.on_value_change(lambda _: refresh_group_dirty())
                    negative_input.on_value_change(lambda _: refresh_group_dirty())

                    with ui.row().classes("gap-2"):
                        ui.button("Удалить", icon="delete", on_click=delete_group).props("flat dense")
                    with group_actions:
                        with ui.row().classes("rag-dirty-actions-inner"):
                            ui.button("Отменить", icon="close", on_click=reset_group_fields).props("flat dense")
                            ui.button("Сохранить", icon="save", on_click=save_group).props("outline dense")

            candidates = telemetry.suggest_search_alias_candidates(limit=12) if hasattr(telemetry, "suggest_search_alias_candidates") else []
            with ui.expansion("Кандидаты из истории поиска", icon="psychology", value=False).classes("w-full"):
                if not candidates:
                    ui.label("Пока нет кандидатов. Они появятся после положительных реакций на результаты поиска.").classes("rag-meta")
                for item in candidates:
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.label(str(item.get("candidate") or "")).classes("font-medium")
                        ui.label(f"запрос: {item.get('query') or ''}").classes("rag-meta")
                        ui.label(str(item.get("title") or item.get("path") or "")).classes("rag-path flex-1")

    def render_settings_screen() -> None:
        auth_db = _get_auth_db(state)

        # ── Форма входа (без боковой панели) ────────────────────────────
        if state.current_user is None:
            ui.label("Настройки").classes("text-2xl font-semibold")
            with ui.column().classes("rag-card w-full max-w-xl p-4 gap-3"):
                ui.label("Вход пользователя").classes("text-xl font-semibold")
                ui.label("Для первого входа администратора используйте admin / admin, затем смените пароль.").classes("rag-meta")
                username_input = ui.input("Логин").props("dense outlined").classes("w-full")
                password_input = ui.input("Пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")

                def login() -> None:
                    user = auth_db.login(username=str(username_input.value or ""), password=str(password_input.value or ""))
                    if not user:
                        ui.notify("Неверный логин или пароль.", type="negative")
                        return
                    state.current_user = user
                    state.auth_token = auth_db.create_session(username=str(user.get("username") or ""))
                    try:
                        app.storage.user["auth_token"] = state.auth_token
                    except Exception:
                        pass
                    ui.notify("Вход выполнен.", type="positive")
                    render()

                password_input.on("keyup.enter", lambda _: login())
                ui.button("Войти", icon="login", on_click=login).props("unelevated")
            return

        user = state.current_user
        is_admin = str(user.get("role") or "user") == "admin"

        # ── Реестр секций: (key, icon, label, keywords) ─────────────────
        user_sections: List[tuple] = [
            ("profile",   "person",          "Профиль",              ["имя", "аккаунт", "профиль"]),
            ("telegram_sync", "sync",         "Синхронизация Telegram", ["telegram", "бот", "синхронизация"]),
            ("explorer",  "folder_open",      "Проводник",            ["файлы", "вид", "сортировка"]),
            ("favorites", "star_border",      "Избранное",            ["закладки"]),
            ("password",  "key",              "Пароль и выход",       ["смена", "выход", "logout"]),
        ]
        admin_sections: List[tuple] = [
            ("paths",         "storage",        "Пути и Qdrant",          ["каталог", "база", "url", "коллекция"]),
            ("llm",           "smart_toy",      "Нейросеть",              ["ollama", "модель", "ai", "llm", "rag"]),
            ("aliases",       "travel_explore", "Синонимы поиска",        ["группы", "расширение", "запросы"]),
            ("indexing",      "build",          "Индексация",             ["индекс", "статус", "прогресс"]),
            ("security",      "security",       "Сессии и безопасность",  ["сессии", "системные файлы"]),
            ("users",         "group",          "Пользователи",           ["роль", "статус", "логин"]),
            ("registrations", "person_add",     "Регистрации",            ["заявки", "одобрить"]),
            ("telegram_bot",   "send",           "Telegram бот",           ["бот", "chat id", "telegram"]),
        ]

        active = [state.settings_section]  # сохраняем между ре-рендерами
        q_ref  = [""]

        # ── IDE-лейаут ───────────────────────────────────────────────────
        with ui.row().classes("w-full gap-0 items-start"):

            # Левая боковая панель
            with ui.column().classes("flex-none gap-1").style(
                "width:220px; min-width:220px; border-right:1px solid #e5e7eb; padding-right:12px; margin-right:16px"
            ):
                ui.label("Настройки").classes("text-xl font-semibold mb-2")
                search_box = ui.input(
                    placeholder="Поиск настроек…",
                    on_change=lambda e: (q_ref.__setitem__(0, str(e.value or "").lower()), render_nav()),
                ).props("dense outlined clearable").classes("w-full")

                nav_col = ui.column().classes("w-full gap-0")

            # Правая область контента
            content_col = ui.column().classes("flex-1 gap-3 min-w-0")

        # ── Навигация ────────────────────────────────────────────────────
        def _visible(entry: tuple) -> bool:
            q = q_ref[0]
            if not q:
                return True
            key, icon, label, kws = entry
            return q in label.lower() or any(q in kw.lower() for kw in kws)

        def render_nav() -> None:
            nav_col.clear()
            with nav_col:
                groups: List[tuple] = [("", user_sections)]
                if is_admin:
                    groups.append(("Администратор", admin_sections))
                for group_label, sections in groups:
                    filtered = [s for s in sections if _visible(s)]
                    if not filtered:
                        continue
                    if group_label:
                        ui.label(group_label.upper()).classes(
                            "text-xs text-gray-400 font-semibold mt-3 mb-1 px-2"
                        )
                    for key, icon, label, _ in filtered:
                        is_active = active[0] == key
                        bg = "background:#eef2ff;" if is_active else ""
                        with ui.row().classes("w-full items-center gap-2 px-2 py-1 rounded cursor-pointer").style(
                            bg + "user-select:none"
                        ).on("click", lambda k=key: navigate(k)):
                            ui.icon(icon, size="16px").classes(
                                "text-indigo-600" if is_active else "text-gray-400"
                            )
                            ui.label(label).classes(
                                "text-sm font-medium text-indigo-700" if is_active else "text-sm text-gray-700"
                            )

        # ── Контент секции ───────────────────────────────────────────────
        def render_section() -> None:
            content_col.clear()
            with content_col:
                sec = active[0]

                if sec == "profile":
                    with ui.column().classes("rag-card w-full p-4 gap-3"):
                        initial_profile = {
                            "display_name": str(user.get("display_name") or ""),
                        }
                        ui.label("Профиль").classes("text-xl font-semibold")
                        ui.label(
                            f"Логин: {user.get('username')} · роль: {user.get('role')} · статус: {user.get('status')}"
                        ).classes("rag-meta")
                        disp_in = ui.input("Имя", value=str(user.get("display_name") or "")).props("dense outlined").classes("w-full")
                        linked_tg_id = str(user.get("telegram_chat_id") or "").strip()
                        linked_tg_un = str(user.get("telegram_username") or "").strip()

                        def save_profile() -> None:
                            auth_db.update_profile(
                                username=str(user.get("username") or ""),
                                display_name=str(disp_in.value or ""),
                                telegram_chat_id=linked_tg_id,
                                telegram_username=linked_tg_un,
                            )
                            initial_profile["display_name"] = str(disp_in.value or "")
                            _refresh_current_user(state)
                            profile_actions.set_visibility(False)
                            ui.notify("Профиль сохранён.", type="positive")

                        profile_actions = ui.row().classes("rag-dirty-actions")
                        profile_actions.set_visibility(False)

                        def refresh_profile_dirty() -> None:
                            profile_actions.set_visibility(str(disp_in.value or "") != initial_profile["display_name"])

                        def reset_profile() -> None:
                            disp_in.set_value(initial_profile["display_name"])
                            profile_actions.set_visibility(False)

                        disp_in.on_value_change(lambda _: refresh_profile_dirty())
                        with profile_actions:
                            with ui.row().classes("rag-dirty-actions-inner"):
                                ui.button("Отменить", icon="close", on_click=reset_profile).props("flat dense")
                                ui.button("Сохранить профиль", icon="save", on_click=save_profile).props("outline dense")

                elif sec == "telegram_sync":
                    with ui.column().classes("rag-card w-full p-4 gap-3"):
                        linked_tg_id = str(user.get("telegram_chat_id") or "").strip()
                        linked_tg_un = str(user.get("telegram_username") or "").strip()
                        linked_label = f"@{linked_tg_un}" if linked_tg_un else linked_tg_id
                        ui.label("Синхронизация Telegram").classes("text-xl font-semibold")
                        ui.label("Связь нужна для входа через Telegram и команд бота от вашего имени.").classes("rag-meta")
                        with ui.row().classes("w-full items-center gap-2"):
                            ui.icon("check_circle" if linked_tg_id else "radio_button_unchecked").classes(
                                "text-green-600" if linked_tg_id else "text-gray-400"
                            )
                            ui.label(f"Привязан: {linked_label}" if linked_tg_id else "Telegram не привязан").classes("font-medium")

                        def bind_tg() -> None:
                            bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
                            if not bot_link:
                                ui.notify("В config.json не задан telegram_bot_link.", type="warning")
                                return
                            out = auth_db.create_telegram_link_token(username=str(user.get("username") or ""))
                            if not out.get("ok"):
                                ui.notify(f"Ошибка: {out.get('reason')}", type="negative")
                                return
                            link = _telegram_deeplink(bot_link, "link", str(out.get("token") or ""))
                            if not link:
                                ui.notify("Не удалось создать ссылку привязки.", type="negative")
                                return
                            ui.run_javascript(
                                "(() => {"
                                f"const url = {json.dumps(link)};"
                                "const w = window.open(url, '_blank', 'noopener,noreferrer');"
                                "if (!w) { window.location.href = url; }"
                                "})();"
                            )
                            ui.notify("Откройте Telegram и подтвердите привязку.", type="positive")

                        def unlink_tg() -> None:
                            if not linked_tg_id:
                                return
                            auth_db.unlink_telegram_chat_id(linked_tg_id)
                            _refresh_current_user(state)
                            ui.notify("Telegram отвязан.", type="warning")
                            render_section()

                        with ui.row().classes("gap-2"):
                            ui.button("Синхронизировать", icon="link", on_click=bind_tg).props("outline")
                            if linked_tg_id:
                                ui.button("Отвязать", icon="link_off", on_click=unlink_tg).props("flat color=negative")

                elif sec == "explorer":
                    with ui.column().classes("rag-card w-full p-4 gap-3"):
                        ui.label("Проводник").classes("text-xl font-semibold")
                        ui.label(
                            f"Вид: {state.explorer_view} · сортировка: {state.explorer_sort} · "
                            f"{'убывание' if state.explorer_desc else 'возрастание'} · тип: {state.explorer_ext}"
                        ).classes("rag-meta")

                        def reset_explorer() -> None:
                            auth_db.reset_user_settings(username=str(user.get("username") or ""))
                            state.explorer_view = "Таблица"
                            state.explorer_sort = "По имени"
                            state.explorer_desc = False
                            state.explorer_ext = "Все"
                            _log_app_event(state, "settings", "reset_explorer")
                            ui.notify("Настройки проводника сброшены.", type="positive")
                            render_section()

                        ui.button("Сбросить настройки проводника", icon="restart_alt", on_click=reset_explorer).props("outline")

                elif sec == "favorites":
                    with ui.column().classes("rag-card w-full p-4 gap-3"):
                        ui.label("Избранное").classes("text-xl font-semibold")
                        if not state.favorites:
                            ui.label("Закладок пока нет. Добавьте файл или папку звёздочкой в проводнике.").classes("rag-meta")
                        for fav in state.favorites:
                            fav_path = Path(str(fav.get("path") or ""))
                            item_type = str(fav.get("item_type") or "")
                            with ui.row().classes("w-full items-center gap-2"):
                                ui.icon("folder" if item_type == "folder" else "description")
                                ui.label(str(fav.get("title") or fav_path.name or fav_path)).classes("font-medium")
                                ui.label(str(fav_path)).classes("rag-path flex-1")
                                if item_type == "folder":
                                    ui.button("Открыть", on_click=lambda p=fav_path: go_explorer(str(p))).props("outline dense")
                                else:
                                    ui.button("Открыть", on_click=lambda p=fav_path: open_file_viewer(p)).props("outline dense")
                                ui.button(icon="delete", on_click=lambda p=fav_path: (
                                    _toggle_favorite(state, p), render_section()
                                )).props("flat round dense")

                elif sec == "password":
                    with ui.column().classes("rag-card w-full p-4 gap-3"):
                        ui.label("Смена пароля").classes("text-xl font-semibold")
                        old_pw = ui.input("Текущий пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                        new_pw = ui.input("Новый пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                        new_pw2 = ui.input("Повторите пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")

                        def change_pw() -> None:
                            if str(new_pw.value or "") != str(new_pw2.value or ""):
                                ui.notify("Пароли не совпадают.", type="warning")
                                return
                            ok = auth_db.change_password(
                                username=str(user.get("username") or ""),
                                old_password=str(old_pw.value or ""),
                                new_password=str(new_pw.value or ""),
                            )
                            if ok:
                                _refresh_current_user(state)
                            ui.notify("Пароль изменён." if ok else "Не удалось изменить пароль.",
                                      type="positive" if ok else "negative")

                        with ui.row().classes("gap-2"):
                            ui.button("Сменить пароль", icon="key", on_click=change_pw).props("outline")
                            ui.button("Выйти", icon="logout", on_click=do_logout).props("flat")

                elif sec == "paths":
                    render_admin_path_settings()
                elif sec == "llm":
                    render_admin_llm_settings()
                elif sec == "aliases":
                    render_admin_search_aliases()
                elif sec == "indexing":
                    render_index_dashboard()
                elif sec == "security":
                    render_admin_security_settings(auth_db)
                elif sec == "users":
                    render_admin_users(auth_db)
                elif sec == "registrations":
                    render_admin_registration_requests(auth_db)
                elif sec == "telegram_bot":
                    with ui.column().classes("rag-card w-full p-4 gap-3"):
                        enabled = bool(state.cfg.get("telegram_enabled"))
                        token_set = bool(str(state.cfg.get("telegram_bot_token") or "").strip())
                        bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
                        ui.label("Управление Telegram ботом").classes("text-xl font-semibold")
                        with ui.row().classes("gap-2 flex-wrap"):
                            ui.label(f"Статус: {'включен' if enabled else 'выключен'}").classes("rag-chip")
                            ui.label(f"Токен: {'задан' if token_set else 'не задан'}").classes("rag-chip")
                            ui.label(f"Ссылка: {'задана' if bot_link else 'не задана'}").classes("rag-chip")
                        if bot_link:
                            ui.link("Открыть бота", bot_link, new_tab=True).classes("rag-link")
                    render_admin_telegram_chats(auth_db)

        def navigate(key: str) -> None:
            active[0] = key
            state.settings_section = key
            render_nav()
            render_section()

        render_nav()
        render_section()

    def render_telegram_screen() -> None:
        enabled = bool(state.cfg.get("telegram_enabled"))
        token_set = bool(str(state.cfg.get("telegram_bot_token") or "").strip())
        with ui.column().classes("rag-card w-full p-4 gap-2"):
            ui.label(f"Статус: {'включен' if enabled else 'выключен'}").classes("text-lg font-semibold")
            ui.label(f"Токен: {'задан' if token_set else 'не задан'}").classes("rag-meta")
            bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
            if bot_link:
                ui.link("Открыть бота", bot_link, new_tab=True)

    def render_stats_screen() -> None:
        if str((state.current_user or {}).get("role") or "") != "admin":
            ui.label("Раздел доступен только администратору.").classes("rag-card p-4 text-red-700")
            return
        telemetry_path = _telemetry_db_path(state.cfg)
        auth_db = _get_auth_db(state)
        ui.label("Аналитика").classes("text-2xl font-semibold")
        with ui.row().classes("rag-analytics-tabs w-full gap-2 flex-wrap"):
            for label in ["Обзор", "Запросы", "Пользователи", "Производительность", "Аудит", "Ошибки"]:
                ui.button(label).props("outline dense no-caps" if label != "Обзор" else "unelevated dense no-caps")

        overview = _db_query_dicts(
            telemetry_path,
            """
            SELECT
              COUNT(*) AS searches,
              COALESCE(AVG(duration_ms), 0) AS avg_ms,
              SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS errors,
              COUNT(DISTINCT COALESCE(NULLIF(username, ''), source, 'unknown')) AS users
            FROM search_logs
            """,
        )
        overview_row = overview[0] if overview else {}

        def render_kpi(label: str, value: str, icon: str) -> None:
            with ui.column().classes("rag-card rag-kpi p-4 gap-2"):
                with ui.row().classes("items-center gap-2"):
                    ui.icon(icon).classes("text-xl")
                    ui.label(label).classes("rag-meta")
                ui.label(value).classes("rag-kpi-value")
                ui.element("div").classes("rag-mini-bar")

        with ui.row().classes("w-full gap-3"):
            render_kpi("Запросов", str(int(overview_row.get("searches") or 0)), "search")
            render_kpi("Средняя задержка", f"{int(float(overview_row.get('avg_ms') or 0))} мс", "speed")
            render_kpi("Пользователей", str(int(overview_row.get("users") or 0)), "group")
            render_kpi("Ошибок", str(int(overview_row.get("errors") or 0)), "error")

        with ui.row().classes("w-full gap-3 items-stretch"):
            with ui.column().classes("rag-card flex-1 p-4 gap-2"):
                ui.label("AI summary").classes("text-xl font-semibold")
                ui.label(
                    "Сводка формируется из телеметрии: пики запросов, ошибки, медленные операции и Telegram/Web источники. "
                    "Подключение генерации отчета оставлено как следующий слой поверх текущей таблицы событий."
                ).classes("rag-meta")
            with ui.column().classes("rag-card p-4 gap-2 min-w-56"):
                ui.label("Экспорт").classes("text-xl font-semibold")
                ui.button("CSV", icon="table_view", on_click=lambda: ui.notify("CSV экспорт будет подключен к текущим фильтрам.", type="warning")).props("outline")
                ui.button("PDF", icon="picture_as_pdf", on_click=lambda: ui.notify("PDF экспорт будет подключен после шаблона отчета.", type="warning")).props("outline")

        searches_by_day = _db_query_dicts(
            telemetry_path,
            """
            SELECT substr(ts, 1, 10) AS day, COUNT(*) AS count
            FROM search_logs
            GROUP BY substr(ts, 1, 10)
            ORDER BY day
            LIMIT 30
            """,
        )
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Поиски по дням").classes("text-xl font-semibold")
            ui.echart({
                "tooltip": {"trigger": "axis"},
                "xAxis": {"type": "category", "data": [row["day"] for row in searches_by_day]},
                "yAxis": {"type": "value"},
                "series": [{"type": "bar", "data": [row["count"] for row in searches_by_day], "name": "Поиски"}],
            }).classes("w-full h-72")

        top_queries = _db_query_dicts(
            telemetry_path,
            """
            SELECT query, COUNT(*) AS count
            FROM search_logs
            WHERE query <> ''
            GROUP BY lower(query)
            ORDER BY count DESC
            LIMIT 20
            """,
        )
        top_users = _db_query_dicts(
            telemetry_path,
            """
            SELECT COALESCE(NULLIF(username, ''), source, 'unknown') AS username, COUNT(*) AS count
            FROM search_logs
            GROUP BY COALESCE(NULLIF(username, ''), source, 'unknown')
            ORDER BY count DESC
            LIMIT 20
            """,
        )
        top_features = _db_query_dicts(
            telemetry_path,
            """
            SELECT feature || ':' || action AS name, COUNT(*) AS count
            FROM app_events
            GROUP BY feature, action
            ORDER BY count DESC
            LIMIT 20
            """,
        )
        auth_events = auth_db.list_auth_events(limit=50)

        with ui.row().classes("w-full gap-3 items-start"):
            with ui.column().classes("rag-card flex-1 p-4 gap-2"):
                ui.label("Топ запросов").classes("text-xl font-semibold")
                for row in top_queries:
                    ui.label(f"{row['query']}: {row['count']}").classes("rag-meta")
            with ui.column().classes("rag-card flex-1 p-4 gap-2"):
                ui.label("Топ пользователей").classes("text-xl font-semibold")
                for row in top_users:
                    ui.label(f"{row['username']}: {row['count']}").classes("rag-meta")
            with ui.column().classes("rag-card flex-1 p-4 gap-2"):
                ui.label("Функции").classes("text-xl font-semibold")
                for row in top_features:
                    ui.label(f"{row['name']}: {row['count']}").classes("rag-meta")

        with ui.expansion("История запросов", value=True).classes("rag-group-panel w-full"):
            with ui.column().classes("w-full gap-2 p-3"):
                with ui.row().classes("w-full gap-2"):
                    search_source_filter = ui.select(
                        ["Все", "Telegram", "Web/прочее"],
                        value="Все",
                        label="Источник",
                    ).props("dense outlined").classes("w-44")
                    search_user_filter = ui.input("Пользователь").props("dense outlined clearable").classes("w-48")
                    search_query_filter = ui.input("Запрос").props("dense outlined clearable").classes("flex-1")
                    search_ok_filter = ui.select(
                        ["Все", "OK", "Ошибки"],
                        value="Все",
                        label="OK",
                    ).props("dense outlined").classes("w-32")

                search_table = ui.table(
                    rows=[],
                    columns=[
                        {"name": "ts", "label": "Время", "field": "ts"},
                        {"name": "source", "label": "Источник", "field": "source"},
                        {"name": "username", "label": "Пользователь", "field": "username"},
                        {"name": "query", "label": "Запрос", "field": "query"},
                        {"name": "results_count", "label": "Результаты", "field": "results_count"},
                        {"name": "duration_ms", "label": "мс", "field": "duration_ms"},
                        {"name": "error", "label": "Ошибка", "field": "error"},
                    ],
                    pagination=10,
                ).classes("w-full")

                def refresh_search_table() -> None:
                    # Каждый вызов делает свежий запрос к БД — данные не устаревают
                    rows = _db_query_dicts(
                        telemetry_path,
                        """
                        SELECT ts, source, username, query, results_count, duration_ms, ok, error
                        FROM search_logs
                        ORDER BY id DESC
                        LIMIT 500
                        """,
                    )
                    source_mode = str(search_source_filter.value or "Все")
                    if source_mode == "Telegram":
                        rows = [row for row in rows if str(row.get("source") or "").startswith("telegram_bot:")]
                    elif source_mode == "Web/прочее":
                        rows = [row for row in rows if not str(row.get("source") or "").startswith("telegram_bot:")]

                    user_needle = str(search_user_filter.value or "").strip().lower()
                    if user_needle:
                        rows = [row for row in rows if user_needle in str(row.get("username") or "").lower()]

                    query_needle = str(search_query_filter.value or "").strip().lower()
                    if query_needle:
                        rows = [row for row in rows if query_needle in str(row.get("query") or "").lower()]

                    ok_mode = str(search_ok_filter.value or "Все")
                    if ok_mode == "OK":
                        rows = [row for row in rows if int(row.get("ok") or 0) == 1]
                    elif ok_mode == "Ошибки":
                        rows = [row for row in rows if int(row.get("ok") or 0) == 0]

                    search_table.rows = rows
                    search_table.update()

                search_source_filter.on_value_change(lambda e: refresh_search_table())
                search_user_filter.on_value_change(lambda e: refresh_search_table())
                search_query_filter.on_value_change(lambda e: refresh_search_table())
                search_ok_filter.on_value_change(lambda e: refresh_search_table())
                refresh_search_table()

        with ui.expansion("История входов", value=False).classes("rag-group-panel w-full"):
            with ui.column().classes("w-full gap-2 p-3"):
                with ui.row().classes("w-full gap-2"):
                    auth_source_filter = ui.select(
                        ["Все", "Telegram", "Web/прочее"],
                        value="Все",
                        label="Источник",
                    ).props("dense outlined").classes("w-44")
                    auth_user_filter = ui.input("Пользователь").props("dense outlined clearable").classes("w-48")
                    auth_event_filter = ui.input("Событие").props("dense outlined clearable").classes("flex-1")
                    auth_ok_filter = ui.select(
                        ["Все", "OK", "Ошибки"],
                        value="Все",
                        label="OK",
                    ).props("dense outlined").classes("w-32")

                auth_table = ui.table(
                    rows=[],
                    columns=[
                        {"name": "ts", "label": "Время", "field": "ts"},
                        {"name": "username", "label": "Пользователь", "field": "username"},
                        {"name": "event_type", "label": "Событие", "field": "event_type"},
                        {"name": "ok", "label": "OK", "field": "ok"},
                        {"name": "error", "label": "Ошибка", "field": "error"},
                    ],
                    pagination=10,
                ).classes("w-full")

                def refresh_auth_table() -> None:
                    rows = auth_events
                    source_mode = str(auth_source_filter.value or "Все")
                    if source_mode == "Telegram":
                        rows = [row for row in rows if str(row.get("event_type") or "").startswith("telegram_")]
                    elif source_mode == "Web/прочее":
                        rows = [row for row in rows if not str(row.get("event_type") or "").startswith("telegram_")]

                    user_needle = str(auth_user_filter.value or "").strip().lower()
                    if user_needle:
                        rows = [row for row in rows if user_needle in str(row.get("username") or "").lower()]

                    event_needle = str(auth_event_filter.value or "").strip().lower()
                    if event_needle:
                        rows = [row for row in rows if event_needle in str(row.get("event_type") or "").lower()]

                    ok_mode = str(auth_ok_filter.value or "Все")
                    if ok_mode == "OK":
                        rows = [row for row in rows if int(row.get("ok") or 0) == 1]
                    elif ok_mode == "Ошибки":
                        rows = [row for row in rows if int(row.get("ok") or 0) == 0]

                    auth_table.rows = rows
                    auth_table.update()

                auth_source_filter.on_value_change(lambda e: refresh_auth_table())
                auth_user_filter.on_value_change(lambda e: refresh_auth_table())
                auth_event_filter.on_value_change(lambda e: refresh_auth_table())
                auth_ok_filter.on_value_change(lambda e: refresh_auth_table())
                refresh_auth_table()

    def render() -> None:
        page_root.classes(remove="search")
        if state.screen == "search":
            page_root.classes(add="search")
        header_title.set_text({
            "search": "Поиск",
            "explorer": "Проводник",
            "index": "Индекс",
            "settings": "Настройки",
            "stats": "Аналитика",
        }.get(state.screen, "Поиск"))
        if state.header_breadcrumbs is not None:
            state.header_breadcrumbs.clear()
        if state.header_explorer_actions is not None:
            state.header_explorer_actions.clear()
        update_nav()
        content.clear()
        with content:
            if state.current_user is None:
                try:
                    drawer.set_value(False)
                except Exception:
                    pass
                try:
                    drawer.set_visibility(False)
                except Exception:
                    pass
                try:
                    menu_button.set_visibility(False)
                except Exception:
                    pass
                try:
                    theme_button.set_visibility(False)
                except Exception:
                    pass
                render_login_screen()
                return
            try:
                drawer.set_visibility(True)
            except Exception:
                pass
            try:
                menu_button.set_visibility(True)
            except Exception:
                pass
            try:
                theme_button.set_visibility(True)
                theme_button.set_icon("light_mode" if state.theme == "dark" else "dark_mode")
            except Exception:
                pass
            dark_mode.set_value(state.theme == "dark")
            touch_activity()
            if state.screen == "explorer":
                try:
                    drawer.set_visibility(True)
                except Exception:
                    pass
                render_explorer_screen()
            elif state.screen == "index":
                render_index_screen()
            elif state.screen == "telegram":
                state.screen = "settings"
                state.settings_section = "telegram_sync"
                render_settings_screen()
            elif state.screen == "settings":
                render_settings_screen()
            elif state.screen == "stats":
                render_stats_screen()
            else:
                render_search_screen()

    render()


@ui.page("/")
def root_page() -> None:
    ui.navigate.to("/search")


@ui.page("/search")
def search_page() -> None:
    _build_page("search")


@ui.page("/explorer")
def explorer_page() -> None:
    _build_page("explorer")


@ui.page("/index")
def index_page() -> None:
    _build_page("index")


@ui.page("/telegram")
def telegram_page() -> None:
    _build_page("settings")


@ui.page("/settings")
def settings_page() -> None:
    _build_page("settings")


@ui.page("/stats")
def stats_page() -> None:
    _build_page("stats")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Запустить NiceGUI-интерфейс RAG Каталога.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-show", action="store_true", help="Не открывать браузер автоматически.")
    args = parser.parse_args(argv)
    cfg = load_config()
    try:
        _run_recovery_cycle(
            cfg,
            recovery_note="server_restart_recovery",
            allow_failed_restart=True,
        )
    except Exception as exc:
        print(f"[nice_app] background recovery skipped: {exc}", file=sys.stderr)
    _start_recovery_watchdog(cfg)
    ui.run(
        title="RAG Каталог",
        host=args.host,
        port=args.port,
        favicon=APP_ICON_PATH if APP_ICON_PATH.exists() else None,
        language="ru",
        reload=False,
        show=not args.no_show,
        dark=False,
        storage_secret="rag-catalog-local-secret",
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
