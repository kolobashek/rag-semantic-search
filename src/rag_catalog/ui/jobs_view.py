"""
jobs_view.py — Global process queue screen.

Shows all background tasks (indexing, OCR, cloud bootstrap/reindex) in one place
with live auto-refresh every 5 seconds.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from nicegui import run, ui

from .helpers import (
    _db_query_dicts,
    _format_duration_seconds,
    _format_relative_time,
)
from .state import PageState, _log_app_event
from .system import _telemetry_db_path, _safe_int, _stop_managed_timer

PROJECT_ROOT = Path(__file__).resolve().parents[3]

_STAGE_LABELS: Dict[str, str] = {
    "all": "все этапы",
    "metadata": "metadata",
    "small": "small chunks",
    "large": "large chunks",
    "ocr": "OCR",
}

_STATUS_LABEL: Dict[str, str] = {
    "running": "выполняется",
    "pending": "ожидает",
    "done": "завершено",
    "completed": "завершено",
    "failed": "ошибка",
    "cancelled": "отменено",
    "idle": "ожидание",
}

_STATUS_COLOR: Dict[str, str] = {
    "running": "text-blue-500",
    "pending": "text-amber-500",
    "done": "text-positive",
    "completed": "text-positive",
    "failed": "text-negative",
    "cancelled": "text-slate-400",
    "idle": "text-slate-400",
}

_STATUS_ICON: Dict[str, str] = {
    "running": "sync",
    "pending": "schedule",
    "done": "check_circle",
    "completed": "check_circle",
    "failed": "error",
    "cancelled": "cancel",
    "idle": "radio_button_unchecked",
}

_TYPE_ICON: Dict[str, str] = {
    "index": "analytics",
    "ocr": "document_scanner",
    "bootstrap": "cloud_upload",
    "reindex": "cloud_sync",
}

_TYPE_COLOR: Dict[str, str] = {
    "index": "text-indigo-500",
    "ocr": "text-teal-500",
    "bootstrap": "text-sky-500",
    "reindex": "text-violet-500",
}


def _fetch_all_jobs(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Aggregate jobs from all sources into a unified list."""
    jobs: List[Dict[str, Any]] = []

    db_path = _telemetry_db_path(cfg)

    # ── Indexing runs ───────────────────────────────────────────────────────
    index_runs = _db_query_dicts(
        db_path,
        """
        SELECT run_id, status, note, worker_pid, ts_started, ts_finished,
               CAST((julianday(COALESCE(ts_finished, CURRENT_TIMESTAMP)) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM index_runs
        ORDER BY ts_started DESC
        LIMIT 30
        """,
    )
    for ir in index_runs:
        run_id = str(ir.get("run_id") or "")
        note = str(ir.get("note") or "")
        stage_match = re.search(r"stage=(all|metadata|small|large)", note.lower())
        stage_key = stage_match.group(1) if stage_match else "all"
        run_status = str(ir.get("status") or "")

        # get current stage progress for running runs
        pct: Optional[int] = None
        detail = ""
        if run_status == "running" and run_id:
            stages = _db_query_dicts(
                db_path,
                "SELECT stage, status, total_files, processed_files FROM index_stage_progress WHERE run_id=? ORDER BY ts_started",
                (run_id,),
            )
            active_stage = next((s for s in stages if s.get("status") == "running"), None)
            if active_stage:
                total = _safe_int(active_stage.get("total_files"), 0)
                processed = _safe_int(active_stage.get("processed_files"), 0)
                pct = min(100, round(processed * 100 / total)) if total > 0 else None
                stage_name = _STAGE_LABELS.get(str(active_stage.get("stage") or ""), str(active_stage.get("stage") or ""))
                detail = f"{stage_name} · {processed:,} / {total:,}".replace(",", " ")
            elif stages:
                done_stages = [s for s in stages if s.get("status") == "done"]
                if done_stages:
                    last = done_stages[-1]
                    stage_name = _STAGE_LABELS.get(str(last.get("stage") or ""), str(last.get("stage") or ""))
                    detail = f"последний: {stage_name}"

        jobs.append({
            "id": run_id or f"ir-{ir.get('ts_started')}",
            "type": "index",
            "label": f"Индексация · {_STAGE_LABELS.get(stage_key, stage_key)}",
            "status": run_status,
            "pct": pct,
            "started": str(ir.get("ts_started") or ""),
            "finished": str(ir.get("ts_finished") or ""),
            "duration_sec": _safe_int(ir.get("duration_sec"), 0),
            "detail": detail,
        })

    # ── OCR runs ────────────────────────────────────────────────────────────
    ocr_runs = _db_query_dicts(
        db_path,
        """
        SELECT *,
               CAST((julianday(COALESCE(ts_finished, CURRENT_TIMESTAMP)) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM ocr_runs
        ORDER BY ts_started DESC
        LIMIT 15
        """,
    )
    for ocr in ocr_runs:
        ocr_status = str(ocr.get("status") or "")
        total = _safe_int(ocr.get("found_scanned"), 0)
        processed = _safe_int(ocr.get("processed_pdfs"), 0)
        pct = None
        detail = f"{processed:,} / {total:,} PDF".replace(",", " ") if total > 0 else ""
        if ocr_status == "running" and total > 0:
            pct = min(100, round(processed * 100 / total))
        jobs.append({
            "id": f"ocr-{ocr.get('ts_started')}",
            "type": "ocr",
            "label": "OCR",
            "status": ocr_status,
            "pct": pct,
            "started": str(ocr.get("ts_started") or ""),
            "finished": str(ocr.get("ts_finished") or ""),
            "duration_sec": _safe_int(ocr.get("duration_sec"), 0),
            "detail": detail,
        })

    # ── Cloud Drive jobs ─────────────────────────────────────────────────────
    if bool(cfg.get("cloud_drive_enabled")):
        try:
            from rag_catalog.core.cloud_drive import CloudDriveService
            service = CloudDriveService.from_config(cfg)
            cloud_jobs = service.registry.list_jobs(limit=30)
            for cj in cloud_jobs:
                progress = dict(cj.progress or {})
                total = _safe_int(progress.get("total_files"), 0)
                imported = _safe_int(progress.get("imported_files"), 0)
                pct = None
                if total > 0:
                    pct = min(100, round(imported * 100 / total))
                import_flag = bool(progress.get("import_files", False))
                if cj.job_type == "reindex":
                    label = "Cloud · реиндексация"
                elif import_flag:
                    label = "Cloud · импорт файлов"
                else:
                    label = "Cloud · сканирование"
                cj_status = str(cj.status or "")
                finished_ts = ""
                if cj_status not in ("running", "pending"):
                    finished_ts = str(cj.updated_at or "")
                detail = f"{imported:,} / {total:,} файлов".replace(",", " ") if total > 0 else ""
                if cj.last_error:
                    detail = (detail + " · " if detail else "") + str(cj.last_error)[:60]
                jobs.append({
                    "id": str(cj.id),
                    "type": "reindex" if cj.job_type == "reindex" else "bootstrap",
                    "label": label,
                    "status": cj_status,
                    "pct": pct,
                    "started": str(cj.created_at or ""),
                    "finished": finished_ts,
                    "duration_sec": 0,
                    "detail": detail,
                })
        except Exception:
            pass

    # Two-pass stable sort: newest-first within each status group
    jobs.sort(key=lambda j: str(j.get("started") or ""), reverse=True)
    jobs.sort(key=lambda j: 0 if j.get("status") == "running" else (1 if j.get("status") == "pending" else 2))
    return jobs


def _render_job_row(job: Dict[str, Any]) -> None:
    status = str(job.get("status") or "")
    jtype = str(job.get("type") or "")
    pct = job.get("pct")
    is_active = status in ("running", "pending")

    status_color = _STATUS_COLOR.get(status, "text-slate-400")
    status_icon = _STATUS_ICON.get(status, "help")
    type_icon = _TYPE_ICON.get(jtype, "task")
    type_color = _TYPE_COLOR.get(jtype, "text-slate-500")

    with ui.row().classes("w-full items-center gap-3 py-2 border-b border-slate-100 dark:border-slate-800"):
        # type icon
        ui.icon(type_icon).classes(f"text-xl {type_color} shrink-0")

        # label + detail
        with ui.column().classes("flex-1 min-w-0 gap-0.5"):
            ui.label(str(job.get("label") or "")).classes("font-medium text-sm leading-tight")
            detail = str(job.get("detail") or "")
            if detail:
                ui.label(detail).classes("rag-meta text-xs truncate")

        # progress bar (if running with known %)
        if is_active and pct is not None:
            with ui.column().classes("w-24 gap-0.5 shrink-0"):
                ui.linear_progress(value=pct / 100, size="8px").props("rounded color=primary")
                ui.label(f"{pct}%").classes("rag-meta text-xs text-center")
        elif is_active:
            with ui.column().classes("w-24 shrink-0"):
                ui.linear_progress(value=None, size="8px").props("rounded color=primary indeterminate")
        else:
            ui.element("div").classes("w-24 shrink-0")

        # status
        with ui.row().classes("items-center gap-1 shrink-0 w-28 justify-end"):
            spin = status == "running"
            icon_el = ui.icon(status_icon).classes(f"text-base {status_color}")
            if spin:
                icon_el.classes("animate-spin")
            ui.label(_STATUS_LABEL.get(status, status)).classes(f"text-xs {status_color}")

        # time
        with ui.column().classes("w-24 shrink-0 text-right gap-0"):
            ts = job.get("finished") if not is_active and job.get("finished") else job.get("started")
            ui.label(_format_relative_time(ts)).classes("rag-meta text-xs")
            dur = _safe_int(job.get("duration_sec"), 0)
            if dur > 0:
                ui.label(_format_duration_seconds(dur)).classes("rag-meta text-xs")


def render_jobs_screen(
    state: PageState,
    *,
    render_fn: Callable[..., None],
) -> None:
    _log_app_event(state, "navigation", "open_screen", details={"screen": "jobs"})

    active_badge_label: Optional[ui.label] = None

    with ui.row().classes("items-center gap-3 mb-1"):
        ui.icon("queue").classes("text-3xl text-primary")
        ui.label("Очередь задач").classes("text-2xl font-semibold")
        active_badge_label = ui.label("").classes("rag-chip hidden")

    ui.label("Все фоновые процессы: индексация, OCR, Cloud Drive.").classes("rag-meta mb-4")

    # header row
    with ui.row().classes("w-full items-center gap-3 py-1 text-xs rag-meta border-b border-slate-200 dark:border-slate-700"):
        ui.element("div").classes("text-xl shrink-0 w-6")
        ui.label("Задача").classes("flex-1")
        ui.label("Прогресс").classes("w-24 shrink-0 text-center")
        ui.label("Статус").classes("w-28 shrink-0 text-right")
        ui.label("Время").classes("w-24 shrink-0 text-right")

    jobs_container = ui.column().classes("w-full gap-0")

    async def _refresh() -> None:
        try:
            jobs = await run.io_bound(_fetch_all_jobs, state.cfg)
        except Exception:
            return
        active_count = sum(1 for j in jobs if j.get("status") in ("running", "pending"))
        if active_badge_label is not None:
            if active_count > 0:
                active_badge_label.set_text(f"{active_count} активных")
                active_badge_label.classes(remove="hidden")
            else:
                active_badge_label.classes(add="hidden")
        jobs_container.clear()
        if not jobs:
            with jobs_container:
                with ui.column().classes("w-full items-center py-12 gap-2"):
                    ui.icon("inbox").classes("text-4xl text-slate-300")
                    ui.label("Задач нет").classes("rag-meta")
            return
        with jobs_container:
            for job in jobs:
                _render_job_row(job)

    _stop_managed_timer(getattr(state, "jobs_refresh_timer", None))
    state.jobs_refresh_timer = ui.timer(5.0, _refresh)  # type: ignore[attr-defined]
    asyncio.get_event_loop().create_task(_refresh())
