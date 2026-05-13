"""
index_view.py — Index / scheduler screen renderer.

Depends on: .state, .helpers, .system, nicegui, rag_catalog.core.
Imported by: nice_app.py.
"""

from __future__ import annotations

import json
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]

from nicegui import run, ui

from .helpers import (
    _CADENCE_LABELS,
    _DAY_LABELS,
    _DAY_RU,
    _format_bytes,
    _format_duration_seconds,
    _format_log_entries_html,
    _format_log_entries_text,
    _format_relative_time,
    _is_admin,
    _read_index_stats,
    _read_index_telemetry,
    _read_log_entries,
    _schedule_display_label,
)
from .state import (
    PageState,
    _get_telemetry,
    _log_app_event,
    _save_config_patch,
    _username,
)
from .system import (
    _STAGE_LABELS,
    _find_live_running_index_run,
    _launch_indexer,
    _launch_ocr,
    _safe_int,
    _stop_managed_timer,
)


def render_index_screen(state: PageState, *, render_fn: Callable, access_denied: Callable) -> None:  # noqa: C901
    if not _is_admin(state):
        access_denied(hint="Управление индексом и расписание индексации доступны только администраторам.")
        return
    stats = _read_index_stats(state.cfg)
    telemetry = _read_index_telemetry(state.cfg)
    settings_db = _get_telemetry(state)
    settings = settings_db.get_index_settings() if hasattr(settings_db, "get_index_settings") else {}

    with ui.row().classes("w-full items-center gap-2"):
        ui.label("Индексация").classes("text-2xl font-semibold")
        active_stage_names = [
            _STAGE_LABELS.get(str(row.get("stage") or ""), str(row.get("stage") or ""))
            for row in (telemetry.get("active_stages") or [])
        ]
        if telemetry.get("active_ocr"):
            active_stage_names.append("OCR")
        active_label = "Запущено: " + ", ".join(active_stage_names) if active_stage_names else "Нет активных задач"
        header_active_chip = ui.label(active_label).classes("rag-chip")
        ui.space()
        if active_stage_names:
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
            ui.timer(1.5, _refresh_progress, once=True)

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
    _phase_refs: Dict[str, Any] = {}
    _progress_rendered = [False]
    _current_log_entries: List[Dict[str, Any]] = []

    with ui.dialog() as stage_status_dialog, ui.card().classes("w-[min(1200px,96vw)] max-h-[90vh] flex flex-col p-0 gap-0"):
        # ── Header ──────────────────────────────────────────────────────────
        with ui.row().classes("w-full items-start justify-between gap-2 p-4 pb-2"):
            with ui.column().classes("gap-0 min-w-0"):
                stage_status_title = ui.label("Статус этапа").classes("text-lg font-semibold")
                stage_status_run = ui.label("Run ID: -").classes("rag-meta text-xs")
                stage_status_note_title = ui.label("Сообщение рана").classes("font-semibold text-sm")
                stage_status_note_value = ui.label("").classes("rag-meta text-xs")
            ui.button(icon="close", on_click=stage_status_dialog.close).props("flat dense round").classes("shrink-0 self-start")
        # ── Filters ─────────────────────────────────────────────────────────
        with ui.row().classes("w-full items-end gap-2 flex-wrap px-4 pb-2"):
            stage_status_lines = ui.number("Записей", value=200, min=20, max=5000, step=20).props("dense outlined").classes("w-28")
            stage_status_level = ui.select(
                options={"all": "Все уровни", "INFO": "INFO", "WARNING": "WARNING", "ERROR": "ERROR", "DEBUG": "DEBUG", "CRITICAL": "CRITICAL"},
                value="all",
                label="Уровень",
            ).props("dense outlined").classes("w-40")
            stage_status_date_from = ui.input("Дата с").props("dense outlined clearable mask='####-##-##' placeholder='ГГГГ-ММ-ДД'").classes("w-36")
            stage_status_date_to = ui.input("Дата по").props("dense outlined clearable mask='####-##-##' placeholder='ГГГГ-ММ-ДД'").classes("w-36")
            stage_status_autorefresh = ui.checkbox("Автообновление", value=True)
        stage_status_log_path = ui.label("").classes("rag-meta text-xs px-4 pb-1")
        # ── Log content ─────────────────────────────────────────────────────
        with ui.element("div").style(
            "flex:1;min-height:300px;max-height:55vh;overflow-y:auto;overflow-x:hidden;"
            "border-top:1px solid #e5e7eb;border-bottom:1px solid #e5e7eb;padding:6px 12px;"
            "font-family:var(--rag-font-mono,monospace);font-size:12px;background:#fff;"
            "word-break:break-word;"
        ):
            stage_status_log_html = ui.html("")
        # ── Footer actions ───────────────────────────────────────────────────
        with ui.row().classes("w-full justify-end gap-2 p-3"):
            ui.button("Обновить", icon="refresh", on_click=lambda: _refresh_stage_status_log(force=True)).props("outline dense")
            ui.button("Копировать", icon="content_copy", on_click=lambda: _copy_stage_status_log()).props("outline dense")
            ui.button("Скачать", icon="download", on_click=lambda: _download_stage_status_log()).props("outline dense")
            ui.button("Закрыть", on_click=stage_status_dialog.close).props("unelevated dense")

    def _refresh_stage_status_log(*, force: bool = False) -> None:
        is_open = bool(getattr(stage_status_dialog, "value", False))
        if not is_open:
            return
        if not force and not bool(stage_status_autorefresh.value):
            return
        log_path = Path(stage_status_ctx.get("log_path") or PROJECT_ROOT / "logs" / "indexer.log")
        entry_count = max(20, _safe_int(stage_status_lines.value, 200))
        level = str(stage_status_level.value or "all")
        date_from = str(stage_status_date_from.value or "").strip()
        date_to = str(stage_status_date_to.value or "").strip()
        entries = _read_log_entries(log_path, max_entries=entry_count, level=level, date_from=date_from, date_to=date_to)
        _current_log_entries.clear()
        _current_log_entries.extend(entries)
        stage_status_log_html.set_content(_format_log_entries_html(entries))
        stage_status_log_path.set_text(f"Лог: {log_path}  |  показано {len(entries)} записей")

    def _copy_stage_status_log() -> None:
        if not _current_log_entries:
            ui.notify("Нечего копировать.", type="warning")
            return
        text = _format_log_entries_text(_current_log_entries)
        ui.run_javascript(f"navigator.clipboard && navigator.clipboard.writeText({json.dumps(text)})")
        ui.notify("Лог скопирован в буфер.", type="positive")

    def _download_stage_status_log() -> None:
        if not _current_log_entries:
            ui.notify("Нечего скачивать.", type="warning")
            return
        text = _format_log_entries_text(_current_log_entries)
        export_dir = PROJECT_ROOT / "logs" / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        run_id = str((stage_status_ctx.get("row") or {}).get("run_id") or "unknown")
        stage_name = str((stage_status_ctx.get("row") or {}).get("stage") or "stage")
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = export_dir / f"indexer_{stage_name}_{run_id}_{stamp}.log"
        file_path.write_text(text, encoding="utf-8")
        ui.download(file_path, filename=file_path.name)

    _stop_managed_timer(state.stage_status_timer)
    state.stage_status_timer = ui.timer(1.5, lambda: _refresh_stage_status_log())
    stage_status_level.on_value_change(lambda _: _refresh_stage_status_log(force=True))
    stage_status_lines.on_value_change(lambda _: _refresh_stage_status_log(force=True))
    stage_status_date_from.on_value_change(lambda _: _refresh_stage_status_log(force=True))
    stage_status_date_to.on_value_change(lambda _: _refresh_stage_status_log(force=True))

    def show_stage_status_details(row: Dict[str, Any]) -> None:
        try:
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
        except Exception as exc:
            ui.notify(f"Ошибка открытия лога этапа: {exc}", type="negative")

    with ui.column().classes("rag-card w-full p-4 gap-3"):
        with ui.row().classes("w-full items-center gap-2"):
            ui.icon("account_tree").classes("text-2xl text-indigo-500")
            ui.label("Pipeline индексации").classes("text-xl font-semibold")
            active_chip = ui.label(active_label).classes("rag-chip")
            ui.space()
            refresh_btn = ui.button(icon="refresh", on_click=lambda: _refresh_progress()).props("flat dense round").tooltip("Обновить")
        ui.label(
            "Запускаемые фазы: metadata, small chunks, large chunks и OCR. "
            "Покрытие содержимым показано отдельно, это агрегат state DB, а не отдельная команда запуска."
        ).classes("rag-meta")

        _coverage_refs: Dict[str, Any] = {}
        with ui.element("div").classes("rag-content-coverage w-full"):
            with ui.row().classes("w-full items-center gap-2"):
                ui.icon("article").classes("text-indigo-500")
                ui.label("Покрытие содержимым").classes("font-semibold")
                _coverage_refs["count"] = ui.label("").classes("rag-meta")
                ui.space()
                _coverage_refs["pct"] = ui.label("").classes("rag-meta")
            _coverage_refs["bar"] = ui.linear_progress(value=0).props("color=indigo-5").classes("w-full mt-1")
            ui.label(
                "Файлы со stage=content уже имеют проиндексированное содержимое. "
                "Остальные пока представлены метаданными или ждут фаз small/large/OCR."
            ).classes("rag-meta")

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

        def _phase_row_data(row: Dict[str, Any], *, label: str, is_ocr: bool) -> Dict[str, Any]:
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
            status_icon_name = (
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
            shared_row = dict(row)
            shared_row["stage"] = label
            if is_ocr:
                shared_row["_log_path"] = PROJECT_ROOT / "logs" / "ocr.log"
            return {
                "status_str": status_str,
                "is_running": is_running,
                "processed": processed,
                "total_f": total_f,
                "pct": pct,
                "pct_label": f"{pct * 100:.0f}%",
                "duration_value": duration_value,
                "last_ts": last_ts,
                "status_cls": status_cls,
                "stats_text": stats_text,
                "status_icon_name": status_icon_name,
                "status_title": status_title,
                "shared_row": shared_row,
            }

        def render_phase_row(
            *,
            key: str,
            label: str,
            row: Dict[str, Any],
            is_ocr: bool = False,
        ) -> None:
            d = _phase_row_data(row, label=label, is_ocr=is_ocr)
            shared_row = d["shared_row"]
            with ui.element("div").classes(f"rag-pipeline-row rag-pipeline-row-card {d['status_cls']}"):
                with ui.row().classes("items-center gap-2 min-w-0"):
                    icon_e = ui.icon(d["status_icon_name"]).classes(f"rag-phase-status {d['status_cls']}")
                    with ui.column().classes("gap-0 min-w-0"):
                        ui.label(label).classes("font-semibold truncate")
                        time_e = ui.label(_format_relative_time(d["last_ts"])).classes("rag-meta")
                with ui.column().classes("rag-progress-stack min-w-0"):
                    with ui.row().classes("rag-progress-topline w-full items-center gap-2"):
                        chip_e = ui.button(
                            d["status_title"],
                            on_click=lambda r=shared_row: show_stage_status_details(r),
                            color=None,
                        ).props("flat dense no-caps").classes(f"rag-chip rag-status-chip {d['status_cls']}")
                        count_e = ui.label(f"{d['processed']:,} / {d['total_f']:,}".replace(",", " ")).classes("rag-meta")
                        ui.space()
                        pct_e = ui.label(d["pct_label"]).classes("rag-meta")
                        dur_e = ui.label(_format_duration_seconds(d["duration_value"])).classes("rag-meta")
                    prog_e = ui.linear_progress(value=d["pct"], show_value=False).props("color=indigo-5" if d["is_running"] else "").classes("w-full rag-progressbar")
                stats_e = ui.label(d["stats_text"]).classes("rag-meta")
                with ui.row().classes("rag-pipeline-actions"):
                    if is_ocr:
                        play_e = ui.button(icon="play_arrow", on_click=run_ocr_now).props("flat dense round").tooltip("Запустить")
                        restart_e = ui.button(icon="restart_alt", on_click=run_ocr_now).props("flat dense round").tooltip("Рестарт")
                    else:
                        play_e = ui.button(icon="play_arrow", on_click=make_run_handler(key)).props("flat dense round").tooltip("Запустить")
                        restart_e = ui.button(icon="restart_alt", on_click=make_run_handler(key)).props("flat dense round").tooltip("Рестарт")
                    pause_e = ui.button(icon="pause", on_click=lambda l=label: pause_phase(l)).props("flat dense round").tooltip("Пауза")
                    stop_e = ui.button(icon="stop", on_click=lambda l=label: stop_phase(l)).props("flat dense round").tooltip("Остановить")
                    play_e.set_visibility(not d["is_running"])
                    restart_e.set_visibility(d["is_running"])
                    pause_e.set_visibility(d["is_running"])
                    stop_e.set_visibility(d["is_running"])
            _phase_refs[key] = {
                "shared_row": shared_row,
                "icon_e": icon_e,
                "time_e": time_e,
                "chip_e": chip_e,
                "count_e": count_e,
                "pct_e": pct_e,
                "dur_e": dur_e,
                "prog_e": prog_e,
                "stats_e": stats_e,
                "play_e": play_e,
                "restart_e": restart_e,
                "pause_e": pause_e,
                "stop_e": stop_e,
            }

        def update_phase_row(*, key: str, label: str, row: Dict[str, Any], is_ocr: bool = False) -> None:
            if key not in _phase_refs:
                return
            refs = _phase_refs[key]
            d = _phase_row_data(row, label=label, is_ocr=is_ocr)
            refs["shared_row"].clear()
            refs["shared_row"].update(d["shared_row"])
            refs["icon_e"]._props["name"] = d["status_icon_name"]
            refs["icon_e"].update()
            refs["time_e"].set_text(_format_relative_time(d["last_ts"]))
            refs["chip_e"].set_text(d["status_title"])
            refs["count_e"].set_text(f"{d['processed']:,} / {d['total_f']:,}".replace(",", " "))
            refs["pct_e"].set_text(d["pct_label"])
            refs["dur_e"].set_text(_format_duration_seconds(d["duration_value"]))
            refs["prog_e"].set_value(d["pct"])
            refs["stats_e"].set_text(d["stats_text"])
            refs["play_e"].set_visibility(not d["is_running"])
            refs["restart_e"].set_visibility(d["is_running"])
            refs["pause_e"].set_visibility(d["is_running"])
            refs["stop_e"].set_visibility(d["is_running"])

        def _get_stage_rows(fresh: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
            active_by_stage = {str(r.get("stage") or ""): r for r in (fresh.get("active_stages") or [])}
            latest_by_stage = {str(r.get("stage") or ""): r for r in (fresh.get("latest_stages") or [])}
            summary_by_stage = {str(r.get("stage") or ""): r for r in (fresh.get("stage_summary") or [])}
            result: Dict[str, Dict[str, Any]] = {}
            for stage_key in ["metadata", "small", "large"]:
                row = dict(summary_by_stage.get(stage_key) or {})
                row.update(latest_by_stage.get(stage_key) or {})
                row.update(active_by_stage.get(stage_key) or {})
                if not row and stage_key == "metadata":
                    live_index = _find_live_running_index_run(_get_telemetry(state))
                    if live_index:
                        row = {"stage": stage_key, "status": "running", "processed_files": 0, "total_files": 0, "duration_sec": 0}
                result[stage_key] = row
            ocr_row = dict(fresh.get("last_ocr") or {})
            if fresh.get("active_ocr"):
                ocr_row.update(fresh.get("active_ocr") or {})
            result["ocr"] = ocr_row
            return result

        def _refresh_progress() -> None:
            fresh = _read_index_telemetry(state.cfg)
            stats = _read_index_stats(state.cfg)
            active_names = [
                _STAGE_LABELS.get(str(row.get("stage") or ""), str(row.get("stage") or ""))
                for row in (fresh.get("active_stages") or [])
            ]
            if not active_names:
                # Fallback: check latest_stages for any stage still marked running
                # (happens when index_runs row lags behind index_stage_progress)
                active_names = [
                    _STAGE_LABELS.get(str(row.get("stage") or ""), str(row.get("stage") or ""))
                    for row in (fresh.get("latest_stages") or [])
                    if str(row.get("status") or "") == "running"
                ]
            if fresh.get("active_ocr"):
                active_names.append("OCR")
            chip_text = "Запущено: " + ", ".join(active_names) if active_names else "Нет активных задач"
            active_chip.set_text(chip_text)
            header_active_chip.set_text(chip_text)
            by_stage = dict(stats.get("by_stage") or {})
            total_files = int(stats.get("total") or 0)
            content_files = int(by_stage.get("content") or 0)
            coverage_pct = min(1.0, content_files / total_files) if total_files > 0 else 0.0
            _coverage_refs["count"].set_text(f"{content_files:,} / {total_files:,}".replace(",", " "))
            _coverage_refs["pct"].set_text(f"{coverage_pct * 100:.0f}%")
            _coverage_refs["bar"].set_value(coverage_pct)
            stage_rows = _get_stage_rows(fresh)
            if not _progress_rendered[0]:
                _progress_rendered[0] = True
                progress_area.clear()
                with progress_area:
                    with ui.element("div").classes("rag-pipeline-row rag-pipeline-row-card rag-pipeline-head"):
                        ui.label("Этап").classes("rag-meta font-semibold")
                        ui.label("Прогресс").classes("rag-meta font-semibold")
                        ui.label("Статистика").classes("rag-meta font-semibold")
                        ui.label("Действия").classes("rag-meta font-semibold text-right")
                    for stage_key in ["metadata", "small", "large"]:
                        render_phase_row(key=stage_key, label=_STAGE_LABELS.get(stage_key, stage_key), row=stage_rows[stage_key])
                    render_phase_row(key="ocr", label="OCR", row=stage_rows["ocr"], is_ocr=True)
            else:
                for stage_key in ["metadata", "small", "large"]:
                    update_phase_row(key=stage_key, label=_STAGE_LABELS.get(stage_key, stage_key), row=stage_rows[stage_key])
                update_phase_row(key="ocr", label="OCR", row=stage_rows["ocr"], is_ocr=True)

        # Initial render
        _refresh_progress()
        # Auto-refresh every 5 seconds
        _stop_managed_timer(state.index_progress_timer)
        state.index_progress_timer = ui.timer(5.0, _refresh_progress)

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
                            cadence_key = str(sched.get("cadence") or "daily")
                            days_str = " ".join(_DAY_RU.get(d, d) for d in (sched.get("days") or []))
                            cadence_str = _CADENCE_LABELS.get(cadence_key, "")
                            stage_str = _STAGE_LABELS.get(str(sched.get("stage") or "all"), str(sched.get("stage") or ""))
                            last_run = str(sched.get("last_run_at") or "—")
                            enabled_val = bool(int(sched.get("enabled") or 0))
                            color_cls = "" if enabled_val else "opacity-50"
                            schedule_label = _schedule_display_label(sched)
                            cadence_display = cadence_str if cadence_key == "hourly" else f"{cadence_str} в {sched.get('time') or '?'}"
                            with ui.row().classes(f"w-full items-center gap-2 p-2 border border-gray-200 rounded {color_cls}"):
                                ui.icon("check_circle" if enabled_val else "radio_button_unchecked").classes(
                                    "text-xl " + ("text-green-500" if enabled_val else "text-gray-400")
                                )
                                ui.label(schedule_label).classes("font-semibold min-w-32")
                                ui.label(stage_str).classes("rag-chip")
                                ui.label(cadence_display).classes("rag-meta")
                                if days_str and cadence_key != "hourly":
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
                        def refresh_schedule_form() -> None:
                            is_hourly = str(dlg_cadence.value or "daily") == "hourly"
                            dlg_time.set_visibility(not is_hourly)
                            if is_hourly:
                                dlg_time.set_value("")
                        refresh_schedule_form()
                        dlg_cadence.on_value_change(lambda _: refresh_schedule_form())
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
                    render_fn()

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

def render_index_dashboard(state: PageState) -> None:
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

# ── Auth / login / access denied screens ──────────────────────────────────

