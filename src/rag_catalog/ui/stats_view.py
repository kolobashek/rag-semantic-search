"""
stats_view.py — Analytics / stats screen renderer.

Depends on: .state, .helpers, .system, nicegui, rag_catalog.core.
Imported by: nice_app.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List

from nicegui import run, ui

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.search_eval import evaluate_search, load_golden_queries

from .helpers import (
    _cd_get_service,
    _db_query_dicts,
    _ensure_searcher,
    _format_bytes,
    _run_catalog_search,
)
from .state import (
    PageState,
    _get_auth_db,
    _get_telemetry,
    _log_app_event,
)
from .system import _telemetry_db_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def render_stats_screen(
    state: PageState,
    *,
    access_denied: Callable[..., None],
    query_handler: Callable[[str], Any],
) -> None:
    if str((state.current_user or {}).get("role") or "") != "admin":
        access_denied(hint="Статистика поиска, аудит и бенчмарк доступны только администраторам.")
        return
    telemetry_path = _telemetry_db_path(state.cfg)
    auth_db = _get_auth_db(state)

    # ── KPI (всегда видны над табами) ──────────────────────────────
    overview = _db_query_dicts(
        telemetry_path,
        """
        SELECT
          COUNT(*) AS searches,
          COALESCE(AVG(duration_ms), 0) AS avg_ms,
          SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS errors,
          COUNT(DISTINCT COALESCE(NULLIF(username, ''), source, 'unknown')) AS users,
          SUM(CASE WHEN results_count = 0 THEN 1 ELSE 0 END) AS zero_results
        FROM search_logs
        """,
    )
    overview_row = overview[0] if overview else {}

    def render_kpi(label: str, value: str, icon: str, color: str = "") -> None:
        with ui.column().classes("rag-card rag-kpi p-4 gap-2"):
            with ui.row().classes("items-center gap-2"):
                ui.icon(icon).classes(f"text-xl {color}".strip())
                ui.label(label).classes("rag-meta")
            ui.label(value).classes(f"rag-kpi-value {color}".strip())

    with ui.row().classes("w-full gap-3"):
        render_kpi("Запросов", str(int(overview_row.get("searches") or 0)), "search")
        render_kpi("Средняя задержка", f"{int(float(overview_row.get('avg_ms') or 0))} мс", "speed")
        render_kpi("Пользователей", str(int(overview_row.get("users") or 0)), "group")
        zero = int(overview_row.get("zero_results") or 0)
        render_kpi("Нулевых результатов", str(zero), "search_off", "text-negative" if zero else "")
        errors = int(overview_row.get("errors") or 0)
        render_kpi("Ошибок", str(errors), "error", "text-negative" if errors else "")

    # ── Табы ───────────────────────────────────────────────────────
    with ui.tabs().classes("w-full").props("align=left dense") as tabs:
        tab_overview = ui.tab("Обзор", icon="bar_chart")
        tab_quality = ui.tab("Качество поиска", icon="thumbs_up_down")
        tab_synonyms = ui.tab("Синонимы", icon="auto_awesome")
        tab_queries = ui.tab("Запросы", icon="manage_search")
        tab_benchmark = ui.tab("Бенчмарк", icon="assessment")
        tab_audit = ui.tab("Аудит", icon="security")
        tab_cd = ui.tab("Cloud Drive", icon="cloud") if bool(state.cfg.get("cloud_drive_enabled")) else None

    with ui.tab_panels(tabs, value=tab_overview).classes("w-full"):

        # ── Обзор ─────────────────────────────────────────────────
        with ui.tab_panel(tab_overview):
            searches_by_day = _db_query_dicts(
                telemetry_path,
                """
                SELECT substr(ts, 1, 10) AS day, COUNT(*) AS count,
                       SUM(CASE WHEN results_count = 0 THEN 1 ELSE 0 END) AS zero_count
                FROM search_logs
                GROUP BY substr(ts, 1, 10)
                ORDER BY day
                LIMIT 30
                """,
            )
            with ui.column().classes("rag-card w-full p-4 gap-3"):
                ui.label("Поиски по дням").classes("font-semibold")
                ui.echart({
                    "tooltip": {"trigger": "axis"},
                    "legend": {"data": ["Поиски", "Нулевые результаты"]},
                    "xAxis": {"type": "category", "data": [row["day"] for row in searches_by_day]},
                    "yAxis": {"type": "value"},
                    "series": [
                        {"type": "bar", "data": [row["count"] for row in searches_by_day], "name": "Поиски"},
                        {"type": "line", "data": [row["zero_count"] for row in searches_by_day], "name": "Нулевые результаты", "itemStyle": {"color": "#ef4444"}},
                    ],
                }).classes("w-full h-64")

            top_queries = _db_query_dicts(
                telemetry_path,
                """
                SELECT query, COUNT(*) AS count,
                       ROUND(AVG(results_count), 1) AS avg_results,
                       ROUND(AVG(duration_ms)) AS avg_ms
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
                SELECT COALESCE(NULLIF(username, ''), source, 'unknown') AS username,
                       COUNT(*) AS count,
                       ROUND(AVG(results_count), 1) AS avg_results
                FROM search_logs
                GROUP BY COALESCE(NULLIF(username, ''), source, 'unknown')
                ORDER BY count DESC
                LIMIT 15
                """,
            )
            with ui.row().classes("w-full gap-3 items-start"):
                with ui.column().classes("rag-card flex-1 p-4 gap-1"):
                    ui.label("Топ запросов").classes("font-semibold mb-1")
                    for row in top_queries:
                        with ui.row().classes("w-full items-center gap-2"):
                            ui.label(str(row["query"])).classes("flex-1 text-sm truncate")
                            ui.label(str(row["count"])).classes("rag-chip text-xs")
                            avg_r = float(row.get("avg_results") or 0)
                            color = "text-negative" if avg_r < 1 else "rag-meta"
                            ui.label(f"~{avg_r:.0f} рез.").classes(f"text-xs {color}")
                with ui.column().classes("rag-card flex-1 p-4 gap-1"):
                    ui.label("Активность пользователей").classes("font-semibold mb-1")
                    for row in top_users:
                        with ui.row().classes("w-full items-center gap-2"):
                            ui.icon("person", size="16px").classes("rag-meta")
                            ui.label(str(row["username"])).classes("flex-1 text-sm truncate")
                            ui.label(str(row["count"])).classes("rag-chip text-xs")

            # ── Cloud Drive usage section ─────────────────────────
            if bool(state.cfg.get("cloud_drive_enabled")):
                cd_search_stats = _db_query_dicts(
                    telemetry_path,
                    """
                    SELECT
                      COUNT(*) AS total,
                      SUM(CASE WHEN json_extract(details_json, '$.cloud_results') > 0 THEN 1 ELSE 0 END) AS with_cloud,
                      COUNT(DISTINCT username) AS users
                    FROM app_events
                    WHERE feature='search' AND action='search'
                    """,
                )
                cd_top_files = _db_query_dicts(
                    telemetry_path,
                    """
                    SELECT
                      json_extract(details_json, '$.cloud_path') AS path,
                      COUNT(*) AS hits
                    FROM app_events
                    WHERE feature='search' AND action='result_use'
                      AND json_extract(details_json, '$.source') = 'cloud_drive'
                      AND json_extract(details_json, '$.cloud_path') IS NOT NULL
                      AND json_extract(details_json, '$.cloud_path') <> ''
                    GROUP BY path
                    ORDER BY hits DESC
                    LIMIT 10
                    """,
                )
                cd_ops = _db_query_dicts(
                    telemetry_path,
                    """
                    SELECT action, COUNT(*) AS cnt
                    FROM app_events
                    WHERE feature='cloud_drive'
                    GROUP BY action
                    ORDER BY cnt DESC
                    """,
                )
                cds = cd_search_stats[0] if cd_search_stats else {}
                cd_total = int(cds.get("total") or 0)
                cd_with_cloud = int(cds.get("with_cloud") or 0)
                with ui.column().classes("rag-card w-full p-4 gap-3"):
                    with ui.row().classes("items-center gap-2"):
                        ui.icon("cloud", size="20px").classes("text-blue-500")
                        ui.label("Cloud Drive — аналитика").classes("font-semibold")
                    with ui.row().classes("w-full gap-3"):
                        with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                            ui.icon("cloud_search").classes("text-2xl text-blue-500")
                            ui.label(str(cd_with_cloud)).classes("text-xl font-semibold")
                            ui.label("Поисков с Cloud Drive").classes("rag-meta text-xs")
                        with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                            pct = round(100 * cd_with_cloud / max(cd_total, 1))
                            c = "text-positive" if pct >= 30 else "text-warning" if pct >= 5 else "rag-meta"
                            ui.icon("percent").classes(f"text-2xl {c}")
                            ui.label(f"{pct}%").classes(f"text-xl font-semibold {c}")
                            ui.label("Доля Cloud Drive").classes("rag-meta text-xs")
                    if cd_top_files:
                        ui.label("Топ Cloud Drive файлов").classes("font-semibold text-sm mt-1")
                        for row in cd_top_files:
                            with ui.row().classes("w-full items-center gap-2"):
                                ui.icon("cloud", size="14px").classes("text-blue-400 shrink-0")
                                pth = str(row.get("path") or "")
                                ui.label(pth.rsplit("/", 1)[-1] if "/" in pth else pth).classes("flex-1 text-sm truncate")
                                ui.label(str(row.get("hits") or "")).classes("rag-chip text-xs shrink-0")
                    if cd_ops:
                        ui.label("Операции Cloud Drive").classes("font-semibold text-sm mt-1")
                        with ui.row().classes("w-full gap-2 flex-wrap"):
                            for row in cd_ops:
                                ui.label(f"{row.get('action')}: {row.get('cnt')}").classes("rag-chip text-xs")

        # ── Качество поиска ────────────────────────────────────────
        with ui.tab_panel(tab_quality):
            zero_queries = _db_query_dicts(
                telemetry_path,
                """
                SELECT query, COUNT(*) AS count, MAX(ts) AS last_seen
                FROM search_logs
                WHERE results_count = 0 AND query <> ''
                GROUP BY lower(query)
                ORDER BY count DESC
                LIMIT 30
                """,
            )
            neg_feedback = _db_query_dicts(
                telemetry_path,
                """
                SELECT query, SUM(feedback) AS score, COUNT(*) AS hits
                FROM search_feedback
                WHERE feedback < 0 AND query <> ''
                GROUP BY lower(query)
                ORDER BY score ASC
                LIMIT 20
                """,
            )
            pos_docs = _db_query_dicts(
                telemetry_path,
                """
                SELECT result_title, result_path,
                       SUM(feedback) AS score, COUNT(*) AS hits,
                       COUNT(DISTINCT lower(query)) AS distinct_queries
                FROM search_feedback
                WHERE feedback > 0 AND result_path <> ''
                GROUP BY result_path
                ORDER BY score DESC
                LIMIT 20
                """,
            )
            query_health = _db_query_dicts(
                telemetry_path,
                """
                SELECT
                  ROUND(100.0 * SUM(CASE WHEN results_count = 0 THEN 1 ELSE 0 END) / MAX(COUNT(*), 1), 1) AS zero_pct,
                  ROUND(AVG(results_count), 1) AS avg_results,
                  ROUND(AVG(duration_ms)) AS avg_ms,
                  COUNT(*) AS total
                FROM search_logs
                WHERE ts >= datetime('now', '-7 days')
                """,
            )
            qh = query_health[0] if query_health else {}
            zero_pct = float(qh.get("zero_pct") or 0)
            avg_res = float(qh.get("avg_results") or 0)
            avg_ms_val = int(float(qh.get("avg_ms") or 0))

            # Health summary tiles (last 7 days)
            with ui.row().classes("w-full gap-3 mb-2"):
                with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                    c = "text-negative" if zero_pct > 20 else ("text-warning" if zero_pct > 10 else "text-positive")
                    ui.icon("search_off").classes(f"text-2xl {c}")
                    ui.label(f"{zero_pct:.1f}%").classes(f"text-xl font-semibold {c}")
                    ui.label("Нулевых рез. (7д)").classes("rag-meta text-xs")
                with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                    c2 = "text-positive" if avg_res >= 5 else ("text-warning" if avg_res >= 1 else "text-negative")
                    ui.icon("format_list_numbered").classes(f"text-2xl {c2}")
                    ui.label(f"{avg_res:.1f}").classes(f"text-xl font-semibold {c2}")
                    ui.label("Среднее рез. (7д)").classes("rag-meta text-xs")
                with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                    c3 = "text-negative" if avg_ms_val > 3000 else ("text-warning" if avg_ms_val > 1000 else "text-positive")
                    ui.icon("speed").classes(f"text-2xl {c3}")
                    ui.label(f"{avg_ms_val} мс").classes(f"text-xl font-semibold {c3}")
                    ui.label("Латентность (7д)").classes("rag-meta text-xs")

            with ui.row().classes("w-full gap-3 items-start"):
                # Zero-result queries
                with ui.column().classes("rag-card flex-1 p-4 gap-1"):
                    with ui.row().classes("items-center gap-2 mb-1"):
                        ui.icon("search_off").classes("text-negative")
                        ui.label("Нулевые результаты").classes("font-semibold")
                    if zero_queries:
                        for row in zero_queries:
                            with ui.row().classes("w-full items-center gap-2"):
                                ui.label(str(row["query"])).classes("flex-1 text-sm truncate")
                                ui.label(f"×{row['count']}").classes("rag-chip text-xs bg-red-50 text-red-600")
                                ui.button(icon="search", on_click=query_handler(str(row["query"])), color=None).props("flat round dense").tooltip("Выполнить этот запрос")
                    else:
                        with ui.row().classes("items-center gap-2"):
                            ui.icon("check_circle").classes("text-positive")
                            ui.label("Нет запросов без результатов.").classes("rag-meta")

                # Negative feedback
                with ui.column().classes("rag-card flex-1 p-4 gap-1"):
                    with ui.row().classes("items-center gap-2 mb-1"):
                        ui.icon("thumb_down").classes("text-negative")
                        ui.label("Отрицательный фидбек").classes("font-semibold")
                    if neg_feedback:
                        for row in neg_feedback:
                            with ui.row().classes("w-full items-center gap-2"):
                                ui.label(str(row["query"])).classes("flex-1 text-sm truncate")
                                ui.label(f"{int(row['score'])}").classes("rag-chip text-xs bg-red-50 text-red-600")
                                ui.button(icon="search", on_click=query_handler(str(row["query"])), color=None).props("flat round dense").tooltip("Выполнить этот запрос")
                    else:
                        with ui.row().classes("items-center gap-2"):
                            ui.icon("check_circle").classes("text-positive")
                            ui.label("Нет отрицательного фидбека.").classes("rag-meta")

            # Positive documents
            with ui.column().classes("rag-card w-full p-4 gap-2 mt-0"):
                with ui.row().classes("items-center gap-2 mb-1"):
                    ui.icon("thumb_up").classes("text-positive")
                    ui.label("Документы с положительным фидбеком").classes("font-semibold")
                if pos_docs:
                    ui.table(
                        rows=[{
                            "title": str(r.get("result_title") or r.get("result_path") or ""),
                            "score": str(int(r.get("score") or 0)),
                            "hits": str(int(r.get("hits") or 0)),
                            "queries": str(int(r.get("distinct_queries") or 0)),
                        } for r in pos_docs],
                        columns=[
                            {"name": "title", "label": "Документ", "field": "title", "align": "left"},
                            {"name": "score", "label": "Балл", "field": "score"},
                            {"name": "hits", "label": "Оценок", "field": "hits"},
                            {"name": "queries", "label": "Запросов", "field": "queries"},
                        ],
                        pagination=10,
                    ).classes("w-full")
                else:
                    ui.label("Нет данных об оценках.").classes("rag-meta")

        # ── Синонимы ───────────────────────────────────────────────
        with ui.tab_panel(tab_synonyms):
            tdb = _get_telemetry(state)
            alias_groups = tdb.list_search_alias_groups() if tdb else []
            candidates = tdb.suggest_search_alias_candidates(limit=30) if tdb else []

            with ui.row().classes("w-full gap-3 items-start"):
                # Existing alias groups
                with ui.column().classes("rag-card flex-1 p-4 gap-2"):
                    ui.label(f"Группы синонимов ({len(alias_groups)})").classes("font-semibold")
                    if alias_groups:
                        for grp in alias_groups[:20]:
                            aliases = grp.get("aliases") or []
                            active = [a for a in aliases if str(a.get("status") or "") == "active"]
                            with ui.column().classes("rag-card p-2 gap-1 w-full"):
                                with ui.row().classes("items-center gap-2"):
                                    ui.icon("auto_awesome", size="16px").classes("text-indigo-400")
                                    ui.label(str(grp.get("label") or grp.get("key") or "")).classes("font-medium text-sm")
                                if active:
                                    with ui.row().classes("flex-wrap gap-1"):
                                        for a in active[:8]:
                                            ui.label(str(a.get("alias") or "")).classes("rag-chip text-xs")
                    else:
                        ui.label("Нет настроенных групп синонимов.").classes("rag-meta")

                # Candidates from feedback
                with ui.column().classes("rag-card flex-1 p-4 gap-2"):
                    ui.label("Кандидаты в синонимы").classes("font-semibold")
                    ui.label(
                        "Фразы из документов, которые часто открывали по похожим запросам — "
                        "кандидаты на добавление как синоним."
                    ).classes("rag-meta text-xs mb-1")
                    if candidates:
                        tdb_ref = _get_telemetry(state)

                        def _add_synonym_from_candidate(cq: str, cp: str) -> None:
                            if not tdb_ref:
                                return
                            import re as _re
                            _key = _re.sub(r"[^a-z0-9]+", "_", cq.lower()).strip("_") or "alias"
                            try:
                                tdb_ref.save_search_alias_group(
                                    key=_key,
                                    label=cq,
                                    aliases=[cq, cp],
                                    source="analytics",
                                )
                                _log_app_event(state, "settings", "search_alias_add", details={"key": _key, "from": "analytics_candidate"})
                                ui.notify(f"Синоним добавлен: «{cq}» = «{cp}»", type="positive")
                            except Exception as exc:
                                ui.notify(f"Не удалось добавить: {exc}", type="negative")

                        for cand in candidates[:20]:
                            q = str(cand.get("query") or "")
                            phrase = str(cand.get("candidate") or "")
                            title = str(cand.get("title") or "")
                            score = int(cand.get("score") or 0)
                            with ui.row().classes("w-full items-center gap-2"):
                                with ui.column().classes("flex-1 gap-0"):
                                    with ui.row().classes("items-center gap-1"):
                                        ui.label(q).classes("text-xs rag-meta")
                                        ui.icon("arrow_forward", size="12px").classes("rag-meta")
                                        ui.label(phrase).classes("text-sm font-medium")
                                    if title:
                                        ui.label(title).classes("rag-path text-xs truncate")
                                ui.label(f"+{score}").classes("rag-chip text-xs bg-green-50 text-green-700")
                                ui.button(icon="add", on_click=lambda cq=q, cp=phrase: _add_synonym_from_candidate(cq, cp), color=None).props("flat round dense").tooltip("Добавить как синоним")
                    else:
                        ui.label("Недостаточно данных для предложений.").classes("rag-meta")

        # ── Запросы ────────────────────────────────────────────────
        with ui.tab_panel(tab_queries):
            with ui.column().classes("w-full gap-2"):
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
                        {"name": "ts", "label": "Время", "field": "ts", "sortable": True},
                        {"name": "source", "label": "Источник", "field": "source"},
                        {"name": "username", "label": "Пользователь", "field": "username"},
                        {"name": "query", "label": "Запрос", "field": "query", "align": "left"},
                        {"name": "results_count", "label": "Рез.", "field": "results_count"},
                        {"name": "duration_ms", "label": "мс", "field": "duration_ms"},
                        {"name": "error", "label": "Ошибка", "field": "error"},
                    ],
                    pagination=15,
                ).classes("w-full")

                def refresh_search_table() -> None:
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
                        rows = [r for r in rows if str(r.get("source") or "").startswith("telegram_bot:")]
                    elif source_mode == "Web/прочее":
                        rows = [r for r in rows if not str(r.get("source") or "").startswith("telegram_bot:")]
                    user_needle = str(search_user_filter.value or "").strip().lower()
                    if user_needle:
                        rows = [r for r in rows if user_needle in str(r.get("username") or "").lower()]
                    query_needle = str(search_query_filter.value or "").strip().lower()
                    if query_needle:
                        rows = [r for r in rows if query_needle in str(r.get("query") or "").lower()]
                    ok_mode = str(search_ok_filter.value or "Все")
                    if ok_mode == "OK":
                        rows = [r for r in rows if int(r.get("ok") or 0) == 1]
                    elif ok_mode == "Ошибки":
                        rows = [r for r in rows if int(r.get("ok") or 0) == 0]
                    search_table.rows = rows
                    search_table.update()

                search_source_filter.on_value_change(lambda e: refresh_search_table())
                search_user_filter.on_value_change(lambda e: refresh_search_table())
                search_query_filter.on_value_change(lambda e: refresh_search_table())
                search_ok_filter.on_value_change(lambda e: refresh_search_table())
                refresh_search_table()

        # ── Бенчмарк ───────────────────────────────────────────────
        with ui.tab_panel(tab_benchmark):
            _DEFAULT_GOLDEN = str(PROJECT_ROOT / "eval" / "search_golden.json")
            _bench_state: Dict[str, Any] = {"result": None, "running": False, "error": ""}

            with ui.column().classes("rag-card w-full p-4 gap-3"):
                ui.label("Оффлайн-бенчмарк качества поиска").classes("text-xl font-semibold")
                ui.label(
                    "Запускает поиск по набору эталонных запросов и вычисляет Recall@k, MRR@k, nDCG@k. "
                    "Файл golden-запросов — JSON-список {query, expected[]}."
                ).classes("rag-meta")
                with ui.row().classes("w-full items-end gap-3"):
                    golden_path_input = ui.input(
                        "Путь к golden-файлу", value=_DEFAULT_GOLDEN
                    ).props("dense outlined").classes("flex-1")
                    k_input = ui.number("K (глубина)", value=10, min=1, max=50, step=1).props("dense outlined").classes("w-28")
                    run_btn = ui.button("Запустить", icon="play_arrow").props("outline")

            bench_result_area = ui.column().classes("w-full gap-3")

            def _render_bench_result() -> None:
                bench_result_area.clear()
                with bench_result_area:
                    err = _bench_state.get("error", "")
                    if err:
                        with ui.row().classes("items-center gap-2 text-negative"):
                            ui.icon("error_outline")
                            ui.label(err)
                        return
                    result = _bench_state.get("result")
                    if not result:
                        return

                    rows: list = result.get("rows", [])
                    k_val = int(result.get("limit", 10))
                    recall = float(result.get("recall_at_k", 0))
                    mrr = float(result.get("mrr_at_k", 0))
                    ndcg = float(result.get("ndcg_at_k", 0))
                    p50 = int(result.get("latency_p50_ms", 0))

                    # Summary tiles
                    def _metric_color(v: float, thresholds: tuple) -> str:
                        lo, hi = thresholds
                        return "text-positive" if v >= hi else ("text-warning" if v >= lo else "text-negative")

                    with ui.row().classes("w-full gap-3"):
                        for label, val, fmt, thr, icon_name in [
                            (f"Recall@{k_val}", recall, f"{recall:.2f}", (0.5, 0.75), "rule"),
                            (f"MRR@{k_val}", mrr, f"{mrr:.2f}", (0.4, 0.65), "leaderboard"),
                            (f"nDCG@{k_val}", ndcg, f"{ndcg:.2f}", (0.4, 0.65), "bar_chart"),
                            ("P50 латентность", p50, f"{p50} мс", None, "speed"),
                        ]:
                            color = _metric_color(val, thr) if thr else (
                                "text-positive" if p50 < 500 else ("text-warning" if p50 < 2000 else "text-negative")
                            )
                            with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                                ui.icon(icon_name).classes(f"text-2xl {color}")
                                ui.label(fmt).classes(f"text-xl font-semibold {color}")
                                ui.label(label).classes("rag-meta text-xs")

                    # Per-query table
                    with ui.column().classes("rag-card w-full p-4 gap-2"):
                        ui.label("Результаты по запросам").classes("font-semibold")
                        with ui.element("div").classes("w-full overflow-x-auto"):
                            with ui.element("table").classes("w-full text-xs border-collapse"):
                                with ui.element("thead"):
                                    with ui.element("tr").classes("border-b rag-section-label"):
                                        for col in ("Запрос", f"Recall@{k_val}", f"MRR@{k_val}", f"nDCG@{k_val}", "Мс", "Результатов"):
                                            ui.element("th").classes("text-left p-2 font-semibold").text = col
                                with ui.element("tbody"):
                                    for qrow in sorted(rows, key=lambda r: r.get("recall_at_k", 0)):
                                        r_val = float(qrow.get("recall_at_k", 0))
                                        row_cls = "border-b hover:bg-slate-50 dark:hover:bg-slate-800"
                                        if r_val == 0:
                                            row_cls += " text-negative"
                                        with ui.element("tr").classes(row_cls):
                                            ui.element("td").classes("p-2 font-medium max-w-xs truncate").text = str(qrow.get("query", ""))
                                            for metric in ("recall_at_k", "mrr_at_k", "ndcg_at_k"):
                                                ui.element("td").classes("p-2 text-center font-mono").text = f"{float(qrow.get(metric, 0)):.2f}"
                                            ui.element("td").classes("p-2 text-center font-mono").text = str(qrow.get("latency_ms", 0))
                                            ui.element("td").classes("p-2 text-center").text = str(qrow.get("results_count", 0))

                    # Failures detail
                    failures = [r for r in rows if float(r.get("recall_at_k", 0)) == 0]
                    if failures:
                        with ui.column().classes("rag-card w-full p-4 gap-2"):
                            with ui.row().classes("items-center gap-2 mb-1"):
                                ui.icon("search_off").classes("text-negative")
                                ui.label(f"Провалы ({len(failures)}) — нет ни одного попадания в топ-{k_val}").classes("font-semibold text-negative")
                            for fail in failures:
                                with ui.column().classes("w-full gap-1 p-2 border-b"):
                                    with ui.row().classes("items-center gap-2"):
                                        ui.icon("close", size="16px").classes("text-negative")
                                        ui.label(str(fail.get("query", ""))).classes("font-medium text-sm")
                                    ui.label(f"Ожидалось: {', '.join(fail.get('expected', []))}").classes("rag-meta text-xs")
                                    top = fail.get("top", [])
                                    if top:
                                        ui.label(f"Топ-1: {top[0].get('filename', top[0].get('path', '—'))} (score {top[0].get('score', 0):.3f})").classes("rag-path text-xs")

            async def _run_benchmark() -> None:
                if _bench_state.get("running"):
                    return
                _bench_state["running"] = True
                _bench_state["error"] = ""
                _bench_state["result"] = None
                run_btn.props("loading")
                try:
                    golden_path = str(golden_path_input.value or _DEFAULT_GOLDEN).strip()
                    k_val = int(k_input.value or 10)
                    golden = load_golden_queries(golden_path)
                    searcher = _ensure_searcher(state)
                    if searcher is None:
                        _bench_state["error"] = "Поиск не инициализирован — проверьте настройки Qdrant и коллекции."
                        return
                    def _search_fn(q: str, lim: int) -> list:
                        return _run_catalog_search(
                            searcher,
                            query=q, query_original=q, query_used=q,
                            limit=lim, file_type=None,
                            content_only=False, title_only=False,
                        )
                    import asyncio
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: evaluate_search(golden, _search_fn, limit=k_val)
                    )
                    _bench_state["result"] = result
                except Exception as exc:
                    _bench_state["error"] = str(exc)
                finally:
                    _bench_state["running"] = False
                    run_btn.props(remove="loading")
                _render_bench_result()

            run_btn.on("click", lambda: _run_benchmark())

        # ── Аудит ──────────────────────────────────────────────────
        with ui.tab_panel(tab_audit):
            auth_events = auth_db.list_auth_events(limit=200)
            with ui.column().classes("w-full gap-2"):
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
                        {"name": "ts", "label": "Время", "field": "ts", "sortable": True},
                        {"name": "username", "label": "Пользователь", "field": "username"},
                        {"name": "event_type", "label": "Событие", "field": "event_type"},
                        {"name": "ok", "label": "OK", "field": "ok"},
                        {"name": "error", "label": "Ошибка", "field": "error"},
                    ],
                    pagination=15,
                ).classes("w-full")

                def refresh_auth_table() -> None:
                    rows = list(auth_events)
                    source_mode = str(auth_source_filter.value or "Все")
                    if source_mode == "Telegram":
                        rows = [r for r in rows if str(r.get("event_type") or "").startswith("telegram_")]
                    elif source_mode == "Web/прочее":
                        rows = [r for r in rows if not str(r.get("event_type") or "").startswith("telegram_")]
                    user_needle = str(auth_user_filter.value or "").strip().lower()
                    if user_needle:
                        rows = [r for r in rows if user_needle in str(r.get("username") or "").lower()]
                    event_needle = str(auth_event_filter.value or "").strip().lower()
                    if event_needle:
                        rows = [r for r in rows if event_needle in str(r.get("event_type") or "").lower()]
                    ok_mode = str(auth_ok_filter.value or "Все")
                    if ok_mode == "OK":
                        rows = [r for r in rows if int(r.get("ok") or 0) == 1]
                    elif ok_mode == "Ошибки":
                        rows = [r for r in rows if int(r.get("ok") or 0) == 0]
                    auth_table.rows = rows
                    auth_table.update()

                auth_source_filter.on_value_change(lambda e: refresh_auth_table())
                auth_user_filter.on_value_change(lambda e: refresh_auth_table())
                auth_event_filter.on_value_change(lambda e: refresh_auth_table())
                auth_ok_filter.on_value_change(lambda e: refresh_auth_table())
                refresh_auth_table()

            if bool(state.cfg.get("cloud_drive_enabled")):
                ui.separator().classes("my-2")
                ui.label("Cloud Drive — журнал операций").classes("font-semibold text-sm")
                tdb = _get_telemetry(state)
                cd_events_raw = tdb.list_app_events(feature="cloud_drive", limit=200) if tdb else []

                with ui.row().classes("w-full gap-2"):
                    cd_action_filter = ui.input("Операция").props("dense outlined clearable").classes("w-48")
                    cd_user_filter2 = ui.input("Пользователь").props("dense outlined clearable").classes("w-48")

                cd_events_table = ui.table(
                    rows=cd_events_raw,
                    columns=[
                        {"name": "ts", "label": "Время", "field": "ts", "sortable": True},
                        {"name": "username", "label": "Пользователь", "field": "username"},
                        {"name": "action", "label": "Операция", "field": "action"},
                        {"name": "ok", "label": "OK", "field": "ok"},
                    ],
                    pagination=15,
                ).classes("w-full")

                def refresh_cd_audit() -> None:
                    rows = list(cd_events_raw)
                    if str(cd_action_filter.value or "").strip():
                        needle = cd_action_filter.value.strip().lower()
                        rows = [r for r in rows if needle in str(r.get("action") or "").lower()]
                    if str(cd_user_filter2.value or "").strip():
                        needle2 = cd_user_filter2.value.strip().lower()
                        rows = [r for r in rows if needle2 in str(r.get("username") or "").lower()]
                    cd_events_table.rows = rows
                    cd_events_table.update()

                cd_action_filter.on_value_change(lambda e: refresh_cd_audit())
                cd_user_filter2.on_value_change(lambda e: refresh_cd_audit())

        # ── Cloud Drive — аналитика ───────────────────────────────────────
        if tab_cd is not None:
            with ui.tab_panel(tab_cd):
                cd_svc = _cd_get_service(state.cfg)
                if cd_svc is None:
                    with ui.element("div").classes("cd-empty-state w-full py-8"):
                        ui.icon("cloud_off", size="36px").classes("opacity-30")
                        ui.label("Cloud Drive не инициализирован. Сначала настройте и инициализируйте реестр.").classes("text-center")
                else:
                    # ── Controls ─────────────────────────────────────────
                    with ui.row().classes("w-full gap-2 items-center flex-wrap"):
                        period_select = ui.select(
                            {"day": "По дням", "hour": "По часам", "week": "По неделям", "month": "По месяцам"},
                            value="day",
                            label="Детализация",
                        ).props("dense outlined").classes("w-44")
                        top_n_select = ui.select(
                            {10: "Топ 10", 20: "Топ 20", 50: "Топ 50"},
                            value=20,
                            label="Топ N файлов",
                        ).props("dense outlined").classes("w-36")
                        min_size_select = ui.select(
                            {0: "Любой размер", 1024: "> 1 КБ", 1048576: "> 1 МБ", 10485760: "> 10 МБ"},
                            value=0,
                            label="Мин. размер дублей",
                        ).props("dense outlined").classes("w-44")
                        ui.button(icon="refresh", on_click=lambda: ui.timer(0.05, refresh_cd_analytics, once=True)).props("flat dense round").classes("text-indigo-400").tooltip("Обновить")

                    cd_analytics_box = ui.column().classes("w-full gap-4")

                    async def refresh_cd_analytics() -> None:
                        bucket = str(period_select.value or "day")
                        top_n = int(top_n_select.value or 20)
                        min_sz = int(min_size_select.value or 0)
                        try:
                            timeline = await run.io_bound(
                                cd_svc.registry.get_change_timeline, bucket=bucket, limit=120
                            )
                            top_changed = await run.io_bound(
                                cd_svc.registry.get_top_changed_files, limit=top_n
                            )
                            duplicates = await run.io_bound(
                                cd_svc.registry.find_duplicates, min_size_bytes=min_sz
                            )
                            savings = await run.io_bound(cd_svc.registry.get_storage_savings)
                        except Exception as exc:
                            cd_analytics_box.clear()
                            with cd_analytics_box:
                                ui.label(f"Ошибка загрузки: {exc}").classes("text-negative text-sm")
                            return

                        cd_analytics_box.clear()
                        with cd_analytics_box:
                            # ── Storage savings KPIs ──────────────────────
                            saved = int(savings.get("saved_bytes") or 0)
                            total_files = int(savings.get("total_files") or 0)
                            unique_keys = int(savings.get("unique_storage_keys") or 0)
                            dup_files = total_files - unique_keys
                            with ui.row().classes("w-full gap-3 flex-wrap"):
                                for lbl, val, ico, clr in [
                                    ("Всего файлов", str(total_files), "description", ""),
                                    ("Уникальных блобов", str(unique_keys), "fingerprint", ""),
                                    ("Дублей", str(max(0, dup_files)), "content_copy", "text-amber-600" if dup_files > 0 else ""),
                                    ("Сэкономлено", _format_bytes(saved), "savings", "text-green-600" if saved > 0 else ""),
                                ]:
                                    with ui.column().classes("rag-card p-3 gap-0 items-center flex-1 min-w-28"):
                                        ui.icon(ico, size="20px").classes(f"text-indigo-400 {clr}".strip())
                                        ui.label(val).classes(f"text-lg font-semibold leading-tight {clr}".strip())
                                        ui.label(lbl).classes("rag-meta text-xs")

                            # ── Timeline chart ────────────────────────────
                            if timeline:
                                ui.label("Хронология изменений").classes("font-semibold text-sm mt-2")
                                labels = [r["bucket"] for r in timeline]
                                counts = [r["count"] for r in timeline]
                                unique_files_series = [r["unique_files"] for r in timeline]
                                ui.echart({
                                    "tooltip": {"trigger": "axis"},
                                    "legend": {"data": ["Версий", "Уникальных файлов"]},
                                    "xAxis": {"type": "category", "data": labels, "axisLabel": {"rotate": 30, "fontSize": 10}},
                                    "yAxis": {"type": "value", "minInterval": 1},
                                    "series": [
                                        {"name": "Версий", "type": "bar", "data": counts, "itemStyle": {"color": "#6366f1"}},
                                        {"name": "Уникальных файлов", "type": "line", "data": unique_files_series, "itemStyle": {"color": "#f59e0b"}, "smooth": True},
                                    ],
                                    "grid": {"left": "3%", "right": "4%", "bottom": "15%", "containLabel": True},
                                }).classes("w-full").style("height: 260px")
                            else:
                                with ui.element("div").classes("cd-empty-state w-full"):
                                    ui.icon("history", size="28px").classes("opacity-30")
                                    ui.label("Нет данных об изменениях — запустите импорт.").classes("text-center")

                            # ── Top changed files ─────────────────────────
                            if top_changed:
                                ui.label(f"Топ {top_n} файлов по количеству изменений").classes("font-semibold text-sm mt-2")
                                ui.table(
                                    rows=[
                                        {
                                            "name": r["name"],
                                            "path": r["path"],
                                            "versions": r["version_count"],
                                            "last_change": r["last_change"][:19].replace("T", " ") if r["last_change"] else "",
                                            "changed_by": ", ".join(r["changed_by"]) or "system",
                                            "size": _format_bytes(r["size_bytes"]),
                                        }
                                        for r in top_changed
                                    ],
                                    columns=[
                                        {"name": "name", "label": "Файл", "field": "name", "align": "left"},
                                        {"name": "versions", "label": "Версий", "field": "versions", "sortable": True},
                                        {"name": "last_change", "label": "Последнее изменение", "field": "last_change", "sortable": True},
                                        {"name": "changed_by", "label": "Кто менял", "field": "changed_by"},
                                        {"name": "size", "label": "Размер", "field": "size"},
                                    ],
                                    pagination=10,
                                ).classes("w-full")
                            else:
                                ui.label("Топ файлов пуст — версии ещё не накоплены.").classes("rag-meta text-sm")

                            # ── Duplicates ────────────────────────────────
                            ui.separator()
                            with ui.row().classes("w-full items-center gap-2 mt-1"):
                                ui.icon("content_copy", size="18px").classes("text-indigo-400")
                                ui.label("Дубли файлов").classes("font-semibold text-sm")
                                ui.label("(одинаковое содержимое, разные пути)").classes("rag-meta text-xs")
                            if not duplicates:
                                ui.label("Дублей не найдено. Содержимое всех файлов уникально.").classes("rag-meta text-sm")
                            else:
                                total_wasted = sum(d["wasted_bytes"] for d in duplicates)
                                ui.label(
                                    f"{len(duplicates)} групп дублей · можно освободить {_format_bytes(total_wasted)}"
                                ).classes("rag-meta text-xs mb-1")
                                for dup in duplicates[:30]:
                                    with ui.expansion(
                                        f"{dup['file_count']} копии · {_format_bytes(dup['size_bytes'])} каждая · экономия {_format_bytes(dup['wasted_bytes'])}",
                                        icon="content_copy",
                                    ).classes("w-full rag-card"):
                                        for finfo in dup["files"]:
                                            ui.label(finfo["path"]).classes("rag-path text-xs")
                                if len(duplicates) > 30:
                                    ui.label(f"…ещё {len(duplicates) - 30} групп").classes("rag-meta text-xs")

                    ui.timer(0.3, refresh_cd_analytics, once=True)

