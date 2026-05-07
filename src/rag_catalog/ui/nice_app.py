"""NiceGUI web frontend for RAG Catalog."""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from nicegui import app, events, run, ui

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.rag_core import load_config, save_config
from rag_catalog.core.user_auth_db import UserAuthDB

from . import api as _api_routes  # noqa: F401 — import triggers route registration
from . import index_view as _index_view
from . import settings_view as _settings_view
from . import stats_view as _stats_view
from .css import _install_css
from .helpers import (
    _CADENCE_LABELS,
    _DAY_LABELS,
    _DAY_RU,
    FILE_PREVIEW_EXTENSIONS,
    INLINE_IMAGE_EXTENSIONS,
    OFFICE_PREVIEW_EXTENSIONS,
    PAGE_SIZE,
    _apply_explorer_filter_input,
    _cd_acl_allows,
    _cd_breadcrumb_chain,
    _cd_file_jobs_map,
    _cd_file_size,
    _cd_get_service,
    _cd_list_children,
    _cd_search_by_name,
    _clean_text,
    _cloud_query_set,
    _count_exact_name_matches,
    _db_query_dicts,
    _dedupe_queries,
    _directory_children,
    _ensure_searcher,
    _file_icon_svg,
    _file_rows,
    _filter_log_text,
    _format_bytes,
    _format_duration_seconds,
    _format_file_size,
    _format_relative_time,
    _highlight_query_terms,
    _is_admin,
    _is_system_file,
    _load_user_state,
    _merge_search_results,
    _my_recent_queries,
    _open_os_path,
    _popular_queries,
    _preview_file,
    _preview_office_file,
    _read_index_stats,
    _read_index_telemetry,
    _read_log_tail_lines,
    _remember_query,
    _resolve_catalog_file,
    _result_group,
    _result_kind,
    _run_catalog_search,
    _run_quick_name_search,
    _safe_explorer_path,
    _save_explorer_settings,
    _save_ui_settings,
    _schedule_display_label,
    _search_suggestions,
    _select_in_os_explorer,
    _telegram_deeplink,
    _viewer_file_url,
)
from .state import (
    CONFIG_PATH_KEYS,
    PageState,
    _get_auth_db,
    _get_telemetry,
    _is_favorite,
    _is_saved_search,
    _log_app_event,
    _refresh_current_user,
    _save_config_patch,
    _toggle_favorite,
    _toggle_saved_search,
    _username,
)
from .system import (
    _STAGE_LABELS,
    _find_live_running_index_run,
    _launch_indexer,
    _launch_ocr,
    _read_cloud_bootstrap_status,
    _recover_cloud_drive_jobs,
    _run_recovery_cycle,
    _safe_int,
    _start_global_scheduler,
    _start_recovery_watchdog,
    _stop_managed_timer,
    _telemetry_db_path,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
APP_ICON_PATH = PROJECT_ROOT / "assets" / "brand" / "ico" / "favicon.ico"
LOGO_PATH = PROJECT_ROOT / "assets" / "brand" / "svg" / "rag-search-mark.svg"

SEARCH_PRESETS = [
    ("Договоры", "договор поставки"),
    ("Счета", "счет на оплату"),
    ("Паспорта", "паспорт техника"),
    ("PDF", "pdf скан"),
    ("Таблицы", "реестр xlsx"),
]

if LOGO_PATH.exists():
    app.add_static_file(local_file=LOGO_PATH, url_path="/rag-logo.png")


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
            else:
                state.session_expired = True
                state.auth_token = ""
                try:
                    app.storage.user.pop("auth_token", None)
                except Exception:
                    pass
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

    _stop_managed_timer(state.activity_timer)
    state.activity_timer = None
    if state.auth_token and state.current_user:
        state.activity_timer = ui.timer(3600.0, touch_activity)

    _stop_managed_timer(state.scheduler_timer)
    state.scheduler_timer = None

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
        state.doc_explain_path = ""
        state.doc_explain_text = ""
        state.doc_explain_loading = False
        state.selected_result_paths = []
        state.selection_summary_text = ""
        state.selection_summary_loading = False
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
            quick_results = [
                item for item in quick_results
                if not (item.get("cloud_file_id") or item.get("cloud_path"))
                or _cd_acl_allows(state.cfg, state.current_user, str(item.get("cloud_path") or item.get("path") or ""))
            ]
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
            state.results = [
                item for item in state.results
                if not (item.get("cloud_file_id") or item.get("cloud_path"))
                or _cd_acl_allows(state.cfg, state.current_user, str(item.get("cloud_path") or item.get("path") or ""))
            ]
            cloud_semantic_count = sum(
                1
                for item in state.results
                if item.get("cloud_file_id") or item.get("cloud_path")
            )
            state.search_stats_hint = (
                f"{state.search_stats_hint} · после догрузки: {len(state.results)}"
                if state.search_stats_hint else f"После догрузки: {len(state.results)}"
            )
            if cloud_semantic_count:
                state.search_stats_hint = f"{state.search_stats_hint} · Cloud Drive: {cloud_semantic_count}"
            _log_app_event(
                state,
                "search",
                "run_full",
                details={
                    "query": query,
                    "query_used": search_query,
                    "results": len(state.results),
                    "cloud_results": cloud_semantic_count,
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

    # ── Search screen ─────────────────────────────────────────────────────────

    def render_suggestions(area: ui.column, typed: str) -> None:
        area.clear()
        username = _username(state)
        personal = _dedupe_queries([*state.history, *_my_recent_queries(state.cfg, username, limit=12)], limit=12)
        popular = _popular_queries(state.cfg, exclude_username=username, limit=10)
        cloud_qs = _cloud_query_set(state.cfg, username) if bool(state.cfg.get("cloud_drive_enabled")) else set()
        saved_qs = [str(s.get("query") or "") for s in state.saved_searches if s.get("query")]

        needle = typed.strip().lower()
        if needle:
            personal = [q for q in personal if needle in q.lower()]
            popular = [q for q in popular if needle in q.lower()]
            saved_show = [q for q in saved_qs if needle in q.lower()]
        else:
            personal = personal[:8]
            popular = popular[:8]
            saved_show = saved_qs[:6]

        if not personal and not popular and not saved_show:
            return

        with area:
            with ui.row().classes("rag-suggest p-3 gap-0 w-full"):
                # Сохранённые запросы (если есть)
                if saved_show:
                    has_right = bool(personal or popular)
                    col_cls = "flex-1 gap-1 min-w-0" + (" pr-3 border-r border-gray-200" if has_right else "")
                    with ui.column().classes(col_cls):
                        ui.label("Сохранённые").classes("rag-meta px-2 py-1 font-semibold text-xs uppercase tracking-wide")
                        for item in saved_show:
                            with ui.row().classes("w-full items-center gap-1"):
                                btn = ui.button(item, icon="bookmark", on_click=choose_query_handler(item), color=None).props("flat align=left no-caps").classes("rag-nav-button rag-suggest-item flex-1")
                                btn.tooltip(item)
                                def _remove_ss(q: str = item) -> None:
                                    _toggle_saved_search(state, q)
                                    render_suggestions(area, needle)
                                rm = ui.button(icon="close", on_click=_remove_ss, color=None).props("flat round dense")
                                rm.classes("rag-feedback-btn shrink-0")
                                rm.tooltip("Удалить из сохранённых")
                # Личная история
                if personal:
                    has_right = bool(popular)
                    col_cls = "flex-1 gap-1 min-w-0" + (" pr-3 border-r border-gray-200" if has_right else "") + (" pl-3" if saved_show else "")
                    with ui.column().classes(col_cls):
                        ui.label("Моя история").classes("rag-meta px-2 py-1 font-semibold text-xs uppercase tracking-wide")
                        for item in personal:
                            with ui.row().classes("w-full items-center gap-1"):
                                btn = ui.button(item, icon="history", on_click=choose_query_handler(item), color=None).props("flat align=left no-caps").classes("rag-nav-button rag-suggest-item flex-1")
                                btn.tooltip(item)
                                if item.lower() in cloud_qs:
                                    ci = ui.icon("cloud", size="14px").classes("text-blue-400 shrink-0")
                                    ci.tooltip("Этот запрос ранее возвращал Cloud Drive документы")
                # Часто ищут
                if popular:
                    col_cls = "flex-1 gap-1 min-w-0" + (" pl-3" if personal or saved_show else "")
                    with ui.column().classes(col_cls):
                        ui.label("Часто ищут").classes("rag-meta px-2 py-1 font-semibold text-xs uppercase tracking-wide")
                        for item in popular:
                            with ui.row().classes("w-full items-center gap-1"):
                                btn = ui.button(item, icon="trending_up", on_click=choose_query_handler(item), color=None).props("flat align=left no-caps").classes("rag-nav-button rag-suggest-item flex-1")
                                btn.tooltip(item)
                                if item.lower() in cloud_qs:
                                    ci = ui.icon("cloud", size="14px").classes("text-blue-400 shrink-0")
                                    ci.tooltip("Этот запрос ранее возвращал Cloud Drive документы")

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

                ui.button(icon="search", on_click=submit_click, color="primary").props("unelevated round").tooltip("Поиск (Ctrl+K для фокуса)")

            def handle_input(_: events.GenericEventArguments | None = None) -> None:
                state.query = str(search_input.value or "")
                render_suggestions(suggest_area, state.query)

            async def submit_from_input(_: events.GenericEventArguments | None = None) -> None:
                typed = str(search_input.value or "")
                suggest_area.clear()
                await run_search(typed)

            def close_suggestions(_: events.GenericEventArguments | None = None) -> None:
                suggest_area.clear()

            search_input.on("focus", handle_input)
            search_input.on("input", handle_input)
            search_input.on("keyup.enter", submit_from_input)
            search_input.on("keyup.escape", close_suggestions)

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
                    max=200,
                    step=10,
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
                ui.button("Найти в ОС", icon="open_in_new", on_click=lambda p=candidate: _select_in_os_explorer(str(p))).props("outline dense").tooltip("Выделить файл в проводнике Windows")
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

    def _parse_rag_answer(text: str) -> tuple[str, List[str]]:
        """Split RAG answer into (body, list_of_source_filenames)."""
        marker = "Источники:"
        idx = text.rfind(marker)
        if idx == -1:
            return text.strip(), []
        body = text[:idx].strip()
        sources_raw = text[idx + len(marker):].strip()
        sources = [s.strip() for s in sources_raw.split(",") if s.strip()]
        return body, sources

    async def ask_explain(result: Dict[str, Any]) -> None:
        """Run rag_answer() focused on a single document and display inline."""
        if not bool(state.cfg.get("llm_enabled")):
            ui.notify("LLM не включён в настройках.", type="warning")
            return
        path = str(result.get("full_path") or result.get("path") or "")
        fname = str(result.get("filename") or path)
        state.doc_explain_path = path or fname
        state.doc_explain_text = ""
        state.doc_explain_loading = True
        render()
        try:
            from rag_catalog.core.llm import rag_answer  # noqa: PLC0415
            ollama_url = str(state.cfg.get("ollama_url") or "http://localhost:11434")
            rag_model = str(state.cfg.get("llm_rag_model") or "qwen3:8b")
            query = state.searched_query or "Опиши содержимое этого документа"
            answer = await run.io_bound(
                rag_answer, query, [result], model=rag_model, ollama_url=ollama_url
            )
            state.doc_explain_text = answer or "Модель не дала ответа."
        except Exception as exc:
            state.doc_explain_text = f"Ошибка: {exc}"
        finally:
            state.doc_explain_loading = False
        render()

    async def summarize_selection() -> None:
        """Run rag_answer() over currently selected results."""
        if not bool(state.cfg.get("llm_enabled")):
            ui.notify("LLM не включён в настройках.", type="warning")
            return
        selected = [r for r in state.results if str(r.get("full_path") or r.get("path") or "") in state.selected_result_paths]
        if len(selected) < 2:
            ui.notify("Выберите хотя бы 2 документа.", type="warning")
            return
        state.selection_summary_text = ""
        state.selection_summary_loading = True
        render()
        try:
            from rag_catalog.core.llm import rag_answer  # noqa: PLC0415
            ollama_url = str(state.cfg.get("ollama_url") or "http://localhost:11434")
            rag_model = str(state.cfg.get("llm_rag_model") or "qwen3:8b")
            query = state.searched_query or "Сделай сводку по выбранным документам"
            answer = await run.io_bound(
                rag_answer, query, selected, model=rag_model, ollama_url=ollama_url
            )
            state.selection_summary_text = answer or "Модель не дала ответа."
        except Exception as exc:
            state.selection_summary_text = f"Ошибка: {exc}"
        finally:
            state.selection_summary_loading = False
        render()

    def render_result(result: Dict[str, Any], index: int, cloud_jobs: Optional[Dict[str, Dict[str, str]]] = None) -> None:
        name = str(result.get("filename") or "Без имени")
        path = str(result.get("path") or "")
        full_path = str(result.get("full_path") or "")
        score = float(result.get("rank_score") or result.get("score") or 0)
        chunk_index = result.get("chunk_index")
        is_rrf = str(result.get("fusion") or "") == "rrf"
        kind = _result_kind(result)
        text = _clean_text(result.get("text") or "")
        preview = text[:280] + ("..." if len(text) > 280 else "")
        p = Path(full_path) if full_path else None
        cloud_file_id = str(result.get("cloud_file_id") or "")
        cloud_version_id = str(result.get("cloud_version_id") or "")
        cloud_path = str(result.get("cloud_path") or "")
        is_cloud_result = bool(cloud_file_id or cloud_path)
        cloud_job = (cloud_jobs or {}).get(cloud_file_id) if cloud_file_id else None

        def go_cloud_explorer(cloud_item_path: str) -> None:
            item_path = str(cloud_item_path or "").strip().strip("/")
            if not item_path:
                state.explorer_cd_path = ""
            elif kind == "Каталог":
                state.explorer_cd_path = item_path
            else:
                state.explorer_cd_path = item_path.rsplit("/", 1)[0] if "/" in item_path else ""
            state.explorer_page = 0
            state.screen = "explorer"
            ui.run_javascript("history.pushState(null, '', '/explorer')")
            render()

        def render_cloud_job_badge() -> None:
            if not cloud_job:
                return
            status = str(cloud_job.get("status") or "")
            if status == "completed":
                return
            job_type = str(cloud_job.get("job_type") or "reindex")
            icon = {
                "pending": "hourglass_empty",
                "running": "sync",
                "failed": "error_outline",
                "cancelled": "block",
            }.get(status)
            css = {
                "pending": "cd-status-pending",
                "running": "cd-status-running",
                "failed": "cd-status-error",
                "cancelled": "cd-status-error",
            }.get(status, "cd-status-pending")
            _job_type_labels: Dict[str, Dict[str, str]] = {
                "reindex": {"pending": "В очереди", "running": "Индексируется", "failed": "Ошибка индексации", "cancelled": "Отменено"},
                "cleanup": {"pending": "Очистка", "running": "Очищается", "failed": "Ошибка очистки", "cancelled": "Отменено"},
                "ocr": {"pending": "OCR ожидает", "running": "OCR…", "failed": "Ошибка OCR", "cancelled": "Отменено"},
                "preview": {"pending": "Preview ожидает", "running": "Preview…", "failed": "Ошибка preview", "cancelled": "Отменено"},
            }
            label = _job_type_labels.get(job_type, _job_type_labels["reindex"]).get(status, status)
            tip = label
            if status == "failed" and cloud_job.get("last_error"):
                tip = f"Ошибка: {str(cloud_job.get('last_error'))[:160]}"
            with ui.element("span").classes(f"cd-status-badge {css}"):
                if icon:
                    ui.icon(icon, size="14px")
                ui.label(label)
                ui.tooltip(tip)

        def rate_result(value: int, result: Dict[str, Any] = result, index: int = index) -> None:
            result_path = str(result.get("full_path") or result.get("path") or "")
            telemetry_details = {
                "screen": "search",
                "reason": "explicit",
                "cloud_file_id": cloud_file_id,
                "cloud_version_id": cloud_version_id,
                "cloud_path": cloud_path,
                "source": "cloud_drive" if is_cloud_result else "filesystem",
            }
            _get_telemetry(state).log_search_feedback(
                username=_username(state),
                source="nicegui",
                query=state.searched_query,
                result_path=result_path,
                result_title=str(result.get("filename") or result_path),
                feedback=value,
                result_rank=index,
                result_score=float(result.get("score") or 0),
                details=telemetry_details,
            )
            _log_app_event(
                state,
                "search",
                "feedback",
                details={**telemetry_details, "value": value, "path": result_path, "query": state.searched_query},
            )
            ui.notify("Оценка сохранена.", type="positive")

        def track_result_use(reason: str, result: Dict[str, Any] = result, index: int = index) -> None:
            result_path = str(result.get("full_path") or result.get("path") or "")
            telemetry_details = {
                "screen": "search",
                "reason": reason,
                "cloud_file_id": cloud_file_id,
                "cloud_version_id": cloud_version_id,
                "cloud_path": cloud_path,
                "source": "cloud_drive" if is_cloud_result else "filesystem",
            }
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
                    details=telemetry_details,
                )
            except Exception:
                pass
            _log_app_event(
                state,
                "search",
                "result_use",
                details={**telemetry_details, "path": result_path, "query": state.searched_query},
            )

        def open_primary() -> None:
            if kind == "Каталог":
                track_result_use("open_folder")
                if is_cloud_result and cloud_path:
                    go_cloud_explorer(cloud_path)
                else:
                    go_explorer(full_path)
                return
            if p and p.exists() and p.is_file():
                track_result_use("open_viewer")
                open_file_viewer(p)
            elif is_cloud_result and cloud_path:
                track_result_use("open_cloud_drive")
                go_cloud_explorer(cloud_path)

        result_key = full_path or path or name
        llm_on = bool(state.cfg.get("llm_enabled"))
        is_selected = result_key in state.selected_result_paths
        is_explaining = state.doc_explain_path == result_key

        with ui.column().classes("rag-result gap-2"):
            with ui.row().classes("w-full items-start gap-2"):
                if llm_on:
                    def _toggle_select(rk: str = result_key) -> None:
                        if rk in state.selected_result_paths:
                            state.selected_result_paths = [x for x in state.selected_result_paths if x != rk]
                        else:
                            state.selected_result_paths = [*state.selected_result_paths, rk]
                        render()
                    _cb = ui.checkbox(value=is_selected, on_change=lambda _: _toggle_select()).props("dense")
                    _cb.classes("mt-1")
                opener = ui.row().classes("flex-1 min-w-0 items-start gap-2 cursor-pointer").on("click", open_primary)
                with opener:
                    ui.html(_file_icon_svg(full_path or path, kind), sanitize=False)
                    with ui.column().classes("flex-1 min-w-0 gap-0"):
                        title = ui.label(f"{index}. {name}").classes("text-base font-semibold truncate")
                        title.tooltip(name)
                        path_label = ui.label(path or full_path).classes("rag-path truncate")
                        path_label.tooltip(path or full_path)
                with ui.row().classes("items-center gap-1 flex-wrap justify-end"):
                    if is_cloud_result:
                        ui.label("Cloud Drive").classes("rag-chip")
                        if cloud_version_id:
                            v_label = ui.label(f"v {cloud_version_id[:8]}").classes("rag-chip")
                            v_label.tooltip(f"Cloud Drive version_id: {cloud_version_id}")
                        render_cloud_job_badge()
                    chip_text = kind
                    if chunk_index is not None:
                        chip_text += f" · фр.{chunk_index}"
                    chip_text += f" · {score:.3f}"
                    ui.label(chip_text).classes("rag-chip")
                    if is_rrf:
                        rrf_badge = ui.label("RRF").classes("rag-chip text-xs bg-indigo-50 text-indigo-600 dark:bg-indigo-900 dark:text-indigo-300")
                        rrf_badge.tooltip("Результат получен методом Reciprocal Rank Fusion")

            with ui.row().classes("w-full items-center justify-between gap-2"):
                with ui.row().classes("rag-actions items-center"):
                    if is_cloud_result and cloud_path:
                        ui.button(
                            "В Cloud Drive",
                            icon="cloud",
                            on_click=lambda pth=cloud_path: go_cloud_explorer(pth),
                        ).props("outline dense no-caps")
                        if kind != "Каталог":
                            _dl_url = f"/api/cloud-drive/download?path={quote(cloud_path, safe='')}"
                            def _cd_download(url: str = _dl_url, pth: str = cloud_path) -> None:
                                track_result_use("cloud_download")
                                ui.navigate.to(url, new_tab=True)
                            ui.button(icon="download", on_click=_cd_download).props("outline dense round").tooltip(f"Скачать из Cloud Drive: {cloud_path.rsplit('/', 1)[-1]}")
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
                        if kind == "Каталог":
                            ui.button("Открыть в ОС", icon="open_in_new", on_click=lambda pth=full_path: _open_os_path(pth)).props("outline dense")
                        else:
                            ui.button("Найти в ОС", icon="open_in_new", on_click=lambda pth=full_path: _select_in_os_explorer(pth)).props("outline dense").tooltip("Выделить файл в проводнике Windows")
                    if llm_on and kind != "Каталог":
                        if is_explaining and state.doc_explain_loading:
                            ui.spinner(size="xs").classes("ml-1")
                        else:
                            async def _explain_click(r: Dict[str, Any] = result) -> None:
                                if state.doc_explain_path == (str(r.get("full_path") or r.get("path") or "")):
                                    state.doc_explain_path = ""
                                    state.doc_explain_text = ""
                                    render()
                                else:
                                    await ask_explain(r)
                            _explain_label = "Скрыть" if (is_explaining and state.doc_explain_text) else "Пояснить"
                            ui.button(_explain_label, icon="psychology", on_click=_explain_click).props("flat dense no-caps").classes("text-indigo-600")
                with ui.row().classes("items-center justify-end gap-1"):
                    _copy_src = cloud_path or full_path or path
                    if _copy_src:
                        ui.button(icon="content_copy", on_click=lambda cp=_copy_src: (
                            ui.run_javascript(f"navigator.clipboard && navigator.clipboard.writeText({json.dumps(cp)})"),
                            ui.notify("Путь скопирован.", type="positive"),
                        ), color=None).props("flat round dense").tooltip("Скопировать путь")
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
            else:
                if preview:
                    _hl = _highlight_query_terms(preview, state.searched_query or "")
                    ui.html(f'<span class="rag-meta">{_hl}</span>', sanitize=False)
                # Inline explain result
                if is_explaining:
                    if state.doc_explain_loading:
                        with ui.row().classes("items-center gap-2 bg-indigo-50 border border-indigo-200 rounded p-2 w-full"):
                            ui.spinner(size="xs")
                            ui.label("Анализирую документ…").classes("rag-meta text-xs")
                    elif state.doc_explain_text:
                        _exp_body, _exp_sources = _parse_rag_answer(state.doc_explain_text)
                        with ui.column().classes("bg-indigo-50 border border-indigo-200 rounded p-3 gap-1 w-full"):
                            with ui.row().classes("items-center gap-1"):
                                ui.icon("psychology", size="16px").classes("text-indigo-500")
                                ui.label("Пояснение по документу").classes("text-xs font-semibold text-indigo-700")
                            ui.label(_exp_body).classes("text-sm whitespace-pre-wrap")

    def _render_cd_search_hints(query: str) -> None:
        """Render a compact Cloud Drive registry section above main search results."""
        cd_svc = _cd_get_service(state.cfg)
        if cd_svc is None or not query:
            return
        try:
            q = query.strip()
            if not q:
                return
            root = cd_svc.registry.get_root_folder()
            if root is None:
                return
            matched_folders, matched_files = _cd_search_by_name(cd_svc.registry, q)

            if not matched_folders and not matched_files:
                return

            with ui.column().classes("rag-card w-full p-3 gap-2"):
                with ui.row().classes("items-center gap-2"):
                    ui.icon("cloud", size="16px").classes("text-indigo-400")
                    ui.label("Cloud Drive").classes("font-semibold text-sm text-indigo-700")
                    ui.label("— совпадения в реестре").classes("rag-meta text-xs")
                if matched_folders:
                    with ui.row().classes("w-full gap-2 flex-wrap"):
                        for folder in matched_folders:
                            def _go_folder(fp: str = folder.path) -> None:
                                state.explorer_cd_path = fp
                                state.screen = "explorer"
                                render()
                            with ui.element("div").classes(
                                "rag-card p-2 gap-1 flex flex-row items-center cursor-pointer hover:bg-slate-50"
                            ).on("click", _go_folder):
                                ui.icon("folder", size="18px").classes("text-yellow-500")
                                with ui.column().classes("gap-0"):
                                    ui.label(folder.name).classes("text-sm font-medium leading-tight")
                                    ui.label(folder.path or "/").classes("rag-path text-xs")
                if matched_files:
                    with ui.row().classes("w-full gap-2 flex-wrap"):
                        for f in matched_files:
                            def _go_file(fpath: str = str(f.source_path or f.path or ""), fname: str = f.name) -> None:
                                p = Path(fpath) if fpath else None
                                if p and p.exists() and p.is_file():
                                    open_file_viewer(p)
                                else:
                                    ui.notify(f"Файл «{fname}» недоступен на диске.", type="warning")
                            def _show_in_explorer(fp: str = f.path) -> None:
                                parent = fp.rsplit("/", 1)[0] if "/" in fp else ""
                                state.explorer_cd_path = parent
                                state.screen = "explorer"
                                render()
                            with ui.row().classes("rag-card p-2 gap-2 items-center"):
                                with ui.element("div").classes(
                                    "flex flex-row items-center gap-2 cursor-pointer hover:bg-slate-50 flex-1"
                                ).on("click", _go_file):
                                    ui.html(_file_icon_svg(f.name, "Файл"), sanitize=False)
                                    with ui.column().classes("gap-0"):
                                        ui.label(f.name).classes("text-sm font-medium leading-tight")
                                        parent_lbl = f.path.rsplit("/", 1)[0] if "/" in f.path else "Корень"
                                        ui.label(f"{parent_lbl} · {_cd_file_size(f.size_bytes)}").classes("rag-path text-xs")
                                ui.button(
                                    icon="folder_open",
                                    on_click=_show_in_explorer,
                                    color=None,
                                ).props("flat round dense").tooltip("Показать в Cloud Drive")
        except Exception:
            pass  # don't break search if registry lookup fails

    def render_search_screen() -> None:
        render_search_header()
        if state.search_error:
            ui.label(state.search_error).classes("text-red-700 rag-card p-4")
        if not state.searched_query:
            with ui.row().classes("w-full gap-3"):
                for label, query in SEARCH_PRESETS:
                    ui.button(label, on_click=choose_query_handler(query)).props("outline")
            return
        # Cloud Drive registry quick-match hints (shown before semantic results)
        _render_cd_search_hints(state.searched_query)

        # Заголовок с опциональной подсказкой о расширении запроса
        with ui.row().classes("w-full items-center gap-2 mt-2"):
            ui.label(f"Результаты по запросу: {state.searched_query}").classes("text-xl font-semibold")
            if state.expanded_query:
                ui.label(f"→ расширен: {state.expanded_query}").classes("rag-meta text-sm italic")
            _ss_active = _is_saved_search(state, state.searched_query)
            _ss_icon = "bookmark" if _ss_active else "bookmark_border"
            _ss_tip = "Удалить из сохранённых запросов" if _ss_active else "Сохранить этот запрос"
            def _toggle_ss(q: str = state.searched_query) -> None:
                _toggle_saved_search(state, q)
                render()
            _ss_btn = ui.button(icon=_ss_icon, on_click=_toggle_ss, color=None).props("flat round dense")
            _ss_btn.classes("text-amber-500" if _ss_active else "text-slate-400")
            _ss_btn.tooltip(_ss_tip)
            def _export_results_csv() -> None:
                import csv as _csv
                import io as _io
                buf = _io.StringIO()
                writer = _csv.writer(buf)
                writer.writerow(["#", "Имя файла", "Путь", "Тип", "Оценка", "Фрагмент"])
                for i, r in enumerate(state.results, 1):
                    writer.writerow([
                        i,
                        str(r.get("filename") or ""),
                        str(r.get("full_path") or r.get("path") or ""),
                        _result_kind(r),
                        f"{float(r.get('rank_score') or r.get('score') or 0):.4f}",
                        _clean_text(r.get("text") or "")[:200],
                    ])
                content_bytes = buf.getvalue().encode("utf-8-sig")
                safe_q = re.sub(r"[^\w\-а-яёА-ЯЁ ]", "_", state.searched_query or "results")[:40]
                ui.download(content_bytes, filename=f"search_{safe_q}.csv")
                _log_app_event(state, "search", "export_csv", details={"query": state.searched_query, "count": len(state.results)})
            ui.button(icon="download", on_click=_export_results_csv, color=None).props("flat round dense").tooltip("Экспорт результатов в CSV")
        if state.search_stats_hint:
            ui.label(state.search_stats_hint).classes("rag-meta")
        if state.search_lazy_loading:
            with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
                ui.spinner(size="sm")
                ui.label("Догружаю дополнительные совпадения…").classes("rag-meta")

        # RAG Q&A карточка (основной ответ по всем результатам)
        if state.rag_answer_loading:
            with ui.row().classes("rag-card w-full p-3 gap-2 items-center"):
                ui.spinner(size="sm")
                ui.label("Анализирую документы…").classes("rag-meta")
        elif state.rag_answer_text:
            _body, _sources = _parse_rag_answer(state.rag_answer_text)
            with ui.column().classes("rag-card w-full p-3 gap-2"):
                with ui.row().classes("items-center gap-1"):
                    ui.icon("smart_toy", size="18px").classes("text-indigo-500")
                    ui.label("Ответ ИИ").classes("font-semibold text-sm text-indigo-700")
                ui.label(_body).classes("text-sm whitespace-pre-wrap")
                if _sources:
                    ui.separator()
                    with ui.row().classes("items-center gap-2 flex-wrap"):
                        ui.label("Источники:").classes("rag-meta text-xs font-medium")
                        for _src in _sources:
                            _src_result = next(
                                (r for r in state.results if str(r.get("filename") or "").lower() == _src.lower()),
                                None,
                            )
                            _src_path = Path(str(_src_result.get("full_path") or "")) if _src_result else None
                            if _src_path and _src_path.exists() and _src_path.is_file():
                                ui.button(_src, icon="description", on_click=lambda p=_src_path: open_file_viewer(p)).props("outline dense no-caps").classes("text-xs")
                            else:
                                ui.label(_src).classes("rag-chip text-xs")

        # Сводка по выбранным
        if state.selection_summary_loading:
            with ui.row().classes("rag-card w-full p-3 gap-2 items-center bg-violet-50 border border-violet-200"):
                ui.spinner(size="sm")
                ui.label("Формирую сводку по выбранным документам…").classes("rag-meta")
        elif state.selection_summary_text:
            _sel_body, _sel_sources = _parse_rag_answer(state.selection_summary_text)
            with ui.column().classes("rag-card w-full p-3 gap-2 bg-violet-50 border border-violet-200"):
                with ui.row().classes("items-center justify-between w-full"):
                    with ui.row().classes("items-center gap-1"):
                        ui.icon("summarize", size="18px").classes("text-violet-600")
                        ui.label("Сводка по выбранным").classes("font-semibold text-sm text-violet-700")
                    ui.button(icon="close", on_click=lambda: (
                        state.__setattr__("selection_summary_text", ""),
                        render(),
                    ), color=None).props("flat round dense")
                ui.label(_sel_body).classes("text-sm whitespace-pre-wrap")
                if _sel_sources:
                    ui.separator()
                    with ui.row().classes("items-center gap-2 flex-wrap"):
                        ui.label("Источники:").classes("rag-meta text-xs font-medium")
                        for _src in _sel_sources:
                            ui.label(_src).classes("rag-chip text-xs")

        if not state.results:
            with ui.column().classes("rag-card w-full p-6 gap-3 items-center"):
                ui.icon("search_off", size="40px").classes("text-slate-300 dark:text-slate-600")
                ui.label("Совпадений не найдено.").classes("text-lg font-semibold text-slate-500")
                q = state.searched_query or ""
                hints: List[str] = []
                if state.content_only or state.title_only:
                    hints.append("Снимите фильтр «Только содержимое» или «Только название»")
                if state.file_type and state.file_type != "Все":
                    hints.append(f"Попробуйте сбросить фильтр типа файла «{state.file_type}»")
                if len(q.split()) > 4:
                    hints.append("Сократите запрос до ключевых слов")
                hints.append("Проверьте, что индекс создан и Qdrant доступен в настройках")
                with ui.column().classes("gap-1 items-center"):
                    for hint in hints:
                        ui.label(f"• {hint}").classes("rag-meta text-sm")
                if q and _is_admin(state):
                    ui.separator().classes("w-full my-1")
                    ui.button(
                        "Добавить синоним для этого запроса",
                        icon="auto_awesome",
                        on_click=lambda: (
                            setattr(state, "settings_section", "aliases"),
                            set_screen("settings"),
                        ),
                        color=None,
                    ).props("flat dense no-caps").classes("rag-meta text-xs")
            return

        # Все результаты — плоский список, отсортированный по релевантности
        sorted_results = sorted(
            state.results,
            key=lambda r: float(r.get("rank_score", r.get("score") or 0) or 0),
            reverse=True,
        )

        # Count unique source documents per filter group (not raw chunk count)
        _doc_keys_by_group: Dict[str, set] = {}
        for r in sorted_results:
            grp = _result_group(r)
            key = str(r.get("cloud_file_id") or r.get("full_path") or r.get("path") or id(r))
            _doc_keys_by_group.setdefault(grp, set()).add(key)
        group_counts: Dict[str, int] = {g: len(ks) for g, ks in _doc_keys_by_group.items()}
        total_doc_count = len({
            str(r.get("cloud_file_id") or r.get("full_path") or r.get("path") or id(r))
            for r in sorted_results
        })

        # Порядок групп как был в _grouped_results
        group_order = [
            "Каталоги", "Техпаспорта ТС", "Паспорта и удостоверения",
            "Договоры", "Счета и платежи", "Таблицы", "PDF", "Другие файлы",
        ]

        def set_filter(gname: Optional[str]) -> None:
            state.active_type_filter = gname
            state.displayed_count = 10
            render()

        # Бар выбранных документов (показывается когда выбрано ≥1)
        llm_enabled_for_select = bool(state.cfg.get("llm_enabled"))
        if llm_enabled_for_select and state.selected_result_paths:
            n_sel = len(state.selected_result_paths)
            with ui.row().classes("w-full items-center gap-2 bg-violet-50 border border-violet-200 rounded p-2"):
                ui.icon("checklist").classes("text-violet-500")
                ui.label(f"Выбрано: {n_sel}").classes("text-violet-700 text-sm font-medium flex-1")
                if n_sel >= 2:
                    ui.button("Сводка", icon="summarize", on_click=summarize_selection).props("unelevated dense no-caps").classes("bg-violet-600 text-white")
                def clear_selection() -> None:
                    state.selected_result_paths = []
                    state.selection_summary_text = ""
                    render()
                ui.button("Снять выбор", icon="close", on_click=clear_selection, color=None).props("flat dense no-caps")

        # Чипы-фильтры
        with ui.row().classes("w-full gap-2 flex-wrap"):
            # «Все»
            all_active = state.active_type_filter is None
            all_chip = ui.label(f"Все: {total_doc_count}").classes(
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

        # Group all visible results by source document, then paginate groups
        _all_seen: Dict[str, int] = {}
        _all_groups: List[tuple[Any, List[Any]]] = []
        for _r in visible:
            _key = str(_r.get("cloud_file_id") or _r.get("full_path") or _r.get("path") or id(_r))
            if _key in _all_seen:
                _all_groups[_all_seen[_key]][1].append(_r)
            else:
                _all_seen[_key] = len(_all_groups)
                _all_groups.append((_r, []))

        groups_to_show = _all_groups[: state.displayed_count]
        to_show_flat = [r for grp, extras in groups_to_show for r in [grp, *extras]]

        cloud_result_jobs: Dict[str, Dict[str, str]] = {}
        cloud_file_ids = [
            str(r.get("cloud_file_id") or "")
            for r in to_show_flat
            if str(r.get("cloud_file_id") or "")
        ]
        if cloud_file_ids:
            try:
                svc = _cd_get_service(state.cfg)
                if svc:
                    cloud_result_jobs = _cd_file_jobs_map(svc.registry, list(dict.fromkeys(cloud_file_ids)))
            except Exception:
                cloud_result_jobs = {}

        with ui.column().classes("w-full gap-3"):
            for idx, (primary, extras) in enumerate(groups_to_show, 1):
                render_result(primary, idx, cloud_result_jobs)
                if extras:
                    with ui.expansion(f"{len(extras)} дополн. фрагм.", icon="unfold_more").classes(
                        "w-full border border-slate-200 dark:border-slate-700 rounded-lg -mt-2 mb-1 text-xs text-slate-500"
                    ):
                        with ui.column().classes("w-full gap-3 pt-1"):
                            for extra in extras:
                                render_result(extra, idx, cloud_result_jobs)

        # Кнопка «Загрузить ещё»
        remaining_groups = len(_all_groups) - state.displayed_count
        if remaining_groups > 0:
            def load_more() -> None:
                state.displayed_count += 10
                render()

            ui.button(
                f"Загрузить ещё ({remaining_groups})",
                on_click=load_more,
                icon="expand_more",
            ).props("outline no-caps").classes("w-full mt-1")

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

    # ── Explorer / Cloud Drive screen ─────────────────────────────────────────

    def _render_cd_explorer(page_state: PageState, svc: "CloudDriveService") -> None:  # noqa: PLR0912,PLR0915
        """Registry-backed Cloud Drive explorer screen."""
        from rag_catalog.core.cloud_drive.models import CloudDriveFile, CloudDriveFolder  # noqa: PLC0415

        def _cd_open_folder(cd_path: str) -> None:
            page_state.explorer_cd_path = cd_path
            page_state.explorer_page = 0
            _log_app_event(page_state, "cd_explorer", "open_folder", details={"cd_path": cd_path})
            render()

        async def _cd_upload_dialog() -> None:
            """File-picker dialog that uploads files to the current Cloud Drive folder."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-96"):
                ui.label("Загрузить файлы").classes("text-lg font-semibold")
                parent_label = page_state.explorer_cd_path or "/"
                ui.label(f"В папку: {parent_label}").classes("rag-path text-xs")
                upload_results: list[dict] = []

                async def _handle_upload(e: Any) -> None:
                    filename = str(getattr(e, "name", "") or "").strip()
                    content = getattr(e, "content", None)
                    if not filename or content is None:
                        return
                    import tempfile  # noqa: PLC0415
                    suffix = Path(filename).suffix
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                            tmp.write(content.read())
                            tmp_path = tmp.name
                        result = await run.io_bound(
                            svc.upload_file,
                            parent_path=page_state.explorer_cd_path or "",
                            filename=filename,
                            source_path=tmp_path,
                            mime_type="",
                        )
                        Path(tmp_path).unlink(missing_ok=True)
                        upload_results.append({"name": filename, "ok": True})
                        _log_app_event(
                            page_state, "cd_explorer", "upload_file",
                            details={"parent": page_state.explorer_cd_path, "name": filename},
                        )
                        ui.notify(f"Файл «{filename}» загружен.", type="positive")
                    except Exception as exc:
                        upload_results.append({"name": filename, "ok": False, "err": str(exc)})
                        ui.notify(f"Ошибка загрузки «{filename}»: {exc}", type="negative")

                uploader = ui.upload(
                    multiple=True,
                    on_upload=_handle_upload,
                    auto_upload=True,
                    label="Перетащите файлы сюда или нажмите для выбора",
                ).props("flat bordered").classes("w-full")

                with ui.row().classes("w-full justify-end gap-2 mt-2"):
                    ui.button(
                        "Закрыть", icon="check",
                        on_click=lambda: (dlg.close(), render()),
                    ).props("unelevated dense")
            dlg.open()

        async def _cd_versions_dialog(file: "Any") -> None:
            """Show version history for a Cloud Drive file."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-[480px]"):
                ui.label(f"Версии: {file.name}").classes("text-lg font-semibold")
                ui.label(file.path).classes("rag-path text-xs")
                ui.separator()
                try:
                    result = await run.io_bound(svc.list_versions, file.path)
                    versions = result.get("versions", [])
                    if not versions:
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("history", size="24px").classes("opacity-30")
                            ui.label("История версий пуста.").classes("text-center")
                    else:
                        current_id = str(result.get("file", {}).get("current_version_id") or "")
                        for ver in versions:
                            is_cur = str(ver.get("id", "")) == current_id
                            with ui.row().classes("w-full items-center gap-2 py-1"):
                                ui.icon(
                                    "radio_button_checked" if is_cur else "radio_button_unchecked",
                                    size="16px",
                                ).classes("text-indigo-500" if is_cur else "text-slate-400")
                                with ui.column().classes("flex-1 gap-0"):
                                    ts = str(ver.get("created_at") or "")
                                    label = f"Текущая · {ts[:19].replace('T', ' ')}" if is_cur else ts[:19].replace("T", " ")
                                    ui.label(label).classes("text-sm" + (" font-semibold" if is_cur else " rag-meta"))
                                    size = int(ver.get("size_bytes") or 0)
                                    if size:
                                        ui.label(_cd_file_size(size)).classes("rag-meta text-xs")
                except Exception as exc:
                    ui.label(f"Не удалось загрузить версии: {exc}").classes("text-negative text-sm")
                with ui.row().classes("w-full justify-end mt-2"):
                    ui.button("Закрыть", on_click=dlg.close).props("flat dense")
            dlg.open()

        async def _cd_new_folder_dialog() -> None:
            """Show an inline dialog to create a new folder in the current directory."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-80"):
                ui.label("Новая папка").classes("text-lg font-semibold")
                parent_label = page_state.explorer_cd_path or "/"
                ui.label(f"В: {parent_label}").classes("rag-path text-xs")
                name_input = ui.input(
                    "Имя папки",
                    placeholder="Введите имя",
                ).props("dense outlined autofocus").classes("w-full")

                async def _do_create() -> None:
                    name = str(name_input.value or "").strip()
                    if not name:
                        ui.notify("Введите имя папки.", type="warning")
                        return
                    try:
                        await run.io_bound(
                            svc.create_folder,
                            parent_path=page_state.explorer_cd_path or "",
                            name=name,
                        )
                        dlg.close()
                        _log_app_event(
                            page_state, "cd_explorer", "create_folder",
                            details={"parent": page_state.explorer_cd_path, "name": name},
                        )
                        ui.notify(f"Папка «{name}» создана.", type="positive")
                        render()
                    except Exception as exc:
                        ui.notify(f"Не удалось создать папку: {exc}", type="negative")

                name_input.on("keydown.enter", lambda _: _do_create())
                with ui.row().classes("w-full justify-end gap-2 mt-1"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button("Создать", icon="create_new_folder", on_click=_do_create).props("unelevated dense")
            dlg.open()

        async def _cd_rename_dialog(node_path: str, node_name: str) -> None:
            """Dialog to rename a file or folder in Cloud Drive."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-80"):
                ui.label("Переименовать").classes("text-lg font-semibold")
                ui.label(node_path).classes("rag-path text-xs")
                name_input = ui.input(
                    "Новое имя",
                    value=node_name,
                ).props("dense outlined autofocus").classes("w-full")

                async def _do_rename() -> None:
                    new_name = str(name_input.value or "").strip()
                    if not new_name:
                        ui.notify("Введите новое имя.", type="warning")
                        return
                    if new_name == node_name:
                        dlg.close()
                        return
                    try:
                        parent_path = node_path.rsplit("/", 1)[0] if "/" in node_path else ""
                        await run.io_bound(
                            svc.move_node,
                            source_path=node_path,
                            dest_parent_path=parent_path,
                            new_name=new_name,
                        )
                        dlg.close()
                        _log_app_event(
                            page_state, "cd_explorer", "rename",
                            details={"path": node_path, "new_name": new_name},
                        )
                        ui.notify(f"Переименовано в «{new_name}».", type="positive")
                        render()
                    except Exception as exc:
                        ui.notify(f"Ошибка переименования: {exc}", type="negative")

                name_input.on("keydown.enter", lambda _: _do_rename())
                with ui.row().classes("w-full justify-end gap-2 mt-1"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button("Сохранить", icon="drive_file_rename_outline", on_click=_do_rename).props("unelevated dense")
            dlg.open()

        async def _cd_delete_dialog(node_path: str, node_name: str, is_folder: bool = False) -> None:
            """Confirmation dialog before deleting a file or folder."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-96"):
                ui.label("Удалить?").classes("text-lg font-semibold text-red-700")
                kind = "папку" if is_folder else "файл"
                ui.label(f"{'Папка' if is_folder else 'Файл'}: {node_name}").classes("text-sm font-medium")
                ui.label(node_path).classes("rag-path text-xs")
                if is_folder:
                    ui.label(
                        "Все файлы и вложенные папки будут безвозвратно удалены."
                    ).classes("text-xs text-red-600 mt-1")
                else:
                    ui.label("Файл и все его версии будут удалены безвозвратно.").classes("text-xs text-red-600 mt-1")

                async def _do_delete() -> None:
                    try:
                        await run.io_bound(svc.delete_node, path=node_path)
                        dlg.close()
                        _log_app_event(
                            page_state, "cd_explorer", "delete",
                            details={"path": node_path, "is_folder": is_folder},
                        )
                        ui.notify(f"{'Папка' if is_folder else 'Файл'} «{node_name}» удалён.", type="positive")
                        # If we deleted the current folder, navigate up
                        if is_folder and page_state.explorer_cd_path == node_path:
                            parent = node_path.rsplit("/", 1)[0] if "/" in node_path else ""
                            page_state.explorer_cd_path = parent
                        render()
                    except Exception as exc:
                        ui.notify(f"Ошибка удаления: {exc}", type="negative")

                with ui.row().classes("w-full justify-end gap-2 mt-2"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button(
                        f"Удалить {kind}", icon="delete_forever",
                        on_click=_do_delete,
                        color="negative",
                    ).props("unelevated dense")
            dlg.open()

        async def _cd_move_dialog(node_path: str, node_name: str, is_folder: bool = False) -> None:
            """Dialog to move a file or folder to another folder in Cloud Drive."""
            # Load all available target folders from registry
            try:
                with svc.registry._connect() as _conn:
                    _rows = _conn.execute(
                        "SELECT * FROM cloud_folders ORDER BY path"
                    ).fetchall()
                all_folders = [svc.registry._folder_from_row(r) for r in _rows]
            except Exception:
                all_folders = []

            # Exclude self (and descendants if it's a folder)
            if is_folder:
                candidates = [
                    fo for fo in all_folders
                    if fo.path != node_path and not fo.path.startswith(node_path + "/")
                ]
            else:
                candidates = all_folders

            selected_path: list = [page_state.explorer_cd_path or ""]

            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-96"):
                ui.label("Переместить в папку").classes("text-lg font-semibold")
                ui.label(f"{'Папка' if is_folder else 'Файл'}: {node_name}").classes("text-sm")
                ui.separator()

                if not candidates:
                    ui.label("Нет доступных папок.").classes("rag-meta text-sm")
                else:
                    folder_options = {
                        fo.path: ("Cloud Drive (корень)" if fo.is_root else fo.path)
                        for fo in candidates
                    }
                    sel = ui.select(
                        options=folder_options,
                        value=selected_path[0],
                        label="Целевая папка",
                    ).props("dense outlined emit-value map-options").classes("w-full")
                    sel.on("update:model-value", lambda e: selected_path.__setitem__(0, e.args))

                async def _do_move() -> None:
                    dest = str(selected_path[0] or "").strip()
                    if dest == (node_path.rsplit("/", 1)[0] if "/" in node_path else ""):
                        ui.notify("Файл уже находится в этой папке.", type="info")
                        dlg.close()
                        return
                    try:
                        await run.io_bound(
                            svc.move_node,
                            source_path=node_path,
                            dest_parent_path=dest,
                            new_name=node_name,
                        )
                        dlg.close()
                        _log_app_event(
                            page_state, "cd_explorer", "move",
                            details={"path": node_path, "dest": dest},
                        )
                        ui.notify(f"«{node_name}» перемещён.", type="positive")
                        render()
                    except Exception as exc:
                        ui.notify(f"Ошибка перемещения: {exc}", type="negative")

                with ui.row().classes("w-full justify-end gap-2 mt-2"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button("Переместить", icon="drive_file_move", on_click=_do_move).props("unelevated dense")
            dlg.open()

        def _cd_open_file(file: CloudDriveFile) -> None:
            src = str(file.source_path or file.path or "")
            if src:
                p = Path(src)
                if p.exists() and p.is_file():
                    _log_app_event(page_state, "cd_explorer", "open_file", details={"path": src})
                    open_file_viewer(p)
                    return
            ui.notify("Исходный файл недоступен на диске.", type="warning")

        async def _cd_reindex_file(file_path: str) -> None:
            try:
                job = await run.io_bound(svc.enqueue_reindex, file_path)
                ui.notify(f"Переиндексация запущена (job {str(job.id or '')[:8]})", type="positive")
                _log_app_event(page_state, "cd_explorer", "reindex_file", details={"path": file_path})
            except Exception as exc:
                ui.notify(f"Ошибка: {exc}", type="negative")
            render()

        # ── Layout skeleton ───────────────────────────────────────────────
        with ui.row().classes("rag-explorer-v2-layout w-full gap-3 items-start"):
            tree_col = ui.column().classes("rag-explorer-tree rag-card p-3 gap-2")
            main_col = ui.column().classes("rag-explorer-files rag-card p-3 gap-3")
            details_col = ui.column().classes("rag-explorer-details rag-card p-3 gap-3")

        cd_path = page_state.explorer_cd_path or ""
        _is_trash_view = cd_path == "__trash__"
        if _is_trash_view:
            cd_path = ""  # don't pass __trash__ to backend helpers
        child_folders, child_files = _cd_list_children(svc, cd_path)
        breadcrumbs = _cd_breadcrumb_chain(svc, cd_path)
        root_folder = svc.registry.get_root_folder()

        # ── Sync header breadcrumbs ───────────────────────────────────────
        if page_state.header_breadcrumbs is not None:
            page_state.header_breadcrumbs.clear()
            with page_state.header_breadcrumbs:
                for idx, folder in enumerate(breadcrumbs):
                    label = "Cloud Drive" if folder.is_root else folder.name
                    ui.button(
                        label,
                        on_click=lambda p=folder.path: _cd_open_folder(p),
                        color=None,
                    ).props("flat dense no-caps")
                    if idx < len(breadcrumbs) - 1:
                        ui.icon("chevron_right").classes("text-slate-400")

        # filter & sort
        name_q = page_state.explorer_filter.strip().lower()
        ext_q = page_state.explorer_ext if page_state.explorer_ext != "Все" else ""
        if name_q:
            child_folders = [f for f in child_folders if name_q in f.name.lower()]
            child_files   = [f for f in child_files   if name_q in f.name.lower()]
        if ext_q:
            child_files = [f for f in child_files if f.name.lower().endswith(ext_q.lower())]

        sort_key = page_state.explorer_sort
        rev = page_state.explorer_desc
        if sort_key == "По имени":
            child_folders.sort(key=lambda x: x.name.lower(), reverse=rev)
            child_files.sort(key=lambda x: x.name.lower(), reverse=rev)
        elif sort_key == "По размеру":
            child_files.sort(key=lambda x: x.size_bytes, reverse=rev)
        elif sort_key == "По дате":
            child_files.sort(key=lambda x: x.updated_at, reverse=rev)

        # pagination of files
        total_files = len(child_files)
        page_size = PAGE_SIZE
        page_state.explorer_page = max(0, min(page_state.explorer_page, max(0, (total_files - 1) // page_size)))
        page_files = child_files[page_state.explorer_page * page_size : (page_state.explorer_page + 1) * page_size]

        # Per-file job status map (single DB query for the current page)
        _page_file_ids = [f.id for f in page_files]
        _file_jobs = _cd_file_jobs_map(svc.registry, _page_file_ids)

        _JOB_ICON = {"pending": "hourglass_empty", "running": "sync", "completed": "check_circle", "failed": "error_outline"}
        _JOB_CSS  = {"pending": "cd-status-pending", "running": "cd-status-running", "completed": "cd-status-done", "failed": "cd-status-error"}
        _JOB_TYPE_LABEL = {
            "reindex": {"pending": "В очереди", "running": "Индексируется", "failed": "Ошибка индексации"},
            "cleanup": {"pending": "Очистка", "running": "Очищается", "failed": "Ошибка очистки"},
            "ocr": {"pending": "OCR ожидает", "running": "OCR…", "failed": "Ошибка OCR"},
            "preview": {"pending": "Preview ожидает", "running": "Preview…", "failed": "Ошибка Preview"},
        }
        _JOB_TYPE_TIP = {
            "reindex": {"pending": "Файл ожидает индексации", "running": "Индексируется…", "completed": "Проиндексировано", "failed": "Ошибка индексации"},
            "cleanup": {"pending": "Очистка в очереди", "running": "Удаление из индекса…", "completed": "Очищено", "failed": "Ошибка очистки"},
            "ocr": {"pending": "OCR в очереди", "running": "Распознавание текста…", "completed": "OCR завершён", "failed": "Ошибка OCR"},
            "preview": {"pending": "Preview в очереди", "running": "Создание preview…", "completed": "Preview готов", "failed": "Ошибка preview"},
        }

        def _render_file_status(file_id: str) -> None:
            job = _file_jobs.get(file_id)
            if not job:
                return
            status = job.get("status", "")
            job_type = job.get("job_type", "reindex")
            if status not in _JOB_ICON or status == "completed":
                return
            type_labels = _JOB_TYPE_LABEL.get(job_type, _JOB_TYPE_LABEL["reindex"])
            type_tips = _JOB_TYPE_TIP.get(job_type, _JOB_TYPE_TIP["reindex"])
            label_text = type_labels.get(status, "")
            tip = type_tips.get(status, label_text)
            if status == "failed" and job.get("last_error"):
                tip = f"Ошибка: {job['last_error'][:120]}"
            with ui.element("span").classes(f"cd-status-badge {_JOB_CSS[status]}"):
                ui.icon(_JOB_ICON[status], size="14px")
                if label_text:
                    ui.label(label_text)
                ui.tooltip(tip)

        # ── Tree column ───────────────────────────────────────────────────
        with tree_col:
            ui.label("ДЕРЕВО").classes("rag-section-label")
            if root_folder is None:
                with ui.element("div").classes("cd-empty-state"):
                    ui.icon("cloud_off", size="24px").classes("opacity-30")
                    ui.label("Реестр пуст. Запустите импорт в настройках Cloud Drive.").classes("text-center text-xs")
            else:
                def _render_tree_node_cd(folder: CloudDriveFolder, depth: int) -> None:
                    is_current = folder.path == cd_path or (not cd_path and folder.is_root)
                    icon = "folder_open" if is_current else "folder"
                    label = "Корень" if folder.is_root else folder.name
                    btn = ui.button(
                        label, icon=icon,
                        on_click=lambda p=folder.path: _cd_open_folder(p),
                        color=None,
                    ).props("flat align=left no-caps dense").classes(
                        "rag-nav-button rag-tree-button w-full" + (" active" if is_current else "")
                    ).style(f"padding-left: {depth * 12}px")
                    btn.tooltip(folder.path)
                    if is_current or (not cd_path and folder.is_root):
                        for child in svc.registry.list_child_folders(folder.id):
                            _render_tree_node_cd(child, depth + 1)

                _render_tree_node_cd(root_folder, 0)

            # Корзина (scaffold — soft delete pending backend)
            ui.separator().classes("my-1")
            _is_trash = page_state.explorer_cd_path == "__trash__"
            trash_btn = ui.button(
                "Корзина", icon="delete_outline",
                on_click=lambda: _cd_open_folder("__trash__"),
                color=None,
            ).props("flat align=left no-caps dense").classes(
                "rag-nav-button rag-tree-button w-full" + (" active" if _is_trash else "")
            )
            trash_btn.tooltip("Удалённые файлы (функция в разработке)")

        # ── Details column ────────────────────────────────────────────────
        with details_col:
            ui.label("Свойства").classes("font-semibold text-sm")
            if breadcrumbs:
                current_node = breadcrumbs[-1]
                ui.label(current_node.name or "Корень").classes("font-semibold truncate")
                ui.label(current_node.path or "/").classes("rag-path text-xs")
            else:
                ui.label("Корень").classes("font-semibold")
            total_size = sum(f.size_bytes for f in child_files)
            ui.label(f"Папок: {len(child_folders)}").classes("rag-meta text-xs")
            ui.label(f"Файлов: {total_files}").classes("rag-meta text-xs")
            if total_size:
                ui.label(f"Размер: {_cd_file_size(total_size)}").classes("rag-meta text-xs")
            ui.separator()
            ui.label("Действия").classes("font-semibold text-sm")
            ui.button(
                "Новая папка", icon="create_new_folder",
                on_click=_cd_new_folder_dialog, color=None,
            ).props("flat dense no-caps align=left").classes("w-full")
            ui.button(
                "Загрузить файлы", icon="upload_file",
                on_click=_cd_upload_dialog, color=None,
            ).props("flat dense no-caps align=left").classes("w-full")
            if cd_path:
                ui.button(
                    "Найти в этой папке", icon="search",
                    on_click=choose_query_handler(f"path:{cd_path}"), color=None,
                ).props("flat dense no-caps align=left").classes("w-full")
            ui.separator()
            ui.label("Фильтры").classes("font-semibold text-sm")
            with ui.column().classes("w-full gap-1"):
                ui.label(f"Тип: {page_state.explorer_ext}").classes(
                    "rag-chip rag-filter-chip" + (" active" if page_state.explorer_ext != "Все" else "")
                )
                ui.label(f"Вид: {page_state.explorer_view}").classes("rag-chip rag-filter-chip")
                ui.label(f"Сорт.: {page_state.explorer_sort}").classes(
                    "rag-chip rag-filter-chip" + (" active" if page_state.explorer_sort != "По имени" else "")
                )

        # ── Main column ───────────────────────────────────────────────────
        with main_col:
            if _is_trash_view:
                with ui.column().classes("w-full items-center justify-center py-12 gap-4"):
                    ui.icon("delete_outline", size="56px").classes("text-slate-300 dark:text-slate-600")
                    ui.label("Корзина").classes("text-xl font-semibold text-slate-500")
                    ui.label("Soft delete ещё не реализован в backend.").classes("rag-meta text-sm")
                    ui.label("Когда функция будет готова, здесь появятся удалённые файлы с возможностью восстановления.").classes(
                        "rag-meta text-xs text-center max-w-sm"
                    )
                    ui.button("В корневую папку", icon="arrow_back", on_click=lambda: _cd_open_folder("")).props("flat no-caps")
                return

            # Breadcrumbs toolbar
            with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
                parent_path = breadcrumbs[-2].path if len(breadcrumbs) >= 2 else ""
                up_btn = ui.button(
                    icon="arrow_upward",
                    on_click=lambda: _cd_open_folder(parent_path),
                    color=None,
                ).props("flat round dense")
                if not cd_path or root_folder is None or (root_folder and cd_path == root_folder.path):
                    up_btn.disable()
                with ui.row().classes("rag-breadcrumbs flex-1 min-w-0 items-center gap-1 no-wrap"):
                    for idx, folder in enumerate(breadcrumbs):
                        label = "Корень" if folder.is_root else folder.name
                        ui.button(
                            label,
                            on_click=lambda p=folder.path: _cd_open_folder(p),
                            color=None,
                        ).props("flat dense no-caps").tooltip(folder.path)
                        if idx < len(breadcrumbs) - 1:
                            ui.icon("chevron_right").classes("text-slate-400")
                ui.button(icon="refresh", on_click=lambda: render(), color=None).props("flat round dense").tooltip("Обновить")
                ui.button(
                    icon="create_new_folder",
                    on_click=_cd_new_folder_dialog,
                    color=None,
                ).props("flat round dense").tooltip("Создать папку")
                ui.button(
                    icon="upload_file",
                    on_click=_cd_upload_dialog,
                    color=None,
                ).props("flat round dense").tooltip("Загрузить файлы")

            # Filter / view toolbar
            with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
                fi = ui.input(
                    placeholder="Фильтр по имени",
                    value=page_state.explorer_filter,
                ).props("dense outlined clearable debounce=0").classes("min-w-48 flex-1")

                def _apply_cd_filter(event: Any = None) -> None:
                    _apply_explorer_filter_input(page_state, event, fi.value)
                    render()

                fi.on_value_change(_apply_cd_filter)

                ui.select(
                    ["Все", ".docx", ".xlsx", ".xls", ".pdf"],
                    value=page_state.explorer_ext,
                    on_change=lambda e: (setattr(page_state, "explorer_ext", e.value), setattr(page_state, "explorer_page", 0), render()),
                ).props("dense outlined").classes("w-36")
                ui.select(
                    ["Таблица", "Список"],
                    value=page_state.explorer_view if page_state.explorer_view in ("Таблица", "Список") else "Таблица",
                    on_change=lambda e: (setattr(page_state, "explorer_view", e.value), render()),
                ).props("dense outlined").classes("w-36")
                ui.select(
                    ["По имени", "По размеру", "По дате"],
                    value=page_state.explorer_sort,
                    on_change=lambda e: (setattr(page_state, "explorer_sort", e.value), render()),
                ).props("dense outlined").classes("w-40")
                ui.select(
                    ["По возрастанию", "По убыванию"],
                    value="По убыванию" if page_state.explorer_desc else "По возрастанию",
                    on_change=lambda e: (setattr(page_state, "explorer_desc", e.value == "По убыванию"), render()),
                ).props("dense outlined").classes("w-44")

            # Entry stats bar
            with ui.row().classes("w-full items-center gap-2 px-1"):
                ui.label(f"Папок: {len(child_folders)} · Файлов: {total_files}").classes("rag-path flex-1")
                with ui.element("span").classes("cd-status-badge cd-status-done text-xs"):
                    ui.icon("cloud_done", size="14px")
                    ui.label("Cloud Drive")

            # Empty state
            if root_folder is None:
                with ui.element("div").classes("cd-empty-state w-full"):
                    ui.icon("cloud_off", size="32px").classes("opacity-20")
                    ui.label("Реестр пуст — запустите импорт в Настройки → Cloud Drive.").classes("text-center")
            elif not child_folders and not child_files:
                with ui.element("div").classes("cd-empty-state w-full"):
                    ui.icon("folder_open", size="32px").classes("opacity-20")
                    ui.label("Папка пуста или элементы не соответствуют фильтру.").classes("text-center")
                    ui.button(
                        "Загрузить файлы", icon="upload_file",
                        on_click=_cd_upload_dialog,
                        color=None,
                    ).props("outline dense").classes("mt-2")
            else:
                # Folders first
                if child_folders:
                    if page_state.explorer_view == "Список":
                        with ui.column().classes("rag-explorer-list w-full"):
                            for folder in child_folders:
                                with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                                    ui.icon("folder", size="24px").classes("text-yellow-500")
                                    with ui.column().classes("flex-1 gap-0"):
                                        ui.button(
                                            folder.name,
                                            on_click=lambda p=folder.path: _cd_open_folder(p),
                                            color=None,
                                        ).props("flat align=left no-caps dense").classes("rag-nav-button w-full")
                                    render_star(Path(folder.source_path or folder.path), item_type="folder")
                                    if not folder.is_root:
                                        with ui.button(icon="more_vert", color=None).props("flat round dense") as _menu_btn:
                                            with ui.menu():
                                                ui.menu_item(
                                                    "Переименовать",
                                                    on_click=lambda fo=folder: _cd_rename_dialog(fo.path, fo.name),
                                                    auto_close=True,
                                                )
                                                ui.menu_item(
                                                    "Переместить в…",
                                                    on_click=lambda fo=folder: _cd_move_dialog(fo.path, fo.name, is_folder=True),
                                                    auto_close=True,
                                                )
                                                ui.separator()
                                                ui.menu_item(
                                                    "Удалить папку…",
                                                    on_click=lambda fo=folder: _cd_delete_dialog(fo.path, fo.name, is_folder=True),
                                                    auto_close=True,
                                                ).classes("text-negative")
                    else:
                        with ui.column().classes("w-full gap-1"):
                            for folder in child_folders:
                                with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                                    ui.icon("folder", size="24px").classes("text-yellow-500")
                                    with ui.column().classes("flex-1 gap-0"):
                                        ui.button(
                                            folder.name,
                                            on_click=lambda p=folder.path: _cd_open_folder(p),
                                            color=None,
                                        ).props("flat align=left no-caps dense").classes("rag-nav-button w-full")
                                        ui.label(f"Папка · {folder.path}").classes("rag-meta text-xs truncate")
                                    render_star(Path(folder.source_path or folder.path), item_type="folder")
                                    if not folder.is_root:
                                        with ui.button(icon="more_vert", color=None).props("flat round dense"):
                                            with ui.menu():
                                                ui.menu_item(
                                                    "Переименовать",
                                                    on_click=lambda fo=folder: _cd_rename_dialog(fo.path, fo.name),
                                                    auto_close=True,
                                                )
                                                ui.menu_item(
                                                    "Переместить в…",
                                                    on_click=lambda fo=folder: _cd_move_dialog(fo.path, fo.name, is_folder=True),
                                                    auto_close=True,
                                                )
                                                ui.separator()
                                                ui.menu_item(
                                                    "Удалить папку…",
                                                    on_click=lambda fo=folder: _cd_delete_dialog(fo.path, fo.name, is_folder=True),
                                                    auto_close=True,
                                                ).classes("text-negative")

                # Files
                if page_files:
                    def _cd_download_url(file_path: str) -> str:
                        return f"/api/cloud-drive/download?path={quote(file_path, safe='')}"

                    if page_state.explorer_view == "Список":
                        with ui.column().classes("rag-explorer-list w-full"):
                            for f in page_files:
                                with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                                    ui.html(_file_icon_svg(f.name, "Файл"), sanitize=False)
                                    with ui.column().classes("flex-1 gap-0"):
                                        ui.button(
                                            f.name,
                                            on_click=lambda fi=f: _cd_open_file(fi),
                                            color=None,
                                        ).props("flat align=left no-caps dense").classes("rag-nav-button w-full")
                                    ui.button(
                                        icon="history",
                                        on_click=lambda fi=f: _cd_versions_dialog(fi),
                                        color=None,
                                    ).props("flat round dense").tooltip("История версий")
                                    if f.storage_key:
                                        ui.button(
                                            icon="download",
                                            on_click=lambda url=_cd_download_url(f.path): ui.navigate.to(url, new_tab=True),
                                            color=None,
                                        ).props("flat round dense").tooltip("Скачать файл")
                                    _render_file_status(f.id)
                                    with ui.button(icon="more_vert", color=None).props("flat round dense"):
                                        with ui.menu():
                                            ui.menu_item(
                                                "Переименовать",
                                                on_click=lambda fi=f: _cd_rename_dialog(fi.path, fi.name),
                                                auto_close=True,
                                            )
                                            ui.menu_item(
                                                "Переместить в…",
                                                on_click=lambda fi=f: _cd_move_dialog(fi.path, fi.name, is_folder=False),
                                                auto_close=True,
                                            )
                                            if _is_admin(state):
                                                ui.separator()
                                                ui.menu_item(
                                                    "Переиндексировать",
                                                    on_click=lambda fi=f: _cd_reindex_file(fi.path),
                                                    auto_close=True,
                                                )
                                            ui.separator()
                                            ui.menu_item(
                                                "Удалить файл…",
                                                on_click=lambda fi=f: _cd_delete_dialog(fi.path, fi.name, is_folder=False),
                                                auto_close=True,
                                            ).classes("text-negative")
                    else:
                        with ui.column().classes("w-full gap-1"):
                            for f in page_files:
                                ext = Path(f.name).suffix or "без расширения"
                                with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                                    ui.html(_file_icon_svg(f.name, "Файл"), sanitize=False)
                                    with ui.column().classes("flex-1 gap-0"):
                                        ui.button(
                                            f.name,
                                            on_click=lambda fi=f: _cd_open_file(fi),
                                            color=None,
                                        ).props("flat align=left no-caps dense").classes("rag-nav-button w-full")
                                        ui.label(
                                            f"{ext} · {_cd_file_size(f.size_bytes)} · {f.updated_at[:10] if f.updated_at else ''}".strip(" ·")
                                        ).classes("rag-meta text-xs")
                                    src = str(f.source_path or f.path or "")
                                    if src:
                                        ui.button(
                                            icon="open_in_new",
                                            on_click=lambda p=src: _select_in_os_explorer(p),
                                            color=None,
                                        ).props("flat round dense").tooltip("Выделить файл в Проводнике Windows")
                                    ui.button(
                                        icon="history",
                                        on_click=lambda fi=f: _cd_versions_dialog(fi),
                                        color=None,
                                    ).props("flat round dense").tooltip("История версий")
                                    if f.storage_key:
                                        ui.button(
                                            icon="download",
                                            on_click=lambda url=_cd_download_url(f.path): ui.navigate.to(url, new_tab=True),
                                            color=None,
                                        ).props("flat round dense").tooltip("Скачать файл")
                                    render_star(Path(f.source_path or f.path or f.name), item_type="file")
                                    _render_file_status(f.id)
                                    with ui.button(icon="more_vert", color=None).props("flat round dense"):
                                        with ui.menu():
                                            ui.menu_item(
                                                "Переименовать",
                                                on_click=lambda fi=f: _cd_rename_dialog(fi.path, fi.name),
                                                auto_close=True,
                                            )
                                            ui.menu_item(
                                                "Переместить в…",
                                                on_click=lambda fi=f: _cd_move_dialog(fi.path, fi.name, is_folder=False),
                                                auto_close=True,
                                            )
                                            if _is_admin(state):
                                                ui.separator()
                                                ui.menu_item(
                                                    "Переиндексировать",
                                                    on_click=lambda fi=f: _cd_reindex_file(fi.path),
                                                    auto_close=True,
                                                )
                                            ui.separator()
                                            ui.menu_item(
                                                "Удалить файл…",
                                                on_click=lambda fi=f: _cd_delete_dialog(fi.path, fi.name, is_folder=False),
                                                auto_close=True,
                                            ).classes("text-negative")

                # Pagination
                if total_files > page_size:
                    with ui.row().classes("items-center gap-2 mt-2"):
                        ui.button("Назад", on_click=lambda: (setattr(page_state, "explorer_page", max(0, page_state.explorer_page - 1)), render())).props("outline")
                        ui.label(f"Стр. {page_state.explorer_page + 1} / {(total_files + page_size - 1) // page_size}").classes("rag-meta")
                        ui.button("Вперёд", on_click=lambda: (setattr(page_state, "explorer_page", page_state.explorer_page + 1), render())).props("outline")

                # ── Drop zone ─────────────────────────────────────────────────
                with ui.element("div").classes("w-full mt-3"):
                    async def _handle_drop_upload(e: "Any") -> None:
                        filename = str(getattr(e, "name", "") or "").strip()
                        content = getattr(e, "content", None)
                        if not filename or content is None:
                            return
                        import tempfile as _tempfile
                        _suffix = Path(filename).suffix
                        with _tempfile.NamedTemporaryFile(delete=False, suffix=_suffix) as _tmp:
                            _tmp.write(content.read())
                            _tmp_path = _tmp.name
                        try:
                            await run.io_bound(
                                svc.upload_file,
                                parent_path=page_state.explorer_cd_path or "",
                                filename=filename,
                                source_path=_tmp_path,
                                mime_type="",
                            )
                            ui.notify(f"Загружен: «{filename}»", type="positive")
                            render()
                        except Exception as _exc:
                            ui.notify(f"Ошибка загрузки «{filename}»: {_exc}", type="negative")
                        finally:
                            Path(_tmp_path).unlink(missing_ok=True)

                    ui.upload(
                        multiple=True,
                        on_upload=_handle_drop_upload,
                        auto_upload=True,
                        label="Перетащите файлы сюда для загрузки",
                    ).props("flat bordered").classes("w-full cd-drop-zone")

    def render_explorer_screen() -> None:  # noqa: PLR0912,PLR0915
        # ── Cloud Drive registry mode ─────────────────────────────────────
        _cd_svc = _cd_get_service(state.cfg)
        if _cd_svc is not None:
            _render_cd_explorer(state, _cd_svc)
            return

        # ── Legacy os-walk mode ───────────────────────────────────────────
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
                _os_fn_tile = (lambda p=path: _open_os_path(str(p))) if is_dir else (lambda p=path: _select_in_os_explorer(str(p)))
                os_button = ui.button(on_click=_os_fn_tile).props("data-rag-os")
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
                    _os_fn = (lambda p=path: _open_os_path(str(p))) if is_dir else (lambda p=path: _select_in_os_explorer(str(p)))
                    ui.button("ОС", icon="open_in_new", on_click=_os_fn).props("flat dense data-rag-os").tooltip("Открыть в проводнике Windows" if is_dir else "Выделить файл в проводнике Windows")
                else:
                    _os_fn2 = (lambda p=path: _open_os_path(str(p))) if is_dir else (lambda p=path: _select_in_os_explorer(str(p)))
                    os_button = ui.button(on_click=_os_fn2).props("data-rag-os")
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
                with ui.row().classes("w-full gap-1 mt-1"):
                    ui.button(icon="content_copy", on_click=lambda p=current_details: copy_path(p), color=None).props("flat round dense").tooltip("Скопировать путь")
                    ui.button(icon="open_in_new", on_click=lambda p=current_details: _open_os_path(str(p)), color=None).props("flat round dense").tooltip("Открыть в Проводнике Windows")

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
            _tree_filter = [""]
            tree_filter_input = ui.input(placeholder="Фильтр по дереву").props("dense outlined clearable").classes("w-full")
            tree_content = ui.column().classes("w-full gap-0")

            def _find_matching_folders(needle: str, max_results: int = 20) -> List[Path]:
                results: List[Path] = []
                queue = [(root, 0)]
                while queue and len(results) < max_results:
                    cur, depth = queue.pop(0)
                    if depth > 6:
                        continue
                    try:
                        children = sorted(
                            [p for p in cur.iterdir() if p.is_dir() and not p.name.startswith(".")],
                            key=lambda p: p.name.lower(),
                        )
                    except Exception:
                        children = []
                    for child in children:
                        if needle.lower() in child.name.lower():
                            results.append(child)
                        queue.append((child, depth + 1))
                return results

            def _render_tree_content(needle: str = "") -> None:
                tree_content.clear()
                with tree_content:
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
                    if needle:
                        matches = _find_matching_folders(needle)
                        if matches:
                            for m in matches:
                                ui.button(
                                    m.name,
                                    icon="folder",
                                    on_click=lambda p=m: open_folder(p),
                                    color=None,
                                ).props("flat align=left no-caps dense").classes("rag-nav-button rag-tree-button w-full").tooltip(str(m))
                        else:
                            ui.label("Совпадений нет").classes("rag-meta text-xs px-2")
                    else:
                        current_tree_path = _safe_explorer_path(state)
                        current_ancestors = {str(part) for part in _explorer_path_parts(root, current_tree_path)}
                        render_tree_node(root, 0, current_tree_path, current_ancestors)

            def _on_tree_filter_change(e: events.ValueChangeEventArguments) -> None:
                _tree_filter[0] = str(e.value or "").strip()
                _render_tree_content(_tree_filter[0])

            tree_filter_input.on_value_change(_on_tree_filter_change)
            _render_tree_content()

        render_explorer_details()

        with toolbar:
            current_for_toolbar = _safe_explorer_path(state)
            with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
                up_btn = ui.button(icon="arrow_upward", on_click=lambda: open_folder(current_for_toolbar.parent), color=None).props("flat round dense").tooltip("На уровень вверх")
                if current_for_toolbar == root:
                    up_btn.disable()
                render_breadcrumbs(root, current_for_toolbar)
                ui.button(icon="refresh", on_click=lambda: render(), color=None).props("flat round dense").tooltip("Обновить")
                render_star(current_for_toolbar, item_type="folder")
            with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
                ui.icon("search").classes("text-lg")
                _folder_search_input = ui.input(placeholder="Семантический поиск только в этой папке").props("borderless dense").classes("flex-1")

                async def _run_folder_search(_: events.GenericEventArguments | None = None) -> None:
                    q = str(_folder_search_input.value or "").strip()
                    if q:
                        await choose_query(f"{q} path:{current_for_toolbar}")

                _folder_search_input.on("keyup.enter", _run_folder_search)
                ui.button(icon="search", on_click=_run_folder_search, color=None).props("flat round dense")
                _folder_ai_cb = ui.checkbox("AI", value=bool(state.ai_search_expand)).props("dense").classes("rag-ai-expand")
                _folder_ai_cb.tooltip("AI-дополнение запроса")
                if not bool(state.cfg.get("llm_enabled")):
                    _folder_ai_cb.disable()
                _folder_ai_cb.on_value_change(lambda e: (setattr(state, "ai_search_expand", bool(e.value)), _save_ui_settings(state)))
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

    # ── Index / indexing management screen ────────────────────────────────────


    def render_index_screen() -> None:
        _index_view.render_index_screen(
            state,
            render_fn=render,
            access_denied=render_access_denied,
        )

    def render_index_dashboard() -> None:
        _index_view.render_index_dashboard(state)


    def render_access_denied(
        message: str = "Этот раздел доступен только администраторам.",
        *,
        icon: str = "lock",
        hint: str = "",
    ) -> None:
        with ui.column().classes("w-full items-center justify-center py-16 gap-4"):
            ui.icon(icon, size="48px").classes("text-slate-300 dark:text-slate-600")
            ui.label(message).classes("text-lg font-semibold text-slate-500")
            if hint:
                ui.label(hint).classes("rag-meta text-sm text-center max-w-md")
            ui.button("На главную", icon="home", on_click=lambda: set_screen("search")).props("flat")

    def render_force_change_password_screen() -> None:
        auth_db = _get_auth_db(state)
        user = state.current_user or {}
        username = str(user.get("username") or "")
        with ui.column().classes("w-full min-h-[70vh] items-center justify-center"):
            with ui.column().classes("rag-card w-full max-w-xl p-5 gap-4"):
                with ui.row().classes("items-center gap-3"):
                    ui.icon("lock_reset").classes("text-3xl text-warning")
                    ui.label("Смена пароля обязательна").classes("text-2xl font-semibold")
                ui.label(
                    "Администратор установил требование смены пароля. "
                    "Введите текущий временный пароль и задайте новый пароль для продолжения."
                ).classes("rag-meta")
                ui.separator()
                old_pw = ui.input("Текущий пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                new_pw = ui.input("Новый пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                new_pw2 = ui.input("Повторите новый пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                _focus_next_js = (
                    "const ins=document.querySelectorAll('.q-field__native,input[type=password]');"
                    "const i=Array.from(ins).findIndex(el=>el===document.activeElement);"
                    "if(i>=0&&ins[i+1])ins[i+1].focus();"
                )
                old_pw.on("keyup.enter", lambda _: ui.run_javascript(_focus_next_js))
                new_pw.on("keyup.enter", lambda _: ui.run_javascript(_focus_next_js))

                def force_change() -> None:
                    new_password = str(new_pw.value or "")
                    if str(new_pw2.value or "") != new_password:
                        ui.notify("Пароли не совпадают.", type="warning")
                        return
                    if len(new_password) < 6:
                        ui.notify("Пароль должен быть не менее 6 символов.", type="warning")
                        return
                    ok = auth_db.change_password(
                        username=username,
                        old_password=str(old_pw.value or ""),
                        new_password=new_password,
                    )
                    if ok:
                        _refresh_current_user(state)
                        auth_db.log_auth_event(username=username, event_type="password_changed_forced", ok=True)
                        ui.notify("Пароль успешно изменён.", type="positive")
                        render()
                    else:
                        ui.notify("Не удалось изменить пароль. Проверьте текущий пароль.", type="negative")

                new_pw2.on("keyup.enter", lambda _: force_change())
                with ui.row().classes("gap-2"):
                    ui.button("Сменить пароль", icon="key", on_click=force_change).props("unelevated")
                    ui.button("Выйти", icon="logout", on_click=do_logout).props("flat")

    def render_login_screen() -> None:
        auth_db = _get_auth_db(state)
        with ui.column().classes("w-full min-h-[70vh] items-center justify-center"):
            with ui.column().classes("rag-card w-full max-w-xl p-5 gap-3"):
                if state.session_expired:
                    with ui.row().classes("items-center gap-2 bg-orange-50 border border-orange-200 rounded p-3 w-full"):
                        ui.icon("schedule").classes("text-orange-500")
                        ui.label("Сессия истекла — выполните вход снова.").classes("text-orange-700 text-sm")
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
                    result = auth_db.login_with_reason(username=username, password=str(password_input.value or ""))
                    reason = str(result.get("reason") or "")
                    user = result.get("user")
                    if reason == "pending":
                        auth_db.log_auth_event(username=username, event_type="login_failed", ok=False, error="pending")
                        ui.notify("Ваша заявка ещё не активирована администратором.", type="warning", timeout=6000)
                        return
                    if reason == "blocked":
                        auth_db.log_auth_event(username=username, event_type="login_failed", ok=False, error="blocked")
                        ui.notify("Аккаунт заблокирован. Обратитесь к администратору.", type="negative")
                        return
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
                        username_input.on("keyup.enter", lambda _: ui.run_javascript(
                            "const ins=document.querySelectorAll('.q-field__native,input[type=password]');"
                            "const i=Array.from(ins).findIndex(el=>el===document.activeElement);"
                            "if(i>=0&&ins[i+1])ins[i+1].focus();"
                        ))
                        password_input.on("keyup.enter", lambda _: login())
                        ui.button("Войти", icon="login", on_click=login).props("unelevated")
                        ui.separator()
                        ui.button("Войти через Telegram", icon="send", on_click=request_tg_login).props("outline").classes("w-full")
                        ui.label("Стандартный сценарий: как у OAuth-входа — нажали кнопку, подтвердили в Telegram, вернулись в приложение.").classes("rag-meta")

                    with ui.tab_panel(tab_register).classes("w-full gap-3"):
                        reg_username_input = ui.input("Логин").props("dense outlined").classes("w-full")
                        reg_display_input = ui.input("Имя").props("dense outlined").classes("w-full")
                        reg_tg_user_input = ui.input("Telegram username (необязательно)").props("dense outlined").classes("w-full")
                        reg_tg_user_input.on("keyup.enter", lambda _: register_request())
                        ui.button("Отправить заявку", icon="how_to_reg", on_click=register_request).props("unelevated")
                        ui.label("После одобрения администратором вы получите доступ к аккаунту.").classes("rag-meta")

                _stop_managed_timer(state.tg_login_timer)
                state.tg_login_timer = ui.timer(2.0, poll_tg_login)


    # ── Admin / settings screens ──────────────────────────────────────────────

    def render_settings_screen() -> None:
        _settings_view.render_settings_screen(
            state,
            render_fn=render,
            query_handler=choose_query_handler,
        )

    # ── Analytics / stats screen ───────────────────────────────────────────

    def render_stats_screen() -> None:
        _stats_view.render_stats_screen(
            state,
            access_denied=render_access_denied,
            query_handler=choose_query_handler,
        )

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
        if state.screen != "index":
            _stop_managed_timer(state.index_progress_timer)
            state.index_progress_timer = None
            _stop_managed_timer(state.stage_status_timer)
            state.stage_status_timer = None
        if not (state.auth_token and state.current_user):
            _stop_managed_timer(state.activity_timer)
            state.activity_timer = None
        if not _is_admin(state):
            _stop_managed_timer(state.scheduler_timer)
            state.scheduler_timer = None
            _stop_managed_timer(state.cloud_drive_timer)
            state.cloud_drive_timer = None
        if state.screen != "settings" or not _is_admin(state):
            _stop_managed_timer(state.cloud_drive_timer)
            state.cloud_drive_timer = None
        if state.current_user is not None or state.screen != "search":
            _stop_managed_timer(state.tg_login_timer)
            state.tg_login_timer = None
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
            if int((state.current_user or {}).get("must_change_password") or 0):
                try:
                    drawer.set_visibility(False)
                except Exception:
                    pass
                try:
                    menu_button.set_visibility(False)
                except Exception:
                    pass
                render_force_change_password_screen()
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

    def _setup_keyboard_shortcuts() -> None:
        ui.run_javascript(
            "if(!window._ragKbInit){"
            "window._ragKbInit=true;"
            "document.addEventListener('keydown',function(e){"
            "if((e.ctrlKey||e.metaKey)&&e.key==='k'){"
            "e.preventDefault();"
            "const inp=document.querySelector('.rag-search-box .q-field__native');"
            "if(inp){inp.focus();inp.select();}"
            "}"
            "});"
            "}"
        )

    ui.timer(0.0, _setup_keyboard_shortcuts, once=True)


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
    _recover_cloud_drive_jobs(cfg)
    _start_recovery_watchdog(cfg)
    _start_global_scheduler(cfg)
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
