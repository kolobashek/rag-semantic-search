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
                f"Загрузить ещё  ({remaining_groups})",
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
                def _search_this_folder(p: str = cd_path) -> None:
                    state.query = f"path:{p}"
                    state.screen = "search"
                    render()
                ui.button(
                    "Найти в этой папке", icon="search",
                    on_click=_search_this_folder, color=None,
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

    # ── Index / indexing management screen ────────────────────────────────────

    def render_index_screen() -> None:  # noqa: C901
        if not _is_admin(state):
            render_access_denied(hint="Управление индексом и расписание индексации доступны только администраторам.")
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
            ui.label(active_label).classes("rag-chip")
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

        _stop_managed_timer(state.stage_status_timer)
        state.stage_status_timer = ui.timer(1.5, lambda: _refresh_stage_status_log())

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
                active_chip = ui.label(active_label).classes("rag-chip")
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
                pct_label = f"{pct * 100:.0f}%"
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
                with ui.element("div").classes(f"rag-pipeline-row rag-pipeline-row-card {status_cls}"):
                    with ui.row().classes("items-center gap-2 min-w-0"):
                        ui.icon(status_icon).classes(f"rag-phase-status {status_cls}")
                        with ui.column().classes("gap-0 min-w-0"):
                            ui.label(label).classes("font-semibold truncate")
                            ui.label(_format_relative_time(last_ts)).classes("rag-meta")
                    with ui.column().classes("rag-progress-stack min-w-0"):
                        with ui.row().classes("rag-progress-topline w-full items-center gap-2"):
                            ui.button(status_title, on_click=lambda r=row_for_dialog: show_stage_status_details(r), color=None).props("flat dense no-caps").classes(f"rag-chip rag-status-chip {status_cls}")
                            ui.label(f"{processed:,} / {total_f:,}".replace(",", " ")).classes("rag-meta")
                            ui.space()
                            ui.label(pct_label).classes("rag-meta")
                            ui.label(_format_duration_seconds(duration_value)).classes("rag-meta")
                        ui.linear_progress(value=pct).props("color=indigo-5" if is_running else "").classes("w-full rag-progressbar")
                    ui.label(stats_text).classes("rag-meta")
                    with ui.row().classes("rag-pipeline-actions"):
                        if is_running:
                            if is_ocr:
                                ui.button(icon="restart_alt", on_click=run_ocr_now).props("flat dense round").tooltip("Рестарт")
                            else:
                                ui.button(icon="restart_alt", on_click=make_run_handler(key)).props("flat dense round").tooltip("Рестарт")
                            ui.button(icon="pause", on_click=lambda l=label: pause_phase(l)).props("flat dense round").tooltip("Пауза")
                            ui.button(icon="stop", on_click=lambda l=label: stop_phase(l)).props("flat dense round").tooltip("Остановить")
                        else:
                            if is_ocr:
                                ui.button(icon="play_arrow", on_click=run_ocr_now).props("flat dense round").tooltip("Запустить")
                            else:
                                ui.button(icon="play_arrow", on_click=make_run_handler(key)).props("flat dense round").tooltip("Запустить")

            def _refresh_progress() -> None:
                fresh = _read_index_telemetry(state.cfg)
                stats = _read_index_stats(state.cfg)
                active_names = [
                    _STAGE_LABELS.get(str(row.get("stage") or ""), str(row.get("stage") or ""))
                    for row in (fresh.get("active_stages") or [])
                ]
                if fresh.get("active_ocr"):
                    active_names.append("OCR")
                active_chip.set_text("Запущено: " + ", ".join(active_names) if active_names else "Нет активных задач")
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
                    with ui.element("div").classes("rag-pipeline-row rag-pipeline-row-card rag-pipeline-head"):
                        ui.label("Этап").classes("rag-meta font-semibold")
                        ui.label("Прогресс").classes("rag-meta font-semibold")
                        ui.label("Статистика").classes("rag-meta font-semibold")
                        ui.label("Действия").classes("rag-meta font-semibold text-right")
                    for stage_key in ["metadata", "small", "large"]:
                        row = dict(summary_by_stage.get(stage_key) or {})
                        row.update(latest_by_stage.get(stage_key) or {})
                        row.update(active_by_stage.get(stage_key) or {})
                        if not row and stage_key == "metadata":
                            live_index = _find_live_running_index_run(_get_telemetry(state))
                            if live_index:
                                row = {
                                    "stage": stage_key,
                                    "status": "running",
                                    "processed_files": 0,
                                    "total_files": 0,
                                    "duration_sec": 0,
                                }
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

    # ── Auth / login / access denied screens ──────────────────────────────────

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

                _stop_managed_timer(state.tg_login_timer)
                state.tg_login_timer = ui.timer(2.0, poll_tg_login)

    # ── Admin / settings screens ──────────────────────────────────────────────

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
        all_users = auth_db.list_users()
        must_change_users = [u for u in all_users if int(u.get("must_change_password") or 0)]
        recent_events = auth_db.list_auth_events(limit=100)
        failed_logins = [e for e in recent_events if not int(e.get("ok") or 0) and str(e.get("event_type") or "") == "login_failed"]

        # Critical: default admin password still in use
        if auth_db.has_default_admin_password():
            with ui.row().classes("items-center gap-3 bg-red-50 dark:bg-red-950 border border-red-300 dark:border-red-700 rounded-lg p-4 w-full"):
                ui.icon("gpp_bad").classes("text-red-500 text-3xl flex-shrink-0")
                with ui.column().classes("flex-1 gap-1"):
                    ui.label("Критическая уязвимость: пароль admin не изменён").classes("font-semibold text-red-700 dark:text-red-300")
                    ui.label(
                        "Пользователь admin использует пароль по умолчанию «admin». "
                        "Смените пароль немедленно — любой может получить права администратора."
                    ).classes("text-red-600 dark:text-red-400 text-sm")
                ui.button(
                    "Сменить пароль", icon="key",
                    on_click=lambda: setattr(state, "settings_section", "profile") or render(),
                ).props("outline dense color=negative")

        if must_change_users:
            with ui.row().classes("items-center gap-2 bg-orange-50 border border-orange-200 rounded p-3 w-full"):
                ui.icon("warning").classes("text-orange-500")
                with ui.column().classes("gap-0"):
                    ui.label(f"{len(must_change_users)} пользователей должны сменить пароль").classes("text-orange-700 text-sm font-medium")
                    ui.label(", ".join(str(u.get("username") or "") for u in must_change_users)).classes("text-orange-600 text-xs")

        with ui.row().classes("w-full gap-3"):
            with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                ui.icon("group").classes("text-2xl text-primary")
                ui.label(str(len(all_users))).classes("text-xl font-semibold")
                ui.label("Пользователей").classes("rag-meta text-xs")
            with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                count_color = "text-negative" if must_change_users else "text-positive"
                ui.icon("lock_reset").classes(f"text-2xl {count_color}")
                ui.label(str(len(must_change_users))).classes(f"text-xl font-semibold {count_color}")
                ui.label("Смена пароля").classes("rag-meta text-xs")
            with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                fail_color = "text-negative" if failed_logins else "text-positive"
                ui.icon("no_accounts").classes(f"text-2xl {fail_color}")
                ui.label(str(len(failed_logins))).classes(f"text-xl font-semibold {fail_color}")
                ui.label("Неудачных входов").classes("rag-meta text-xs")

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

        # ── Auth event log ────────────────────────────────────────────────────
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Журнал входов").classes("text-xl font-semibold")
            ui.label("Последние 100 событий авторизации: входы, выходы, смены пароля.").classes("rag-meta")
            if not recent_events:
                with ui.element("div").classes("cd-empty-state py-4"):
                    ui.icon("history", size="28px").classes("opacity-30")
                    ui.label("Событий пока нет.").classes("text-xs")
            else:
                _AUTH_ACTION_LABELS = {
                    "login": "Вход",
                    "login_failed": "Ошибка входа",
                    "logout": "Выход",
                    "session_restore": "Восстановление сессии",
                    "password_change": "Смена пароля",
                    "register": "Регистрация",
                }
                with ui.element("div").classes("w-full overflow-x-auto"):
                    with ui.element("table").classes("w-full text-xs border-collapse"):
                        with ui.element("thead"):
                            with ui.element("tr").classes("border-b rag-section-label"):
                                for col in ("Время", "Пользователь", "Событие", "IP / детали"):
                                    ui.element("th").classes("text-left p-2 font-semibold").text = col
                        with ui.element("tbody"):
                            for ev in recent_events[:100]:
                                ok_ev = bool(ev.get("ok", True))
                                row_cls = "border-b hover:bg-slate-50 dark:hover:bg-slate-800" + (" text-negative" if not ok_ev else "")
                                with ui.element("tr").classes(row_cls):
                                    ts = str(ev.get("ts") or "")[:19].replace("T", " ")
                                    ui.element("td").classes("p-2 font-mono whitespace-nowrap").text = ts
                                    ui.element("td").classes("p-2 font-medium").text = str(ev.get("username") or "—")
                                    action_lbl = _AUTH_ACTION_LABELS.get(str(ev.get("event_type") or ""), str(ev.get("event_type") or "—"))
                                    with ui.element("td").classes("p-2"):
                                        with ui.row().classes("items-center gap-1"):
                                            ui.icon("check_circle" if ok_ev else "cancel", size="14px").classes("text-positive" if ok_ev else "text-negative")
                                            ui.label(action_lbl)
                                    details = ev.get("details") or {}
                                    detail_text = str(details.get("ip") or details.get("error") or details.get("reason") or "")
                                    ui.element("td").classes("p-2 rag-meta truncate max-w-xs").text = detail_text

        # ── Cloud Drive audit log ─────────────────────────────────────────────
        telemetry = _get_telemetry(state)
        cd_audit_events = telemetry.list_app_events(feature="cloud_drive", limit=100)
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Аудит Cloud Drive").classes("text-xl font-semibold")
            ui.label("Последние операции с файлами через API: загрузка, скачивание, перемещение, удаление.").classes("rag-meta")
            if not cd_audit_events:
                with ui.element("div").classes("cd-empty-state py-4"):
                    ui.icon("folder_off", size="28px").classes("opacity-30")
                    ui.label("Операций через API пока не было.").classes("text-xs")
            else:
                _CD_ACTION_LABELS = {
                    "upload": "Загрузка",
                    "download": "Скачивание",
                    "delete": "Удаление",
                    "move": "Перемещение",
                    "rename": "Переименование",
                    "create_folder": "Создание папки",
                    "list_directory": "Просмотр папки",
                    "view_node": "Просмотр",
                    "versions": "Версии",
                    "reindex": "Переиндексация",
                }
                _CD_ACTION_ICON = {
                    "upload": "upload", "download": "download", "delete": "delete",
                    "move": "drive_file_move", "rename": "edit", "create_folder": "create_new_folder",
                    "reindex": "refresh",
                }
                with ui.element("div").classes("w-full overflow-x-auto"):
                    with ui.element("table").classes("w-full text-xs border-collapse"):
                        with ui.element("thead"):
                            with ui.element("tr").classes("border-b rag-section-label"):
                                for col in ("Время", "Пользователь", "Действие", "Путь / детали"):
                                    ui.element("th").classes("text-left p-2 font-semibold").text = col
                        with ui.element("tbody"):
                            for ev in cd_audit_events[:100]:
                                ok_ev = bool(ev.get("ok", True))
                                row_cls = "border-b hover:bg-slate-50 dark:hover:bg-slate-800" + (" text-negative" if not ok_ev else "")
                                with ui.element("tr").classes(row_cls):
                                    ts = str(ev.get("ts") or "")[:19].replace("T", " ")
                                    ui.element("td").classes("p-2 font-mono whitespace-nowrap").text = ts
                                    ui.element("td").classes("p-2 font-medium").text = str(ev.get("username") or "—")
                                    action = str(ev.get("action") or "")
                                    action_lbl = _CD_ACTION_LABELS.get(action, action)
                                    icon_name = _CD_ACTION_ICON.get(action, "storage")
                                    with ui.element("td").classes("p-2"):
                                        with ui.row().classes("items-center gap-1"):
                                            ui.icon(icon_name, size="14px").classes("" if ok_ev else "text-negative")
                                            ui.label(action_lbl).classes("" if ok_ev else "text-negative")
                                    details = ev.get("details") or {}
                                    path = str(details.get("path") or details.get("filename") or details.get("name") or details.get("error") or "")
                                    ui.element("td").classes("p-2 rag-meta font-mono truncate max-w-xs").text = path

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

    def render_admin_cloud_drive_settings() -> None:
        default_db_path = str((Path(str(state.cfg.get("qdrant_db_path") or ".")) / "cloud_drive.db").resolve())
        default_storage_root = str((Path(str(state.cfg.get("qdrant_db_path") or ".")) / "cloud_storage").resolve())
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            initial_cloud = {
                "cloud_drive_enabled": bool(state.cfg.get("cloud_drive_enabled")),
                "cloud_drive_db_path": str(state.cfg.get("cloud_drive_db_path") or default_db_path).strip(),
                "cloud_drive_storage": str(state.cfg.get("cloud_drive_storage") or "local").strip() or "local",
                "cloud_drive_storage_root": str(state.cfg.get("cloud_drive_storage_root") or default_storage_root).strip(),
                "catalog_path": str(state.cfg.get("catalog_path") or "").strip(),
            }
            stats_ref: Dict[str, Any] = {"value": None}
            ui.label("Cloud Drive").classes("text-xl font-semibold")
            ui.label(
                "Централизованный реестр файлов и папок: дерево каталогов, версии, фоновые задачи. "
                "Поддерживается local storage; импорт — из указанного каталога источника."
            ).classes("rag-meta")

            enabled_input = ui.checkbox("Включить Cloud Drive", value=initial_cloud["cloud_drive_enabled"])
            enabled_input.tooltip("Включает реестр файлов и хранилище Cloud Drive. После включения в проводнике используется реестр вместо прямого обхода файловой системы.")

            db_input = ui.input("База реестра Cloud Drive", value=initial_cloud["cloud_drive_db_path"]).props("dense outlined").classes("w-full")
            db_input.tooltip("SQLite-база реестра: хранит структуру папок, метаданные файлов, версии и историю фоновых задач.")

            storage_kind = ui.select(
                {"local": "Local storage", "s3": "S3 / MinIO"},
                value=initial_cloud["cloud_drive_storage"],
                label="Хранилище файлов",
            ).props("dense outlined").classes("w-full max-w-sm")
            storage_kind.tooltip("Место физического хранения содержимого файлов. Local — локальная папка на сервере; S3/MinIO — объектное хранилище.")

            storage_root_input = ui.input("Папка хранения файлов", value=initial_cloud["cloud_drive_storage_root"]).props("dense outlined").classes("w-full")
            storage_root_input.tooltip("Корневая папка, куда сохраняются физические файлы при использовании local storage. Должна существовать и быть доступна для записи.")

            catalog_input = ui.input("Источник импорта", value=initial_cloud["catalog_path"]).props("dense outlined").classes("w-full")
            catalog_input.tooltip("Каталог источника для импорта: bootstrap сканирует его дерево папок и файлов и заносит в реестр. Обычно совпадает с основным каталогом документов.")

            bootstrap_limit = ui.number("Лимит импорта файлов (0 = без лимита)", value=0, min=0, step=100).props("dense outlined").classes("w-full max-w-sm")
            bootstrap_limit.tooltip("Ограничивает количество файлов при пробном запуске импорта. 0 — без ограничения, импортируются все файлы из каталога источника.")

            status_box = ui.column().classes("w-full gap-1")
            bootstrap_box = ui.column().classes("w-full gap-1")
            jobs_box = ui.column().classes("w-full gap-2")
            action_row = ui.row().classes("rag-dirty-actions")
            action_row.set_visibility(False)

            def current_cloud_values() -> Dict[str, Any]:
                return {
                    "cloud_drive_enabled": bool(enabled_input.value),
                    "cloud_drive_db_path": str(db_input.value or "").strip(),
                    "cloud_drive_storage": str(storage_kind.value or "local").strip() or "local",
                    "cloud_drive_storage_root": str(storage_root_input.value or "").strip(),
                    "catalog_path": str(catalog_input.value or "").strip(),
                }

            def refresh_cloud_visibility() -> None:
                is_local = str(storage_kind.value or "local") == "local"
                storage_root_input.set_visibility(is_local)

            def refresh_cloud_dirty() -> None:
                action_row.set_visibility(current_cloud_values() != initial_cloud)

            def reset_cloud_settings() -> None:
                enabled_input.set_value(initial_cloud["cloud_drive_enabled"])
                db_input.set_value(initial_cloud["cloud_drive_db_path"])
                storage_kind.set_value(initial_cloud["cloud_drive_storage"])
                storage_root_input.set_value(initial_cloud["cloud_drive_storage_root"])
                catalog_input.set_value(initial_cloud["catalog_path"])
                refresh_cloud_visibility()
                action_row.set_visibility(False)

            def render_cloud_stats(stats_obj: Any, *, title: str) -> None:
                stats_ref["value"] = stats_obj
                status_box.clear()
                with status_box:
                    ui.label(title).classes("font-semibold text-sm")
                    if not stats_obj:
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("cloud_off", size="28px").classes("opacity-30")
                            ui.label("Реестр ещё не инициализирован — нажмите «Инициализировать реестр».").classes("text-center")
                        return
                    with ui.row().classes("w-full gap-2 flex-wrap"):
                        for icon_name, lbl, val in [
                            ("folder",      "Папок",  f"{int(getattr(stats_obj, 'folders', 0)):,}".replace(",", " ")),
                            ("description", "Файлов", f"{int(getattr(stats_obj, 'files', 0)):,}".replace(",", " ")),
                            ("history",     "Версий", f"{int(getattr(stats_obj, 'versions', 0)):,}".replace(",", " ")),
                            ("pending",     "Jobs",   f"{int(getattr(stats_obj, 'pending_jobs', 0)):,}".replace(",", " ")),
                        ]:
                            with ui.column().classes("rag-card p-2 gap-0 items-center min-w-20 flex-1"):
                                ui.icon(icon_name, size="18px").classes("text-indigo-400")
                                ui.label(val).classes("text-base font-semibold leading-tight")
                                ui.label(lbl).classes("rag-meta text-xs")
                    root_path = str(getattr(stats_obj, "root_path", "") or "")
                    if root_path:
                        ui.label(f"Корень: {root_path}").classes("rag-path text-xs")

            _CD_STATUS_META = {
                "pending":   ("schedule",     "cd-status-pending",   "Ожидание"),
                "running":   ("sync",         "cd-status-running",   "Выполняется"),
                "done":      ("check_circle", "cd-status-done",      "Завершён"),
                "error":     ("error",        "cd-status-error",     "Ошибка"),
                "stale":     ("warning",      "cd-status-error",     "Состояние устарело"),
                "cancelled": ("cancel",       "cd-status-cancelled", "Отменён"),
            }

            def _cd_status_badge(status: str) -> None:
                icon_name, css_cls, label_ru = _CD_STATUS_META.get(
                    status, ("help", "cd-status-cancelled", status)
                )
                with ui.element("span").classes(f"cd-status-badge {css_cls}"):
                    ui.icon(icon_name, size="14px")
                    ui.label(label_ru)

            def render_bootstrap_status() -> None:
                bootstrap_state = _read_cloud_bootstrap_status(build_cloud_config())
                bootstrap_box.clear()
                with bootstrap_box:
                    ui.label("Статус импорта").classes("font-semibold text-sm")
                    raw_status = str(bootstrap_state.get("status") or bootstrap_state.get("job_status") or "idle")
                    status = {
                        "pending": "pending",
                        "running": "running",
                        "completed": "done",
                        "failed": "error",
                        "cancelled": "cancelled",
                    }.get(raw_status, raw_status)
                    if status == "idle":
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("cloud_upload", size="24px").classes("opacity-30")
                            ui.label("Импорт не запущен. Нажмите «Импортировать в реестр» ниже.").classes("text-center")
                        return
                    _cd_status_badge(status)
                    imported_files = _safe_int(bootstrap_state.get("imported_files"), 0)
                    imported_folders = _safe_int(bootstrap_state.get("imported_folders"), 0)
                    total_files = _safe_int(bootstrap_state.get("total_files"), 0)
                    if total_files > 0:
                        ratio = max(0.0, min(1.0, imported_files / total_files))
                        ui.linear_progress(value=ratio).classes("w-full")
                        ui.label(
                            f"Файлы: {imported_files:,} / {total_files:,} ({round(ratio * 100)}%)".replace(",", " ")
                        ).classes("rag-meta")
                    else:
                        ui.label(f"Файлы: {imported_files:,}".replace(",", " ")).classes("rag-meta")
                    ui.label(f"Папки: {imported_folders:,}".replace(",", " ")).classes("rag-meta")
                    current_path = str(bootstrap_state.get("current_path") or "").strip()
                    if current_path:
                        ui.label(f"Текущий путь: {current_path}").classes("rag-path")
                    error_text = str(bootstrap_state.get("error") or "").strip()
                    if error_text:
                        ui.label(error_text).classes("text-negative text-sm")
                    started_at = str(bootstrap_state.get("started_at") or "").strip()
                    finished_at = str(bootstrap_state.get("finished_at") or "").strip()
                    if started_at:
                        ui.label(f"Старт: {started_at[:19].replace('T', ' ')}").classes("rag-meta")
                    if finished_at:
                        ui.label(f"Финиш: {finished_at[:19].replace('T', ' ')}").classes("rag-meta")

            def render_bootstrap_jobs() -> None:
                jobs_box.clear()
                cfg_now = build_cloud_config()
                if not str(cfg_now.get("cloud_drive_db_path") or "").strip():
                    with jobs_box:
                        ui.label("Последние задачи").classes("font-semibold text-sm")
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("settings", size="24px").classes("opacity-30")
                            ui.label("Сохраните настройки Cloud Drive, чтобы видеть историю задач.").classes("text-center")
                    return
                try:
                    service = CloudDriveService.from_config(cfg_now)
                    jobs = service.list_bootstrap_jobs(limit=8)
                except Exception as exc:
                    with jobs_box:
                        ui.label("Последние задачи").classes("font-semibold text-sm")
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("error_outline", size="24px").classes("text-red-400 opacity-70")
                            ui.label(f"Не удалось прочитать jobs: {exc}").classes("text-center text-red-600 text-xs")
                    return
                with jobs_box:
                    ui.label("Последние задачи").classes("font-semibold text-sm")
                    if not jobs:
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("history", size="24px").classes("opacity-30")
                            ui.label("История задач пуста. Запустите импорт, чтобы начать.").classes("text-center")
                        return
                    for job in jobs:
                        progress = dict(job.progress or {})
                        raw_status = str(job.status or progress.get("status") or "")
                        norm_status = {
                            "pending": "pending", "running": "running",
                            "completed": "done", "failed": "error", "cancelled": "cancelled",
                        }.get(raw_status, raw_status)
                        imported_files = _safe_int(progress.get("imported_files"), 0)
                        total_files = _safe_int(progress.get("total_files"), 0)
                        current_path = str(progress.get("current_path") or progress.get("catalog") or "").strip()
                        error_text = str(job.last_error or progress.get("error") or "").strip()
                        with ui.element("div").classes("cd-jobs-card w-full"):
                            with ui.row().classes("w-full items-center gap-2"):
                                _cd_status_badge(norm_status)
                                ui.space()
                                ui.label(job.id[:8]).classes("font-mono text-xs rag-meta")
                            if total_files > 0:
                                ratio = max(0.0, min(1.0, imported_files / total_files))
                                ui.linear_progress(value=ratio, size="4px", show_value=False).classes("w-full my-1").props("color=indigo")
                                ui.label(
                                    f"{imported_files:,} / {total_files:,} файлов ({round(ratio * 100)}%)".replace(",", " ")
                                ).classes("rag-meta text-xs")
                            elif imported_files:
                                ui.label(f"{imported_files:,} файлов".replace(",", " ")).classes("rag-meta text-xs")
                            if current_path:
                                ui.label(current_path).classes("rag-path text-xs truncate")
                            if error_text and norm_status in ("error", "cancelled"):
                                ui.label(error_text).classes("text-red-600 text-xs mt-1 truncate")
                            with ui.row().classes("gap-1 mt-1"):
                                if raw_status in {"running", "pending"}:
                                    ui.button(
                                        "Отменить", icon="close",
                                        on_click=lambda _e=None, job_id=job.id: cancel_bootstrap_job(job_id),
                                    ).props("outline dense size=sm")
                                if raw_status in {"failed", "cancelled", "completed"}:
                                    ui.button(
                                        "Повторить", icon="replay",
                                        on_click=lambda _e=None, job_id=job.id: retry_bootstrap_job(job_id),
                                    ).props("outline dense size=sm")

            def build_cloud_config() -> Dict[str, Any]:
                values = current_cloud_values()
                cfg = dict(state.cfg)
                cfg.update(values)
                return cfg

            def persist_cloud_values(values: Dict[str, Any]) -> Dict[str, Any]:
                cfg = load_config()
                cfg.update(values)
                save_config(cfg)
                state.cfg = cfg
                initial_cloud.update(values)
                return cfg

            def save_cloud_settings() -> None:
                values = current_cloud_values()
                if not values["cloud_drive_db_path"]:
                    ui.notify("Укажите путь к базе данных реестра.", type="warning")
                    return
                if values["cloud_drive_storage"] == "local" and not values["cloud_drive_storage_root"]:
                    ui.notify("Укажите папку хранения файлов для локального хранилища.", type="warning")
                    return
                try:
                    persist_cloud_values(values)
                    refresh_cloud_visibility()
                    action_row.set_visibility(False)
                    _log_app_event(state, "settings", "save_cloud_drive", details=values)
                    ui.notify("Настройки Cloud Drive сохранены.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось сохранить настройки: {exc}", type="negative")

            async def init_registry() -> None:
                try:
                    cfg = persist_cloud_values(current_cloud_values())
                    service = await run.io_bound(CloudDriveService.from_config, cfg)
                    stats_obj = await run.io_bound(service.registry.stats)
                    render_cloud_stats(stats_obj, title="Реестр инициализирован")
                    _log_app_event(state, "cloud_drive", "init_registry", details=current_cloud_values())
                    ui.notify("Реестр Cloud Drive инициализирован.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось инициализировать реестр: {exc}", type="negative")

            async def refresh_registry_stats() -> None:
                try:
                    cfg = persist_cloud_values(current_cloud_values())
                    service = await run.io_bound(CloudDriveService.from_config, cfg)
                    stats_obj = await run.io_bound(service.registry.stats)
                    render_cloud_stats(stats_obj, title="Статистика реестра")
                    render_bootstrap_status()
                    render_bootstrap_jobs()
                except Exception as exc:
                    ui.notify(f"Не удалось прочитать stats: {exc}", type="negative")

            def _run_bootstrap_background(cfg: Dict[str, Any], *, job_id: str) -> None:
                try:
                    service = CloudDriveService.from_config(cfg)
                    service.run_bootstrap_job(job_id)
                except Exception:
                    pass

            async def bootstrap_registry(*, import_files: bool) -> None:
                catalog = str(catalog_input.value or "").strip()
                if not catalog:
                    ui.notify("Укажите источник импорта.", type="warning")
                    return
                if not Path(catalog).exists():
                    ui.notify("Источник импорта не найден.", type="negative")
                    return
                limit_value = int(bootstrap_limit.value or 0)
                try:
                    cfg = persist_cloud_values(current_cloud_values())
                    current_state = _read_cloud_bootstrap_status(cfg)
                    if str(current_state.get("job_status") or current_state.get("status") or "") in {"running", "pending"}:
                        ui.notify("Импорт уже выполняется.", type="warning")
                        render_bootstrap_status()
                        return
                    service = CloudDriveService.from_config(cfg)
                    job = service.create_bootstrap_job(
                        catalog_root=catalog,
                        max_files=None if limit_value <= 0 else limit_value,
                        import_files=import_files,
                    )
                    thread = threading.Thread(
                        target=_run_bootstrap_background,
                        kwargs={"cfg": cfg, "job_id": job.id},
                        name="cloud-drive-bootstrap",
                        daemon=True,
                    )
                    thread.start()
                    render_bootstrap_status()
                    render_bootstrap_jobs()
                    _log_app_event(
                        state,
                        "cloud_drive",
                        "bootstrap",
                        details={"catalog": catalog, "import_files": import_files, "limit": limit_value},
                    )
                    ui.notify("Импорт запущен в фоне.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось запустить импорт: {exc}", type="negative")

            async def cancel_bootstrap_job(job_id: str) -> None:
                try:
                    cfg = persist_cloud_values(current_cloud_values())
                    service = await run.io_bound(CloudDriveService.from_config, cfg)
                    await run.io_bound(service.cancel_job, job_id)
                    render_bootstrap_status()
                    render_bootstrap_jobs()
                    ui.notify("Отмена задачи запрошена.", type="warning")
                except Exception as exc:
                    ui.notify(f"Не удалось отменить задачу: {exc}", type="negative")

            async def retry_bootstrap_job(job_id: str) -> None:
                try:
                    cfg = persist_cloud_values(current_cloud_values())
                    current_state = _read_cloud_bootstrap_status(cfg)
                    if str(current_state.get("job_status") or current_state.get("status") or "") in {"running", "pending"}:
                        ui.notify("Сейчас уже выполняется другой импорт.", type="warning")
                        return
                    service = await run.io_bound(CloudDriveService.from_config, cfg)
                    job = await run.io_bound(service.retry_bootstrap_job, job_id)
                    thread = threading.Thread(
                        target=_run_bootstrap_background,
                        kwargs={"cfg": cfg, "job_id": job.id},
                        name="cloud-drive-bootstrap",
                        daemon=True,
                    )
                    thread.start()
                    render_bootstrap_status()
                    render_bootstrap_jobs()
                    ui.notify("Импорт перезапущен.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось повторить импорт: {exc}", type="negative")

            refresh_cloud_visibility()
            render_cloud_stats(None, title="Статистика реестра")
            render_bootstrap_status()
            render_bootstrap_jobs()

            enabled_input.on_value_change(lambda _: refresh_cloud_dirty())
            db_input.on_value_change(lambda _: refresh_cloud_dirty())
            storage_kind.on_value_change(lambda _: (refresh_cloud_visibility(), refresh_cloud_dirty()))
            storage_root_input.on_value_change(lambda _: refresh_cloud_dirty())
            catalog_input.on_value_change(lambda _: refresh_cloud_dirty())

            with action_row:
                with ui.row().classes("rag-dirty-actions-inner"):
                    ui.button("Отменить", icon="close", on_click=reset_cloud_settings).props("flat dense")
                    ui.button("Сохранить настройки", icon="save", on_click=save_cloud_settings).props("outline dense")

            with ui.row().classes("w-full gap-2 flex-wrap"):
                ui.button("Инициализировать реестр", icon="database", on_click=init_registry).props("outline")
                ui.button("Обновить статистику", icon="monitoring", on_click=refresh_registry_stats).props("outline")
                ui.button("Сканировать структуру", icon="sync", on_click=lambda: bootstrap_registry(import_files=False)).props("outline")
                ui.button("Импортировать в реестр", icon="cloud_upload", on_click=lambda: bootstrap_registry(import_files=True)).props("unelevated")
            bootstrap_box
            jobs_box
            _stop_managed_timer(state.cloud_drive_timer)
            state.cloud_drive_timer = ui.timer(3.0, lambda: (render_bootstrap_status(), render_bootstrap_jobs()))

    def render_admin_cloud_sync_settings() -> None:  # noqa: PLR0912,PLR0915
        """Sprint 4: Sync client admin settings — folder pairs, policies, connected clients."""
        cd_enabled = bool(state.cfg.get("cloud_drive_enabled"))

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Sync клиент").classes("text-xl font-semibold")
            ui.label(
                "Управление desktop sync-клиентами: отслеживание подключённых устройств, "
                "настройка пар папок и политики разрешения конфликтов."
            ).classes("rag-meta")

            if not cd_enabled:
                with ui.element("div").classes("cd-empty-state w-full py-6"):
                    ui.icon("cloud_off", size="32px").classes("opacity-30")
                    ui.label("Cloud Drive не включён — активируйте его в настройках Cloud Drive.").classes("text-center")
                    ui.button(
                        "Перейти в Cloud Drive",
                        icon="cloud",
                        on_click=lambda: (setattr(state, "settings_section", "cloud_drive"), render()),
                        color=None,
                    ).props("outline dense")
                return

            ui.separator()

            # ── Подключённые клиенты ──────────────────────────────────────
            with ui.expansion("Подключённые клиенты", icon="computer", value=True).classes("w-full"):
                with ui.element("div").classes("cd-empty-state w-full py-4"):
                    ui.icon("sync_disabled", size="28px").classes("opacity-30")
                    ui.label("Нет подключённых sync-клиентов.").classes("text-center")
                    ui.label(
                        "Desktop sync-агент будет доступен в следующем релизе. "
                        "Клиент подключается автоматически по токену пользователя."
                    ).classes("text-center rag-meta text-xs")

            ui.separator()

            # ── Пары папок ────────────────────────────────────────────────
            sync_pairs_raw = state.cfg.get("cloud_sync_folder_pairs") or []
            if not isinstance(sync_pairs_raw, list):
                sync_pairs_raw = []
            sync_pairs: list = [dict(p) for p in sync_pairs_raw if isinstance(p, dict)]

            pairs_dirty = [False]
            pairs_container = ui.column().classes("w-full gap-2")

            def _save_sync_pairs() -> None:
                cfg_copy = dict(state.cfg)
                cfg_copy["cloud_sync_folder_pairs"] = sync_pairs
                try:
                    save_config(cfg_copy)
                    state.cfg = cfg_copy
                    ui.notify("Пары папок сохранены.", type="positive")
                except Exception as exc:
                    ui.notify(f"Ошибка сохранения: {exc}", type="negative")
                pairs_dirty[0] = False
                render()

            def _render_pairs() -> None:
                pairs_container.clear()
                with pairs_container:
                    if not sync_pairs:
                        with ui.element("div").classes("cd-empty-state w-full py-3"):
                            ui.icon("folder_copy", size="24px").classes("opacity-30")
                            ui.label("Нет настроенных пар для синхронизации.").classes("text-center")
                    else:
                        for idx, pair in enumerate(sync_pairs):
                            with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                                ui.icon("folder_copy", size="20px").classes("text-indigo-400")
                                with ui.column().classes("flex-1 gap-0"):
                                    ui.label(str(pair.get("local_path") or "(не задано)")).classes("text-sm font-medium")
                                    cd_target = str(pair.get("cd_path") or "/")
                                    ui.label(f"→ Cloud Drive: {cd_target}").classes("rag-meta text-xs")
                                policy = str(pair.get("conflict_policy") or "ask")
                                policy_labels = {"ask": "Спрашивать", "server_wins": "Сервер приоритетнее", "local_wins": "Локальная приоритетнее"}
                                ui.badge(policy_labels.get(policy, policy), color="grey-4").classes("text-xs")
                                ui.button(
                                    icon="delete",
                                    color=None,
                                    on_click=lambda i=idx: (sync_pairs.pop(i), _render_pairs()),
                                ).props("flat round dense").tooltip("Удалить пару").classes("text-negative")

            _render_pairs()

            # Add pair dialog
            async def _add_pair_dialog() -> None:
                with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-[480px]"):
                    ui.label("Добавить пару синхронизации").classes("text-lg font-semibold")
                    local_input = ui.input(
                        "Локальная папка",
                        placeholder="C:\\Users\\Иван\\Documents\\Рабочие",
                    ).props("dense outlined").classes("w-full")
                    local_input.tooltip("Путь к локальной папке на компьютере пользователя (заполняется sync-агентом).")

                    # Cloud Drive folder picker
                    try:
                        _cd_svc = _cd_get_service(state.cfg)
                        if _cd_svc is not None:
                            with _cd_svc.registry._connect() as _c:
                                _frows = _c.execute("SELECT * FROM cloud_folders ORDER BY path").fetchall()
                            _cd_folders = {fo.path: (fo.path or "Корень") for fo in [_cd_svc.registry._folder_from_row(r) for r in _frows]}
                        else:
                            _cd_folders = {"/": "Корень"}
                    except Exception:
                        _cd_folders = {"/": "Корень"}

                    cd_sel = ui.select(
                        options=_cd_folders,
                        value=list(_cd_folders.keys())[0] if _cd_folders else "",
                        label="Папка в Cloud Drive",
                    ).props("dense outlined emit-value map-options").classes("w-full")

                    conflict_sel = ui.select(
                        options={
                            "ask": "Спрашивать при конфликте",
                            "server_wins": "Сервер приоритетнее",
                            "local_wins": "Локальная версия приоритетнее",
                        },
                        value="ask",
                        label="Политика конфликтов",
                    ).props("dense outlined emit-value map-options").classes("w-full")

                    def _do_add() -> None:
                        lp = str(local_input.value or "").strip()
                        cdp = str(cd_sel.value or "").strip() or "/"
                        pol = str(conflict_sel.value or "ask")
                        if not lp:
                            ui.notify("Укажите локальную папку.", type="warning")
                            return
                        sync_pairs.append({"local_path": lp, "cd_path": cdp, "conflict_policy": pol})
                        dlg.close()
                        _render_pairs()

                    with ui.row().classes("w-full justify-end gap-2 mt-2"):
                        ui.button("Отмена", on_click=dlg.close).props("flat dense")
                        ui.button("Добавить", icon="add", on_click=_do_add).props("unelevated dense")
                dlg.open()

            with ui.row().classes("w-full gap-2 mt-1"):
                ui.button("Добавить пару", icon="add_link", on_click=_add_pair_dialog).props("outline dense")
                ui.button("Сохранить", icon="save", on_click=_save_sync_pairs).props("unelevated dense")

            ui.separator()

            # ── Глобальная политика конфликтов ────────────────────────────
            with ui.expansion("Глобальная политика конфликтов", icon="merge", value=False).classes("w-full"):
                ui.label(
                    "Эти настройки применяются по умолчанию ко всем парам папок, "
                    "если для них не задана индивидуальная политика."
                ).classes("rag-meta text-xs")
                global_policy = str(state.cfg.get("cloud_sync_conflict_policy") or "ask")
                policy_sel = ui.select(
                    options={
                        "ask": "Всегда спрашивать пользователя",
                        "server_wins": "Серверная версия приоритетнее",
                        "local_wins": "Локальная версия приоритетнее",
                        "newest_wins": "Более новая версия приоритетнее (по времени модификации)",
                    },
                    value=global_policy,
                    label="Политика конфликтов",
                ).props("dense outlined emit-value map-options").classes("w-full max-w-sm")

                def _save_conflict_policy() -> None:
                    cfg_copy = dict(state.cfg)
                    cfg_copy["cloud_sync_conflict_policy"] = str(policy_sel.value or "ask")
                    try:
                        save_config(cfg_copy)
                        state.cfg = cfg_copy
                        ui.notify("Политика конфликтов сохранена.", type="positive")
                    except Exception as exc:
                        ui.notify(f"Ошибка: {exc}", type="negative")

                ui.button("Сохранить политику", icon="save", on_click=_save_conflict_policy).props("outline dense").classes("mt-1")

            ui.separator()

            # ── Выборочная синхронизация ──────────────────────────────────
            with ui.expansion("Выборочная синхронизация (Selective Sync)", icon="checklist", value=False).classes("w-full"):
                ui.label(
                    "Укажите, какие папки Cloud Drive включать в синхронизацию. "
                    "Остальные папки будут доступны только через web-интерфейс."
                ).classes("rag-meta text-xs")

                try:
                    _cd_svc2 = _cd_get_service(state.cfg)
                    if _cd_svc2 is not None:
                        with _cd_svc2.registry._connect() as _c2:
                            _frows2 = _c2.execute(
                                "SELECT * FROM cloud_folders WHERE depth <= 2 AND is_root=0 ORDER BY path"
                            ).fetchall()
                        _top_folders = [_cd_svc2.registry._folder_from_row(r) for r in _frows2]
                    else:
                        _top_folders = []
                except Exception:
                    _top_folders = []

                excluded_raw = state.cfg.get("cloud_sync_excluded_paths") or []
                excluded_set: set = set(excluded_raw if isinstance(excluded_raw, list) else [])

                if not _top_folders:
                    with ui.element("div").classes("cd-empty-state w-full py-3"):
                        ui.icon("folder_off", size="24px").classes("opacity-30")
                        ui.label("Нет папок в реестре. Запустите импорт в Cloud Drive.").classes("text-center")
                else:
                    checkboxes: Dict[str, Any] = {}
                    with ui.column().classes("w-full gap-1"):
                        for _fo in _top_folders:
                            _cb = ui.checkbox(_fo.path, value=(_fo.path not in excluded_set))
                            checkboxes[_fo.path] = _cb

                    def _save_selective_sync() -> None:
                        new_excluded = [p for p, cb in checkboxes.items() if not cb.value]
                        cfg_copy = dict(state.cfg)
                        cfg_copy["cloud_sync_excluded_paths"] = new_excluded
                        try:
                            save_config(cfg_copy)
                            state.cfg = cfg_copy
                            ui.notify("Выборочная синхронизация сохранена.", type="positive")
                        except Exception as exc:
                            ui.notify(f"Ошибка: {exc}", type="negative")

                    ui.button("Сохранить", icon="save", on_click=_save_selective_sync).props("outline dense").classes("mt-1")

            ui.separator()

            # ── История конфликтов ────────────────────────────────────────
            with ui.expansion("Журнал конфликтов", icon="history_toggle_off", value=False).classes("w-full"):
                with ui.element("div").classes("cd-empty-state w-full py-3"):
                    ui.icon("history_toggle_off", size="24px").classes("opacity-30")
                    ui.label("Конфликтов не зафиксировано.").classes("text-center")
                    ui.label("История конфликтов появится после подключения sync-клиента.").classes("text-center rag-meta text-xs")

    def render_admin_llm_settings() -> None:
        def _fetch_ollama_models(ollama_url: str) -> List[str]:
            """Запросить список моделей из Ollama /api/tags. Возвращает [] при ошибке."""
            try:
                import json as _json  # noqa: PLC0415
                import urllib.request as _ur  # noqa: PLC0415
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

                def _quick_add_alias(cq: str, cp: str) -> None:
                    import re as _re
                    _key = _re.sub(r"[^a-z0-9]+", "_", cq.lower()).strip("_") or "alias"
                    try:
                        telemetry.save_search_alias_group(
                            key=_key, label=cq, aliases=[cq, cp], source="analytics"
                        )
                        _log_app_event(state, "settings", "search_alias_add", details={"key": _key, "from": "admin_candidate"})
                        ui.notify(f"Синоним добавлен: «{cq}» = «{cp}»", type="positive")
                        render()
                    except Exception as exc:
                        ui.notify(f"Не удалось добавить: {exc}", type="negative")

                for item in candidates:
                    cand_q = str(item.get("query") or "")
                    cand_p = str(item.get("candidate") or "")
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.label(cand_p).classes("font-medium")
                        ui.label(f"запрос: {cand_q}").classes("rag-meta")
                        ui.label(str(item.get("title") or item.get("path") or "")).classes("rag-path flex-1")
                        ui.button("Добавить", icon="add", on_click=lambda cq=cand_q, cp=cand_p: _quick_add_alias(cq, cp)).props("flat dense no-caps").classes("text-xs")

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
            ("profile",         "person",         "Профиль",                  ["имя", "аккаунт", "профиль"]),
            ("telegram_sync",   "sync",           "Синхронизация Telegram",   ["telegram", "бот", "синхронизация"]),
            ("cloud_sync_user", "sync_alt",       "Cloud Sync",               ["sync", "синхронизация", "папка", "desktop"]),
            ("explorer",        "folder_open",    "Проводник",                ["файлы", "вид", "сортировка"]),
            ("favorites",       "star_border",    "Избранное",                ["закладки"]),
            ("saved_searches",  "bookmark",       "Сохранённые запросы",      ["запросы", "поиск", "сохранено"]),
            ("password",        "key",            "Пароль и выход",           ["смена", "выход", "logout"]),
        ]
        admin_sections: List[tuple] = [
            ("paths",         "storage",        "Пути и Qdrant",          ["каталог", "база", "url", "коллекция"]),
            ("cloud_drive",   "cloud",          "Cloud Drive",            ["cloud", "registry", "bootstrap", "storage", "s3"]),
            ("cloud_sync",    "sync_alt",       "Sync клиент",            ["sync", "синхронизация", "клиент", "desktop", "папка"]),
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

                elif sec == "cloud_sync_user":
                    with ui.column().classes("rag-card w-full p-4 gap-3"):
                        ui.label("Cloud Sync").classes("text-xl font-semibold")
                        ui.label(
                            "Desktop sync-клиент синхронизирует выбранные папки вашего компьютера "
                            "с Cloud Drive. Настройте пары папок и политику конфликтов."
                        ).classes("rag-meta")
                        cd_enabled2 = bool(state.cfg.get("cloud_drive_enabled"))
                        if not cd_enabled2:
                            with ui.element("div").classes("cd-empty-state w-full py-4"):
                                ui.icon("cloud_off", size="28px").classes("opacity-30")
                                ui.label("Cloud Drive не включён — обратитесь к администратору.").classes("text-center")
                        else:
                            # Status card
                            with ui.row().classes("w-full items-center gap-3 p-3 rag-explorer-item"):
                                ui.icon("sync_disabled", size="24px").classes("text-slate-400")
                                with ui.column().classes("flex-1 gap-0"):
                                    ui.label("Sync-клиент не подключён").classes("font-medium")
                                    ui.label("Установите desktop sync-агент, чтобы автоматически синхронизировать файлы.").classes("rag-meta text-xs")
                                ui.badge("Не подключён", color="grey-4").classes("text-xs")

                            ui.separator()
                            ui.label("Мои папки синхронизации").classes("font-semibold")

                            user_pairs_raw = state.cfg.get("cloud_sync_folder_pairs") or []
                            user_pairs = [p for p in (user_pairs_raw if isinstance(user_pairs_raw, list) else []) if isinstance(p, dict)]

                            if not user_pairs:
                                with ui.element("div").classes("cd-empty-state w-full py-3"):
                                    ui.icon("folder_copy", size="24px").classes("opacity-30")
                                    ui.label("Нет настроенных папок для синхронизации.").classes("text-center")
                                    ui.label("Добавьте пары папок в Настройках → Sync клиент (доступно администраторам).").classes("text-center rag-meta text-xs")
                            else:
                                with ui.column().classes("w-full gap-1"):
                                    for _pair in user_pairs:
                                        with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                                            ui.icon("folder_copy", size="20px").classes("text-indigo-400")
                                            with ui.column().classes("flex-1 gap-0"):
                                                ui.label(str(_pair.get("local_path") or "(не задано)")).classes("text-sm font-medium")
                                                ui.label(f"→ Cloud Drive: {_pair.get('cd_path', '/')}").classes("rag-meta text-xs")
                                            _pol = str(_pair.get("conflict_policy") or "ask")
                                            _pol_lbl = {"ask": "Спрашивать", "server_wins": "Сервер", "local_wins": "Локальная"}.get(_pol, _pol)
                                            ui.badge(_pol_lbl, color="grey-4").classes("text-xs")

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

                elif sec == "saved_searches":
                    with ui.column().classes("rag-card w-full p-4 gap-3"):
                        ui.label("Сохранённые запросы").classes("text-xl font-semibold")
                        if not state.saved_searches:
                            ui.label("Нет сохранённых запросов. Нажмите на закладку рядом с результатами поиска, чтобы сохранить запрос.").classes("rag-meta")
                        for ss in state.saved_searches:
                            ss_q = str(ss.get("query") or "")
                            ss_label = str(ss.get("label") or ss_q)
                            with ui.row().classes("w-full items-center gap-2"):
                                ui.icon("bookmark", size="16px").classes("text-amber-500 shrink-0")
                                with ui.column().classes("flex-1 min-w-0 gap-0"):
                                    ui.label(ss_label).classes("text-sm font-medium truncate")
                                    if ss_label != ss_q:
                                        ui.label(ss_q).classes("rag-path text-xs truncate")
                                ui.button(icon="search", on_click=choose_query_handler(ss_q), color=None).props("flat round dense").tooltip("Выполнить этот запрос")
                                ui.button(icon="delete", on_click=lambda q=ss_q: (
                                    _toggle_saved_search(state, q), render_section()
                                ), color=None).props("flat round dense")

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
                elif sec == "cloud_drive":
                    render_admin_cloud_drive_settings()
                elif sec == "cloud_sync":
                    render_admin_cloud_sync_settings()
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

    # ── Analytics / stats screen ──────────────────────────────────────────────

    def render_stats_screen() -> None:
        if str((state.current_user or {}).get("role") or "") != "admin":
            render_access_denied(hint="Статистика поиска, аудит и бенчмарк доступны только администраторам.")
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
                                    ui.button(icon="search", on_click=choose_query_handler(str(row["query"])), color=None).props("flat round dense").tooltip("Выполнить этот запрос")
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
                                    ui.button(icon="search", on_click=choose_query_handler(str(row["query"])), color=None).props("flat round dense").tooltip("Выполнить этот запрос")
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

            # ── Аудит ──────────────────────────────────────────────────
            # ── Бенчмарк ───────────────────────────────────────────────
            with ui.tab_panel(tab_benchmark):
                from rag_catalog.core.search_eval import evaluate_search, load_golden_queries

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
