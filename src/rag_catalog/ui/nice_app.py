"""NiceGUI web frontend for RAG Catalog."""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
import threading
import time as _time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from nicegui import app, events, run, ui

from rag_catalog.core.log_history import install_env_log_handler
from rag_catalog.core.rag_core import load_config

from . import api as _api_routes  # noqa: F401 — import triggers route registration
from . import explorer_view as _explorer_view
from . import index_view as _index_view
from . import jobs_view as _jobs_view
from . import settings_view as _settings_view
from . import stats_view as _stats_view
from .auth_session import complete_login_session, logout_session, restore_session, touch_session
from .css import INTERACTION_JS_PATH, _install_css, _install_interaction_javascript
from .helpers import (
    FILE_PREVIEW_EXTENSIONS,
    INLINE_IMAGE_EXTENSIONS,
    OFFICE_PREVIEW_EXTENSIONS,
    _apply_query_operators,
    _cd_file_jobs_map,
    _cd_file_size,
    _cd_get_service,
    _cd_search_by_name,
    _clean_text,
    _cloud_query_set,
    _count_exact_name_matches,
    _dedupe_queries,
    _directory_children,
    _ensure_searcher,
    _file_icon_svg,
    _filter_cloud_drive_search_results,
    _highlight_query_terms,
    _is_admin,
    _load_user_state,
    _merge_search_results,
    _my_recent_queries,
    _open_os_path,
    _parse_search_query,
    _popular_queries,
    _preview_file,
    _preview_office_file,
    _read_index_telemetry,
    _remember_query,
    _resolve_catalog_file,
    _result_group,
    _result_kind,
    _run_catalog_search,
    _run_quick_name_search,
    _save_ui_settings,
    _search_suggestions,
    _select_in_os_explorer,
    _telegram_deeplink,
    _viewer_file_url,
    _warm_searcher_cache,
)
from .state import (
    PageState,
    _get_auth_db,
    _get_telemetry,
    _is_saved_search,
    _log_app_event,
    _refresh_current_user,
    _toggle_saved_search,
    _username,
    capture_screen_state,
    restore_screen_state,
    should_rebuild_screen_container,
)
from .system import (
    _recover_cloud_drive_jobs,
    _run_recovery_cycle,
    _start_cloud_drive_job_worker,
    _start_global_scheduler,
    _start_recovery_watchdog,
    _stop_managed_timer,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
APP_ICON_PATH = PROJECT_ROOT / "assets" / "brand" / "ico" / "favicon.ico"
LOGO_PATH = PROJECT_ROOT / "assets" / "brand" / "svg" / "rag-search-mark.svg"
install_env_log_handler()

SEARCH_PRESETS = [
    ("Договоры", "договор поставки"),
    ("Счета", "счет на оплату"),
    ("Паспорта", "паспорт техника"),
    ("PDF", "pdf скан"),
    ("Таблицы", "реестр xlsx"),
]

APP_SCREEN_SPECS = (
    {"key": "search", "route": "/search", "title": "Поиск", "label": "Поиск", "icon": "search", "header": True, "drawer": True},
    {"key": "explorer", "route": "/explorer", "title": "Проводник", "label": "Файлы", "icon": "folder", "header": True, "drawer": True},
    {"key": "jobs", "route": "/jobs", "title": "Задачи", "label": "Задачи", "icon": "queue", "header": True, "drawer": True},
    {"key": "index", "route": "/index", "title": "Индекс", "label": "Индекс", "icon": "filter_center_focus", "drawer_icon": "analytics", "header": True, "drawer": True, "admin_only": True},
    {"key": "stats", "route": "/stats", "title": "Аналитика", "label": "Аналитика", "icon": "query_stats", "header": False, "drawer": True, "admin_only": True},
    {"key": "settings", "route": "/settings", "title": "Настройки", "label": "Настройки", "icon": "settings", "header": False, "drawer": False},
)
APP_SCREEN_TITLES = {str(spec["key"]): str(spec["title"]) for spec in APP_SCREEN_SPECS}
APP_SCREEN_ROUTES = {str(spec["key"]): str(spec["route"]) for spec in APP_SCREEN_SPECS}

if LOGO_PATH.exists():
    app.add_static_file(local_file=LOGO_PATH, url_path="/rag-logo.png")
if INTERACTION_JS_PATH.exists():
    app.add_static_file(local_file=INTERACTION_JS_PATH, url_path="/rag-interactions.js")


def _client_alive() -> bool:
    """Return True if the current NiceGUI client is still connected.

    When a browser disconnects and reconnects mid-search, the old async
    handler continues but the client has been removed from Client.instances.
    Calling render() on a deleted client logs a warning and rebuilds dead UI
    elements — we skip those renders instead.
    """
    try:
        from nicegui.client import Client  # noqa: PLC0415
        c = Client.current
        return c is not None and c.id in Client.instances
    except Exception:
        return True  # assume alive if we can't inspect


def _build_page(initial_screen: str = "search") -> None:
    state = PageState(cfg=load_config())
    state.screen = initial_screen
    state.explorer_path = str(Path(str(state.cfg.get("catalog_path") or "")))
    _install_css()
    ui.timer(0.0, _install_interaction_javascript, once=True)
    restore_session(state, on_restored=_load_user_state)

    dark_mode = ui.dark_mode(state.theme == "dark")

    with ui.header(fixed=True, elevated=False).classes("rag-header-v2"):
        with ui.element("div").classes("rag-hdr-grid"):
            # ── Left: brand ──────────────────────────────────
            with ui.element("div").classes("rag-hdr-brand"):
                menu_button = ui.button(icon="menu", on_click=lambda: drawer.toggle(), color=None).props("flat round dense").classes("rag-header-button rag-mobile-menu-button")
                if LOGO_PATH.exists():
                    ui.image("/rag-logo.png").classes("w-7 h-7 rounded")
                else:
                    ui.icon("manage_search").classes("text-2xl")
                ui.label("Rag-search").classes("rag-hdr-brand-name")
                ui.label("v3.4").classes("rag-chip rag-mono-label rag-version-chip")
            # ── Center: nav tabs or explorer path ─────────────
            with ui.element("div").classes("rag-hdr-center"):
                header_nav = ui.element("nav").classes("rag-hdr-nav")
                header_breadcrumbs = ui.row().classes("rag-header-breadcrumbs rag-breadcrumbs items-center gap-1 no-wrap")
            # ── Right: actions ────────────────────────────────
            with ui.element("div").classes("rag-hdr-actions"):
                header_actions = ui.row().classes("rag-header-actions items-center gap-1")
                state.header_breadcrumbs = header_breadcrumbs
                state.header_explorer_actions = header_actions
                header_status_chip = ui.label("").classes("rag-chip rag-header-status")
                header_status_chip.set_visibility(False)
                settings_button = ui.button(
                    icon="settings",
                    on_click=lambda: open_context_settings(),
                    color=None,
                ).props("flat round dense").classes("rag-header-button")
                settings_button.tooltip("Настройки")
                theme_button = ui.button(
                    icon="light_mode" if state.theme == "dark" else "dark_mode",
                    on_click=lambda: toggle_theme(),
                    color=None,
                ).props("flat round dense").classes("rag-header-button")
                theme_button.tooltip("Сменить тему")
                header_title = ui.label("").classes("hidden")
                header_user_label = ui.label("").classes("rag-avatar hidden lg:grid")

    with ui.left_drawer(value=False, fixed=True, bordered=True).classes("rag-drawer w-72 p-4") as drawer:
        with ui.column().classes("rag-drawer-body w-full"):
            ui.label("Меню").classes("text-xl font-semibold mb-2")
            nav_area = ui.column().classes("w-full gap-2")
            settings_area = ui.column().classes("w-full gap-3 mt-4")
            bottom_nav_area = ui.column().classes("rag-drawer-bottom w-full gap-2")

    page_root = ui.column().classes("rag-page gap-5")
    with page_root:
        content = ui.column().classes("w-full gap-5")
    screen_containers: Dict[str, Any] = {}
    initialized_screens: set[str] = set()
    dirty_screens: set[str] = set()
    active_screen_ref: List[Optional[str]] = [None]

    def mark_screen_dirty(screen: str) -> None:
        dirty_screens.add(screen)

    def screen_container(screen: str) -> Any:
        if screen not in screen_containers:
            with content:
                container = ui.column().classes("w-full gap-5")
                container.set_visibility(False)
            screen_containers[screen] = container
        return screen_containers[screen]

    def current_content_container() -> Any:
        return screen_containers.get(state.screen) or content

    def touch_activity() -> None:
        touch_session(state, min_interval_minutes=60)

    _stop_managed_timer(state.activity_timer)
    state.activity_timer = None
    if state.auth_token and state.current_user:
        state.activity_timer = ui.timer(3600.0, touch_activity)

    _stop_managed_timer(state.scheduler_timer)
    state.scheduler_timer = None

    def do_logout() -> None:
        logout_session(state)
        state.theme = "light"
        dark_mode.set_value(False)
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
        if screen == "cloud":
            screen = "explorer"
        ui.run_javascript(
            "window.ragShowBusy && window.ragShowBusy('Открываю экран...', { timeout: 4000, skeleton: true });"
        )
        touch_activity()
        prev_screen = state.screen
        capture_screen_state(state, prev_screen)
        ui.run_javascript(
            "(() => {"
            f"const key = 'rag-scroll-{prev_screen}';"
            "try { sessionStorage.setItem(key, String(window.scrollY || document.documentElement.scrollTop || 0)); } catch(e) {}"
            "})();"
        )
        if close_drawer:
            try:
                drawer.set_value(False)
            except Exception:
                pass
        state.screen = screen
        restore_screen_state(state, screen)
        ui.run_javascript(f"history.pushState(null, '', '/{screen}')")
        _log_app_event(state, "navigation", "open_screen", details={"screen": screen})
        render()

    def go_settings_section(section: str, *, close_drawer: bool = False) -> None:
        state.settings_section = section
        mark_screen_dirty("settings")
        set_screen("settings", close_drawer=close_drawer)

    def open_context_settings() -> None:
        if state.screen == "settings":
            return
        section = {
            "explorer": "explorer",
            "index": "indexing",
        }.get(state.screen, state.settings_section or "profile")
        go_settings_section(section)

    def go_explorer(path: str) -> None:
        value = str(path or "").strip()
        if value:
            p = Path(value)
            state.explorer_path = str(p.parent if p.is_file() else p)
            state.explorer_page = 0
            mark_screen_dirty("explorer")
        set_screen("explorer")

    def go_cloud_explorer_path(cloud_item_path: str, *, is_folder: bool = False) -> None:
        item_path = str(cloud_item_path or "").strip().strip("/")
        if not item_path:
            state.explorer_cd_path = ""
        elif is_folder:
            state.explorer_cd_path = item_path
        else:
            state.explorer_cd_path = item_path.rsplit("/", 1)[0] if "/" in item_path else ""
        state.explorer_page = 0
        mark_screen_dirty("explorer")
        set_screen("explorer")

    def update_nav() -> None:
        is_admin = str((state.current_user or {}).get("role") or "") == "admin"
        nav_items = [
            spec for spec in APP_SCREEN_SPECS
            if spec.get("drawer") and (not spec.get("admin_only") or is_admin)
        ]

        # ── Header nav tabs (desktop) ──────────────────────
        header_nav.clear()
        if state.current_user:
            header_items = [
                spec for spec in APP_SCREEN_SPECS
                if spec.get("header") and (not spec.get("admin_only") or is_admin)
            ]
            with header_nav:
                for spec in header_items:
                    key = str(spec["key"])
                    active_cls = "active" if state.screen == key else ""
                    tab = ui.button(
                        str(spec["label"]), icon=str(spec["icon"]),
                        on_click=lambda s=key: set_screen(s),
                    ).props("flat no-caps").classes(f"rag-nav-tab {active_cls}")
                    tab._props["style"] = "font-size:13px;font-weight:500"

        # ── Avatar label ────────────────────────────────────
        uname = str((state.current_user or {}).get("username") or "")
        header_user_label.set_text(uname[:2].upper() if uname else "")
        header_user_label.set_visibility(bool(uname))

        # ── Header index status chip ────────────────────────
        header_status_chip.set_visibility(False)
        if state.current_user:
            try:
                _now = _time.monotonic()
                if _now - state._telemetry_nav_cache_ts > 5.0:
                    state._telemetry_nav_cache = _read_index_telemetry(state.cfg)
                    state._telemetry_nav_cache_ts = _now
                telemetry = state._telemetry_nav_cache or {}
                active_rows = list(telemetry.get("active_stages") or [])
                if telemetry.get("active_ocr"):
                    active_rows.append(telemetry.get("active_ocr") or {})
                if active_rows:
                    row = active_rows[0]
                    processed = int(row.get("processed_files") or row.get("processed_pdfs") or 0)
                    total = int(row.get("total_files") or row.get("found_scanned") or 0)
                    pct = min(100, max(0, round(processed * 100 / total))) if total > 0 else 0
                    header_status_chip.set_text(f"● {pct}% · индексация")
                    header_status_chip.set_visibility(True)
            except Exception:
                header_status_chip.set_visibility(False)

        # ── Left drawer nav (mobile / supplementary) ────────
        nav_area.clear()
        with nav_area:
            for spec in nav_items:
                screen = str(spec["key"])
                label = str(spec["label"])
                icon_name = str(spec.get("drawer_icon") or spec["icon"])
                color = "primary" if state.screen == screen else None
                ui.button(label, icon=icon_name, on_click=lambda s=screen: set_screen(s, close_drawer=True), color=color).props("flat align=left no-caps").classes("rag-nav-button w-full")

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

    def render_safely() -> None:
        if not _client_alive():
            return
        try:
            render()
        except RuntimeError as exc:
            message = str(exc)
            if (
                "client this element belongs to has been deleted" in message
                or "parent element this slot belongs to has been deleted" in message
            ):
                return
            raise

    async def run_search(explicit_query: Optional[str] = None) -> None:
        touch_activity()
        raw = explicit_query if explicit_query is not None else state.query
        query = re.sub(r"\s+", " ", str(raw or "")).strip()
        if not query:
            ui.notify("Введите запрос.", type="warning")
            return
        parsed_query = _parse_search_query(query)
        semantic_q = parsed_query["semantic_query"] or query
        effective_file_type = parsed_query.get("file_type_filter") or state.file_type
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
        state.rag_answer_ok = True
        state.rag_answer_sources = []
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
            render_safely()
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
                query=semantic_q,
                limit=state.limit,
                file_type=effective_file_type,
            )
            if state.search_request_id != request_id:
                return
            quick_results = _filter_cloud_drive_search_results(state.cfg, state.current_user, quick_results)
            state.results = quick_results
            exact_count = _count_exact_name_matches(query, quick_results)
            state.search_stats_hint = f"Быстро найдено: {len(quick_results)} · точных совпадений: {exact_count}"
            state.search_lazy_loading = True
            if not _client_alive():
                return
            has_numeric_exact = any(
                str(item.get("retrieval_source") or "") in {"numeric_fs_exact", "numeric_exact"}
                for item in quick_results
            )
            if has_numeric_exact:
                state.search_lazy_loading = False
                exact_item = quick_results[0]
                fname = str(exact_item.get("filename") or exact_item.get("path") or "файле")
                fragment = re.sub(r"\s+", " ", str(exact_item.get("text") or "")).strip()
                state.rag_answer_ok = True
                state.rag_answer_sources = [dict(exact_item)]
                state.rag_answer_text = (
                    f"Найдено точное совпадение номера в файле: {fname}."
                    + (f"\n\nФрагмент: {fragment[:700]}" if fragment else "")
                )
                _log_app_event(
                    state,
                    "search",
                    "run_quick",
                    details={
                        "query": query,
                        "results": len(quick_results),
                        "exact_matches": exact_count,
                        "numeric_exact": True,
                    },
                )
                render_safely()
                return
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
            render_safely()
            return

        # Ленивая догрузка: сначала, при необходимости, расширяем запрос через LLM.
        search_query = semantic_q
        if llm_expand_enabled:
            try:
                from rag_catalog.core.llm import expand_query  # noqa: PLC0415
                expanded = await run.io_bound(
                    expand_query, semantic_q, model=expand_model, ollama_url=ollama_url
                )
                if state.search_request_id != request_id:
                    return
                if expanded and expanded.lower() != semantic_q.lower():
                    state.expanded_query = expanded
                    search_query = expanded
            except Exception:
                pass

        try:
            full_results = await run.io_bound(
                _run_catalog_search,
                searcher,
                limit=state.limit,
                file_type=effective_file_type,
                content_only=state.content_only,
                title_only=state.title_only,
                username=_username(state),
                query=search_query,
                query_original=query,
                query_used=search_query,
            )
            if state.search_request_id != request_id:
                return
            merged = _merge_search_results(state.results, full_results, limit=state.limit)
            merged = _filter_cloud_drive_search_results(state.cfg, state.current_user, merged)
            state.results = _apply_query_operators(merged, parsed_query)
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

        if state.search_request_id == request_id:
            state.search_lazy_loading = False
            render_safely()

        # RAG Q&A — только после полной догрузки
        if llm_enabled and state.results and not state.search_error and state.search_request_id == request_id:
            searcher_for_answer = _ensure_searcher(state)
            if searcher_for_answer is not None:
                state.rag_answer_loading = True
                if not _client_alive():
                    return
                try:
                    ans = await run.io_bound(
                        searcher_for_answer.answer_documents,
                        query,
                    )
                    if state.search_request_id != request_id:
                        return
                    state.rag_answer_text = str(ans.get("answer") or "")
                    state.rag_answer_ok = bool(ans.get("ok", True))
                    state.rag_answer_sources = list(ans.get("sources") or [])
                except Exception as exc:
                    if state.search_request_id != request_id:
                        return
                    state.rag_answer_text = f"Ошибка LLM: {exc}"
                    state.rag_answer_ok = False
                    state.rag_answer_sources = []
                finally:
                    if state.search_request_id == request_id:
                        state.rag_answer_loading = False

        if state.search_request_id == request_id:
            state.search_lazy_loading = False
            render_safely()

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

    _SEARCH_HELP_HTML = """
<style>
.rag-sh-t{width:100%;border-collapse:collapse;font-size:13px;line-height:1.5}
.rag-sh-t th{text-align:left;padding:5px 10px;border-bottom:1px solid rgba(128,128,128,.25);font-size:11px;text-transform:uppercase;letter-spacing:.05em;opacity:.55;font-weight:600}
.rag-sh-t td{padding:4px 10px;border-bottom:1px solid rgba(128,128,128,.1);vertical-align:top}
.rag-sh-t tr:last-child td{border-bottom:none}
.rag-sh-t td:first-child{font-family:ui-monospace,monospace;font-weight:600;white-space:nowrap;color:var(--rag-accent,#6366f1)}
.rag-sh-t td:last-child{opacity:.65;font-family:ui-monospace,monospace;font-size:12px}
.rag-sh-sec{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;opacity:.45;padding:14px 10px 4px;display:block}
</style>
<span class="rag-sh-sec">Основные операторы</span>
<table class="rag-sh-t">
<tr><th>Синтаксис</th><th>Описание</th><th>Пример</th></tr>
<tr><td>"фраза"</td><td>Точная фраза (в кавычках)</td><td>"договор подряда"</td></tr>
<tr><td>-слово</td><td>Исключить слово из результатов</td><td>-акт</td></tr>
<tr><td>слово*</td><td>Начинается на... (префикс)</td><td>дог*&nbsp;→&nbsp;договор, договоры</td></tr>
<tr><td>A и B</td><td>Оба условия одновременно</td><td>договор и акт</td></tr>
<tr><td>A или B</td><td>Любое из условий</td><td>счёт или акт</td></tr>
</table>
<span class="rag-sh-sec">Фильтры по файлу и дате</span>
<table class="rag-sh-t">
<tr><th>Синтаксис</th><th>Описание</th><th>Пример</th></tr>
<tr><td>type:расш</td><td>Тип файла по расширению</td><td>type:pdf&nbsp;&nbsp;type:xlsx</td></tr>
<tr><td>after:ГГГГ-ММ-ДД</td><td>Дата изменения — после</td><td>after:2024-01-01</td></tr>
<tr><td>before:ГГГГ-ММ-ДД</td><td>Дата изменения — до</td><td>before:2024-12-31</td></tr>
<tr><td>path:папка</td><td>Путь содержит текст (каталог)</td><td>path:Договоры</td></tr>
</table>
<span class="rag-sh-sec">Фильтры по авторам (из свойств документа)</span>
<table class="rag-sh-t">
<tr><th>Синтаксис</th><th>Описание</th><th>Пример</th></tr>
<tr><td>creator:имя</td><td>Создатель документа (dc:creator / PDF Author)</td><td>creator:ivanov</td></tr>
<tr><td>editor:имя</td><td>Последний или самый частый редактор</td><td>editor:petrov</td></tr>
<tr><td>from:имя</td><td>Любой автор или путь содержит имя</td><td>from:sidorov</td></tr>
</table>
<span class="rag-sh-sec">Примеры комбинаций</span>
<table class="rag-sh-t">
<tr><th>Запрос</th><th>Смысл</th></tr>
<tr><td>"договор подряда" type:pdf after:2024-01-01</td><td>PDF-договоры подряда с 2024 года</td></tr>
<tr><td>счёт -НДС type:xlsx</td><td>Excel-счета без упоминания НДС</td></tr>
<tr><td>дог* и акт path:Финансы</td><td>Начинается на «дог» и содержит «акт», в папке Финансы</td></tr>
<tr><td>"акт сверки" или "акт выполненных"</td><td>Один из двух видов акта</td></tr>
<tr><td>after:2025-01-01 before:2025-12-31 creator:ivanov</td><td>Документы Иванова (создатель) за 2025 год</td></tr>
<tr><td>оплата editor:petrov type:pdf</td><td>PDF об оплате, где редактировал Петров</td></tr>
<tr><td>from:ivanov -черновик path:Договоры</td><td>Файлы Иванова в папке Договоры, без слова «черновик»</td></tr>
</table>
<span class="rag-sh-sec">Поддерживаемые форматы для метаданных авторов</span>
<table class="rag-sh-t">
<tr><td>DOCX, XLSX, PPTX</td><td>Создатель, последний редактор, самый частый редактор (при включённых правках)</td></tr>
<tr><td>PDF</td><td>Поле Author</td></tr>
<tr><td>DOC, XLS, PPT, другие</td><td>Метаданные авторов не извлекаются</td></tr>
</table>
"""

    def render_search_box() -> None:
        with ui.column().classes("rag-search-shell w-full max-w-5xl"):
            suggest_area = ui.column().classes("w-full")
            with ui.dialog() as help_dlg, ui.card().classes("w-[min(620px,96vw)] max-h-[90vh] overflow-auto p-0 gap-0"):
                with ui.row().classes("items-center justify-between px-4 pt-4 pb-1"):
                    ui.label("Операторы поиска").classes("text-base font-semibold")
                    ui.button(icon="close", on_click=help_dlg.close, color=None).props("flat round dense")
                ui.html(_SEARCH_HELP_HTML, sanitize=False).classes("px-2 pb-4")
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

                ui.button(icon="help_outline", on_click=help_dlg.open, color=None).props("flat round dense").tooltip("Синтаксис поиска")

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
        # Stop all periodic timers so they don't fire a concurrent render() while we await
        for _timer_attr in (
            "cloud_drive_timer", "scheduler_timer",
            "index_progress_timer", "stage_status_timer", "tg_login_timer",
        ):
            _t = getattr(state, _timer_attr, None)
            if _t is not None:
                _stop_managed_timer(_t)
                setattr(state, _timer_attr, None)
        target = current_content_container()
        target.clear()
        initialized_screens.discard(state.screen)
        with target:
            render_search_header()
            ui.spinner(size="lg").classes("mt-4")
            ui.label("Ищу совпадения...").classes("rag-meta")
        initialized_screens.add(state.screen)

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

    def _render_preview_body(candidate: Path, viewer_url: str, ext: str) -> None:
        if ext == ".pdf":
            ui.html(
                f'<iframe src="{html.escape(viewer_url, quote=True)}" '
                'style="width:100%;height:calc(100vh - 240px);border:1px solid var(--rag-border);'
                'border-radius:8px;"></iframe>',
                sanitize=False,
            )
        elif ext in INLINE_IMAGE_EXTENSIONS:
            ui.image(viewer_url).style("max-width:100%;height:auto;border-radius:8px")
        elif ext in FILE_PREVIEW_EXTENSIONS:
            ui.label(_preview_file(candidate, limit=32000)).classes("rag-code")
        elif ext in OFFICE_PREVIEW_EXTENSIONS:
            ui.label(_preview_office_file(candidate, limit=32000)).classes("rag-code")
            ui.label("Текстовый извлечённый фрагмент.").classes("rag-meta")
        else:
            ui.label("Встроенный просмотр не поддерживается — используйте «Скачать».").classes("rag-meta")

    def _render_meta_body(candidate: Path) -> None:
        try:
            stat = candidate.stat()
            rows = [
                ("Размер", f"{stat.st_size:,} байт".replace(",", " ")),
                ("Изменён", datetime.fromtimestamp(stat.st_mtime).strftime("%d.%m.%Y %H:%M")),
                ("Создан", datetime.fromtimestamp(stat.st_ctime).strftime("%d.%m.%Y %H:%M")),
                ("Тип", candidate.suffix.lower()),
                ("Путь", str(candidate.parent)),
            ]
        except Exception:
            rows = [("Файл", str(candidate))]
        for k, v in rows:
            with ui.element("div").style(
                "display:grid;grid-template-columns:140px 1fr;padding:10px 0;"
                "border-bottom:1px solid var(--rag-border)"
            ):
                ui.label(k).style(
                    "font-family:var(--rag-font-mono);font-size:10px;text-transform:uppercase;"
                    "letter-spacing:0.1em;color:var(--rag-muted)"
                )
                ui.label(str(v)).style("font-family:var(--rag-font-mono);font-size:12px;word-break:break-all")

    def _render_cloud_preview_body(cloud_path: str, name: str, mime_type: str) -> None:
        preview_url = f"/api/cloud-drive/preview?path={quote(cloud_path, safe='')}"
        ext = Path(name or cloud_path).suffix.lower()
        mime = str(mime_type or "").lower()
        if ext == ".pdf" or mime == "application/pdf":
            ui.html(
                f'<iframe src="{html.escape(preview_url, quote=True)}" '
                'style="width:100%;height:calc(100vh - 240px);border:1px solid var(--rag-border);'
                'border-radius:8px;"></iframe>',
                sanitize=False,
            )
        elif ext in INLINE_IMAGE_EXTENSIONS or mime.startswith("image/"):
            ui.image(preview_url).style("max-width:100%;height:auto;border-radius:8px")
        elif ext in FILE_PREVIEW_EXTENSIONS or mime.startswith("text/"):
            ui.html(
                f'<iframe src="{html.escape(preview_url, quote=True)}" '
                'style="width:100%;height:calc(100vh - 240px);border:1px solid var(--rag-border);'
                'border-radius:8px;background:white;"></iframe>',
                sanitize=False,
            )
        elif ext in OFFICE_PREVIEW_EXTENSIONS:
            ui.label("Для Office-файлов пока доступно скачивание. Текстовый preview будет добавлен отдельным extractor-backed этапом.").classes("rag-meta")
        else:
            ui.label("Встроенный просмотр для этого типа файла недоступен — используйте «Скачать».").classes("rag-meta")

    def _render_cloud_meta_body(file_info: Dict[str, Any]) -> None:
        rows = [
            ("Размер", _cd_file_size(int(file_info.get("size_bytes") or 0)) if file_info.get("size_bytes") else "—"),
            ("Тип", str(file_info.get("mime_type") or Path(str(file_info.get("name") or "")).suffix.lower() or "—")),
            ("Cloud path", str(file_info.get("path") or "")),
            ("Storage key", str(file_info.get("storage_key") or "")),
        ]
        for k, v in rows:
            with ui.element("div").style(
                "display:grid;grid-template-columns:140px 1fr;padding:10px 0;"
                "border-bottom:1px solid var(--rag-border)"
            ):
                ui.label(k).style(
                    "font-family:var(--rag-font-mono);font-size:10px;text-transform:uppercase;"
                    "letter-spacing:0.1em;color:var(--rag-muted)"
                )
                ui.label(str(v)).style("font-family:var(--rag-font-mono);font-size:12px;word-break:break-all")

    def open_file_viewer(path_value: Path | str) -> None:
        candidate = _resolve_catalog_file(state.cfg, str(path_value or ""))
        if candidate is None:
            ui.notify("Файл недоступен для просмотра.", type="warning")
            return
        viewer_url = _viewer_file_url(str(candidate))
        ext = candidate.suffix.lower()

        preview_drawer.clear()
        active_tab: List[str] = ["preview"]
        _tab_refs: Dict[str, Any] = {}

        with preview_drawer:
            # Header
            with ui.element("div").classes("rag-preview-drawer-header"):
                with ui.element("div").style("flex:1;min-width:0"):
                    ui.label(candidate.name).style(
                        "font-family:var(--rag-font-display);font-weight:600;font-size:14px;"
                        "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:block"
                    )
                    ui.label(str(candidate)).classes("rag-path").style("margin-top:2px;display:block")
                ui.button(icon="close", on_click=close_preview_drawer, color=None).props("flat round dense")

            # Tabs row
            tabs_row = ui.element("div").classes("rag-preview-drawer-tabs")
            body_el = ui.element("div").classes("rag-preview-drawer-body")

            def _refresh_body() -> None:
                body_el.clear()
                with body_el:
                    if active_tab[0] == "preview":
                        _render_preview_body(candidate, viewer_url, ext)
                    elif active_tab[0] == "meta":
                        _render_meta_body(candidate)
                    else:
                        ui.label("Чанки документа в индексе (заглушка).").classes("rag-meta")

            def _make_tab(key: str, label: str) -> None:
                btn = ui.element("button").classes(
                    f"rag-preview-drawer-tab {'active' if active_tab[0] == key else ''}"
                )
                btn._text = label  # type: ignore[attr-defined]

                def _click(k: str = key) -> None:
                    active_tab[0] = k
                    for tk, te in _tab_refs.items():
                        te.classes(remove="active")
                        if tk == k:
                            te.classes(add="active")
                    _refresh_body()

                btn.on("click", _click)
                _tab_refs[key] = btn

            with tabs_row:
                _make_tab("preview", "Превью")
                _make_tab("meta", "Метаданные")
                _make_tab("chunks", "Чанки")

            _refresh_body()

            # Footer actions
            with ui.element("div").classes("rag-preview-drawer-actions"):
                ui.button("Скачать", icon="download",
                          on_click=lambda p=candidate: ui.download(p, filename=p.name))\
                    .props("unelevated dense").classes("flex-1")
                ui.button("В проводник", icon="folder_open",
                          on_click=lambda p=candidate: (close_preview_drawer(),
                                                        set_screen("explorer"),
                                                        state.__setattr__("explorer_path", str(p.parent)),
                                                        render()))\
                    .props("outline dense")
                ui.button("Открыть в ОС", icon="open_in_new",
                          on_click=lambda p=candidate: _select_in_os_explorer(str(p)))\
                    .props("outline dense")

        preview_drawer.classes(remove="closed")
        preview_drawer_scrim.classes(remove="closed")

    def open_cloud_file_viewer(file_value: Any) -> None:
        file_info: Dict[str, Any] = {}
        if isinstance(file_value, str):
            cloud_path = str(file_value or "").strip()
            svc = _cd_get_service(state.cfg)
            row = None
            if svc is not None and cloud_path:
                try:
                    row = svc.registry.get_file_by_path(cloud_path)
                except Exception:
                    row = None
            if row is not None:
                file_info = {
                    "path": row.path,
                    "name": row.name,
                    "mime_type": row.mime_type,
                    "size_bytes": row.size_bytes,
                    "storage_key": row.storage_key,
                }
            else:
                file_info = {"path": cloud_path, "name": cloud_path.rsplit("/", 1)[-1]}
        else:
            file_info = {
                "path": str(getattr(file_value, "path", "") or ""),
                "name": str(getattr(file_value, "name", "") or ""),
                "mime_type": str(getattr(file_value, "mime_type", "") or ""),
                "size_bytes": getattr(file_value, "size_bytes", None),
                "storage_key": str(getattr(file_value, "storage_key", "") or ""),
            }
        cloud_path = str(file_info.get("path") or "").strip()
        name = str(file_info.get("name") or cloud_path.rsplit("/", 1)[-1] or "Файл")
        if not cloud_path:
            ui.notify("Файл Cloud Drive недоступен для просмотра.", type="warning")
            return
        preview_url = f"/api/cloud-drive/preview?path={quote(cloud_path, safe='')}"
        download_url = f"/api/cloud-drive/download?path={quote(cloud_path, safe='')}"

        preview_drawer.clear()
        active_tab: List[str] = ["preview"]
        tab_refs: Dict[str, Any] = {}

        with preview_drawer:
            with ui.element("div").classes("rag-preview-drawer-header"):
                with ui.element("div").style("flex:1;min-width:0"):
                    ui.label(name).style(
                        "font-family:var(--rag-font-display);font-weight:600;font-size:14px;"
                        "white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:block"
                    )
                    ui.label(f"Cloud Drive: {cloud_path}").classes("rag-path").style("margin-top:2px;display:block")
                ui.button(icon="close", on_click=close_preview_drawer, color=None).props("flat round dense")

            tabs_row = ui.element("div").classes("rag-preview-drawer-tabs")
            body_el = ui.element("div").classes("rag-preview-drawer-body")

            def _refresh_body() -> None:
                body_el.clear()
                with body_el:
                    try:
                        if active_tab[0] == "preview":
                            _render_cloud_preview_body(cloud_path, name, str(file_info.get("mime_type") or ""))
                        elif active_tab[0] == "meta":
                            _render_cloud_meta_body(file_info)
                        else:
                            ui.label(
                                "Чанки Cloud Drive документа в индексе будут доступны в следующем этапе preview."
                            ).classes("rag-meta")
                    except Exception as exc:
                        ui.label(f"Не удалось построить preview: {exc}").classes("text-negative text-sm")

            def _make_tab(key: str, label: str) -> None:
                btn = ui.element("button").classes(
                    f"rag-preview-drawer-tab {'active' if active_tab[0] == key else ''}"
                )
                btn._text = label  # type: ignore[attr-defined]

                def _click(k: str = key) -> None:
                    active_tab[0] = k
                    for tk, te in tab_refs.items():
                        te.classes(remove="active")
                        if tk == k:
                            te.classes(add="active")
                    _refresh_body()

                btn.on("click", _click)
                tab_refs[key] = btn

            with tabs_row:
                _make_tab("preview", "Превью")
                _make_tab("meta", "Метаданные")
                _make_tab("chunks", "Чанки")

            _refresh_body()

            with ui.element("div").classes("rag-preview-drawer-actions"):
                ui.button("Скачать", icon="download",
                          on_click=lambda url=download_url: ui.navigate.to(url, new_tab=True))\
                    .props("unelevated dense").classes("flex-1")
                ui.button("Открыть", icon="open_in_new",
                          on_click=lambda url=preview_url: ui.navigate.to(url, new_tab=True))\
                    .props("outline dense")
                ui.button("В Cloud Drive", icon="cloud",
                          on_click=lambda p=cloud_path: (close_preview_drawer(), go_cloud_explorer_path(p)))\
                    .props("outline dense")

        preview_drawer.classes(remove="closed")
        preview_drawer_scrim.classes(remove="closed")

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
                track_result_use("open_cloud_preview")
                open_cloud_file_viewer(cloud_path)

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
                            def _cd_preview(pth: str = cloud_path) -> None:
                                track_result_use("cloud_preview")
                                open_cloud_file_viewer(pth)

                            ui.button(
                                "Просмотр",
                                icon="visibility",
                                on_click=_cd_preview,
                            ).props("outline dense no-caps")
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
                            def _go_file(file_row: Any = f) -> None:
                                if getattr(file_row, "storage_key", ""):
                                    open_cloud_file_viewer(file_row)
                                    return
                                fpath = str(getattr(file_row, "source_path", "") or getattr(file_row, "path", "") or "")
                                p = Path(fpath) if fpath else None
                                if p and p.exists() and p.is_file():
                                    open_file_viewer(p)
                                    return
                                ui.notify(f"Файл «{getattr(file_row, 'name', 'Файл')}» недоступен на диске.", type="warning")
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
                                if f.storage_key:
                                    ui.button(
                                        icon="visibility",
                                        on_click=lambda file_row=f: open_cloud_file_viewer(file_row),
                                        color=None,
                                    ).props("flat round dense").tooltip("Просмотреть файл")
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
        if state.search_stats_hint and not state.search_lazy_loading:
            ui.label(state.search_stats_hint).classes("rag-meta")
        if state.search_lazy_loading:
            with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
                ui.spinner(size="sm")
                ui.label("Ищу совпадения…").classes("rag-meta")

        # RAG Q&A карточка (основной ответ по всем результатам) — glow-card
        if state.rag_answer_loading or state.rag_answer_text:
            with ui.element("div").classes("rag-glow-card w-full").style("margin-bottom:16px"):
                # Header
                with ui.row().classes("items-center gap-3 w-full"):
                    with ui.element("div").classes("rag-ai-badge"):
                        ui.icon("auto_awesome", size="14px")
                    with ui.column().classes("gap-0 min-w-0 flex-1"):
                        ui.label("AI ответ").classes("rag-ai-title")
                        _rag_model = str(state.cfg.get("llm_rag_model") or "qwen3:8b")
                        _src_count = len(state.rag_answer_sources or [])
                        if state.rag_answer_loading:
                            _ai_meta = f"{_rag_model} · думает…"
                        else:
                            _ai_meta = f"{_rag_model} · {_src_count} источник{'ов' if _src_count != 1 else ''}"
                        ui.label(_ai_meta).classes("rag-ai-meta")
                    if not state.rag_answer_ok and not state.rag_answer_loading:
                        ui.label("⚠ может быть неточным").classes("rag-chip").style("color:#b45309;background:rgba(251,191,36,.15)")
                # Body
                if state.rag_answer_loading:
                    with ui.row().classes("items-center gap-2 mt-3"):
                        ui.spinner(size="sm")
                        ui.label("Анализирую найденные документы…").classes("rag-meta")
                else:
                    ui.html(
                        f'<p style="font-size:14px;line-height:1.6;margin:12px 0 0;white-space:pre-wrap">'
                        f'{html.escape(state.rag_answer_text)}</p>',
                        sanitize=False,
                    )
                    if state.rag_answer_sources:
                        with ui.row().classes("gap-2 mt-3 flex-wrap"):
                            for _src in list(state.rag_answer_sources)[:6]:
                                _fname = str(_src.get("filename") or _src.get("path") or "—")[:40]
                                _fpath = Path(str(_src.get("full_path") or ""))
                                _page = _src.get("page")
                                _prov = f" · стр.{_page}" if _page is not None else ""
                                _chip = ui.label(_fname + _prov).classes("rag-chip rag-chip-active").style(
                                    "max-width:240px;overflow:hidden;text-overflow:ellipsis;cursor:pointer"
                                )
                                _chip.tooltip(str(_src.get("full_path") or _fname))
                                if _fpath.exists() and _fpath.is_file():
                                    _chip.on("click", lambda p=_fpath: open_file_viewer(p))
                            if len(state.rag_answer_sources) > 6:
                                ui.label(f"+{len(state.rag_answer_sources) - 6} ещё").classes("rag-chip")

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

        if not state.results and not state.search_lazy_loading:
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


    def render_explorer_screen() -> None:
        _explorer_view.render_explorer_screen(
            state,
            render_fn=render,
            go_explorer_fn=go_explorer,
            open_file_viewer_fn=open_file_viewer,
            open_cloud_file_viewer_fn=open_cloud_file_viewer,
            choose_query_fn=choose_query,
            query_handler=choose_query_handler,
        )

    def render_index_screen() -> None:
        _index_view.render_index_screen(
            state,
            render_fn=render,
            access_denied=render_access_denied,
            settings_fn=lambda: go_settings_section("indexing"),
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
        tg_login_token = {"value": ""}

        def _complete_login(user: Dict[str, Any], *, event_type: str) -> None:
            complete_login_session(state, user, event_type=event_type)
            _load_user_state(state)
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
            for inp in (reg_username_input, reg_display_input, reg_tg_user_input):
                inp.value = ""
                inp.update()

        # ── Split layout — break out of page_root padding ──────────────────
        with ui.element("div").classes("rag-login-split").style(
            "position:fixed;inset:56px 0 0 0;z-index:1"
        ):

            # ── LEFT: brand panel ───────────────────────────────────────────
            with ui.element("div").classes("rag-login-brand"):
                ui.element("div").classes("rag-login-brand-liquid")
                ui.element("div").classes("rag-login-brand-grid")

                # Top: logo + version badge
                with ui.element("div").style("display:flex;align-items:center;justify-content:space-between;margin-bottom:auto"):
                    with ui.element("div").style("display:flex;align-items:center;gap:12px"):
                        if LOGO_PATH.exists():
                            ui.image("/rag-logo.png").style("width:32px;height:32px;border-radius:8px")
                        ui.label("Rag-search").style("font-family:var(--rag-font-display);font-weight:700;font-size:16px;letter-spacing:-0.02em;color:#fff")
                    ui.element("div").style(
                        "display:flex;align-items:center;gap:6px;padding:4px 10px;"
                        "background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);"
                        "border-radius:999px;font-family:var(--rag-font-mono);font-size:10px;color:rgba(255,255,255,.7)"
                    ).text = "● онлайн · v3"

                # Hero text
                with ui.element("div").style("margin-top:auto;margin-bottom:52px"):
                    ui.label("ВНУТРЕННИЙ ПОИСК КОМПАНИИ").style(
                        "font-family:var(--rag-font-mono);font-size:10px;font-weight:500;"
                        "letter-spacing:0.12em;color:rgba(255,255,255,.4);margin-bottom:24px;display:block"
                    )
                    ui.html(
                        "<h1 style=\"font-family:'Manrope',system-ui,sans-serif;font-weight:800;"
                        "font-size:clamp(48px,5.5vw,84px);letter-spacing:-0.04em;line-height:0.92;"
                        "margin:0;color:#fff\">"
                        "Найдём всё,<br>"
                        "<span style=\"color:#8aabff\">за секунду.</span>"
                        "</h1>"
                    )

                    # Live stats
                    with ui.element("div").style("display:flex;gap:0;margin-top:44px;max-width:480px"):
                        for i, (num, lbl) in enumerate([
                            ("65 664", "документов в индексе"),
                            ("187",    "поисков сегодня"),
                            ("0.42с",  "среднее время"),
                        ]):
                            style = "display:flex;flex-direction:column;gap:6px;" + (
                                "padding-right:24px;" if i < 2 else ""
                            ) + ("padding-left:24px;border-left:1px solid rgba(255,255,255,.1);" if i > 0 else "")
                            with ui.element("div").style(style):
                                ui.label(num).classes("rag-login-stat-num")
                                ui.label(lbl).style(
                                    "font-family:var(--rag-font-mono);font-size:10px;font-weight:500;"
                                    "text-transform:uppercase;letter-spacing:0.1em;color:rgba(255,255,255,.4)"
                                )

                # Activity feed
                with ui.element("div").style("margin-bottom:24px"):
                    ui.label("↗ команда сейчас ищет").style(
                        "font-family:var(--rag-font-mono);font-size:10px;letter-spacing:0.08em;"
                        "color:rgba(255,255,255,.35);text-transform:uppercase;margin-bottom:10px;display:block"
                    )
                    for t, who, q in [
                        ("14:23", "А. Иванов",  "карточка предприятия Спецмаш"),
                        ("14:21", "М. Петрова", "паспорт цыбусов 2024"),
                        ("14:19", "Д. Сидоров", "договор поставки № 442"),
                    ]:
                        with ui.element("div").classes("rag-login-activity-row"):
                            ui.label(t)
                            ui.label(who)
                            ui.label(q).style("color:#8aabff;overflow:hidden;text-overflow:ellipsis;white-space:nowrap")

                # Bottom
                with ui.element("div").style(
                    "display:flex;justify-content:space-between;font-family:var(--rag-font-mono);"
                    "font-size:10px;color:rgba(255,255,255,.28);text-transform:uppercase;letter-spacing:0.08em"
                ):
                    ui.label("v3 · production")
                    ui.label("internal use only")

            # ── RIGHT: form panel ───────────────────────────────────────────
            with ui.element("div").classes("rag-login-form-panel"):
                # Top bar
                with ui.element("div").style("display:flex;justify-content:space-between;align-items:center;margin-bottom:auto"):
                    with ui.element("div").style("display:flex;align-items:center;gap:8px"):
                        ui.label("прод. среда").style(
                            "font-family:var(--rag-font-mono);font-size:11px;padding:2px 8px;"
                            "border:1px solid var(--rag-border);border-radius:4px;color:var(--rag-muted)"
                        )
                        ui.label("·").style("color:var(--rag-muted)")
                        ui.label("rag-search.local").style("font-family:var(--rag-font-mono);font-size:12px;color:var(--rag-muted)")

                # Session-expired warning
                if state.session_expired:
                    with ui.row().classes("items-center gap-2 w-full").style(
                        "background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px;margin-top:16px"
                    ):
                        ui.icon("schedule").style("color:#f59e0b")
                        ui.label("Сессия истекла — выполните вход снова.").style("color:#92400e;font-size:13px")

                # Form content (centered)
                with ui.element("div").style(
                    "align-self:center;justify-self:center;display:flex;flex-direction:column;"
                    "gap:16px;width:100%;max-width:400px;margin:auto"
                ):
                    with ui.element("div").style("margin-bottom:4px"):
                        ui.label("Вход").style(
                            "font-family:var(--rag-font-display);font-weight:800;"
                            "font-size:40px;letter-spacing:-0.03em;line-height:1;margin:0;display:block"
                        )
                        ui.label("Используйте корпоративный аккаунт.").style(
                            "color:var(--rag-muted);font-size:14px;margin-top:8px;display:block"
                        )

                    tabs = ui.tabs().classes("w-full")
                    with tabs:
                        tab_login = ui.tab("Войти", icon="login")
                        tab_register = ui.tab("Зарегистрироваться", icon="person_add")

                    with ui.tab_panels(tabs, value=tab_login).classes("w-full"):
                        with ui.tab_panel(tab_login).classes("w-full gap-3"):
                            username_input = ui.input("Логин или email").props("dense outlined").classes("w-full")
                            password_input = ui.input("Пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                            username_input.on("keyup.enter", lambda _: ui.run_javascript(
                                "const ins=document.querySelectorAll('.q-field__native,input[type=password]');"
                                "const i=Array.from(ins).findIndex(el=>el===document.activeElement);"
                                "if(i>=0&&ins[i+1])ins[i+1].focus();"
                            ))
                            password_input.on("keyup.enter", lambda _: login())
                            ui.button("Войти", icon="login", on_click=login).props("unelevated").classes("w-full").style(
                                "height:48px;font-family:var(--rag-font-display);font-size:15px;font-weight:600;letter-spacing:-0.01em"
                            )
                            with ui.element("div").classes("rag-divider-text"):
                                ui.label("или")
                            tg_btn = ui.button("Войти через Telegram", icon="send", on_click=request_tg_login).props("outline").classes("w-full")
                            bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
                            tg_btn.set_visibility(bool(bot_link))

                        with ui.tab_panel(tab_register).classes("w-full gap-3"):
                            reg_username_input = ui.input("Логин").props("dense outlined").classes("w-full")
                            reg_display_input = ui.input("Имя").props("dense outlined").classes("w-full")
                            reg_tg_user_input = ui.input("Telegram username (необязательно)").props("dense outlined").classes("w-full")
                            reg_tg_user_input.on("keyup.enter", lambda _: register_request())
                            ui.button("Отправить заявку", icon="how_to_reg", on_click=register_request).props("unelevated").classes("w-full")
                            ui.label("После одобрения администратором вы получите доступ.").classes("rag-meta")

                # Status strip
                with ui.element("div").style(
                    "display:grid;grid-template-columns:1fr 1fr;gap:0;"
                    "border-top:1px solid var(--rag-border);margin:0 -32px;padding:0 32px;margin-top:auto"
                ):
                    for dot_cls, lbl, sub in [
                        ("ok",   "индекс актуален",  "13.05.2026"),
                        ("info", "индексация идёт",  "быстрый проход"),
                    ]:
                        with ui.element("div").style(
                            "display:flex;align-items:center;gap:10px;padding:14px 0;"
                            "font-size:12px;color:var(--rag-text)"
                        ):
                            ui.element("span").classes(f"rag-dot {dot_cls}")
                            ui.label(lbl)
                            ui.label(sub).style("margin-left:auto;font-family:var(--rag-font-mono);font-size:11px;color:var(--rag-muted)")

        _stop_managed_timer(state.tg_login_timer)
        state.tg_login_timer = ui.timer(2.0, poll_tg_login)


    # ── Admin / settings screens ──────────────────────────────────────────────

    def render_settings_screen() -> None:
        _settings_view.render_settings_screen(
            state,
            render_fn=render,
            query_handler=choose_query_handler,
            go_explorer_fn=go_explorer,
            open_file_viewer_fn=open_file_viewer,
            index_dashboard_fn=render_index_screen,
            logout_fn=do_logout,
        )

    # ── Analytics / stats screen ───────────────────────────────────────────

    def render_stats_screen() -> None:
        _stats_view.render_stats_screen(
            state,
            access_denied=render_access_denied,
            query_handler=choose_query_handler,
        )

    def render_jobs_screen() -> None:
        _jobs_view.render_jobs_screen(state, render_fn=render)

    def render() -> None:
        page_root.classes(remove="search")
        if state.screen == "search":
            page_root.classes(add="search")
        header_title.set_text({
            **APP_SCREEN_TITLES,
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
        if state.screen != "jobs":
            _stop_managed_timer(getattr(state, "jobs_refresh_timer", None))
            try:
                state.jobs_refresh_timer = None  # type: ignore[attr-defined]
            except Exception:
                pass
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
        current_screen = state.screen
        capture_screen_state(state, current_screen)
        if state.current_user is None:
            content.clear()
            screen_containers.clear()
            initialized_screens.clear()
            dirty_screens.clear()
            active_screen_ref[0] = None
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
            try:
                settings_button.set_visibility(False)
            except Exception:
                pass
            with content:
                render_login_screen()
            return
        if int((state.current_user or {}).get("must_change_password") or 0):
            content.clear()
            screen_containers.clear()
            initialized_screens.clear()
            dirty_screens.clear()
            active_screen_ref[0] = None
            try:
                drawer.set_visibility(False)
            except Exception:
                pass
            try:
                menu_button.set_visibility(False)
            except Exception:
                pass
            try:
                settings_button.set_visibility(False)
            except Exception:
                pass
            with content:
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
        try:
            settings_button.set_visibility(state.screen != "settings")
        except Exception:
            pass
        dark_mode.set_value(state.theme == "dark")
        touch_activity()

        target = screen_container(current_screen)
        for screen_name, container in screen_containers.items():
            container.set_visibility(screen_name == current_screen)

        previous_screen = active_screen_ref[0]
        rebuild = should_rebuild_screen_container(
            current_screen,
            previous_screen,
            initialized_screens,
            dirty_screens,
        )
        if rebuild:
            target.clear()
            with target:
                if state.screen == "explorer":
                    try:
                        drawer.set_visibility(True)
                    except Exception:
                        pass
                    render_explorer_screen()
                elif state.screen == "index":
                    render_index_screen()
                elif state.screen == "settings":
                    render_settings_screen()
                elif state.screen == "stats":
                    render_stats_screen()
                elif state.screen == "jobs":
                    render_jobs_screen()
                else:
                    render_search_screen()
            initialized_screens.add(current_screen)
            dirty_screens.discard(current_screen)
        active_screen_ref[0] = current_screen
        if _client_alive():
            with target:
                ui.timer(
                    0.05,
                    lambda screen=current_screen: ui.run_javascript(
                        "(() => {"
                        f"const v = Number(sessionStorage.getItem('rag-scroll-{screen}') || 0);"
                        "if (Number.isFinite(v) && v > 0) window.scrollTo({top:v, behavior:'instant'});"
                        "})();"
                    ),
                    once=True,
                )
                ui.timer(
                    0.08,
                    lambda: ui.run_javascript("window.ragHideBusy && window.ragHideBusy();"),
                    once=True,
                )

    # ── Preview drawer (живёт вне content, не сбрасывается при render()) ──
    preview_drawer = ui.element("div").classes("rag-preview-drawer closed")
    preview_drawer_scrim = ui.element("div").classes("rag-preview-drawer-scrim closed")

    def close_preview_drawer() -> None:
        preview_drawer.classes(add="closed")
        preview_drawer_scrim.classes(add="closed")
        preview_drawer.clear()

    preview_drawer_scrim.on("click", lambda: close_preview_drawer())

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


@ui.page("/settings")
def settings_page() -> None:
    _build_page("settings")


@ui.page("/stats")
def stats_page() -> None:
    _build_page("stats")


@ui.page("/jobs")
def jobs_page() -> None:
    _build_page("jobs")


@ui.page("/cloud")
def cloud_page() -> None:
    ui.navigate.to("/explorer")


@ui.page("/auth/device")
def device_auth_page() -> None:
    """Browser approval page for sync client device auth flow."""
    from . import device_auth as _da
    from .state import PageState, _get_auth_db

    cfg = load_config()
    state = PageState(cfg=cfg)

    from .auth_session import complete_login_session, restore_session
    restore_session(state)

    _install_css()

    # Pre-fill code from query string (?code=XXXX-YYYY)
    code_param = ""
    try:
        request_url = str(ui.context.client.url or "")
        if "code=" in request_url:
            code_param = request_url.split("code=")[-1].split("&")[0].strip()
    except Exception:
        pass

    with ui.column().classes("items-center justify-center min-h-screen gap-4 p-4"):
        with ui.card().classes("p-8 gap-4 w-full max-w-sm"):
            with ui.row().classes("items-center gap-3 mb-1"):
                ui.icon("sync", size="28px").classes("text-indigo-400")
                ui.label("Подключение устройства").classes("text-xl font-semibold")

            if state.current_user is None:
                ui.label(
                    "Войдите в RAG Catalog, чтобы подтвердить подключение sync-клиента."
                ).classes("rag-meta text-sm")
                ui.separator()

                username_in = ui.input("Логин").props("outlined dense")
                password_in = ui.input("Пароль", password=True, password_toggle_button=True).props("outlined dense")
                error_lbl = ui.label("").classes("text-negative text-sm")

                def do_login() -> None:
                    auth_db = _get_auth_db(state)
                    result = auth_db.login_with_reason(
                        username=str(username_in.value or "").strip(),
                        password=str(password_in.value or ""),
                    )
                    reason = str(result.get("reason") or "")
                    user = result.get("user")
                    if reason == "ok" and user:
                        complete_login_session(state, user, event_type="login")
                        ui.navigate.reload()
                    elif reason == "pending":
                        error_lbl.set_text("Аккаунт ещё не активирован администратором.")
                    elif reason == "blocked":
                        error_lbl.set_text("Аккаунт заблокирован.")
                    else:
                        error_lbl.set_text("Неверный логин или пароль.")

                password_in.on("keydown.enter", do_login)
                ui.button("Войти", on_click=do_login).props("unelevated").classes("w-full")

            else:
                user = state.current_user
                ui.label(
                    f"Вы вошли как {user.get('display_name') or user.get('username')}."
                ).classes("rag-meta text-sm")
                ui.label(
                    "Введите код, который показывает sync-клиент на вашем компьютере."
                ).classes("text-sm")
                ui.separator()

                code_in = ui.input(
                    "Код устройства",
                    value=code_param,
                    placeholder="ABCD-EFGH",
                ).props("outlined dense")
                result_row = ui.row().classes("items-center gap-2")
                result_lbl = ui.label("").classes("text-sm")

                def do_approve() -> None:
                    code = str(code_in.value or "").strip()
                    if not code:
                        ui.notify("Введите код устройства.", type="warning")
                        return
                    tok = str(app.storage.user.get("auth_token") or "")
                    username = str(user.get("username") or "")
                    ok = _da.approve_code(code, tok, username)
                    if ok:
                        with result_row:
                            ui.icon("check_circle", size="20px").classes("text-positive")
                        result_lbl.set_text("Устройство подтверждено! Можете закрыть это окно.")
                        result_lbl.classes("text-positive")
                        ui.notify("Sync-клиент подключён.", type="positive")
                    else:
                        ui.notify(
                            "Код не найден, уже использован или истёк. Перезапустите sync-клиент.",
                            type="negative",
                        )

                code_in.on("keydown.enter", do_approve)
                ui.button(
                    "Подтвердить подключение", icon="link", on_click=do_approve
                ).props("unelevated").classes("w-full")

                ui.separator()
                ui.label(
                    "Код действителен 5 минут. Если истёк — перезапустите sync-клиент."
                ).classes("rag-meta text-xs")


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
    _start_cloud_drive_job_worker(cfg)
    _start_recovery_watchdog(cfg)
    _start_global_scheduler(cfg)
    if bool(cfg.get("search_warmup_enabled", True)):
        threading.Thread(target=_warm_searcher_cache, args=(cfg,), name="search-warmup", daemon=True).start()
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
