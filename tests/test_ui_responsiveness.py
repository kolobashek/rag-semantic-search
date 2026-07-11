from __future__ import annotations

import inspect
import weakref
from collections import deque

from rag_catalog.ui import css, nice_app, settings_view


def test_primary_screen_registry_covers_all_route_pages() -> None:
    specs = list(nice_app.APP_SCREEN_SPECS)
    keys = {str(spec["key"]) for spec in specs}

    assert keys == {"search", "explorer", "jobs", "index", "stats", "settings"}
    assert nice_app.APP_SCREEN_ROUTES == {
        "search": "/search",
        "explorer": "/explorer",
        "jobs": "/jobs",
        "index": "/index",
        "stats": "/stats",
        "settings": "/settings",
    }
    assert nice_app.APP_SCREEN_TITLES["explorer"] == "Проводник"

    for key in keys:
        assert hasattr(nice_app, f"{key}_page")


def test_navigation_registry_marks_admin_and_public_screens() -> None:
    specs = {str(spec["key"]): spec for spec in nice_app.APP_SCREEN_SPECS}

    public_keys = {key for key, spec in specs.items() if not spec.get("admin_only")}
    admin_keys = {key for key, spec in specs.items() if spec.get("admin_only")}
    header_keys = {key for key, spec in specs.items() if spec.get("header")}
    drawer_keys = {key for key, spec in specs.items() if spec.get("drawer")}

    assert public_keys == {"search", "explorer", "jobs", "settings"}
    assert admin_keys == {"index", "stats"}
    assert header_keys == {"search", "explorer", "jobs", "index"}
    assert drawer_keys == {"search", "explorer", "jobs", "index", "stats"}


def test_screen_transitions_have_busy_feedback_contract() -> None:
    source = inspect.getsource(nice_app._build_page)

    assert "window.ragShowBusy" in source
    assert "Открываю экран" in source
    assert "window.ragHideBusy" in source
    assert "APP_SCREEN_SPECS" in source
    assert "search-empty" in source
    assert "search-active" in source

    for key in nice_app.APP_SCREEN_ROUTES:
        if key == "settings":
            continue
        assert f'render_{key}_screen()' in source or f'"{key}"' in source


def test_global_click_feedback_and_skeleton_are_installed() -> None:
    source = inspect.getsource(css._install_css)
    interaction_js = css.INTERACTION_JS_PATH.read_text(encoding="utf-8")

    for token in (
        "rag-global-busy",
        "rag-busy-spinner",
        "rag-busy-skeleton",
        "/rag-interactions.js",
        "@keyframes rag-loading-bar",
    ):
        assert token in source

    for token in (
        "window.ragShowBusy",
        "window.ragHideBusy",
        "clickBusy",
        "document.addEventListener('click', clickBusy, true)",
    ):
        assert token in interaction_js


def test_dark_theme_has_prepaint_before_frontend_boot() -> None:
    css_source = inspect.getsource(css._install_css)
    page_source = inspect.getsource(nice_app._build_page)

    assert "localStorage.getItem('rag-theme')" in css_source
    assert "document.documentElement.dataset.ragTheme" in css_source
    assert 'html[data-rag-theme="dark"]' in css_source
    assert "#0c0c0f" in css_source
    assert "_install_css(state.theme)" in page_source
    assert "localStorage.setItem('rag-theme'" in page_source


def test_header_nav_not_hidden_by_breadcrumbs_on_desktop() -> None:
    source = inspect.getsource(css._install_css)
    breadcrumb_rule = ".rag-hdr-center:has(.rag-header-breadcrumbs:not(:empty)) .rag-hdr-nav"
    rule_index = source.index(breadcrumb_rule)
    breakpoint_index = source.rindex("@media (max-width: 1100px)", 0, rule_index)

    assert breakpoint_index < rule_index
    assert ".rag-mobile-menu-button" in source[breakpoint_index:rule_index]
    assert ".rag-hdr-nav { display: none; }" in source


def test_desktop_explorer_breadcrumbs_are_compact_but_separate_from_actions() -> None:
    css_source = inspect.getsource(css._install_css)
    explorer_source = inspect.getsource(__import__("rag_catalog.ui.explorer_view", fromlist=["render_explorer_screen"]).render_explorer_screen)

    assert ".rag-explorer-inline-breadcrumbs" in css_source
    assert "flex: 0 1 auto" in css_source
    assert "max-width: min(360px, 32vw)" in css_source
    assert "width: min(720px, 100%)" not in css_source
    assert "body:has(.rag-explorer-v2-layout) .q-page > .nicegui-content" in css_source
    assert ".rag-page:has(.rag-explorer-v2-layout)" in css_source
    assert "padding-top: 0" in css_source
    assert "padding-top: 6px" not in css_source
    assert "_render_cd_inline_breadcrumbs()\n            with ui.row().classes(\"rag-explorer-actionline\"):" in explorer_source
    assert "_render_fs_inline_breadcrumbs()\n            with ui.row().classes(\"w-full items-center gap-2\"):" in explorer_source


def test_search_header_animation_avoids_vertical_jump() -> None:
    source = inspect.getsource(css._install_css)

    assert "rag-search-settle" in source
    assert "translateY(18px)" not in source
    assert "rag-search-rise" not in source


def test_ollama_endpoint_probe_is_fast_and_tolerant(monkeypatch) -> None:
    calls = []

    class DummyConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_create_connection(address, timeout):
        calls.append((address, timeout))
        return DummyConnection()

    monkeypatch.setattr(nice_app.socket, "create_connection", fake_create_connection)

    assert nice_app._ollama_endpoint_available("http://localhost:11434")
    assert calls == [(("localhost", 11434), 0.35)]
    assert not nice_app._ollama_endpoint_available("")

    def failing_create_connection(address, timeout):
        raise OSError("refused")

    monkeypatch.setattr(nice_app.socket, "create_connection", failing_create_connection)

    assert not nice_app._ollama_endpoint_available("http://localhost:11434")


def test_ui_search_timeout_has_safe_bounds() -> None:
    assert nice_app._ui_search_timeout_seconds({}) == 20.0
    assert nice_app._ui_search_timeout_seconds({"ui_search_timeout_sec": "0.5"}) == 5.0
    assert nice_app._ui_search_timeout_seconds({"ui_search_timeout_sec": "60"}) == 45.0
    assert nice_app._ui_search_timeout_seconds({"ui_search_timeout_sec": "90"}) == 45.0
    assert nice_app._ui_search_timeout_seconds({"ui_search_timeout_sec": "bad"}) == 20.0
    assert nice_app._ui_quick_search_timeout_seconds({}) == 8.0
    assert nice_app._ui_quick_search_timeout_seconds({"ui_quick_search_timeout_sec": "0.5"}) == 1.0
    assert nice_app._ui_quick_search_timeout_seconds({"ui_quick_search_timeout_sec": "60"}) == 10.0


def test_ui_reconnect_timeout_preserves_short_lived_sessions() -> None:
    assert nice_app._ui_reconnect_timeout_seconds({}) == 5.0
    assert nice_app._ui_reconnect_timeout_seconds({"ui_reconnect_timeout_sec": "1"}) == 3.0
    assert nice_app._ui_reconnect_timeout_seconds({"ui_reconnect_timeout_sec": "30"}) == 30.0
    assert nice_app._ui_reconnect_timeout_seconds({"ui_reconnect_timeout_sec": "90"}) == 30.0
    assert nice_app._ui_reconnect_timeout_seconds({"ui_reconnect_timeout_sec": "bad"}) == 5.0

    main_source = inspect.getsource(nice_app.main)
    assert "reconnect_timeout=_ui_reconnect_timeout_seconds(cfg)" in main_source


def test_reconnect_overlay_is_delayed_for_brief_transport_jitter() -> None:
    source = inspect.getsource(css._install_css)

    assert '#popup.nicegui-error-popup[aria-hidden="false"]' in source
    assert "rag-reconnect-reveal 0s 1.2s both" in source


def test_nicegui_client_lifecycle_is_logged_for_reconnect_diagnostics() -> None:
    source = inspect.getsource(nice_app._log_nicegui_client_lifecycle)

    assert "nicegui_client action=%s" in source
    assert "reconnect_timeout_sec=%.1f" in source
    assert "_num_connections" in source


def test_nicegui_reconnect_gap_rehydrates_without_hard_reload() -> None:
    from nicegui.outbox import Outbox

    class Element:
        def __init__(self, element_id: int) -> None:
            self.id = element_id

    class Client:
        id = "client-1"
        elements = {7: Element(7)}

    class EnqueueEvent:
        was_set = False

        def set(self) -> None:
            self.was_set = True

    client = Client()
    outbox = object.__new__(Outbox)
    outbox._client = weakref.ref(client)
    outbox.next_message_id = 12
    outbox.message_history = deque()
    outbox.messages = deque()
    outbox.updates = weakref.WeakValueDictionary()
    outbox._enqueue_event = EnqueueEvent()

    outbox.try_rewind(9)

    assert outbox.next_message_id == 9
    assert outbox.updates[7].id == 7
    assert outbox._enqueue_event.was_set is True
    assert getattr(Outbox.try_rewind, "_rag_rehydrate_on_gap", False) is True


def test_search_keeps_websocket_responsive_while_semantic_pass_runs() -> None:
    source = inspect.getsource(nice_app._build_page)

    assert "_cached_searcher_if_ready" in source
    assert "_qdrant_http_ready" in source
    assert "_run_io_bound_with_ui_timeout" in source
    assert "schedule_search" in source
    assert "asyncio.create_task(run_search(query))" in source
    assert "await run_search(typed)" not in source
    assert "asyncio.wait_for" not in source
    assert "run_quick_timeout" in source
    assert "Быстрый файловый проход прогревается" in source
    assert "Файловый индекс еще подготавливается" not in source
    assert "run_full_timeout" in source
    assert "run_start" in source
    assert "run_full_start" in source
    assert "run_render_final" in source
    assert "render_skip_client_dead" in source


def test_render_does_not_attach_busy_timers_to_rebuilt_content() -> None:
    source = inspect.getsource(nice_app._build_page)

    assert "window.ragHideBusy" in source
    assert "setTimeout(() => { window.ragHideBusy" in source
    assert "ui.timer(\n                    0.08" not in source


def test_browser_diagnostics_are_installed() -> None:
    import rag_catalog.ui.api as api
    from rag_catalog.ui.css import INTERACTION_JS_PATH

    api_source = inspect.getsource(api)
    js_source = INTERACTION_JS_PATH.read_text(encoding="utf-8")

    assert "window.ragDiagLog" in js_source
    assert "connection_lost_visible" in js_source
    assert "socket_disconnect" in js_source
    assert "socket_connect_error" in js_source
    assert "socket_reconnected" in js_source
    assert "downtime_ms" in js_source
    assert "javascript_error" in js_source
    assert "unhandled_rejection_error" in js_source
    assert '@app.post("/api/ui-events")' in api_source
    assert "browser_event action=" in api_source


def test_context_menu_avoids_hard_page_reload() -> None:
    from rag_catalog.ui.css import INTERACTION_JS_PATH

    build_source = inspect.getsource(nice_app._build_page)
    js_source = INTERACTION_JS_PATH.read_text(encoding="utf-8")

    assert "data-rag-refresh-screen" in build_source
    assert "data-rag-open-settings" in build_source
    assert "location.reload()" not in js_source
    assert "location.href = '/settings'" not in js_source
    assert "closestElement(event.target" in js_source


def test_explorer_keeps_recursive_folder_sizes_off_the_event_loop() -> None:
    from rag_catalog.ui import explorer_view, helpers

    source = inspect.getsource(explorer_view.render_explorer_screen)
    helper_source = inspect.getsource(helpers._cd_list_children)

    assert "await run.io_bound(svc.registry.folder_size_bytes_map" in source
    assert "folder_sizes_loaded" in source
    assert "explorer_folder_size_loading" in source
    assert "service.user_access_map" in helper_source


def test_cloud_drive_settings_exposes_import_sources_ui() -> None:
    source = inspect.getsource(settings_view.render_settings_screen)

    assert "Источники импорта" in source
    assert "Добавить или обновить источник" in source
    assert "list_import_sources(limit=50)" in source
    assert "service.upsert_import_source" in source
    assert "service.create_import_job" in source
    assert "service.run_import_job(job_id)" in source
    assert "set_import_source_enabled" in source
    assert "list_jobs(limit=8)" in source
    assert "list_bootstrap_jobs(limit=8)" not in source


def test_cloud_drive_settings_exposes_acl_management_ui() -> None:
    source = inspect.getsource(settings_view.render_settings_screen)

    assert "Доступы Cloud Drive" in source
    assert "Добавить правило доступа" in source
    assert "grant_acl_permission" in source
    assert "revoke_acl_permission" in source
    assert "service.grant_path_permission" in source
    assert "service.grant_permission" in source
    assert "service.list_permissions" in source
    assert "service.revoke_permission" in source
    assert "permissions_grant_ui" in source
    assert "permissions_revoke_ui" in source


def test_cloud_drive_explorer_exposes_complete_sharing_workflow() -> None:
    from rag_catalog.ui import explorer_view

    explorer_source = inspect.getsource(explorer_view.render_explorer_screen)
    settings_source = inspect.getsource(settings_view.render_settings_screen)

    assert "Кто имеет доступ" in explorer_source
    assert "Активные публичные ссылки" in explorer_source
    assert "svc.list_permissions" in explorer_source
    assert "svc.revoke_permission" in explorer_source
    assert "svc.create_share_link" in explorer_source
    assert "svc.list_share_links" in explorer_source
    assert "svc.revoke_share_link" in explorer_source
    assert '"token": link.get("token"' not in explorer_source
    assert "cloud_drive_public_links_enabled" in explorer_source
    assert "Разрешить публичные ссылки" in settings_source
    assert 'audit_values[secret_key] = "***"' in settings_source
    assert '"group": "Группа"' in explorer_source
    assert "share_group_options" in explorer_source


def test_settings_exposes_group_membership_management() -> None:
    source = inspect.getsource(settings_view.render_settings_screen)

    assert "Группы доступа" in source
    assert "Создать группу" in source
    assert "auth_db.list_groups" in source
    assert "auth_db.create_group" in source
    assert "auth_db.update_group" in source
    assert "auth_db.add_group_member" in source
    assert "auth_db.remove_group_member" in source
    assert "group_member_add_ui" in source
    assert "group_member_remove_ui" in source


def test_search_recovery_restores_results_after_reload() -> None:
    from rag_catalog.ui.state import PageState

    nice_app._SEARCH_RECOVERY_CACHE.clear()
    state = PageState(cfg={})
    state.current_user = {"username": "release_smoke"}
    state.auth_token = "session-token"
    state.query = "договор поставки"
    state.searched_query = "договор поставки"
    state.search_request_id = 7
    state.search_lazy_loading = True
    state.search_stats_hint = "Быстро найдено: 1"
    state.results = [{"filename": "Договор.pdf", "score": 1.0}]

    nice_app._persist_search_recovery(state, "quick_results")

    restored = PageState(cfg={})
    restored.current_user = {"username": "release_smoke"}
    restored.auth_token = "session-token"

    assert nice_app._restore_search_recovery(restored)
    assert restored.search_request_id == 7
    assert restored.searched_query == "договор поставки"
    assert restored.results == [{"filename": "Договор.pdf", "score": 1.0}]
    assert restored.search_lazy_loading is False
    assert "восстановлено после переподключения" in restored.search_stats_hint


def test_search_embedder_uses_local_model_cache() -> None:
    from rag_catalog.core import rag_core

    source = inspect.getsource(rag_core.RAGSearcher.embedder.fget)

    assert "local_files_only=True" in source


def test_qdrant_readiness_probe_handles_disconnected_server(monkeypatch) -> None:
    from rag_catalog.ui import helpers

    class DummyResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(helpers.urllib.request, "urlopen", lambda req, timeout: DummyResponse())
    assert helpers._qdrant_http_ready({"qdrant_url": "http://127.0.0.1:6333"})

    def failing_urlopen(req, timeout):
        raise OSError("empty reply")

    monkeypatch.setattr(helpers.urllib.request, "urlopen", failing_urlopen)
    assert not helpers._qdrant_http_ready({"qdrant_url": "http://127.0.0.1:6333"})


def test_launcher_status_reports_qdrant_http_readiness() -> None:
    from rag_catalog.cli import launcher

    source = inspect.getsource(launcher._status)

    assert "qdrant.ready" in source
    assert "_http_ready" in source
