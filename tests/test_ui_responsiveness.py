from __future__ import annotations

import inspect

from rag_catalog.ui import css, nice_app


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
