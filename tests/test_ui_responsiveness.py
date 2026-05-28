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
