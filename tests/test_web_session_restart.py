"""A UI simulation must not log a browser out of an independent web server."""
import asyncio
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from nicegui.storage import Storage

from rag_catalog.ui.auth_session import BrowserSessionStorage
from scripts.pilot_ui_smoke import SRC, _browser_executable, _free_port, _prepare_contour, _wait_http


def test_concurrent_first_requests_wait_for_the_same_loaded_session(monkeypatch):
    async def check():
        store = BrowserSessionStorage()
        loading, release = asyncio.Event(), asyncio.Event()
        loads = []

        class Data(dict):
            async def initialize(self):
                loading.set()
                await release.wait()
                self["ready"] = True

        def create(key):
            loads.append(key)
            return Data()

        monkeypatch.setattr(store, "_create_persistent_dict", create)
        first = asyncio.create_task(store._create_user_storage("one"))
        await loading.wait()
        second = asyncio.create_task(store._create_user_storage("one"))
        await asyncio.sleep(0)
        assert "one" not in store._users
        release.set()
        await asyncio.gather(first, second)
        assert store._users["one"]["ready"] and len(loads) == 1

    asyncio.run(check())


def test_cookie_restores_login_after_simulation_and_restart(tmp_path):
    playwright = pytest.importorskip("playwright.sync_api")
    try:
        executable = _browser_executable()
    except RuntimeError as exc:
        pytest.skip(str(exc))

    contour = _prepare_contour(tmp_path, qdrant_url="")
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    server_storage = tmp_path / ".nicegui"
    env = {**os.environ, "PYTHONPATH": str(SRC), "RAG_CONFIG_PATH": contour["config_path"],
           "HF_HUB_OFFLINE": "1", "NICEGUI_STORAGE_PATH": str(server_storage),
           "RAG_UI_SESSION_HTTPS_ONLY": "0", "NICEGUI_SCREEN_TEST_PORT": str(port)}
    server = ("from rag_catalog.ui.nice_app import ui; import sys; "
              "ui.run(host='127.0.0.1', port=int(sys.argv[1]), reload=False, show=False, storage_secret='fixture')")

    @contextmanager
    def running():
        with (tmp_path / "server.log").open("ab") as log:
            proc = subprocess.Popen([sys.executable, "-c", server, str(port)], env=env, cwd=tmp_path,
                                    stdout=log, stderr=subprocess.STDOUT,
                                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            try:
                _wait_http(url + "/api/ping", timeout_sec=45)
                yield
            finally:
                proc.terminate()
                proc.wait(timeout=15)

    with playwright.sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, executable_path=executable)
        try:
            page = browser.new_page()
            logout = page.get_by_role("button", name="Выйти из аккаунта", exact=True)
            with running():
                page.goto(url + "/shifts")
                page.get_by_label("Логин или email", exact=True).fill("pilot-driver")
                page.get_by_label("Пароль", exact=True).fill(contour["password"])
                page.get_by_role("button", name="Войти", exact=True).click()
                logout.wait_for()
                page.reload()
                logout.wait_for()

            assert Storage.path == Path(os.environ["NICEGUI_STORAGE_PATH"])
            assert Storage.path.parent.name.startswith("rag-pytest-nicegui-")
            assert Storage.path != server_storage
            persisted = set(server_storage.glob("storage-user-*.json"))
            assert persisted
            # Simulate a separate test runner without clearing this runner's API routes.
            subprocess.run(
                [sys.executable, "-c", "from nicegui.testing.general import nicegui_reset_globals\n"
                 "with nicegui_reset_globals(): pass"],
                cwd=tmp_path, check=True, timeout=30,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            assert all(p.is_file() for p in persisted)

            with running():
                page.goto(url + "/shifts")
                logout.wait_for()
                assert not page.get_by_label("Пароль", exact=True).count()
        finally:
            browser.close()
