"""Browser sign-in and account linking on the driver-only workspace, using an isolated DB."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from rag_catalog.core.user_auth_db import UserAuthDB
from scripts.pilot_ui_smoke import SRC, _browser_executable, _free_port, _prepare_contour, _wait_http


def test_driver_links_logs_in_and_unlinks_max(tmp_path):
    playwright = pytest.importorskip("playwright.sync_api")
    try:
        executable = _browser_executable()
    except RuntimeError as exc:
        pytest.skip(str(exc))

    contour = _prepare_contour(tmp_path, qdrant_url="")
    bot_dir = tmp_path / "max"
    bot_dir.mkdir()
    (bot_dir / "bot.json").write_text(json.dumps({
        "token": "fixture-only", "secret": "x" * 32, "subscribed": True,
        "office_url": "https://office.example", "bot_link": "https://max.ru/fixture_bot",
    }), encoding="utf-8")
    config = dict(contour["config"], max_state_dir=str(bot_dir))
    Path(contour["config_path"]).write_text(json.dumps(config), encoding="utf-8")
    auth = UserAuthDB(config["users_db_path"])
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    env = {**os.environ, "PYTHONPATH": str(SRC), "RAG_CONFIG_PATH": contour["config_path"],
           "HF_HUB_OFFLINE": "1", "RAG_UI_SESSION_HTTPS_ONLY": "0",
           "NICEGUI_STORAGE_PATH": str(tmp_path / "nicegui"), "NICEGUI_SCREEN_TEST_PORT": str(port)}
    server = ("from rag_catalog.ui.nice_app import ui; import sys; "
              "ui.run(host='127.0.0.1',port=int(sys.argv[1]),reload=False,show=False,storage_secret='fixture')")
    with (tmp_path / "web.log").open("wb") as log:
        proc = subprocess.Popen([sys.executable, "-c", server, str(port)], env=env, cwd=tmp_path,
                                stdout=log, stderr=subprocess.STDOUT,
                                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        try:
            _wait_http(url + "/api/ping", timeout_sec=60)
            with playwright.sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True, executable_path=executable)
                try:
                    page = browser.new_page()
                    page.goto(url + "/shifts")
                    page.get_by_label("Логин или email", exact=True).fill("pilot-driver")
                    page.get_by_label("Пароль", exact=True).fill(contour["password"])
                    page.get_by_role("button", name="Войти", exact=True).click()
                    account = page.get_by_role("button", name="Аккаунт MAX", exact=True)
                    account.click()
                    link = page.get_by_role("link", name="Открыть бота MAX").get_attribute("href")
                    assert link.startswith("https://max.ru/fixture_bot?start=link_")
                    assert auth.consume_max_link_token(token=link.split("link_")[1], max_user_id=701)["ok"]
                    page.get_by_role("button", name="Проверить привязку").click()
                    logout = page.get_by_role("button", name="Выйти из аккаунта", exact=True)
                    logout.click()
                    page.get_by_role("button", name="Войти через MAX").click()
                    link = page.get_by_role("link", name="Подтвердить в MAX").get_attribute("href")
                    assert link.startswith("https://max.ru/fixture_bot?start=login_")
                    assert auth.confirm_max_login(token=link.split("login_")[1], max_user_id=701)["ok"]
                    account.click()
                    page.get_by_text("MAX привязан к сотруднику pilot-driver.").wait_for()
                    page.get_by_role("button", name="Отвязать MAX").click()
                    page.get_by_text("MAX отвязан.", exact=True).wait_for()
                    assert auth.get_user_by_max_user_id(701) is None
                finally:
                    browser.close()
        finally:
            proc.terminate()
            proc.wait(timeout=15)
