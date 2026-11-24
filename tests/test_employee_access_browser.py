"""Employee registration, invitation and account linking through the real local web UI."""

import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import qrcode
from PIL import Image, ImageChops

from rag_catalog.core.user_auth_db import UserAuthDB
from scripts.pilot_ui_smoke import SRC, _browser_executable, _free_port, _prepare_contour, _wait_http

PORTAL = "https://office.example"
BOT = "https://t.me/employee_fixture_bot"
ORG = "Северный филиал"
ADMIN = "north-admin"


@pytest.fixture(scope="module")
def office(tmp_path_factory):
    playwright = pytest.importorskip("playwright.sync_api")
    try:
        executable = _browser_executable()
    except RuntimeError as exc:
        pytest.skip(str(exc))
    root = tmp_path_factory.mktemp("employee_web")
    contour = _prepare_contour(root, qdrant_url="")
    config = dict(contour["config"], telegram_bot_link=BOT, employee_office_url=PORTAL, max_state_dir=str(root / "max"))
    Path(contour["config_path"]).write_text(json.dumps(config), encoding="utf-8")
    auth = UserAuthDB(config["users_db_path"])
    password = contour["password"]
    assert auth.create_employee_organization(admin_username=contour["username"], organization_id="north", name=ORG)[
        "ok"
    ]
    assert auth.admin_create_user(
        username=ADMIN,
        password=password,
        roles=["admin"],
        organization_id="north",
        platform_admin=False,
        must_change_password=False,
    )
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "PYTHONPATH": str(SRC),
        "RAG_CONFIG_PATH": contour["config_path"],
        "HF_HUB_OFFLINE": "1",
        "RAG_UI_SESSION_HTTPS_ONLY": "0",
        "NICEGUI_STORAGE_PATH": str(root / "nicegui"),
        "NICEGUI_SCREEN_TEST_PORT": str(port),
    }
    server = (
        "from rag_catalog.ui.nice_app import ui; import sys; "
        "ui.run(host='127.0.0.1',port=int(sys.argv[1]),reload=False,show=False,storage_secret='fixture')"
    )
    with (root / "web.log").open("wb") as log:
        proc = subprocess.Popen(
            [sys.executable, "-c", server, str(port)],
            cwd=root,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        try:
            _wait_http(url + "/api/ping", timeout_sec=60)
            with playwright.sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True, executable_path=executable)
                try:
                    yield browser, url, auth, password
                finally:
                    browser.close()
        finally:
            proc.terminate()
            proc.wait(timeout=15)


def sign_in(page, url, username, password):
    page.goto(url + "/shifts")
    page.get_by_label("Логин или email", exact=True).fill(username)
    page.get_by_label("Пароль", exact=True).fill(password)
    page.get_by_role("button", name="Войти", exact=True).click()


def activate(page, local_url, personal_link, password, username):
    assert personal_link.startswith(PORTAL + "/auth/activate#token=")
    page.goto(local_url + personal_link.removeprefix(PORTAL))
    page.get_by_label("Новый пароль", exact=False).fill(password)
    page.get_by_label("Повторите пароль").fill(password)
    page.get_by_role("button", name="Сохранить пароль").click()
    page.get_by_text("Пароль установлен. Ваш логин: " + username, exact=True).wait_for()


@pytest.fixture
def pages(office):
    browser, url, _, _ = office
    with browser.new_context() as admin, browser.new_context() as employee:
        for context in (admin, employee):
            context.route(
                "**/*",
                lambda route: (
                    route.continue_() if urlsplit(route.request.url).hostname == "127.0.0.1" else route.abort()
                ),
            )
        yield admin.new_page(), employee.new_page()


def test_invited_driver_only_sees_personal_telegram_and_public_qr(office, pages, tmp_path):
    _, url, auth, password = office
    admin, employee = pages
    sign_in(admin, url, ADMIN, password)
    admin.get_by_role("button", name="Сотрудники").click()
    desk = admin.get_by_role("dialog")
    desk.get_by_text("Пригласить сотрудника", exact=True).click()
    invite = desk.locator(".q-expansion-item").filter(has_text="Создать приглашение")
    assert invite.locator(".q-select").filter(has_text="Роли сотрудника").locator(
        ".q-chip__content"
    ).all_inner_texts() == ["Водитель"]
    invite.get_by_label("Логин", exact=True).fill("web-invited")
    invite.get_by_label("ФИО", exact=True).fill("Приглашённый водитель")
    invite.get_by_role("button", name="Создать приглашение").click()
    field = admin.get_by_label("Одноразовая ссылка")
    personal_link = field.input_value()
    field.locator("xpath=ancestor::*[@role='dialog'][1]").get_by_role("button", name="Закрыть").click()
    assert auth.get_user(username="web-invited")["status"] == "pending"

    with admin.expect_download() as download:
        admin.get_by_role("button", name="Скачать QR-код кабинета").click()
    png = tmp_path / "public-qr.png"
    download.value.save_as(png)
    with Image.open(png) as downloaded:
        expected = qrcode.make(PORTAL).convert("RGB")
        assert downloaded.size == expected.size
        assert ImageChops.difference(downloaded.convert("RGB"), expected).getbbox() is None

    activate(employee, url, personal_link, password, "web-invited")
    assert auth.get_user(username="web-invited")["organization_roles"] == {"north": ["driver"]}
    sign_in(employee, url, "web-invited", password)
    employee.get_by_role("button", name="Аккаунт Telegram").click()
    assert not employee.get_by_role("button", name="Сотрудники").is_visible()
    link = employee.get_by_role("link", name="Открыть бота Telegram").get_attribute("href")
    assert link.startswith(BOT + "?start=link_")
    token = link.removeprefix(BOT + "?start=link_")
    assert auth.consume_telegram_start_token(token=token, telegram_chat_id="4701")["username"] == "web-invited"
    assert not auth.consume_telegram_start_token(token=token, telegram_chat_id="4702")["ok"]
    employee.get_by_role("dialog").get_by_role("button", name="Закрыть").click()
    employee.get_by_role("button", name="Аккаунт Telegram").click()
    employee.get_by_text("Telegram привязан к сотруднику web-invited.", exact=True).wait_for()


def test_scoped_admin_approves_web_application_with_default_driver(office, pages):
    _, url, auth, password = office
    admin, employee = pages
    assert auth.submit_employee_request(
        provider="web",
        sender_id="",
        username="out-of-scope",
        display_name="Заявка другого филиала",
        organization_id="default",
    )["ok"]
    employee.goto(url + "/shifts")
    employee.get_by_role("tab", name="Зарегистрироваться").click()
    employee.get_by_label("Логин", exact=True).fill("web-applicant")
    employee.locator(".q-select:visible").filter(has_text="Организация").click()
    employee.get_by_role("option", name=ORG).click()
    employee.get_by_label("Имя", exact=True).fill("Заявка Северного филиала")
    employee.get_by_role("button", name="Отправить заявку").click()
    employee.get_by_text("Заявка отправлена администратору.", exact=True).wait_for()
    assert auth.get_user(username="web-applicant") is None

    sign_in(admin, url, ADMIN, password)
    admin.get_by_role("button", name="Сотрудники").click()
    desk = admin.get_by_role("dialog")
    applicant = desk.get_by_text("Заявка Северного филиала", exact=True)
    applicant.wait_for()
    assert not desk.get_by_text("Заявка другого филиала", exact=True).is_visible()
    card = applicant.locator("xpath=ancestor::*[contains(@class,'q-card')][1]")
    assert card.locator(".q-chip__content").all_inner_texts() == ["Водитель"]
    card.get_by_role("button", name="Одобрить").click()
    personal = admin.get_by_label("Одноразовая ссылка").input_value()
    assert auth.list_employee_requests(admin_username=ADMIN) == []
    activate(employee, url, personal, password, "web-applicant")
    account = auth.get_user(username="web-applicant")
    assert account["status"] == "active"
    assert account["organization_roles"] == {"north": ["driver"]}


def test_inviting_a_dispatcher_requires_separate_confirmation(office, pages):
    _, url, auth, password = office
    admin, _ = pages
    sign_in(admin, url, ADMIN, password)
    admin.get_by_role("button", name="Сотрудники").click()
    admin.get_by_text("Пригласить сотрудника", exact=True).click()
    invite = admin.locator(".q-expansion-item").filter(has_text="Создать приглашение")
    invite.get_by_label("Логин", exact=True).fill("web-dispatcher")
    roles = invite.locator(".q-select").filter(has_text="Роли сотрудника")
    roles.click()
    admin.get_by_role("option", name="Диспетчер").click()
    admin.keyboard.press("Escape")
    submit = invite.get_by_role("button", name="Создать приглашение")
    submit.click()
    confirmation = admin.get_by_role("dialog").filter(has_text="Назначить дополнительные права?")
    confirmation.get_by_role("button", name="Вернуться").click()
    assert auth.get_user(username="web-dispatcher") is None
    submit.click()
    confirmation.get_by_role("button", name="Назначить выбранные права").click()
    admin.get_by_label("Одноразовая ссылка").wait_for()
    pending = auth.get_user(username="web-dispatcher")
    assert pending["status"] == "pending"
    assert set(pending["organization_roles"]["north"]) == {"driver", "dispatcher"}


def test_telegram_driver_login_returns_to_initiating_shifts_page(office, pages, monkeypatch):
    from rag_catalog.integrations import telegram_bot

    _, url, auth, password = office
    anonymous, driver = pages
    username, sender = "browser-telegram-driver", 4981
    assert auth.admin_create_user(
        username=username, password=password, roles=["driver"], telegram_chat_id=str(sender), must_change_password=False
    )
    replies = []
    monkeypatch.setattr(
        telegram_bot, "send_message", lambda token, chat, text, reply_markup=None: replies.append((text, reply_markup))
    )
    monkeypatch.setattr(telegram_bot, "answer_callback_query", lambda *args: None)
    driver.goto(url + "/shifts")
    driver.get_by_role("button", name="Войти через Telegram").click()
    link = driver.locator(f'a[href^="{BOT}?start=login_"]')
    start = link.get_attribute("href").removeprefix(BOT + "?start=")
    assert link.get_attribute("target") == "_blank"
    assert urlsplit(driver.url).hostname == "127.0.0.1"

    private = {"chat": {"id": sender, "type": "private"}, "from": {"id": sender}}
    telegram_bot.handle_update(
        "fixture",
        auth,
        {},
        {
            "update_id": 1,
            "message": {**private, "text": "/start " + start},
        },
    )
    (button,) = [
        b for row in replies[-1][1]["inline_keyboard"] for b in row if b["text"] == "Подтвердить вход в браузере"
    ]
    assert not driver.get_by_text("Смены и путевые листы", exact=True).is_visible()
    telegram_bot.handle_update(
        "fixture",
        auth,
        {},
        {
            "update_id": 2,
            "callback_query": {
                "id": "local",
                "message": private,
                "from": private["from"],
                "data": button["callback_data"],
            },
        },
    )
    assert "Вход подтверждён" in replies[-1][0]
    driver.get_by_text("Смены и путевые листы", exact=True).wait_for()
    driver.goto(url + "/shifts")
    driver.get_by_role("button", name="Аккаунт Telegram").click()
    driver.get_by_text(f"Telegram привязан к сотруднику {username}.", exact=True).wait_for()
    assert not driver.get_by_role("button", name="Сотрудники").is_visible()
    anonymous.goto(url + "/shifts")
    anonymous.get_by_role("button", name="Войти через Telegram").wait_for()
    assert not anonymous.get_by_text("Смены и путевые листы", exact=True).is_visible()
