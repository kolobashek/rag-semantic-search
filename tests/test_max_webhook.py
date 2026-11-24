import http.client
import json
import socket
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest
import requests
import uvicorn
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rag_catalog.integrations import max_webhook as hook
from rag_catalog.integrations.max_api import MaxEvent


def update(uid=11, mid="mid.1", text="/menu", chat_type="dialog"):
    return {"update_type": "message_created", "message": {
        "sender": {"user_id": uid, "first_name": "Иван"},
        "recipient": {"chat_type": chat_type, "user_id": 102},
        "body": {"mid": mid, "text": text},
    }}


def setup_app(monkeypatch, tmp_path):
    app = FastAPI()
    cfg = {"settings": None}
    settings = SimpleNamespace(secret="private-test", token="test", bot_link="https://max.ru/test",
                               office_url="https://example.org", inbox_path=tmp_path / "inbox.db")
    monkeypatch.setattr(hook, "_configuration", lambda load: (load(), load()["settings"]))
    hook.register_max_webhook(app, lambda: cfg, lambda _: None)
    return app, cfg, settings


def test_webhook_checks_secret_size_and_private_context(monkeypatch, tmp_path):
    app, cfg, settings = setup_app(monkeypatch, tmp_path)
    client = TestClient(app)
    url = "/api/max/webhook"
    headers = {"X-Max-Bot-Api-Secret": settings.secret}
    assert client.post(url, json=update(), headers=headers).status_code == 404
    cfg["settings"] = settings
    assert client.post(url, json=update()).status_code == 403
    assert client.post(url, json=update(), headers={"X-Max-Bot-Api-Secret": "wrong"}).status_code == 403
    assert client.post(url, content=b"{}", headers=headers).status_code == 400
    assert client.post(url, content=b"{", headers=headers).status_code == 400
    assert client.post(url, content=b" " * (hook._MAX_BODY + 1), headers=headers).status_code == 413
    assert client.post(url, json=update(chat_type="chat"), headers=headers).status_code == 200
    assert not settings.inbox_path.exists()


def test_enabled_at_runtime_acknowledges_durable_write_and_deduplicates(monkeypatch, tmp_path):
    app, cfg, settings = setup_app(monkeypatch, tmp_path)
    started = threading.Event()
    finish = threading.Event()
    done = []

    def dispatch(_cfg, _settings, _db, event):
        started.set()
        assert finish.wait(5)
        done.append(event)

    monkeypatch.setattr(hook, "_handle", dispatch)
    with TestClient(app) as client:
        cfg["settings"] = settings
        headers = {"X-Max-Bot-Api-Secret": settings.secret}
        item = update(text="/start login_confidential")
        for _ in range(2):
            assert client.post("/api/max/webhook", json=item, headers=headers).status_code == 200
        assert started.wait(3)
        with sqlite3.connect(settings.inbox_path) as db:
            rows = db.execute("SELECT payload FROM max_inbox").fetchall()
        assert len(rows) == 1 and "login_confidential" in rows[0][0]
        finish.set()
        deadline = time.monotonic() + 4
        while True:
            with sqlite3.connect(settings.inbox_path) as db:
                if db.execute("SELECT payload FROM max_inbox").fetchone() == (None,):
                    break
            assert time.monotonic() < deadline
            time.sleep(.01)
        assert client.post("/api/max/webhook", json=item, headers=headers).status_code == 200
        assert [e.text for e in done] == ["/start login_confidential"]


def test_inbox_retries_do_not_reorder_an_employee_or_block_other_employees(tmp_path, monkeypatch):
    inbox = hook.MaxInbox(str(tmp_path / "inbox.db"))
    for uid, mid in [(1, "first"), (1, "second"), (2, "other")]:
        inbox.enqueue("bot-a", MaxEvent(uid, mid, text=mid))
    inbox.enqueue("bot-b", MaxEvent(1, "first", text="different bot"))
    seen = []

    def handle(event):
        seen.append(event.text)
        if len(seen) == 1:
            raise RuntimeError("try later")

    clock = time.time()
    monkeypatch.setattr(hook.time, "time", lambda: clock)
    assert inbox.deliver_one("bot-a", handle)
    assert inbox.deliver_one("bot-a", handle)
    assert not inbox.deliver_one("bot-a", handle)
    assert seen == ["first", "other"]
    # Reopen the inbox as a fresh process would.
    inbox = hook.MaxInbox(str(tmp_path / "inbox.db"))
    clock += 3
    assert inbox.deliver_one("bot-a", handle) and inbox.deliver_one("bot-a", handle)
    assert inbox.deliver_one("bot-b", handle)
    assert seen == ["first", "other", "first", "second", "different bot"]


def test_worker_lost_mid_delivery_and_concurrent_worker_do_not_lose_event(tmp_path):
    path = str(tmp_path / "inbox.db")
    inbox = hook.MaxInbox(path)
    inbox.enqueue("bot", MaxEvent(7, "mid", "message"))
    other = hook.MaxInbox(path)

    def crash(event):
        assert not other.deliver_one("bot", lambda e: pytest.fail("concurrent processing"))
        raise SystemExit

    with pytest.raises(SystemExit):
        inbox.deliver_one("bot", crash)
    delivered = []
    assert other.deliver_one("bot", delivered.append)
    assert [event.text for event in delivered] == ["message"]
    assert not inbox.deliver_one("bot", delivered.append)


def test_http_delivery_to_employee_roles_with_real_databases(monkeypatch, tmp_path):
    from rag_catalog.core.user_auth_db import UserAuthDB

    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    auth = UserAuthDB(str(tmp_path / "users.sqlite"))
    cfg = {"max_state_dir": str(tmp_path), "work_shifts_db_path": str(tmp_path / "shifts.sqlite")}
    secret = "fixture_" * 6
    (tmp_path / "bot.json").write_text(json.dumps({"token": "fixture-api", "secret": secret,
        "bot_link": "https://max.ru/fixture_employee_bot", "office_url": "https://office.example"}))
    events = []
    for uid, username, role in [(11, "driver", "driver"), (22, "worker", "user"), (33, "chief", "admin")]:
        auth.admin_create_user(username=username, roles=[role], password="fake-password", must_change_password=False)
        session = auth.create_session(username=username)
        token = auth.create_max_link_token(session_token=session)["token"]
        events.append({"update_type": "bot_started", "timestamp": 145000, "chat_id": 500 + uid,
                       "user": {"user_id": uid, "first_name": username, "is_bot": False}, "payload": "link_" + token})
    events.append(update(11, "mid.restricted", "/add_user evan admin"))
    events.append({"update_type": "bot_started", "timestamp": 1573226679188, "chat_id": 599,
                   "user": {"user_id": 99, "first_name": "Гость", "is_bot": False},
                   "user_locale": "ru", "payload": None})
    events.append(update(99, "mid.repeat", "/start"))
    challenge = auth.create_max_login_challenge()
    events.append({"update_type": "bot_started", "timestamp": 145001, "chat_id": 522,
                   "user": {"user_id": 22, "is_bot": False}, "payload": "login_" + challenge["token"]})
    events.append({"update_type": "message_callback", "timestamp": 146000,
                   "message": {"recipient": {"chat_type": "dialog", "user_id": 22}},
                   "callback": {"user": {"user_id": 22, "is_bot": False}, "callback_id": "cb-login",
                                "payload": "/confirm_login " + challenge["token"]}})
    sent = []
    answers = []

    def fake_max(method, url, **kwargs):
        # Every outgoing request terminates in this mock. The inbound webhook
        # crosses a real loopback HTTP socket into the normal ASGI route.
        assert method == "POST" and kwargs["headers"]["Authorization"] == "fixture-api"
        if url == "https://platform-api2.max.ru/messages":
            uid = kwargs["params"]["user_id"]
            sent.append((uid, kwargs["json"]))
            data = {"message": {"body": {"mid": f"out-{len(sent)}"}}}
        else:
            assert url == "https://platform-api2.max.ru/answers"
            answers.append(kwargs["params"]["callback_id"])
            data = {"success": True}
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(data).encode()
        response._content_consumed = True
        return response

    monkeypatch.setattr(requests.Session, "request", staticmethod(fake_max))
    app = FastAPI()
    hook.register_max_webhook(app, lambda: cfg, lambda _: auth)
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning", ws="none"))
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 15
            while not server.started:
                assert thread.is_alive() and time.monotonic() < deadline
                time.sleep(.01)
            connection = http.client.HTTPConnection("127.0.0.1", listener.getsockname()[1], timeout=4)
            try:
                for event in events + [events[-1]]:
                    connection.request("POST", "/api/max/webhook", json.dumps(event).encode(),
                                       {"X-Max-Bot-Api-Secret": secret, "Content-Type": "application/json"})
                    reply = connection.getresponse()
                    assert reply.status == 200
                    reply.read()
            finally:
                connection.close()
            while True:
                with sqlite3.connect(tmp_path / "inbox.sqlite") as db:
                    count = db.execute("SELECT count(*) FROM max_inbox WHERE finished IS NOT NULL").fetchone()[0]
                if count == len(events):
                    break
                assert time.monotonic() < deadline
                time.sleep(.02)
        finally:
            server.should_exit = True
            thread.join(5)
    menus = {}
    for uid, body in sent:
        for attachment in body["attachments"]:
            for row in attachment["payload"]["buttons"]:
                menus.setdefault(uid, set()).update(button.get("payload") for button in row)
    assert "/shifts" in menus[11] and "/office" not in menus[11] and "/add_user" not in menus[11]
    assert "/office" in menus[22] and "/add_user" not in menus[22]
    assert "/add_user" in menus[33] and auth.get_user(username="evan") is None
    welcome, repeated = [body for uid, body in sent if uid == 99]
    assert repeated == welcome and "Здравствуйте" in welcome["text"]
    assert menus[99] == {"ob:existing", "ob:request", "ob:status"}
    assert answers == ["cb-login"]
    assert auth.consume_confirmed_max_login(verifier=challenge["verifier"])["username"] == "worker"
