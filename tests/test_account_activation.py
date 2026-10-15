import hashlib
import json
import secrets
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rag_catalog.core.user_auth_db import UserAuthDB
from rag_catalog.ui.account_activation import register_activation_routes


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    store = UserAuthDB(str(tmp_path / "users.db"))
    assert store.admin_create_user(username="admin", password="admin-test", role="admin", must_change_password=False)
    return store


def invite(db, username="staff"):
    result = db.create_account_activation(username=username, display_name="Staff Member", created_by="admin")
    assert result["ok"]
    return result


@pytest.fixture
def client(db):
    app = FastAPI()
    register_activation_routes(app, lambda: db)
    with TestClient(app) as client:
        yield client


def test_invitation_first_password_and_single_use(db):
    result = invite(db)
    user = db.get_user(username="staff")
    assert user["status"] == "pending"
    assert user["roles"] == ["user"]
    assert user["must_change_password"] == 1
    assert db.login(username="staff", password="") is None
    assert db.login_with_reason(username="staff", password="")["user"] is None
    old_session = db.create_session(username="staff")
    assert db.get_user_by_session(old_session) is None
    with db._connect() as conn:
        stored = dict(conn.execute("SELECT * FROM account_activations").fetchone())
        assert stored["token_hash"] == hashlib.sha256(result["token"].encode()).hexdigest()
        assert result["token"] not in json.dumps(stored)
        assert conn.execute("SELECT password_hash FROM users WHERE username='staff'").fetchone()[0] == ""
    assert db.activate_account(token=result["token"], new_password="my-new-password") == {"ok": True, "username": "staff"}
    user = db.login(username="staff", password="my-new-password")
    assert user["status"] == "active"
    assert user["must_change_password"] == 0
    assert db.get_user_by_session(old_session) is None
    assert db.login(username="staff", password="") is None
    assert not db.activate_account(token=result["token"], new_password="another-password")["ok"]
    assert db.login(username="staff", password="my-new-password") is not None
    with db._connect() as conn:
        events = [dict(row) for row in conn.execute("SELECT * FROM auth_events")]
    assert [event["event_type"] for event in events] == ["account_invited", "account_activated"]
    assert result["token"] not in json.dumps(events)
    assert "my-new-password" not in json.dumps(events)


@pytest.mark.parametrize("password", [None, "", "short", " " * 8, "a" * 129, 123, []])
def test_invalid_password_does_not_consume_token(db, password):
    result = invite(db)
    assert db.activate_account(token=result["token"], new_password=password)["reason"] == "invalid_password"
    assert db.activate_account(token=result["token"], new_password="valid-password")["ok"]


@pytest.mark.parametrize("token", [None, "", "invalid", "x" * 44, "я" * 43, [], secrets.token_urlsafe(32)])
def test_invalid_token_denied(db, token):
    invite(db)
    assert db.activate_account(token=token, new_password="valid-password")["reason"] == "invalid_activation"
    assert db.get_user(username="staff")["status"] == "pending"


@pytest.mark.parametrize("change", ["expired", "blocked", "active", "password_set"])
def test_ineligible_account_cannot_activate(db, change):
    result = invite(db)
    with db._connect() as conn:
        if change == "expired":
            conn.execute("UPDATE account_activations SET expires_at=?", ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),))
        elif change == "password_set":
            conn.execute("UPDATE users SET password_hash='disabled' WHERE username='staff'")
        else:
            conn.execute("UPDATE users SET status=? WHERE username='staff'", (change,))
    assert not db.activate_account(token=result["token"], new_password="valid-password")["ok"]


def test_cannot_overwrite_existing_or_create_without_admin(db):
    result = invite(db)
    assert db.create_account_activation(username="staff", display_name="Changed", created_by="admin")["reason"] == "already_exists"
    assert db.get_user(username="staff")["display_name"] == "Staff Member"
    assert db.create_account_activation(username="other", display_name="Other", created_by="staff")["reason"] == "not_admin"
    assert db.get_user(username="other") is None
    with pytest.raises(ValueError, match="активации"):
        db.request_verification(username="staff", display_name="Attacker", telegram_chat_id="999", password="attacker-password")
    assert db.activate_account(token=result["token"], new_password="valid-password")["ok"]


def test_token_consumption_serialized_across_connections(db):
    result = invite(db)
    other = UserAuthDB(str(db.db_path))
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda store: store.activate_account(token=result["token"], new_password="valid-password"), [db, other]))
    assert sum(response["ok"] for response in responses) == 1


def test_explicit_reset_preserves_identity_and_revokes_old_credentials(client, db):
    db.admin_create_user(username="kris", display_name="Existing Name", telegram_chat_id="777",
                         password="old-password", roles=["user", "driver"], must_change_password=False)
    db.save_user_settings(username="kris", settings={"theme": "dark"})
    session = db.create_session(username="kris")
    tg_link = db.create_telegram_link_token(username="kris")
    verification = db.request_telegram_link_code(username="kris", password="old-password", telegram_chat_id="777")
    reset = db.request_password_reset(username="kris", telegram_chat_id="777")
    assert db.confirm_password_reset(telegram_chat_id="777", code=reset["code"])["ok"]
    with db._connect() as conn:
        before = dict(conn.execute("SELECT * FROM users WHERE username='kris'").fetchone())
    result = db.create_account_activation(username="kris", display_name="Must Not Replace", created_by="admin", reset_existing=True)
    assert result["ok"]
    with db._connect() as conn:
        after = dict(conn.execute("SELECT * FROM users WHERE username='kris'").fetchone())
    for key in before.keys() - {"password_hash", "must_change_password", "status"}:
        assert after[key] == before[key]
    assert db.get_user_settings(username="kris") == {"theme": "dark"}
    assert db.login(username="kris", password="old-password") is None
    assert db.login(username="kris", password="") is None
    assert db.get_user_by_session(session) is None
    assert not db.confirm_verification(telegram_chat_id="777", code=verification["code"])["ok"]
    assert not db.complete_password_reset(username="kris", code=reset["code"], new_password="stale-reset-password")["ok"]
    assert not db.consume_telegram_start_token(token=tg_link["token"], telegram_chat_id="999")["ok"]
    response = client.post("/api/auth/activate", json={"token": result["token"], "new_password": "new-password"})
    assert response.status_code == 200
    assert db.login(username="kris", password="new-password")["roles"] == ["driver", "user"]
    assert db.login(username="kris", password="old-password") is None
    assert db.get_user_by_session(session) is None


def test_reset_link_replaces_previous_invitation(db):
    original = invite(db)
    replacement = db.create_account_activation(username="staff", display_name="", created_by="admin", reset_existing=True)
    assert replacement["ok"]
    assert not db.activate_account(token=original["token"], new_password="old-link-password")["ok"]
    assert db.activate_account(token=replacement["token"], new_password="new-link-password")["ok"]
    assert not db.activate_account(token=replacement["token"], new_password="replayed-password")["ok"]


@pytest.mark.parametrize("target", ["missing", "blocked"])
def test_reset_does_not_create_or_unblock_accounts(db, target):
    if target == "blocked":
        db.admin_create_user(username=target, password="blocked-password", status="blocked")
    result = db.create_account_activation(username=target, display_name="", created_by="admin", reset_existing=True)
    assert result == {"ok": False, "reason": "not_found" if target == "missing" else "blocked"}
    if target == "missing":
        assert db.get_user(username=target) is None
    else:
        assert db.get_user(username=target)["status"] == "blocked"


def test_reset_requires_active_admin_and_does_not_affect_other_accounts(db):
    db.admin_create_user(username="staff", password="original-password")
    session = db.create_session(username="staff")
    denied = db.create_account_activation(username="staff", display_name="", created_by="staff", reset_existing=True)
    assert denied["reason"] == "not_admin"
    assert db.login(username="staff", password="original-password")
    assert db.get_user_by_session(session)
    db.admin_create_user(username="target", password="target-password")
    assert db.create_account_activation(username="target", display_name="", created_by="admin", reset_existing=True)["ok"]
    assert db.login(username="staff", password="original-password")
    assert db.get_user_by_session(session)


def test_activation_page_does_not_consume_token(client, db):
    result = invite(db)
    response = client.get("/auth/activate")
    assert response.status_code == 200
    assert 'location.hash' in response.text
    assert 'history.replaceState' in response.text
    assert 'autocomplete="new-password"' in response.text
    assert 'frame-ancestors' in response.headers["content-security-policy"]
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert result["token"] not in response.text
    assert db.get_user(username="staff")["status"] == "pending"


def test_activation_api_success_and_replay(client, db):
    result = invite(db)
    payload = {"token": result["token"], "new_password": "valid-password"}
    response = client.post("/api/auth/activate", json=payload, headers={"origin": "http://testserver"})
    assert response.status_code == 200
    assert response.json() == {"ok": True, "username": "staff"}
    assert response.headers["cache-control"] == "no-store"
    assert not response.cookies
    assert client.post("/api/auth/activate", json=payload).status_code == 400
    assert db.login(username="staff", password="valid-password")


def test_activation_api_rejects_cross_origin_and_oversized_body(client, db):
    result = invite(db)
    payload = {"token": result["token"], "new_password": "valid-password"}
    assert client.post("/api/auth/activate", json=payload, headers={"origin": "https://evil.example"}).status_code == 403
    assert client.post("/api/auth/activate", content=b"x" * 8193, headers={"content-type": "application/json"}).status_code == 413
    assert client.post("/api/auth/activate", data=payload).status_code == 415
    assert db.get_user(username="staff")["status"] == "pending"


@pytest.mark.parametrize("body", [b"{", b"null", b"[]", b'{"token":123,"new_password":[]}'])
def test_activation_api_bad_body(client, body):
    assert client.post("/api/auth/activate", content=body, headers={"content-type": "application/json"}).status_code == 400
