import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from rag_catalog.core.user_auth_db import UserAuthDB


@pytest.fixture
def auth(tmp_path, monkeypatch):
    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    db = UserAuthDB(str(tmp_path / "users.db"))
    for name, roles, telegram in (("admin", ["admin"], "702"), ("worker", ["driver", "user"], "701")):
        db.admin_create_user(username=name, password="fixture-password", roles=roles,
                             telegram_chat_id=telegram, must_change_password=False)
    return db


def issue_link(auth, username):
    session = auth.create_session(username=username)
    return auth.create_max_link_token(session_token=session)["token"]


def link(auth, username, sender):
    return auth.consume_max_link_token(token=issue_link(auth, username), max_user_id=sender)


def update_worker(auth, *, roles=("driver", "user"), status="active", must_change=False):
    auth.admin_update_user(username="worker", display_name="Employee", telegram_chat_id="701",
                           role="user", roles=list(roles), status=status, must_change_password=must_change)


def test_max_identity_is_independent_and_uses_current_roles_and_groups(auth):
    assert auth.get_user_by_max_user_id("702") is None
    assert link(auth, "worker", 702)["ok"]
    assert auth.get_user_by_telegram_chat_id("702")["username"] == "admin"
    group = auth.create_group(name="Office", created_by="admin")
    auth.add_group_member(group_id=group["id"], username="worker", added_by="admin")
    user = auth.get_user_by_max_user_id("702")
    assert user["username"] == "worker" and user["roles"] == ["driver", "user"]
    assert "password_hash" not in user and user["group_ids"] == [group["id"]]
    assert auth.get_user(username="worker")["max_user_id"] == "702"
    update_worker(auth, roles=["driver"])
    assert auth.get_user_by_max_user_id(702)["roles"] == ["driver"]
    update_worker(auth, status="blocked")
    assert auth.get_user_by_max_user_id(702) is None
    update_worker(auth, must_change=True)
    assert auth.get_user_by_max_user_id(702) is None


def test_max_link_requires_live_session_and_rotates_pending_links(auth):
    assert not auth.create_max_link_token(session_token="")["ok"]
    session = auth.create_session(username="worker")
    first = auth.create_max_link_token(session_token=session)["token"]
    token = auth.create_max_link_token(session_token=session)["token"]
    assert not auth.consume_max_link_token(token=first, max_user_id=701)["ok"]
    with sqlite3.connect(auth.db_path) as conn:
        saved = conn.execute("SELECT token_hash FROM max_link_tokens").fetchone()[0]
    assert saved != token
    auth.revoke_session(session)
    assert not auth.consume_max_link_token(token=token, max_user_id=701)["ok"]
    token = issue_link(auth, "worker")
    for bad_sender in ("@public_handle", "", 0, -701, True, "0007", 2**63):
        assert not auth.consume_max_link_token(token=token, max_user_id=bad_sender)["ok"]
    with sqlite3.connect(auth.db_path) as conn:
        conn.execute("UPDATE max_link_tokens SET expires_at='2000-01-01'")
    assert not auth.consume_max_link_token(token=token, max_user_id=701)["ok"]


def test_binding_is_atomic_across_workers_and_cannot_replace_existing_identity(auth):
    other_worker = UserAuthDB(str(auth.db_path))
    token = issue_link(auth, "worker")
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda pair: pair[0].consume_max_link_token(token=token, max_user_id=pair[1]),
                                ((auth, "111"), (other_worker, "222"))))
    assert sum(r["ok"] for r in results) == 1
    sender = auth.get_user(username="worker")["max_user_id"]
    assert auth.consume_max_link_token(token=token, max_user_id=sender)["ok"] is False
    assert auth.create_max_link_token(session_token=auth.create_session(username="worker"))["reason"] == "already_linked"
    assert link(auth, "admin", sender)["reason"] == "already_linked"
    assert auth.get_user_by_max_user_id(sender)["username"] == "worker"


def test_confirmed_browser_login_uses_separate_one_time_verifier(auth):
    assert link(auth, "worker", 111)["ok"]
    ch = auth.create_max_login_challenge()
    assert ch["verifier"] != ch["token"]
    assert auth.consume_confirmed_max_login(verifier=ch["verifier"])["reason"] == "pending"
    assert not auth.confirm_max_login(token=ch["verifier"], max_user_id=111)["ok"]
    assert not auth.confirm_max_login(token=ch["token"], max_user_id=222)["ok"]
    assert auth.confirm_max_login(token=ch["token"], max_user_id=111)["ok"]
    assert not auth.consume_confirmed_max_login(verifier=ch["token"])["ok"]
    assert not auth.confirm_max_login(token=ch["token"], max_user_id=111)["ok"]
    update_worker(auth, roles=["driver"])
    other_worker = UserAuthDB(str(auth.db_path))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda db: db.consume_confirmed_max_login(verifier=ch["verifier"]), (auth, other_worker)))
    assert [r["user"]["roles"] for r in results if r["ok"]] == [["driver"]]


def test_expired_login_and_unlinked_user_cannot_redeem(auth):
    assert link(auth, "worker", 111)["ok"]
    ch = auth.create_max_login_challenge()
    with sqlite3.connect(auth.db_path) as conn:
        conn.execute("UPDATE max_login_challenges SET expires_at='2000-01-01'")
    assert not auth.confirm_max_login(token=ch["token"], max_user_id=111)["ok"]
    assert auth.consume_confirmed_max_login(verifier=ch["verifier"])["reason"] == "expired"
    ch = auth.create_max_login_challenge()
    assert auth.confirm_max_login(token=ch["token"], max_user_id=111)["ok"]
    assert not auth.unlink_max_for_session(session_token="missing")
    assert auth.unlink_max_for_session(session_token=auth.create_session(username="worker"))
    assert link(auth, "admin", 111)["ok"]
    assert not auth.consume_confirmed_max_login(verifier=ch["verifier"])["ok"]
    assert auth.unlink_max_user(111) == "admin"
    assert auth.get_user_by_max_user_id(111) is None
    assert auth.get_user_by_telegram_chat_id("702")["username"] == "admin"


def test_activation_preserves_invited_roles_and_resets_existing_binding(auth):
    options = dict(username="new_driver", display_name="Employee", roles=["driver", "dispatcher"])
    assert auth.create_account_activation(created_by="worker", **options)["reason"] == "not_admin"
    invite = auth.create_account_activation(created_by="admin", **options)
    assert invite["ok"] and auth.get_user(username="new_driver")["status"] == "pending"
    assert auth.activate_account(token=invite["token"], new_password="new-fixture-password")["ok"]
    assert link(auth, "new_driver", 789)["ok"]
    assert auth.get_user_by_max_user_id(789)["roles"] == ["dispatcher", "driver"]
    assert auth.create_account_activation(username="new_driver", display_name="Same", created_by="admin",
                                         reset_existing=True, roles=["admin"])["ok"]
    assert auth.get_user_by_max_user_id(789) is None
    assert auth.get_user(username="new_driver")["roles"] == ["dispatcher", "driver"]
