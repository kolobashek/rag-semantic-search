from concurrent.futures import ThreadPoolExecutor

import pytest

from rag_catalog.core.user_auth_db import UserAuthDB


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    store = UserAuthDB(str(tmp_path / "employees.sqlite"))
    assert store.admin_create_user(
        username="owner", role="admin", password="testing", must_change_password=False
    )
    assert store.create_employee_organization(admin_username="owner", organization_id="north", name="North")[
        "ok"
    ]
    assert store.admin_create_user(
        username="manager",
        roles=["admin"],
        organization_id="north",
        password="testing",
        must_change_password=False,
    )
    return store


def test_scoped_admin_cannot_grant_system_or_other_organization_access(db):
    manager = db.login(username="manager", password="testing")
    assert manager["organization_roles"] == {"north": ["admin"]}
    assert manager["is_platform_admin"] is False
    assert manager["role"] == "driver" and manager["roles"] == ["driver"]
    assert [o["id"] for o in db.list_employee_organizations(admin_username="manager")] == ["north"]
    assert not db.create_employee_organization(admin_username="manager", name="Another")["ok"]
    assert not db.create_account_activation(username="unknown", display_name="", created_by="manager")["ok"]
    invite = db.create_account_activation(
        username="another", display_name="", created_by="manager", organization_id="north", roles=["admin"]
    )
    assert db.activate_account(token=invite["token"], new_password="another-password")["ok"]
    assert db.get_user(username="another")["organization_roles"] == {"north": ["admin"]}
    assert not db.get_user(username="another")["is_platform_admin"]
    assert not db.can_manage_employee(admin_username="manager", username="owner")
    assert not db.set_employee_org_roles(
        admin_username="manager", username="owner", organization_id="north", roles=["driver"]
    )["ok"]
    assert not db.reset_employee_channel(admin_username="manager", username="owner", provider="telegram")[
        "ok"
    ]


@pytest.mark.parametrize("provider", ["max", "telegram"])
def test_onboarding_deduplicates_review_recovers_activation_and_revokes(db, provider):
    args = dict(provider=provider, sender_id="711", display_name="New Driver", organization_id="north")
    with ThreadPoolExecutor(max_workers=2) as workers:
        submissions = list(workers.map(lambda _: db.submit_employee_request(**args), range(2)))
    assert submissions[0]["id"] == submissions[1]["id"]
    assert sorted(row["created"] for row in submissions) == [False, True]
    req = submissions[0]["id"]
    assert db.admin_create_user(
        username="other_admin", roles=["admin"], password="testing", must_change_password=False
    )
    assert not db.list_employee_requests(admin_username="other_admin")
    assert not db.review_employee_request(request_id=req, admin_username="other_admin", approve=True)["ok"]
    review = db.review_employee_request(request_id=req, admin_username="manager", approve=True)
    assert review["ok"] and review["provider"] == provider and review["sender_id"] == "711"
    assert "temp_password" not in review
    assert "activation_token" not in db.employee_request_status(provider=provider, sender_id="712")
    status = db.employee_request_status(provider=provider, sender_id="711")
    assert status["status"] == "approved"
    assert not db.activate_account(token=review["activation_token"], new_password="test-password")["ok"]
    assert db.activate_account(token=status["activation_token"], new_password="test-password")["ok"]
    who = db.get_user(username=status["username"])
    assert who["roles"] == ["driver"] and who["organization_ids"] == ["north"]
    session = db.create_session(username=who["username"])
    assert db.reset_employee_channel(admin_username="manager", username=who["username"], provider=provider)[
        "ok"
    ]
    assert db.get_user_by_session(session) is None
    assert not db.employee_request_status(provider=provider, sender_id="711").get("activation_token")


def test_contact_renewal_and_cross_provider_claim(db):
    args = dict(
        username="staff",
        display_name="Staff",
        organization_id="north",
        expected_sender_id="555",
        provider="max",
        admin_username="owner",
        request_key="evt1",
    )
    first = db.create_employee_invite(**args)
    second = db.create_employee_invite(**args)
    assert second["username"] == first["username"]
    assert (
        db.create_employee_invite(**{**args, "organization_id": "default"}, renew_pending=True)["reason"]
        == "wrong_organization"
    )
    assert not db.claim_employee_invite(provider="max", sender_id=555, token=first["token"])["ok"]
    assert not db.claim_employee_invite(provider="telegram", sender_id=555, token=second["token"])["ok"]
    assert not db.claim_employee_invite(provider="max", sender_id=556, token=second["token"])["ok"]
    assert db.claim_employee_invite(provider="max", sender_id=555, token=second["token"])["ok"]
    assert not db.create_employee_invite(**args)["ok"]


def test_collision_after_request_and_scoped_recovery(db):
    request = db.submit_employee_request(
        provider="telegram", sender_id=333, username="office", display_name="Staff", organization_id="north"
    )
    assert db.admin_create_user(username="office", password="existing-password", must_change_password=False)
    assert (
        db.review_employee_request(admin_username="manager", request_id=request["id"], approve=True)["reason"]
        == "already_exists"
    )
    assert db.get_user_by_telegram_chat_id(333) is None and db.login(
        username="office", password="existing-password"
    )
    assert db.set_employee_org_roles(
        admin_username="owner", username="office", organization_id="north", roles=["driver"]
    )["ok"]
    assert not db.can_manage_employee(admin_username="manager", username="office")
    assert not db.reset_employee_channel(admin_username="manager", username="office", provider="max")["ok"]
    for row in db.list_managed_employees(admin_username="manager"):
        assert row["organization_ids"] == ["north"]
    session = db.create_session(username="manager")
    with pytest.raises(PermissionError):
        db.save_driver(
            session, username="office", display_name="Other", organization_id="north", archived=True
        )
    db.save_driver(
        session,
        username="newdriver",
        display_name="Driver",
        organization_id="north",
        password="first-test-password",
        create=True,
    )
    assert db.get_user(username="newdriver")["organization_ids"] == ["north"]


def test_telegram_expired_source_and_unlink_invalidate_credentials(db):
    assert db.admin_create_user(username="driver", password="test-pw", must_change_password=False)
    session = db.create_session(username="driver")
    first = db.create_telegram_link_token(session_token=session)
    db.revoke_session(session)
    assert not db.consume_telegram_start_token(token=first["token"], telegram_chat_id="900")["ok"]
    session = db.create_session(username="driver")
    token = db.create_telegram_link_token(session_token=session)["token"]
    assert db.consume_telegram_start_token(token=token, telegram_chat_id="900")["ok"]
    assert not db.consume_telegram_start_token(token=token, telegram_chat_id="901")["ok"]
    assert db.update_profile(username="driver", display_name="Driver", telegram_chat_id="999")
    assert db.get_user_by_telegram_chat_id("900")["username"] == "driver"
    first_binding = db.get_user_by_telegram_chat_id("900")["telegram_linked_at"]
    challenge = db.create_telegram_login_challenge()
    assert db.confirm_telegram_login(token=challenge["token"], telegram_chat_id="900")["ok"]
    assert db.unlink_telegram_for_session(session_token=session)
    assert not db.consume_confirmed_telegram_login(verifier=challenge["verifier"])["ok"]
    again = db.create_telegram_link_token(session_token=session)
    assert db.consume_telegram_start_token(token=again["token"], telegram_chat_id="900")["ok"]
    assert db.get_user_by_telegram_chat_id("900")["telegram_linked_at"] != first_binding


def test_existing_employee_referral_checks_organization_and_preserves_identity(db):
    assert db.admin_create_user(username="remote", password="original", must_change_password=False)
    for name, status in (("local", "active"), ("former", "blocked")):
        assert db.admin_create_user(
            username=name,
            password="original",
            organization_id="north",
            status=status,
            must_change_password=False,
        )
    args = dict(provider="max", sender_id="788", organization_id="north", display_name="Applicant")
    request = db.submit_employee_request(**args)
    review = dict(admin_username="manager", request_id=request["id"])
    assert not db.refer_employee_request(**review, username="remote")["ok"]
    assert not db.refer_employee_request(**review, username="former")["ok"]
    result = db.refer_employee_request(**review, username="local")
    assert result == {"ok": True, "status": "use_existing", "provider": "max", "sender_id": "788"}
    assert not db.refer_employee_request(**review, username="local")["ok"]
    assert not db.review_employee_request(**review, approve=True)["ok"]
    status = db.employee_request_status(provider="max", sender_id="788")
    assert status["status"] == "use_existing" and not status.get("activation_token")
    assert db.get_user_by_max_user_id("788") is None
    assert db.login(username="local", password="original")["roles"] == ["driver"]
    assert db.submit_employee_request(**args)["created"]


@pytest.mark.parametrize("reset", ["platform", "dispatcher"])
def test_admin_password_reset_revokes_bound_channels_and_sessions(db, reset):
    assert db.admin_create_user(
        username="resetdriver",
        password="original",
        telegram_chat_id="633",
        organization_id="north",
        must_change_password=False,
    )
    session = db.create_session(username="resetdriver")
    link = db.create_max_link_token(session_token=session)
    assert db.consume_max_link_token(token=link["token"], max_user_id="973")["ok"]
    if reset == "platform":
        assert db.admin_set_password(username="resetdriver", new_password="initial-password")
    else:
        db.save_driver(
            db.create_session(username="manager"),
            username="resetdriver",
            display_name="Driver",
            password="initial-password",
            organization_id="north",
        )
    assert db.get_user_by_session(session) is None
    assert db.change_password(
        username="resetdriver", old_password="initial-password", new_password="private-new-password"
    )
    assert db.get_user_by_max_user_id("973") is None
    assert db.get_user_by_telegram_chat_id("633") is None


def test_v5_migrates_global_admin_and_disables_ambiguous_legacy_bindings(tmp_path, monkeypatch):
    import sqlite3

    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    path = tmp_path / "v5.sqlite"
    with sqlite3.connect(path) as old:
        old.execute("""CREATE TABLE users (id INTEGER PRIMARY KEY,username TEXT UNIQUE,display_name TEXT DEFAULT '',
            telegram_chat_id TEXT DEFAULT '',telegram_username TEXT DEFAULT '',password_hash TEXT DEFAULT '',
            role TEXT DEFAULT 'user',roles_json TEXT DEFAULT '',must_change_password INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active',created_at TEXT DEFAULT '',verified_at TEXT,last_login_at TEXT)""")
        old.executemany(
            "INSERT INTO users(username,role,telegram_chat_id) VALUES(?,?,?)",
            [
                ("legacy_admin", "admin", "700"),
                ("legacy_office", "user", "700"),
                ("legacy_driver", "driver", "701"),
            ],
        )
    migrated = UserAuthDB(str(path))
    assert migrated.get_user(username="legacy_admin")["is_platform_admin"]
    assert migrated.get_user(username="legacy_office")["organization_roles"] == {"default": ["user"]}
    assert migrated.get_user_by_telegram_chat_id("700") is None
    assert migrated.get_user_by_telegram_chat_id("701")["username"] == "legacy_driver"
    assert not migrated.admin_create_user(username="conflict", password="pw", telegram_chat_id="701")
    assert UserAuthDB(str(path)).get_user(username="legacy_admin")["roles"] == ["admin"]
