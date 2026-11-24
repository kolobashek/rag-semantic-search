from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from rag_catalog.core.user_auth_db import LOGIN_MAX_FAILURES
from user_auth_db import UserAuthDB


@pytest.fixture(autouse=True)
def _explicit_test_bootstrap_admin(monkeypatch) -> None:
    monkeypatch.setenv("RAG_BOOTSTRAP_ADMIN_PASSWORD", "admin")
    monkeypatch.delenv("RAG_DISABLE_DEFAULT_ADMIN", raising=False)


def test_public_registration_requires_review_and_new_password(tmp_path):
    db = UserAuthDB(str(tmp_path / "users.db"))
    req = db.request_verification(
        username="Ivan.Petrov", display_name="Ivan", telegram_chat_id="777", password="secret"
    )
    assert req["status"] == "pending" and "code" not in req
    assert db.get_user(username="ivan.petrov") is None
    assert db.change_password(username="admin", old_password="admin", new_password="changed-test-password")
    approved = db.review_employee_request(admin_username="admin", request_id=req["id"], approve=True)
    assert db.activate_account(token=approved["activation_token"], new_password="first-private-password")[
        "ok"
    ]
    assert db.login(username="ivan.petrov", password="secret") is None
    user = db.login(username="ivan.petrov", password="first-private-password")
    assert user["roles"] == ["driver"] and not user["telegram_chat_id"]


def test_admin_is_not_created_without_explicit_bootstrap_password(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("RAG_BOOTSTRAP_ADMIN_PASSWORD", raising=False)
    monkeypatch.delenv("RAG_DISABLE_DEFAULT_ADMIN", raising=False)
    db = UserAuthDB(str(tmp_path / "users.db"))

    assert db.login(username="admin", password="admin") is None
    assert db.get_user(username="admin") is None
    assert db.has_default_admin_password() is False


def test_bootstrap_admin_requires_password_change(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    user = db.login(username="admin", password="admin")
    assert user is not None
    assert user["role"] == "admin"
    assert user["must_change_password"] == 1

    assert db.change_password(username="admin", old_password="admin", new_password="strong-pass")
    changed = db.login(username="admin", password="strong-pass")
    assert changed is not None
    assert changed["must_change_password"] == 0


def test_default_admin_can_use_explicit_bootstrap_password(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("RAG_BOOTSTRAP_ADMIN_PASSWORD", "temporary-secret")
    db = UserAuthDB(str(tmp_path / "users.db"))

    assert db.login(username="admin", password="admin") is None
    user = db.login(username="admin", password="temporary-secret")
    assert user is not None
    assert user["role"] == "admin"
    assert user["must_change_password"] == 1


def test_default_admin_can_be_disabled(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    db = UserAuthDB(str(tmp_path / "users.db"))

    assert db.login(username="admin", password="admin") is None
    assert db.get_user(username="admin") is None


def test_password_login_is_rate_limited_after_repeated_failures(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    for _ in range(5):
        assert db.login_with_reason(username="admin", password="wrong")["reason"] == "invalid_credentials"
        db.log_auth_event(username="admin", event_type="login_failed", ok=False, error="bad_credentials")

    limited = db.login_with_reason(username="admin", password="admin")
    assert limited["reason"] == "rate_limited"
    assert 0 < limited["retry_after_seconds"] <= 15 * 60

    with db._connect() as conn:
        old = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
        conn.execute("UPDATE auth_events SET ts=? WHERE username='admin'", (old,))

    assert db.login_with_reason(username="admin", password="admin")["reason"] == "ok"


def test_successful_login_event_resets_failure_streak(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    for _ in range(4):
        db.log_auth_event(username="admin", event_type="login_failed", ok=False)
    db.log_auth_event(username="admin", event_type="login", ok=True)
    db.log_auth_event(username="admin", event_type="login_failed", ok=False)

    status = db.login_throttle_status(username="admin")
    assert status == {"limited": False, "retry_after_seconds": 0, "failures": 1}


def test_session_token_restores_and_revokes_user(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    user = db.login(username="admin", password="admin")
    assert user is not None

    token = db.create_session(username=user["username"], ttl_days=1)
    restored = db.get_user_by_session(token)
    assert restored is not None
    assert restored["username"] == "admin"
    assert "password_hash" not in restored

    db.revoke_session(token)
    assert db.get_user_by_session(token) is None


def test_group_membership_is_exposed_in_login_and_session(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    assert db.admin_create_user(
        username="alice",
        display_name="Alice",
        password="secret",
        role="user",
        status="active",
        must_change_password=False,
    )
    group = db.create_group(name="Бухгалтерия", description="Финансовые документы", created_by="admin")

    assert db.add_group_member(group_id=group["id"], username="alice", added_by="admin") is True
    assert db.add_group_member(group_id=group["id"], username="alice", added_by="admin") is False

    login_user = db.login(username="alice", password="secret")
    assert login_user is not None
    assert login_user["group_ids"] == [group["id"]]
    assert login_user["groups"][0]["name"] == "Бухгалтерия"

    session = db.create_session(username="alice")
    restored = db.get_user_by_session(session)
    assert restored is not None
    assert restored["group_ids"] == [group["id"]]

    renamed = db.update_group(
        group_id=group["id"],
        name="Финансы",
        description="Обновлено",
        status="active",
    )
    assert renamed is not None
    assert renamed["id"] == group["id"]
    assert db.get_user(username="alice")["groups"][0]["name"] == "Финансы"

    archived = db.update_group(
        group_id=group["id"],
        name="Финансы",
        description="Обновлено",
        status="archived",
    )
    assert archived is not None
    assert db.get_user(username="alice")["group_ids"] == []
    assert db.list_user_groups(username="alice", active_only=False)[0]["status"] == "archived"
    with pytest.raises(RuntimeError, match="архивную группу"):
        db.add_group_member(group_id=group["id"], username="admin", added_by="admin")

    assert db.remove_group_member(group_id=group["id"], username="alice") is True
    assert db.remove_group_member(group_id=group["id"], username="alice") is False


def test_group_names_are_unique_case_insensitively(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    db.create_group(name="Managers")

    with pytest.raises(RuntimeError, match="уже существует"):
        db.create_group(name="managers")


def test_session_default_ttl_is_seven_days(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    token = db.create_session(username="admin")

    with db._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT created_at, expires_at FROM user_sessions WHERE token=?",
            (token,),
        ).fetchone()

    created_at = datetime.fromisoformat(row["created_at"])
    expires_at = datetime.fromisoformat(row["expires_at"])
    assert expires_at - created_at == timedelta(days=7)


def test_session_ttl_setting_controls_new_sessions_and_is_clamped(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))

    assert db.set_session_ttl_days(3) == 3
    token = db.create_session(username="admin")
    with db._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT created_at, expires_at FROM user_sessions WHERE token=?",
            (token,),
        ).fetchone()
    assert datetime.fromisoformat(row["expires_at"]) - datetime.fromisoformat(row["created_at"]) == timedelta(days=3)

    assert db.set_session_ttl_days(30) == 7
    assert db.get_session_ttl_days() == 7


def test_touch_session_extends_active_session_after_interval(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    token = db.create_session(username="admin", ttl_days=1)
    old_seen = datetime.now(timezone.utc) - timedelta(hours=2)
    old_expires = datetime.now(timezone.utc) + timedelta(hours=1)
    with db._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE user_sessions SET last_seen_at=?, expires_at=? WHERE token=?",
            (old_seen.isoformat(), old_expires.isoformat(), token),
        )

    assert db.touch_session(token, min_interval_minutes=60, ttl_days=1) is True

    with db._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT last_seen_at, expires_at FROM user_sessions WHERE token=?",
            (token,),
        ).fetchone()
    assert datetime.fromisoformat(row["last_seen_at"]) > old_seen
    assert datetime.fromisoformat(row["expires_at"]) > old_expires + timedelta(hours=20)


def test_touch_session_is_throttled_for_recent_activity(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    token = db.create_session(username="admin", ttl_days=1)
    with db._connect() as conn:  # noqa: SLF001
        before = conn.execute(
            "SELECT last_seen_at, expires_at FROM user_sessions WHERE token=?",
            (token,),
        ).fetchone()

    assert db.touch_session(token, min_interval_minutes=60, ttl_days=7) is False

    with db._connect() as conn:  # noqa: SLF001
        after = conn.execute(
            "SELECT last_seen_at, expires_at FROM user_sessions WHERE token=?",
            (token,),
        ).fetchone()
    assert after["last_seen_at"] == before["last_seen_at"]
    assert after["expires_at"] == before["expires_at"]


def test_touch_session_does_not_revive_expired_session(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    token = db.create_session(username="admin", ttl_days=1)
    old_seen = datetime.now(timezone.utc) - timedelta(days=2)
    old_expires = datetime.now(timezone.utc) - timedelta(minutes=1)
    with db._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE user_sessions SET last_seen_at=?, expires_at=? WHERE token=?",
            (old_seen.isoformat(), old_expires.isoformat(), token),
        )

    assert db.touch_session(token, min_interval_minutes=60, ttl_days=1) is False

    with db._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT last_seen_at, expires_at FROM user_sessions WHERE token=?",
            (token,),
        ).fetchone()
    assert row["last_seen_at"] == old_seen.isoformat()
    assert row["expires_at"] == old_expires.isoformat()


def test_get_user_by_session_touches_old_activity(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    token = db.create_session(username="admin", ttl_days=1)
    old_seen = datetime.now(timezone.utc) - timedelta(hours=2)
    old_expires = datetime.now(timezone.utc) + timedelta(hours=1)
    with db._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE user_sessions SET last_seen_at=?, expires_at=? WHERE token=?",
            (old_seen.isoformat(), old_expires.isoformat(), token),
        )

    assert db.get_user_by_session(token)["username"] == "admin"

    with db._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT last_seen_at, expires_at FROM user_sessions WHERE token=?",
            (token,),
        ).fetchone()
    assert datetime.fromisoformat(row["last_seen_at"]) > old_seen
    assert datetime.fromisoformat(row["expires_at"]) > old_expires + timedelta(hours=20)


def test_show_system_files_setting_is_persisted(tmp_path) -> None:
    db_path = tmp_path / "users.db"
    db = UserAuthDB(str(db_path))

    assert db.get_show_system_files_for_admin() is False
    assert db.set_show_system_files_for_admin(True) is True

    reopened = UserAuthDB(str(db_path))
    assert reopened.get_show_system_files_for_admin() is True


def test_active_user_can_be_found_by_telegram_chat_id(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    db.admin_create_user(
        username="ivan",
        display_name="Ivan",
        telegram_chat_id="777",
        password="pw",
        role="user",
        status="active",
        must_change_password=False,
    )

    user = db.get_user_by_telegram_chat_id("777")
    assert user is not None
    assert user["username"] == "ivan"
    assert db.get_user_by_telegram_chat_id("missing") is None


def test_telegram_chat_id_can_be_unlinked(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    db.admin_create_user(
        username="ivan",
        display_name="Ivan",
        telegram_chat_id="777",
        password="pw",
        role="user",
        status="active",
        must_change_password=False,
    )

    assert db.unlink_telegram_chat_id("777") == "ivan"
    assert db.get_user_by_telegram_chat_id("777") is None
    assert db.unlink_telegram_chat_id("777") is None


def test_contact_requires_admin_and_matching_sender_before_activation(tmp_path):
    db = UserAuthDB(str(tmp_path / "users.db"))
    contact = dict(telegram_chat_id="777", username_hint="ivan", display_name="Ivan")
    assert not db.upsert_user_from_telegram_contact(**contact)["ok"]
    assert db.change_password(username="admin", old_password="admin", new_password="changed-password")
    invitation = db.upsert_user_from_telegram_contact(**contact, created_by="admin")
    assert invitation["ok"] and "temp_password" not in invitation
    assert not db.claim_employee_invite(provider="telegram", sender_id="778", token=invitation["token"])["ok"]
    claimed = db.claim_employee_invite(provider="telegram", sender_id="777", token=invitation["token"])
    assert claimed["ok"] and db.get_user_by_telegram_chat_id("777") is None
    assert db.activate_account(token=claimed["activation_token"], new_password="private-password")["ok"]
    assert db.get_user_by_telegram_chat_id("777")["username"] == invitation["username"]
    assert not db.upsert_user_from_telegram_contact(**contact, created_by="admin")["ok"]


def test_list_telegram_chats_returns_bound_users(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    db.admin_create_user(
        username="ivan",
        display_name="Ivan",
        telegram_chat_id="777",
        password="pw",
        role="user",
        status="active",
        must_change_password=False,
    )
    db.log_auth_event(username="ivan", event_type="telegram_search", ok=True)
    rows = db.list_telegram_chats()
    assert len(rows) == 1
    assert rows[0]["username"] == "ivan"
    assert rows[0]["telegram_chat_id"] == "777"
    assert rows[0]["last_telegram_event_at"]


def test_verification_expired(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    assert db.admin_create_user(username="user1", password="pw", must_change_password=False)
    req = db.request_telegram_link_code(username="user1", password="pw", telegram_chat_id="111")

    # Принудительно делаем токен просроченным.
    with db._connect() as conn:  # noqa: SLF001 - тестовая проверка состояния
        conn.execute(
            "UPDATE verification_tokens SET expires_at=? WHERE username=?",
            ((datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(), "user1"),
        )

    out = db.confirm_verification(telegram_chat_id="111", code=req["code"])
    assert out["ok"] is False
    assert out["reason"] == "expired"


def test_password_reset_requires_telegram_confirmation(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    assert db.admin_create_user(
        username="user2", password="old", telegram_chat_id="222", must_change_password=False
    )

    reset = db.request_password_reset(username="user2", telegram_chat_id="222")
    assert reset["ok"] is True
    assert db.complete_password_reset(
        username="user2",
        code=reset["code"],
        new_password="new",
    )["ok"] is False

    confirmed = db.confirm_password_reset(telegram_chat_id="222", code=reset["code"])
    assert confirmed["ok"] is True
    assert db.complete_password_reset(
        username="user2",
        code=reset["code"],
        new_password="new",
    )["ok"] is True
    assert db.login(username="user2", password="new") is not None


def test_expired_session_is_rejected(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    token = db.create_session(username="admin", ttl_days=1)
    # Принудительно просрочиваем сессию.
    with db._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE user_sessions SET expires_at=? WHERE token=?",
            ((datetime.now(timezone.utc) - timedelta(days=1)).isoformat(), token),
        )
    assert db.get_user_by_session(token) is None


def test_registration_cannot_overwrite_active_user_password(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    with pytest.raises(ValueError):
        db.request_verification(
            username="admin",
            display_name="Fake Admin",
            telegram_chat_id="999",
            password="hacked",
        )
    assert db.login(username="admin", password="admin") is not None


def test_request_telegram_link_code_for_active_user(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    assert db.change_password(username="admin", old_password="admin", new_password="changed-password")
    out = db.request_telegram_link_code(
        username="admin",
        password="changed-password",
        telegram_chat_id="777",
    )
    assert out["ok"] is True
    confirmed = db.confirm_verification(telegram_chat_id="777", code=str(out["code"]))
    assert confirmed["ok"] is True
    user = db.get_user_by_telegram_chat_id("777")
    assert user is not None
    assert user["username"] == "admin"


def test_request_telegram_link_code_rejects_bad_password(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    out = db.request_telegram_link_code(
        username="admin",
        password="wrong",
        telegram_chat_id="777",
    )
    assert out["ok"] is False
    assert out["reason"] == "bad_credentials"


def test_request_telegram_deeplink_code_confirms_from_any_chat(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    assert db.change_password(username="admin", old_password="admin", new_password="changed-password")
    out = db.request_telegram_deeplink_code(
        username="admin",
        password="changed-password",
    )
    assert out["ok"] is True
    confirmed = db.confirm_verification(telegram_chat_id="555", code=str(out["code"]))
    assert confirmed["ok"] is True
    user = db.get_user_by_telegram_chat_id("555")
    assert user is not None
    assert user["username"] == "admin"


def test_telegram_link_token_binds_chat_to_active_user(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    assert db.change_password(username="admin", old_password="admin", new_password="changed-password")
    token = db.create_telegram_link_token(session_token=db.create_session(username="admin"))
    assert token["ok"] is True

    out = db.consume_telegram_start_token(
        token=str(token["token"]),
        telegram_chat_id="777",
        telegram_username="admin_tg",
        display_name="Admin",
    )
    assert out["ok"] is True
    assert out["purpose"] == "link"
    user = db.get_user_by_telegram_chat_id("777")
    assert user is not None
    assert user["username"] == "admin"
    assert user["telegram_username"] == "admin_tg"


def test_telegram_login_explicit_confirmation_and_independent_browser_verifier(tmp_path):
    db = UserAuthDB(str(tmp_path / "users.db"))
    assert db.admin_create_user(
        username="botuser", password="secret", telegram_chat_id="777", must_change_password=False
    )
    challenge = db.create_telegram_login_challenge()
    token, verifier = challenge["token"], challenge["verifier"]
    assert not db.consume_telegram_start_token(token=token, telegram_chat_id="777")["ok"]
    assert db.consume_confirmed_telegram_login(verifier=verifier)["reason"] == "pending"
    assert not db.confirm_telegram_login(token=token, telegram_chat_id="778")["ok"]
    assert db.confirm_telegram_login(token=token, telegram_chat_id="777")["ok"]
    assert not db.confirm_telegram_login(token=token, telegram_chat_id="777")["ok"]
    assert not db.consume_confirmed_telegram_login(verifier=token)["ok"]
    assert db.consume_confirmed_telegram_login(verifier=verifier)["username"] == "botuser"
    assert not db.consume_confirmed_telegram_login(verifier=verifier)["ok"]


def test_registration_request_can_be_approved(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    assert db.change_password(username="admin", old_password="admin", new_password="changed-password")
    req = db.create_registration_request(
        username="ivan",
        display_name="Ivan",
        telegram_chat_id="777",
        telegram_username="ivan_tg",
        source="telegram",
    )
    rows = db.list_registration_requests(status="pending")
    assert rows[0]["id"] == req["id"]

    approved = db.review_registration_request(
        request_id=int(req["id"]),
        reviewed_by="admin",
        decision="approved",
    )
    assert approved["ok"] is True
    assert db.get_user_by_telegram_chat_id("777") is None
    assert db.activate_account(token=approved["activation_token"], new_password="initial-password")["ok"]
    user = db.get_user_by_telegram_chat_id("777")
    assert user is not None
    assert user["username"] == "ivan"


def test_admin_invite_requires_sender_claim_and_password(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    assert db.change_password(username="admin", old_password="admin", new_password="changed-password")
    out = db.create_admin_invite(
        created_by="admin",
        username="ivan",
        display_name="Ivan",
        telegram_username="@ivan_tg",
    )
    assert out["ok"] is True
    assert "temp_password" not in out
    consumed = db.claim_employee_invite(token=out["token"], provider="telegram", sender_id="777")
    assert consumed["ok"] and db.get_user_by_telegram_chat_id("777") is None
    assert db.activate_account(token=consumed["activation_token"], new_password="initial-password")["ok"]
    user = db.get_user_by_telegram_chat_id("777")
    assert user is not None
    assert user["username"] == "ivan"


def test_user_settings_are_saved_reset_and_isolated(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))

    db.save_user_settings(
        username="admin",
        settings={"explorer": {"view": "Список", "sort": "По дате", "desc": True, "ext": ".pdf"}},
    )
    db.save_user_settings(
        username="other",
        settings={"explorer": {"view": "Таблица"}},
    )

    assert db.get_user_settings(username="admin")["explorer"]["view"] == "Список"
    assert db.get_user_settings(username="admin")["explorer"]["desc"] is True
    assert db.get_user_settings(username="other")["explorer"]["view"] == "Таблица"

    db.reset_user_settings(username="admin")
    assert db.get_user_settings(username="admin") == {}
    assert db.get_user_settings(username="other")["explorer"]["view"] == "Таблица"


def test_user_favorites_support_files_folders_dedupe_and_isolation(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))

    db.add_favorite(username="admin", item_type="folder", path="O:\\Docs", title="Docs")
    db.add_favorite(username="admin", item_type="file", path="O:\\Docs\\a.pdf", title="A")
    db.add_favorite(username="admin", item_type="file", path="O:\\Docs\\a.pdf", title="A2")
    db.add_favorite(username="other", item_type="folder", path="O:\\Docs", title="Other Docs")

    admin_favorites = db.list_favorites(username="admin")
    assert len(admin_favorites) == 2
    assert {item["item_type"] for item in admin_favorites} == {"folder", "file"}
    assert any(item["title"] == "A2" for item in admin_favorites)
    assert len(db.list_favorites(username="other")) == 1

    assert db.remove_favorite(username="admin", path="O:\\Docs\\a.pdf") is True
    assert len(db.list_favorites(username="admin")) == 1
    assert len(db.list_favorites(username="other")) == 1


def test_auth_events_are_logged(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))

    db.log_auth_event(username="admin", event_type="login", ok=True)
    db.log_auth_event(username="admin", event_type="login_failed", ok=False, error="bad_credentials")

    events = db.list_auth_events(limit=10)
    assert [event["event_type"] for event in events] == ["login_failed", "login"]
    assert events[0]["ok"] == 0
    assert events[0]["error"] == "bad_credentials"


def test_ui_login_forms_never_use_unthrottled_plain_login() -> None:
    """Все формы входа обязаны идти через login_with_reason.

    Плейн ``login()`` не проверяет login_throttle_status и не пишет события
    ``login_failed``, поэтому любая форма на нём — неограниченный оракул для
    перебора паролей и слепая зона аудита.
    """
    ui_dir = Path(__file__).resolve().parent.parent / "src" / "rag_catalog" / "ui"
    offenders: list[str] = []
    for source in sorted(ui_dir.rglob("*.py")):
        for number, line in enumerate(source.read_text(encoding="utf-8").splitlines(), start=1):
            if "auth_db.login(" in line:
                offenders.append(f"{source.name}:{number}: {line.strip()}")
    assert offenders == [], "Формы входа должны использовать login_with_reason: " + "; ".join(offenders)


def test_login_with_reason_locks_out_after_repeated_failures(tmp_path: Path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    db.admin_create_user(
        username="alice",
        display_name="Alice",
        telegram_chat_id="",
        password="correct-horse",
        role="user",
        status="active",
        must_change_password=False,
    )

    for _ in range(LOGIN_MAX_FAILURES):
        assert db.login_with_reason(username="alice", password="wrong")["reason"] == "invalid_credentials"
        db.log_auth_event(username="alice", event_type="login_failed", ok=False, error="bad_credentials")

    limited = db.login_with_reason(username="alice", password="correct-horse")
    assert limited["reason"] == "rate_limited"
    assert int(limited.get("retry_after_seconds") or 0) > 0
