from datetime import datetime, timedelta, timezone

import pytest

from user_auth_db import UserAuthDB


def test_user_verification_and_login_flow(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    req = db.request_verification(
        username="Ivan.Petrov",
        display_name="Иван",
        telegram_chat_id="777",
        password="secret",
        ttl_minutes=30,
    )
    assert len(req["code"]) == 6
    assert db.login(username="ivan.petrov", password="secret") is None

    out = db.confirm_verification(telegram_chat_id="777", code=req["code"])
    assert out["ok"] is True
    assert out["username"] == "ivan.petrov"

    user = db.login(username="IVAN.PETROV", password="secret")
    assert user is not None
    assert user["status"] == "active"
    assert user["role"] == "user"


def test_default_admin_requires_password_change(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    user = db.login(username="admin", password="admin")
    assert user is not None
    assert user["role"] == "admin"
    assert user["must_change_password"] == 1

    assert db.change_password(username="admin", old_password="admin", new_password="strong-pass")
    changed = db.login(username="admin", password="strong-pass")
    assert changed is not None
    assert changed["must_change_password"] == 0


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


def test_upsert_user_from_telegram_contact_creates_active_user(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    out = db.upsert_user_from_telegram_contact(
        telegram_chat_id="777",
        username_hint="Ivan.Petrov",
        display_name="Ivan Petrov",
    )
    assert out["created"] is True
    assert out["username"].startswith("ivan.petrov")
    assert out["temp_password"]

    user = db.get_user_by_telegram_chat_id("777")
    assert user is not None
    assert user["status"] == "active"


def test_upsert_user_from_telegram_contact_updates_existing_binding(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    created = db.upsert_user_from_telegram_contact(
        telegram_chat_id="777",
        username_hint="ivan",
        display_name="Ivan",
    )
    updated = db.upsert_user_from_telegram_contact(
        telegram_chat_id="777",
        username_hint="new_name",
        display_name="Ivan Updated",
    )
    assert updated["created"] is False
    assert updated["username"] == created["username"]
    assert updated["temp_password"] == ""


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
    req = db.request_verification(
        username="user1",
        display_name="U1",
        telegram_chat_id="111",
        password="pw",
        ttl_minutes=1,
    )

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
    req = db.request_verification(
        username="user2",
        display_name="U2",
        telegram_chat_id="222",
        password="old",
    )
    db.confirm_verification(telegram_chat_id="222", code=req["code"])

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
    out = db.request_telegram_link_code(
        username="admin",
        password="admin",
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
    out = db.request_telegram_deeplink_code(
        username="admin",
        password="admin",
    )
    assert out["ok"] is True
    confirmed = db.confirm_verification(telegram_chat_id="555", code=str(out["code"]))
    assert confirmed["ok"] is True
    user = db.get_user_by_telegram_chat_id("555")
    assert user is not None
    assert user["username"] == "admin"


def test_telegram_link_token_binds_chat_to_active_user(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    token = db.create_telegram_link_token(username="admin")
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


def test_telegram_login_challenge_requires_linked_active_user(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    challenge = db.create_telegram_login_challenge(target="web")
    denied = db.consume_telegram_start_token(
        token=str(challenge["token"]),
        telegram_chat_id="777",
        telegram_username="admin_tg",
    )
    assert denied["ok"] is False
    assert denied["reason"] == "telegram_not_linked"

    link = db.create_telegram_link_token(username="admin")
    db.consume_telegram_start_token(token=str(link["token"]), telegram_chat_id="777", telegram_username="admin_tg")
    challenge = db.create_telegram_login_challenge(target="web")
    confirmed = db.consume_telegram_start_token(
        token=str(challenge["token"]),
        telegram_chat_id="777",
        telegram_username="admin_tg",
    )
    assert confirmed["ok"] is True
    assert confirmed["purpose"] == "login"
    consumed = db.consume_confirmed_telegram_login(token=str(challenge["token"]))
    assert consumed["ok"] is True
    assert consumed["username"] == "admin"


def test_registration_request_can_be_approved(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
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
    user = db.get_user_by_telegram_chat_id("777")
    assert user is not None
    assert user["username"] == "ivan"


def test_admin_invite_creates_active_user_and_token(tmp_path) -> None:
    db = UserAuthDB(str(tmp_path / "users.db"))
    out = db.create_admin_invite(
        created_by="admin",
        username="ivan",
        display_name="Ivan",
        telegram_username="@ivan_tg",
    )
    assert out["ok"] is True
    consumed = db.consume_telegram_start_token(
        token=str(out["token"]),
        telegram_chat_id="777",
        telegram_username="ivan_tg",
    )
    assert consumed["ok"] is True
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
