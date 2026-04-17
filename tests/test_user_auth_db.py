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


def test_show_system_files_setting_is_persisted(tmp_path) -> None:
    db_path = tmp_path / "users.db"
    db = UserAuthDB(str(db_path))

    assert db.get_show_system_files_for_admin() is False
    assert db.set_show_system_files_for_admin(True) is True

    reopened = UserAuthDB(str(db_path))
    assert reopened.get_show_system_files_for_admin() is True


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
