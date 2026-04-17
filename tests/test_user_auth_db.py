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
