"""
user_auth_db.py — SQLite-хранилище пользователей и Telegram-подтверждений.
"""

from __future__ import annotations

import secrets
import sqlite3
import threading
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_password(password: str, salt: Optional[str] = None) -> str:
    salt_value = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        (password or "").encode("utf-8"),
        bytes.fromhex(salt_value),
        120_000,
    ).hex()
    return f"pbkdf2_sha256${salt_value}${digest}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        algo, salt, digest = (stored or "").split("$", 2)
    except ValueError:
        return False
    if algo != "pbkdf2_sha256":
        return False
    return hmac.compare_digest(_hash_password(password, salt), f"{algo}${salt}${digest}")


class UserAuthDB:
    """Потокобезопасное хранилище пользователей и verification-кодов."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL UNIQUE,
                        display_name TEXT NOT NULL DEFAULT '',
                        telegram_chat_id TEXT NOT NULL DEFAULT '',
                        password_hash TEXT NOT NULL DEFAULT '',
                        role TEXT NOT NULL DEFAULT 'user',
                        must_change_password INTEGER NOT NULL DEFAULT 0,
                        status TEXT NOT NULL DEFAULT 'pending',
                        created_at TEXT NOT NULL,
                        verified_at TEXT,
                        last_login_at TEXT
                    );

                    CREATE TABLE IF NOT EXISTS verification_tokens (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        telegram_chat_id TEXT NOT NULL,
                        code TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL,
                        confirmed_at TEXT
                    );

                    CREATE TABLE IF NOT EXISTS password_reset_tokens (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        telegram_chat_id TEXT NOT NULL,
                        code TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL,
                        confirmed_at TEXT
                    );

                    CREATE TABLE IF NOT EXISTS user_sessions (
                        token TEXT PRIMARY KEY,
                        username TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL,
                        revoked_at TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_users_status
                      ON users(status);
                    CREATE INDEX IF NOT EXISTS idx_tokens_code
                      ON verification_tokens(code, telegram_chat_id, status);
                    CREATE INDEX IF NOT EXISTS idx_reset_tokens_code
                      ON password_reset_tokens(code, telegram_chat_id, status);
                    CREATE INDEX IF NOT EXISTS idx_user_sessions_username
                      ON user_sessions(username, revoked_at, expires_at);
                    """
                )
                self._migrate_schema(conn)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)")
                self._ensure_default_admin(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
        migrations = {
            "password_hash": "ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT ''",
            "role": "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'",
            "must_change_password": "ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0",
        }
        for col, sql in migrations.items():
            if col not in cols:
                conn.execute(sql)

    def _ensure_default_admin(self, conn: sqlite3.Connection) -> None:
        row = conn.execute("SELECT id FROM users WHERE role='admin' LIMIT 1").fetchone()
        if row is not None:
            return
        now = _utc_now()
        conn.execute(
            """
            INSERT INTO users (
                username, display_name, telegram_chat_id, password_hash,
                role, must_change_password, status, created_at, verified_at
            )
            VALUES ('admin', 'Administrator', '', ?, 'admin', 1, 'active', ?, ?)
            """,
            (_hash_password("admin"), now, now),
        )

    def request_verification(
        self,
        *,
        username: str,
        display_name: str,
        telegram_chat_id: str,
        password: str = "",
        ttl_minutes: int = 30,
    ) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        dname = (display_name or "").strip()
        chat = str(telegram_chat_id or "").strip()
        if not usr or not chat:
            raise ValueError("username и telegram_chat_id обязательны")
        if not password:
            raise ValueError("password обязателен")

        now = _utc_now()
        expires = (datetime.now(timezone.utc) + timedelta(minutes=max(1, int(ttl_minutes)))).isoformat()
        code = f"{secrets.randbelow(1_000_000):06d}"

        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT id, status FROM users WHERE username=?",
                    (usr,),
                ).fetchone()
                is_new = row is None
                if row is not None and str(row["status"]) == "active":
                    raise ValueError("Пользователь уже существует")
                if is_new:
                    conn.execute(
                        """
                        INSERT INTO users (
                            username, display_name, telegram_chat_id, password_hash,
                            role, must_change_password, status, created_at
                        )
                        VALUES (?, ?, ?, ?, 'user', 0, 'pending', ?)
                        """,
                        (usr, dname, chat, _hash_password(password), now),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE users
                        SET display_name=?, telegram_chat_id=?, password_hash=?,
                            status=CASE WHEN status='active' THEN status ELSE 'pending' END
                        WHERE username=?
                        """,
                        (dname, chat, _hash_password(password), usr),
                    )

                conn.execute(
                    "UPDATE verification_tokens SET status='expired' WHERE username=? AND status='pending'",
                    (usr,),
                )
                conn.execute(
                    """
                    INSERT INTO verification_tokens (
                        username, telegram_chat_id, code, status, created_at, expires_at
                    ) VALUES (?, ?, ?, 'pending', ?, ?)
                    """,
                    (usr, chat, code, now, expires),
                )

        return {"username": usr, "code": code, "expires_at": expires, "is_new_user": is_new}

    def confirm_verification(self, *, telegram_chat_id: str, code: str) -> Dict[str, Any]:
        chat = str(telegram_chat_id or "").strip()
        token = (code or "").strip()
        if not chat or not token:
            raise ValueError("telegram_chat_id и code обязательны")

        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT id, username, expires_at
                    FROM verification_tokens
                    WHERE telegram_chat_id=? AND code=? AND status='pending'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (chat, token),
                ).fetchone()
                if row is None:
                    return {"ok": False, "reason": "not_found"}

                expires_at = datetime.fromisoformat(row["expires_at"])
                if datetime.now(timezone.utc) > expires_at:
                    conn.execute(
                        "UPDATE verification_tokens SET status='expired' WHERE id=?",
                        (int(row["id"]),),
                    )
                    return {"ok": False, "reason": "expired"}

                username = str(row["username"])
                now = _utc_now()
                conn.execute(
                    "UPDATE verification_tokens SET status='confirmed', confirmed_at=? WHERE id=?",
                    (now, int(row["id"])),
                )
                conn.execute(
                    """
                    UPDATE users
                    SET status='active', verified_at=?, telegram_chat_id=?
                    WHERE username=?
                    """,
                    (now, chat, username),
                )
                return {"ok": True, "username": username}

    def request_password_reset(
        self,
        *,
        username: str,
        telegram_chat_id: str,
        ttl_minutes: int = 30,
    ) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        chat = str(telegram_chat_id or "").strip()
        if not usr or not chat:
            raise ValueError("username и telegram_chat_id обязательны")

        now = _utc_now()
        expires = (datetime.now(timezone.utc) + timedelta(minutes=max(1, int(ttl_minutes)))).isoformat()
        code = f"{secrets.randbelow(1_000_000):06d}"
        with self._lock:
            with self._connect() as conn:
                user = conn.execute(
                    "SELECT username FROM users WHERE username=? AND telegram_chat_id=? AND status='active'",
                    (usr, chat),
                ).fetchone()
                if user is None:
                    return {"ok": False, "reason": "user_not_found"}
                conn.execute(
                    "UPDATE password_reset_tokens SET status='expired' WHERE username=? AND status='pending'",
                    (usr,),
                )
                conn.execute(
                    """
                    INSERT INTO password_reset_tokens (
                        username, telegram_chat_id, code, status, created_at, expires_at
                    ) VALUES (?, ?, ?, 'pending', ?, ?)
                    """,
                    (usr, chat, code, now, expires),
                )
        return {"ok": True, "username": usr, "code": code, "expires_at": expires}

    def confirm_password_reset(self, *, telegram_chat_id: str, code: str) -> Dict[str, Any]:
        chat = str(telegram_chat_id or "").strip()
        token = (code or "").strip()
        if not chat or not token:
            raise ValueError("telegram_chat_id и code обязательны")
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT id, username, expires_at
                    FROM password_reset_tokens
                    WHERE telegram_chat_id=? AND code=? AND status='pending'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (chat, token),
                ).fetchone()
                if row is None:
                    return {"ok": False, "reason": "not_found"}
                expires_at = datetime.fromisoformat(row["expires_at"])
                if datetime.now(timezone.utc) > expires_at:
                    conn.execute(
                        "UPDATE password_reset_tokens SET status='expired' WHERE id=?",
                        (int(row["id"]),),
                    )
                    return {"ok": False, "reason": "expired"}
                now = _utc_now()
                conn.execute(
                    "UPDATE password_reset_tokens SET status='confirmed', confirmed_at=? WHERE id=?",
                    (now, int(row["id"])),
                )
                return {"ok": True, "username": str(row["username"])}

    def complete_password_reset(self, *, username: str, code: str, new_password: str) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        token = (code or "").strip()
        if not usr or not token or not new_password:
            raise ValueError("username, code и new_password обязательны")
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT id FROM password_reset_tokens
                    WHERE username=? AND code=? AND status='confirmed'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (usr, token),
                ).fetchone()
                if row is None:
                    return {"ok": False, "reason": "not_confirmed"}
                conn.execute(
                    "UPDATE users SET password_hash=?, must_change_password=0 WHERE username=?",
                    (_hash_password(new_password), usr),
                )
                conn.execute(
                    "UPDATE password_reset_tokens SET status='used' WHERE id=?",
                    (int(row["id"]),),
                )
                return {"ok": True, "username": usr}

    def change_password(self, *, username: str, old_password: str, new_password: str) -> bool:
        user = self.login(username=username, password=old_password, update_login=False)
        if user is None or not new_password:
            return False
        usr = (username or "").strip().lower()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE users SET password_hash=?, must_change_password=0 WHERE username=?",
                    (_hash_password(new_password), usr),
                )
        return True

    def login(
        self,
        *,
        username: str,
        password: str = "",
        update_login: bool = True,
    ) -> Optional[Dict[str, Any]]:
        usr = (username or "").strip().lower()
        if not usr:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT username, display_name, telegram_chat_id, password_hash,
                           role, must_change_password, status, verified_at, created_at
                    FROM users
                    WHERE username=?
                    """,
                    (usr,),
                ).fetchone()
                if row is None or str(row["status"]) != "active":
                    return None
                data = dict(row)
                if not _verify_password(password, str(data.get("password_hash", ""))):
                    return None
                if update_login:
                    conn.execute(
                        "UPDATE users SET last_login_at=? WHERE username=?",
                        (_utc_now(), usr),
                    )
                data.pop("password_hash", None)
                return data

    def create_session(self, *, username: str, ttl_days: int = 14) -> str:
        usr = (username or "").strip().lower()
        if not usr:
            raise ValueError("username is required")
        token = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        expires = now + timedelta(days=max(1, int(ttl_days)))
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO user_sessions (token, username, created_at, expires_at, revoked_at)
                    VALUES (?, ?, ?, ?, NULL)
                    """,
                    (token, usr, now.isoformat(), expires.isoformat()),
                )
        return token

    def get_user_by_session(self, token: str) -> Optional[Dict[str, Any]]:
        value = (token or "").strip()
        if not value:
            return None
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT u.username, u.display_name, u.telegram_chat_id, u.role,
                           u.must_change_password, u.status, u.verified_at, u.created_at,
                           u.last_login_at
                    FROM user_sessions s
                    JOIN users u ON u.username = s.username
                    WHERE s.token=? AND s.revoked_at IS NULL AND s.expires_at > ?
                    LIMIT 1
                    """,
                    (value, now),
                ).fetchone()
                if row is None or str(row["status"]) != "active":
                    return None
                return dict(row)

    def revoke_session(self, token: str) -> None:
        value = (token or "").strip()
        if not value:
            return
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE user_sessions SET revoked_at=? WHERE token=? AND revoked_at IS NULL",
                    (_utc_now(), value),
                )

    def get_user(self, *, username: str) -> Optional[Dict[str, Any]]:
        usr = (username or "").strip().lower()
        if not usr:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT username, display_name, telegram_chat_id, role, must_change_password,
                           status, verified_at, created_at, last_login_at
                    FROM users
                    WHERE username=?
                    """,
                    (usr,),
                ).fetchone()
                return dict(row) if row else None

    def list_users(self) -> list[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT username, display_name, telegram_chat_id, role, must_change_password,
                           status, verified_at, created_at, last_login_at
                    FROM users
                    ORDER BY role='admin' DESC, username
                    """
                ).fetchall()
                return [dict(row) for row in rows]

    def update_profile(self, *, username: str, display_name: str, telegram_chat_id: str) -> bool:
        usr = (username or "").strip().lower()
        if not usr:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    UPDATE users
                    SET display_name=?, telegram_chat_id=?
                    WHERE username=?
                    """,
                    ((display_name or "").strip(), str(telegram_chat_id or "").strip(), usr),
                )
                return cur.rowcount > 0

    def admin_update_user(
        self,
        *,
        username: str,
        display_name: str,
        telegram_chat_id: str,
        role: str,
        status: str,
        must_change_password: bool,
    ) -> bool:
        usr = (username or "").strip().lower()
        role_value = "admin" if str(role or "").strip().lower() == "admin" else "user"
        status_value = str(status or "").strip().lower()
        if status_value not in {"active", "pending", "blocked"}:
            status_value = "active"
        if not usr:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    UPDATE users
                    SET display_name=?, telegram_chat_id=?, role=?, status=?, must_change_password=?
                    WHERE username=?
                    """,
                    (
                        (display_name or "").strip(),
                        str(telegram_chat_id or "").strip(),
                        role_value,
                        status_value,
                        1 if must_change_password else 0,
                        usr,
                    ),
                )
                return cur.rowcount > 0

    def admin_set_password(self, *, username: str, new_password: str, must_change_password: bool = True) -> bool:
        usr = (username or "").strip().lower()
        if not usr or not new_password:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "UPDATE users SET password_hash=?, must_change_password=? WHERE username=?",
                    (_hash_password(new_password), 1 if must_change_password else 0, usr),
                )
                return cur.rowcount > 0

    def admin_create_user(
        self,
        *,
        username: str,
        display_name: str = "",
        telegram_chat_id: str = "",
        password: str,
        role: str = "user",
        status: str = "active",
        must_change_password: bool = True,
    ) -> bool:
        usr = (username or "").strip().lower()
        role_value = "admin" if str(role or "").strip().lower() == "admin" else "user"
        status_value = str(status or "").strip().lower()
        if status_value not in {"active", "pending", "blocked"}:
            status_value = "active"
        if not usr or not password:
            return False
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                try:
                    conn.execute(
                        """
                        INSERT INTO users (
                            username, display_name, telegram_chat_id, password_hash,
                            role, must_change_password, status, created_at,
                            verified_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            usr,
                            (display_name or "").strip(),
                            str(telegram_chat_id or "").strip(),
                            _hash_password(password),
                            role_value,
                            1 if must_change_password else 0,
                            status_value,
                            now,
                            now if status_value == "active" else None,
                        ),
                    )
                except sqlite3.IntegrityError:
                    return False
        return True
