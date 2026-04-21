"""
user_auth_db.py — SQLite-хранилище пользователей и Telegram-подтверждений.
"""

from __future__ import annotations

import json
import secrets
import sqlite3
import threading
import hashlib
import hmac
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_SESSION_TTL_DAYS = 7
MIN_SESSION_TTL_DAYS = 1
MAX_SESSION_TTL_DAYS = 7
SESSION_TTL_SETTING_KEY = "session_ttl_days"
SHOW_SYSTEM_FILES_SETTING_KEY = "show_system_files_for_admin"
ANY_TELEGRAM_CHAT_ID = "*"


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
                        telegram_username TEXT NOT NULL DEFAULT '',
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
                        last_seen_at TEXT,
                        expires_at TEXT NOT NULL,
                        revoked_at TEXT
                    );

                    CREATE TABLE IF NOT EXISTS user_settings (
                        username TEXT PRIMARY KEY,
                        settings_json TEXT NOT NULL DEFAULT '{}',
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS app_settings (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS telegram_tokens (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        token TEXT NOT NULL UNIQUE,
                        purpose TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        username TEXT NOT NULL DEFAULT '',
                        telegram_chat_id TEXT NOT NULL DEFAULT '',
                        telegram_username TEXT NOT NULL DEFAULT '',
                        display_name TEXT NOT NULL DEFAULT '',
                        created_by TEXT NOT NULL DEFAULT '',
                        target TEXT NOT NULL DEFAULT '',
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL,
                        used_at TEXT
                    );

                    CREATE TABLE IF NOT EXISTS registration_requests (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL DEFAULT '',
                        display_name TEXT NOT NULL DEFAULT '',
                        telegram_chat_id TEXT NOT NULL DEFAULT '',
                        telegram_username TEXT NOT NULL DEFAULT '',
                        source TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL DEFAULT 'pending',
                        note TEXT NOT NULL DEFAULT '',
                        requested_at TEXT NOT NULL,
                        reviewed_at TEXT,
                        reviewed_by TEXT
                    );

                    CREATE TABLE IF NOT EXISTS user_favorites (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        item_type TEXT NOT NULL,
                        path TEXT NOT NULL,
                        title TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        last_used_at TEXT,
                        UNIQUE(username, path)
                    );

                    CREATE TABLE IF NOT EXISTS auth_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts TEXT NOT NULL,
                        username TEXT NOT NULL DEFAULT '',
                        event_type TEXT NOT NULL,
                        ok INTEGER NOT NULL DEFAULT 1,
                        ip TEXT NOT NULL DEFAULT '',
                        user_agent TEXT NOT NULL DEFAULT '',
                        error TEXT NOT NULL DEFAULT ''
                    );

                    CREATE INDEX IF NOT EXISTS idx_users_status
                      ON users(status);
                    CREATE INDEX IF NOT EXISTS idx_tokens_code
                      ON verification_tokens(code, telegram_chat_id, status);
                    CREATE INDEX IF NOT EXISTS idx_reset_tokens_code
                      ON password_reset_tokens(code, telegram_chat_id, status);
                    CREATE INDEX IF NOT EXISTS idx_user_sessions_username
                      ON user_sessions(username, revoked_at, expires_at);
                    CREATE INDEX IF NOT EXISTS idx_user_favorites_username
                      ON user_favorites(username, item_type, title);
                    CREATE INDEX IF NOT EXISTS idx_auth_events_ts
                      ON auth_events(ts);
                    CREATE INDEX IF NOT EXISTS idx_telegram_tokens_token
                      ON telegram_tokens(token, status, purpose);
                    CREATE INDEX IF NOT EXISTS idx_registration_requests_status
                      ON registration_requests(status, requested_at);
                    """
                )
                self._migrate_schema(conn)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)")
                self._ensure_default_admin(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
        migrations = {
            "telegram_username": "ALTER TABLE users ADD COLUMN telegram_username TEXT NOT NULL DEFAULT ''",
            "password_hash": "ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT ''",
            "role": "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'",
            "must_change_password": "ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0",
        }
        for col, sql in migrations.items():
            if col not in cols:
                conn.execute(sql)
        session_cols = {row["name"] for row in conn.execute("PRAGMA table_info(user_sessions)").fetchall()}
        if "last_seen_at" not in session_cols:
            conn.execute("ALTER TABLE user_sessions ADD COLUMN last_seen_at TEXT")

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

    def request_telegram_link_code(
        self,
        *,
        username: str,
        password: str,
        telegram_chat_id: str,
        ttl_minutes: int = 30,
    ) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        chat = str(telegram_chat_id or "").strip()
        if not usr or not chat or not password:
            raise ValueError("username, password и telegram_chat_id обязательны")

        now = _utc_now()
        expires = (datetime.now(timezone.utc) + timedelta(minutes=max(1, int(ttl_minutes)))).isoformat()
        code = f"{secrets.randbelow(1_000_000):06d}"

        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT status, password_hash
                    FROM users
                    WHERE username=?
                    """,
                    (usr,),
                ).fetchone()
                if row is None or str(row["status"]) != "active":
                    return {"ok": False, "reason": "user_not_found"}
                if not _verify_password(password, str(row["password_hash"] or "")):
                    return {"ok": False, "reason": "bad_credentials"}

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

        return {"ok": True, "username": usr, "code": code, "expires_at": expires}

    def request_telegram_deeplink_code(
        self,
        *,
        username: str,
        password: str,
        ttl_minutes: int = 30,
    ) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        if not usr or not password:
            raise ValueError("username и password обязательны")

        now = _utc_now()
        expires = (datetime.now(timezone.utc) + timedelta(minutes=max(1, int(ttl_minutes)))).isoformat()
        code = f"{secrets.randbelow(1_000_000):06d}"

        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT status, password_hash
                    FROM users
                    WHERE username=?
                    """,
                    (usr,),
                ).fetchone()
                if row is None or str(row["status"]) != "active":
                    return {"ok": False, "reason": "user_not_found"}
                if not _verify_password(password, str(row["password_hash"] or "")):
                    return {"ok": False, "reason": "bad_credentials"}

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
                    (usr, ANY_TELEGRAM_CHAT_ID, code, now, expires),
                )

        return {"ok": True, "username": usr, "code": code, "expires_at": expires}

    def _new_token(self) -> str:
        return secrets.token_urlsafe(32)

    def create_telegram_token(
        self,
        *,
        purpose: str,
        username: str = "",
        telegram_chat_id: str = "",
        telegram_username: str = "",
        display_name: str = "",
        created_by: str = "",
        target: str = "",
        payload: Optional[Dict[str, Any]] = None,
        ttl_minutes: int = 30,
    ) -> Dict[str, Any]:
        purpose_value = str(purpose or "").strip().lower()
        if purpose_value not in {"link", "invite", "login", "register"}:
            raise ValueError("unknown telegram token purpose")
        token = self._new_token()
        now = datetime.now(timezone.utc)
        expires = now + timedelta(minutes=max(1, int(ttl_minutes)))
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO telegram_tokens (
                        token, purpose, status, username, telegram_chat_id, telegram_username,
                        display_name, created_by, target, payload_json, created_at, expires_at
                    )
                    VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        token,
                        purpose_value,
                        (username or "").strip().lower(),
                        str(telegram_chat_id or "").strip(),
                        self._normalize_telegram_username(telegram_username),
                        (display_name or "").strip(),
                        (created_by or "").strip().lower(),
                        (target or "").strip().lower(),
                        json.dumps(payload or {}, ensure_ascii=False, sort_keys=True),
                        now.isoformat(),
                        expires.isoformat(),
                    ),
                )
        return {"ok": True, "token": token, "purpose": purpose_value, "expires_at": expires.isoformat()}

    def create_telegram_link_token(self, *, username: str, ttl_minutes: int = 30) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        if not usr:
            raise ValueError("username is required")
        user = self.get_user(username=usr)
        if user is None or str(user.get("status") or "") != "active":
            return {"ok": False, "reason": "user_not_found"}
        return self.create_telegram_token(purpose="link", username=usr, ttl_minutes=ttl_minutes)

    def request_telegram_link(self, *, username: str, password: str, ttl_minutes: int = 30) -> Dict[str, Any]:
        user = self.login(username=username, password=password, update_login=False)
        if user is None:
            return {"ok": False, "reason": "bad_credentials"}
        return self.create_telegram_link_token(username=str(user.get("username") or ""), ttl_minutes=ttl_minutes)

    def create_telegram_login_challenge(self, *, target: str = "web", ttl_minutes: int = 5) -> Dict[str, Any]:
        target_value = str(target or "web").strip().lower()
        if target_value not in {"web", "native"}:
            target_value = "web"
        return self.create_telegram_token(purpose="login", target=target_value, ttl_minutes=ttl_minutes)

    def consume_confirmed_telegram_login(self, *, token: str) -> Dict[str, Any]:
        token_value = str(token or "").strip()
        if not token_value:
            return {"ok": False, "reason": "missing_token"}
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT username, expires_at
                    FROM telegram_tokens
                    WHERE token=? AND purpose='login' AND status='confirmed'
                    LIMIT 1
                    """,
                    (token_value,),
                ).fetchone()
                if row is None:
                    return {"ok": False, "reason": "pending"}
                if datetime.now(timezone.utc) > datetime.fromisoformat(str(row["expires_at"])):
                    conn.execute("UPDATE telegram_tokens SET status='expired' WHERE token=?", (token_value,))
                    return {"ok": False, "reason": "expired"}
                username = str(row["username"] or "")
                conn.execute(
                    "UPDATE telegram_tokens SET status='used', used_at=? WHERE token=?",
                    (now, token_value),
                )
        user = self.get_user(username=username)
        if user is None or str(user.get("status") or "") != "active":
            return {"ok": False, "reason": "user_not_found"}
        return {"ok": True, "username": username, "user": user}

    def create_admin_invite(
        self,
        *,
        created_by: str,
        username: str,
        display_name: str = "",
        telegram_username: str = "",
        password: str = "",
        role: str = "user",
        ttl_minutes: int = 7 * 24 * 60,
    ) -> Dict[str, Any]:
        admin = self.get_user(username=created_by)
        if admin is None or str(admin.get("role") or "") != "admin" or str(admin.get("status") or "") != "active":
            return {"ok": False, "reason": "not_admin"}
        usr = (username or "").strip().lower()
        if not usr:
            usr = self._sanitize_username_hint(telegram_username)
        if not usr:
            return {"ok": False, "reason": "missing_username"}
        temp_password = password or secrets.token_urlsafe(10)
        self.admin_create_user(
            username=usr,
            display_name=display_name or usr,
            telegram_chat_id="",
            telegram_username=telegram_username,
            password=temp_password,
            role=role,
            status="active",
            must_change_password=True,
        )
        token = self.create_telegram_token(
            purpose="invite",
            username=usr,
            telegram_username=telegram_username,
            display_name=display_name,
            created_by=created_by,
            payload={"temp_password": temp_password},
            ttl_minutes=ttl_minutes,
        )
        return {**token, "username": usr, "temp_password": temp_password}

    def _normalize_telegram_username(self, value: str) -> str:
        return str(value or "").strip().lstrip("@").lower()

    def consume_telegram_start_token(
        self,
        *,
        token: str,
        telegram_chat_id: str,
        telegram_username: str = "",
        display_name: str = "",
    ) -> Dict[str, Any]:
        token_value = str(token or "").strip()
        chat = str(telegram_chat_id or "").strip()
        if not token_value or not chat:
            return {"ok": False, "reason": "missing_token"}
        tg_username = self._normalize_telegram_username(telegram_username)
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT *
                    FROM telegram_tokens
                    WHERE token=? AND status='pending'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (token_value,),
                ).fetchone()
                if row is None:
                    return {"ok": False, "reason": "not_found"}
                if now_dt > datetime.fromisoformat(str(row["expires_at"])):
                    conn.execute("UPDATE telegram_tokens SET status='expired' WHERE id=?", (int(row["id"]),))
                    return {"ok": False, "reason": "expired"}

                purpose = str(row["purpose"] or "")
                expected_username = self._normalize_telegram_username(str(row["telegram_username"] or ""))
                if expected_username and expected_username != tg_username:
                    return {"ok": False, "reason": "telegram_username_mismatch"}

                if purpose in {"link", "invite"}:
                    username = str(row["username"] or "").strip().lower()
                    user = conn.execute("SELECT username, status FROM users WHERE username=?", (username,)).fetchone()
                    if user is None or str(user["status"]) != "active":
                        return {"ok": False, "reason": "user_not_found"}
                    conn.execute(
                        """
                        UPDATE users
                        SET telegram_chat_id=?, telegram_username=?,
                            verified_at=COALESCE(verified_at, ?)
                        WHERE username=?
                        """,
                        (chat, tg_username, now, username),
                    )
                    conn.execute(
                        "UPDATE telegram_tokens SET status='used', telegram_chat_id=?, telegram_username=?, used_at=? WHERE id=?",
                        (chat, tg_username, now, int(row["id"])),
                    )
                    return {"ok": True, "purpose": purpose, "username": username}

                if purpose == "login":
                    user = conn.execute(
                        """
                        SELECT username, status
                        FROM users
                        WHERE telegram_chat_id=? AND status='active'
                        LIMIT 1
                        """,
                        (chat,),
                    ).fetchone()
                    if user is None:
                        return {"ok": False, "reason": "telegram_not_linked"}
                    username = str(user["username"] or "")
                    conn.execute(
                        """
                        UPDATE telegram_tokens
                        SET status='confirmed', username=?, telegram_chat_id=?, telegram_username=?, used_at=?
                        WHERE id=?
                        """,
                        (username, chat, tg_username, now, int(row["id"])),
                    )
                    return {"ok": True, "purpose": purpose, "username": username, "target": str(row["target"] or "web")}

                if purpose == "register":
                    req = self.create_registration_request(
                        username=str(row["username"] or ""),
                        display_name=display_name or str(row["display_name"] or ""),
                        telegram_chat_id=chat,
                        telegram_username=tg_username,
                        source="telegram_link",
                        note="",
                        _conn=conn,
                    )
                    conn.execute(
                        "UPDATE telegram_tokens SET status='used', telegram_chat_id=?, telegram_username=?, used_at=? WHERE id=?",
                        (chat, tg_username, now, int(row["id"])),
                    )
                    return {"ok": True, "purpose": purpose, "request_id": req.get("id")}
        return {"ok": False, "reason": "unsupported_purpose"}

    def create_registration_request(
        self,
        *,
        username: str = "",
        display_name: str = "",
        telegram_chat_id: str = "",
        telegram_username: str = "",
        source: str = "web",
        note: str = "",
        _conn: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        now = _utc_now()
        params = (
            usr,
            (display_name or "").strip(),
            str(telegram_chat_id or "").strip(),
            self._normalize_telegram_username(telegram_username),
            (source or "web").strip(),
            (note or "").strip(),
            now,
        )
        sql = """
            INSERT INTO registration_requests (
                username, display_name, telegram_chat_id, telegram_username,
                source, status, note, requested_at
            )
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        """
        if _conn is not None:
            cur = _conn.execute(sql, params)
            return {"ok": True, "id": int(cur.lastrowid), "status": "pending"}
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(sql, params)
                return {"ok": True, "id": int(cur.lastrowid), "status": "pending"}

    def list_registration_requests(self, *, status: str = "pending", limit: int = 100) -> list[Dict[str, Any]]:
        status_value = str(status or "").strip().lower()
        with self._lock:
            with self._connect() as conn:
                if status_value == "all":
                    rows = conn.execute(
                        """
                        SELECT *
                        FROM registration_requests
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (max(1, int(limit)),),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT *
                        FROM registration_requests
                        WHERE status=?
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (status_value or "pending", max(1, int(limit))),
                    ).fetchall()
                return [dict(row) for row in rows]

    def review_registration_request(
        self,
        *,
        request_id: int,
        reviewed_by: str,
        decision: str,
        password: str = "",
        role: str = "user",
    ) -> Dict[str, Any]:
        reviewer = self.get_user(username=reviewed_by)
        if reviewer is None or str(reviewer.get("role") or "") != "admin":
            return {"ok": False, "reason": "not_admin"}
        decision_value = str(decision or "").strip().lower()
        if decision_value not in {"approved", "rejected"}:
            return {"ok": False, "reason": "bad_decision"}
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM registration_requests WHERE id=? AND status='pending'",
                    (int(request_id),),
                ).fetchone()
                if row is None:
                    return {"ok": False, "reason": "not_found"}
                conn.execute(
                    """
                    UPDATE registration_requests
                    SET status=?, reviewed_at=?, reviewed_by=?
                    WHERE id=?
                    """,
                    (decision_value, now, (reviewed_by or "").strip().lower(), int(request_id)),
                )
                if decision_value == "rejected":
                    return {"ok": True, "status": "rejected"}
                username = str(row["username"] or "").strip().lower() or self._pick_unique_username(
                    conn,
                    str(row["telegram_username"] or row["display_name"] or ""),
                    str(row["telegram_chat_id"] or int(request_id)),
                )
                temp_password = password or secrets.token_urlsafe(10)
                existing = conn.execute("SELECT username FROM users WHERE username=?", (username,)).fetchone()
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO users (
                            username, display_name, telegram_chat_id, telegram_username,
                            password_hash, role, must_change_password, status, created_at, verified_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, 1, 'active', ?, ?)
                        """,
                        (
                            username,
                            str(row["display_name"] or username),
                            str(row["telegram_chat_id"] or ""),
                            self._normalize_telegram_username(str(row["telegram_username"] or "")),
                            _hash_password(temp_password),
                            "admin" if str(role or "").lower() == "admin" else "user",
                            now,
                            now,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE users
                        SET status='active',
                            telegram_chat_id=CASE WHEN ? <> '' THEN ? ELSE telegram_chat_id END,
                            telegram_username=CASE WHEN ? <> '' THEN ? ELSE telegram_username END,
                            verified_at=COALESCE(verified_at, ?)
                        WHERE username=?
                        """,
                        (
                            str(row["telegram_chat_id"] or ""),
                            str(row["telegram_chat_id"] or ""),
                            self._normalize_telegram_username(str(row["telegram_username"] or "")),
                            self._normalize_telegram_username(str(row["telegram_username"] or "")),
                            now,
                            username,
                        ),
                    )
        return {"ok": True, "status": "approved", "username": username, "temp_password": temp_password}

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
                    WHERE code=? AND status='pending'
                      AND (telegram_chat_id=? OR telegram_chat_id=?)
                    ORDER BY CASE WHEN telegram_chat_id=? THEN 0 ELSE 1 END, id DESC
                    LIMIT 1
                    """,
                    (token, chat, ANY_TELEGRAM_CHAT_ID, chat),
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
                    SELECT username, display_name, telegram_chat_id, telegram_username, password_hash,
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

    def _normalize_session_ttl_days(self, value: Any) -> int:
        try:
            days = int(value)
        except (TypeError, ValueError):
            days = DEFAULT_SESSION_TTL_DAYS
        return max(MIN_SESSION_TTL_DAYS, min(MAX_SESSION_TTL_DAYS, days))

    def get_session_ttl_days(self) -> int:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT value FROM app_settings WHERE key=?",
                    (SESSION_TTL_SETTING_KEY,),
                ).fetchone()
        if row is None:
            return DEFAULT_SESSION_TTL_DAYS
        return self._normalize_session_ttl_days(row["value"])

    def _get_app_setting(self, key: str, default: str = "") -> str:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
        if row is None:
            return default
        return str(row["value"] or "")

    def _set_app_setting(self, key: str, value: str) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO app_settings (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value=excluded.value,
                        updated_at=excluded.updated_at
                    """,
                    (key, value, _utc_now()),
                )

    def set_session_ttl_days(self, ttl_days: int) -> int:
        value = self._normalize_session_ttl_days(ttl_days)
        self._set_app_setting(SESSION_TTL_SETTING_KEY, str(value))
        return value

    def get_show_system_files_for_admin(self) -> bool:
        return self._get_app_setting(SHOW_SYSTEM_FILES_SETTING_KEY, "0") == "1"

    def set_show_system_files_for_admin(self, enabled: bool) -> bool:
        value = bool(enabled)
        self._set_app_setting(SHOW_SYSTEM_FILES_SETTING_KEY, "1" if value else "0")
        return value

    def create_session(self, *, username: str, ttl_days: Optional[int] = None) -> str:
        usr = (username or "").strip().lower()
        if not usr:
            raise ValueError("username is required")
        token = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        ttl = self._normalize_session_ttl_days(ttl_days) if ttl_days is not None else self.get_session_ttl_days()
        expires = now + timedelta(days=ttl)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO user_sessions (token, username, created_at, last_seen_at, expires_at, revoked_at)
                    VALUES (?, ?, ?, ?, ?, NULL)
                    """,
                    (token, usr, now.isoformat(), now.isoformat(), expires.isoformat()),
                )
        return token

    def touch_session(
        self,
        token: str,
        *,
        min_interval_minutes: int = 60,
        ttl_days: Optional[int] = None,
    ) -> bool:
        value = (token or "").strip()
        if not value:
            return False
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        interval = timedelta(minutes=max(1, int(min_interval_minutes)))
        ttl = self._normalize_session_ttl_days(ttl_days) if ttl_days is not None else self.get_session_ttl_days()
        new_expires = (now_dt + timedelta(days=ttl)).isoformat()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT last_seen_at, expires_at
                    FROM user_sessions
                    WHERE token=? AND revoked_at IS NULL
                    LIMIT 1
                    """,
                    (value,),
                ).fetchone()
                if row is None:
                    return False
                try:
                    expires_at = datetime.fromisoformat(str(row["expires_at"]))
                except ValueError:
                    return False
                if expires_at <= now_dt:
                    return False
                last_seen_raw = str(row["last_seen_at"] or "")
                if last_seen_raw:
                    try:
                        last_seen = datetime.fromisoformat(last_seen_raw)
                    except ValueError:
                        last_seen = None
                    if last_seen is not None and now_dt - last_seen < interval:
                        return False
                conn.execute(
                    """
                    UPDATE user_sessions
                    SET last_seen_at=?, expires_at=?
                    WHERE token=? AND revoked_at IS NULL AND expires_at > ?
                    """,
                    (now, new_expires, value, now),
                )
                return True

    def get_user_by_session(self, token: str) -> Optional[Dict[str, Any]]:
        value = (token or "").strip()
        if not value:
            return None
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT u.username, u.display_name, u.telegram_chat_id, u.telegram_username, u.role,
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
                user = dict(row)
        self.touch_session(value)
        return user

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
                    SELECT username, display_name, telegram_chat_id, telegram_username, role, must_change_password,
                           status, verified_at, created_at, last_login_at
                    FROM users
                    WHERE username=?
                    """,
                    (usr,),
                ).fetchone()
                return dict(row) if row else None

    def get_user_by_telegram_chat_id(self, telegram_chat_id: str) -> Optional[Dict[str, Any]]:
        chat = str(telegram_chat_id or "").strip()
        if not chat:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT username, display_name, telegram_chat_id, telegram_username, role, must_change_password,
                           status, verified_at, created_at, last_login_at
                    FROM users
                    WHERE telegram_chat_id=? AND status='active'
                    LIMIT 1
                    """,
                    (chat,),
                ).fetchone()
                return dict(row) if row else None

    def _sanitize_username_hint(self, value: str) -> str:
        normalized = re.sub(r"[^a-z0-9_.-]+", "_", str(value or "").strip().lower())
        normalized = re.sub(r"_+", "_", normalized).strip("_.-")
        return normalized[:48]

    def _pick_unique_username(self, conn: sqlite3.Connection, hint: str, fallback_chat_id: str) -> str:
        base = self._sanitize_username_hint(hint) or f"tg_{fallback_chat_id}"
        candidate = base
        suffix = 1
        while conn.execute("SELECT 1 FROM users WHERE username=? LIMIT 1", (candidate,)).fetchone():
            suffix += 1
            candidate = f"{base}_{suffix}"
        return candidate

    def upsert_user_from_telegram_contact(
        self,
        *,
        telegram_chat_id: str,
        username_hint: str = "",
        display_name: str = "",
    ) -> Dict[str, Any]:
        chat = str(telegram_chat_id or "").strip()
        if not chat:
            raise ValueError("telegram_chat_id is required")
        display = (display_name or "").strip()
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                existing = conn.execute(
                    """
                    SELECT username
                    FROM users
                    WHERE telegram_chat_id=?
                    LIMIT 1
                    """,
                    (chat,),
                ).fetchone()
                if existing is not None:
                    username = str(existing["username"] or "")
                    conn.execute(
                        """
                        UPDATE users
                        SET status='active',
                            verified_at=COALESCE(verified_at, ?),
                            display_name=CASE WHEN ? <> '' THEN ? ELSE display_name END
                        WHERE username=?
                        """,
                        (now, display, display, username),
                    )
                    return {"username": username, "created": False, "temp_password": ""}

                username = self._pick_unique_username(conn, username_hint, chat)
                temp_password = secrets.token_urlsafe(10)
                conn.execute(
                    """
                    INSERT INTO users (
                        username, display_name, telegram_chat_id, password_hash,
                        role, must_change_password, status, created_at, verified_at
                    )
                    VALUES (?, ?, ?, ?, 'user', 1, 'active', ?, ?)
                    """,
                    (
                        username,
                        display,
                        chat,
                        _hash_password(temp_password),
                        now,
                        now,
                    ),
                )
                return {"username": username, "created": True, "temp_password": temp_password}

    def unlink_telegram_chat_id(self, telegram_chat_id: str) -> Optional[str]:
        chat = str(telegram_chat_id or "").strip()
        if not chat:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT username
                    FROM users
                    WHERE telegram_chat_id=?
                    LIMIT 1
                    """,
                    (chat,),
                ).fetchone()
                if row is None:
                    return None
                username = str(row["username"] or "")
                conn.execute(
                    "UPDATE users SET telegram_chat_id='' WHERE telegram_chat_id=?",
                    (chat,),
                )
                return username

    def list_telegram_chats(self) -> list[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        u.username,
                        u.display_name,
                        u.role,
                        u.status,
                        u.telegram_chat_id,
                        u.telegram_username,
                        u.last_login_at,
                        (
                            SELECT MAX(ae.ts)
                            FROM auth_events ae
                            WHERE ae.username = u.username
                              AND ae.event_type LIKE 'telegram_%'
                        ) AS last_telegram_event_at
                    FROM users u
                    WHERE COALESCE(u.telegram_chat_id, '') <> ''
                    ORDER BY u.role='admin' DESC, lower(u.username)
                    """
                ).fetchall()
                return [dict(row) for row in rows]

    def list_users(self) -> list[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT username, display_name, telegram_chat_id, telegram_username, role, must_change_password,
                           status, verified_at, created_at, last_login_at
                    FROM users
                    ORDER BY role='admin' DESC, username
                    """
                ).fetchall()
                return [dict(row) for row in rows]

    def update_profile(
        self,
        *,
        username: str,
        display_name: str,
        telegram_chat_id: str,
        telegram_username: str = "",
    ) -> bool:
        usr = (username or "").strip().lower()
        if not usr:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    UPDATE users
                    SET display_name=?, telegram_chat_id=?, telegram_username=?
                    WHERE username=?
                    """,
                    (
                        (display_name or "").strip(),
                        str(telegram_chat_id or "").strip(),
                        self._normalize_telegram_username(telegram_username),
                        usr,
                    ),
                )
                return cur.rowcount > 0

    def admin_update_user(
        self,
        *,
        username: str,
        display_name: str,
        telegram_chat_id: str,
        telegram_username: str = "",
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
                    SET display_name=?, telegram_chat_id=?, telegram_username=?, role=?, status=?, must_change_password=?
                    WHERE username=?
                    """,
                    (
                        (display_name or "").strip(),
                        str(telegram_chat_id or "").strip(),
                        self._normalize_telegram_username(telegram_username),
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
        telegram_username: str = "",
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
                            username, display_name, telegram_chat_id, telegram_username, password_hash,
                            role, must_change_password, status, created_at,
                            verified_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            usr,
                            (display_name or "").strip(),
                            str(telegram_chat_id or "").strip(),
                            self._normalize_telegram_username(telegram_username),
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

    def get_user_settings(self, *, username: str) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        if not usr:
            return {}
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT settings_json FROM user_settings WHERE username=?",
                    (usr,),
                ).fetchone()
                if row is None:
                    return {}
                try:
                    data = json.loads(str(row["settings_json"] or "{}"))
                except json.JSONDecodeError:
                    return {}
                return data if isinstance(data, dict) else {}

    def save_user_settings(self, *, username: str, settings: Dict[str, Any]) -> None:
        usr = (username or "").strip().lower()
        if not usr:
            return
        payload = json.dumps(settings or {}, ensure_ascii=False, sort_keys=True)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO user_settings (username, settings_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(username) DO UPDATE SET
                        settings_json=excluded.settings_json,
                        updated_at=excluded.updated_at
                    """,
                    (usr, payload, _utc_now()),
                )

    def reset_user_settings(self, *, username: str) -> None:
        usr = (username or "").strip().lower()
        if not usr:
            return
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM user_settings WHERE username=?", (usr,))

    def list_favorites(self, *, username: str) -> list[Dict[str, Any]]:
        usr = (username or "").strip().lower()
        if not usr:
            return []
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT id, username, item_type, path, title, created_at, last_used_at
                    FROM user_favorites
                    WHERE username=?
                    ORDER BY lower(title), lower(path)
                    """,
                    (usr,),
                ).fetchall()
                return [dict(row) for row in rows]

    def add_favorite(self, *, username: str, item_type: str, path: str, title: str = "") -> bool:
        usr = (username or "").strip().lower()
        path_value = str(path or "").strip()
        type_value = "folder" if str(item_type or "").strip().lower() == "folder" else "file"
        title_value = (title or "").strip() or Path(path_value).name or path_value
        if not usr or not path_value:
            return False
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO user_favorites (username, item_type, path, title, created_at, last_used_at)
                    VALUES (?, ?, ?, ?, ?, NULL)
                    ON CONFLICT(username, path) DO UPDATE SET
                        item_type=excluded.item_type,
                        title=excluded.title
                    """,
                    (usr, type_value, path_value, title_value, now),
                )
        return True

    def remove_favorite(self, *, username: str, path: str) -> bool:
        usr = (username or "").strip().lower()
        path_value = str(path or "").strip()
        if not usr or not path_value:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM user_favorites WHERE username=? AND path=?",
                    (usr, path_value),
                )
                return cur.rowcount > 0

    def touch_favorite(self, *, username: str, path: str) -> None:
        usr = (username or "").strip().lower()
        path_value = str(path or "").strip()
        if not usr or not path_value:
            return
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE user_favorites SET last_used_at=? WHERE username=? AND path=?",
                    (_utc_now(), usr, path_value),
                )

    def log_auth_event(
        self,
        *,
        username: str,
        event_type: str,
        ok: bool,
        ip: str = "",
        user_agent: str = "",
        error: str = "",
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO auth_events (ts, username, event_type, ok, ip, user_agent, error)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now(),
                        (username or "").strip().lower(),
                        (event_type or "unknown").strip(),
                        1 if ok else 0,
                        ip or "",
                        user_agent or "",
                        error or "",
                    ),
                )

    def list_auth_events(self, *, limit: int = 200) -> list[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT ts, username, event_type, ok, ip, user_agent, error
                    FROM auth_events
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (max(1, int(limit)),),
                ).fetchall()
                return [dict(row) for row in rows]
