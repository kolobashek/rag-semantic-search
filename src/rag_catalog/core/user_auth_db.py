"""
user_auth_db.py — SQLite-хранилище пользователей и Telegram-подтверждений.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import threading
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from .db_contract import ensure_schema_version
from .employee_onboarding import (
    DEFAULT_ORG,
    EmployeeOnboarding,
    _all_in_scope,
    _audit,
    _binding,
    _memberships,
    _scope,
    assign_new_membership,
    create_pending,
    finish_employee_referral,
    issue_activation,
    migrate_employee_onboarding,
    normalize_sender_id,
    project_roles,
    token_hash,
    with_organizations,
)
from .roles import normalize_roles, primary_role, user_roles
from .sqlite_runtime import ClosingConnection, prepare_sqlite_connection

DEFAULT_SESSION_TTL_DAYS = 7
MIN_SESSION_TTL_DAYS = 1
MAX_SESSION_TTL_DAYS = 7
SESSION_TTL_SETTING_KEY = "session_ttl_days"
SHOW_SYSTEM_FILES_SETTING_KEY = "show_system_files_for_admin"
SCHEMA_VERSION = 7
LOGIN_MAX_FAILURES = 5
LOGIN_FAILURE_WINDOW_SECONDS = 5 * 60
LOGIN_LOCKOUT_SECONDS = 15 * 60


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


class UserAuthDB(EmployeeOnboarding):
    """Потокобезопасное хранилище пользователей и verification-кодов."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, factory=ClosingConnection)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _with_roles(conn, user):
        row = conn.execute(
            """SELECT u.roles_json, u.telegram_linked_at, m.max_user_id
               FROM users u LEFT JOIN max_bindings m ON m.user_id=u.id WHERE u.username=?""",
            (user["username"],),
        ).fetchone()
        user["roles_json"] = row["roles_json"] if row else ""
        user["max_user_id"] = (row["max_user_id"] or "") if row else ""
        user["telegram_linked_at"] = row["telegram_linked_at"] if row else ""
        user["roles"] = sorted(user_roles(user))
        return with_organizations(conn, user)

    def _prepare_connection(self, conn: sqlite3.Connection) -> None:
        prepare_sqlite_connection(conn)

    def _init_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                self._prepare_connection(conn)
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL UNIQUE,
                        display_name TEXT NOT NULL DEFAULT '',
                        telegram_chat_id TEXT NOT NULL DEFAULT '',
                        telegram_username TEXT NOT NULL DEFAULT '',
                        password_hash TEXT NOT NULL DEFAULT '',
                        role TEXT NOT NULL DEFAULT 'driver',
                        must_change_password INTEGER NOT NULL DEFAULT 0,
                        status TEXT NOT NULL DEFAULT 'pending',
                        created_at TEXT NOT NULL,
                        verified_at TEXT,
                        last_login_at TEXT
                    );

                    CREATE TABLE IF NOT EXISTS user_groups (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL DEFAULT 'active',
                        created_by TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS user_group_members (
                        group_id TEXT NOT NULL,
                        username TEXT NOT NULL,
                        added_by TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        PRIMARY KEY(group_id, username),
                        FOREIGN KEY(group_id) REFERENCES user_groups(id) ON DELETE CASCADE,
                        FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE
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

                    CREATE TABLE IF NOT EXISTS max_bindings (
                        user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                        max_user_id TEXT NOT NULL UNIQUE,
                        linked_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS max_link_tokens (
                        user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                        token_hash TEXT NOT NULL UNIQUE,
                        session_token TEXT NOT NULL REFERENCES user_sessions(token) ON DELETE CASCADE,
                        expires_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS max_login_challenges (
                        token_hash TEXT PRIMARY KEY,
                        verifier_hash TEXT NOT NULL UNIQUE,
                        max_user_id TEXT,
                        expires_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_max_login_expiry ON max_login_challenges(expires_at);

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

                    CREATE TABLE IF NOT EXISTS saved_searches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        query TEXT NOT NULL,
                        label TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        UNIQUE(username, query)
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

                    CREATE TABLE IF NOT EXISTS account_activations (
                        token_hash TEXT PRIMARY KEY,
                        username TEXT NOT NULL UNIQUE,
                        created_by TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL,
                        used_at TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_users_status
                      ON users(status);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_user_groups_name
                      ON user_groups(lower(name));
                    CREATE INDEX IF NOT EXISTS idx_user_groups_status
                      ON user_groups(status, lower(name));
                    CREATE INDEX IF NOT EXISTS idx_user_group_members_username
                      ON user_group_members(username, group_id);
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
                    CREATE INDEX IF NOT EXISTS idx_saved_searches_username
                      ON saved_searches(username, created_at);
                    """
                )
                self._migrate_schema(conn)
                migrate_employee_onboarding(conn)
                ensure_schema_version(
                    conn,
                    db_kind="user_auth",
                    db_path=self.db_path,
                    expected_version=SCHEMA_VERSION,
                    code_root=Path(__file__).resolve().parents[3],
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)")
                self._ensure_default_admin(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
        migrations = {
            "telegram_username": "ALTER TABLE users ADD COLUMN telegram_username TEXT NOT NULL DEFAULT ''",
            "password_hash": "ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT ''",
            "role": "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'",
            "roles_json": "ALTER TABLE users ADD COLUMN roles_json TEXT NOT NULL DEFAULT ''",
            "must_change_password": "ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0",
        }
        for col, sql in migrations.items():
            if col not in cols:
                conn.execute(sql)
        session_cols = {row["name"] for row in conn.execute("PRAGMA table_info(user_sessions)").fetchall()}
        if "last_seen_at" not in session_cols:
            conn.execute("ALTER TABLE user_sessions ADD COLUMN last_seen_at TEXT")

    def _ensure_default_admin(self, conn: sqlite3.Connection) -> None:
        row = conn.execute(
            "SELECT username FROM users WHERE username='admin' UNION SELECT username FROM employee_platform_admins LIMIT 1"
        ).fetchone()
        if row is not None:
            return
        disable_default = str(os.environ.get("RAG_DISABLE_DEFAULT_ADMIN") or "").strip().lower()
        if disable_default in {"1", "true", "yes", "on"}:
            return
        bootstrap_password = str(os.environ.get("RAG_BOOTSTRAP_ADMIN_PASSWORD") or "").strip()
        if not bootstrap_password:
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
            (_hash_password(bootstrap_password), now, now),
        )

        conn.execute("INSERT INTO employee_platform_admins VALUES('admin')")

    def request_verification(
        self, *, username, display_name, telegram_chat_id="", password="", ttl_minutes=30
    ):
        """Legacy public registration now queues administrator review."""
        result = self.create_registration_request(username=username, display_name=display_name, source="web")
        if not result["ok"]:
            raise ValueError(result["reason"])
        return result

    def request_telegram_link_code(
        self,
        *,
        username: str,
        password: str,
        telegram_chat_id: str,
        ttl_minutes: int = 30,
    ) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        chat = normalize_sender_id(telegram_chat_id)
        if not usr or not chat or not password:
            raise ValueError("username, password и telegram_chat_id обязательны")

        now = _utc_now()
        expires = (datetime.now(timezone.utc) + timedelta(minutes=max(1, int(ttl_minutes)))).isoformat()
        code = f"{secrets.randbelow(1_000_000):06d}"

        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT status, password_hash, must_change_password
                    FROM users
                    WHERE username=?
                    """,
                    (usr,),
                ).fetchone()
                if row is None or str(row["status"]) != "active":
                    return {"ok": False, "reason": "user_not_found"}
                if not _verify_password(password, str(row["password_hash"] or "")):
                    return {"ok": False, "reason": "bad_credentials"}

                if row["must_change_password"]:
                    return {"ok": False, "reason": "password_change_required"}
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

    def request_telegram_deeplink_code(self, *, username, password, ttl_minutes=10):
        """Legacy result shape for password-authenticated deep links."""
        result = self.request_telegram_link(username=username, password=password, ttl_minutes=ttl_minutes)
        if result["ok"]:
            result["code"] = result["token"]
        return result

    def _new_token(self) -> str:
        return secrets.token_urlsafe(32)

    def create_telegram_link_token(self, *, session_token: str, ttl_minutes: int = 10):
        now = datetime.now(timezone.utc)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            user = conn.execute(
                """SELECT u.username,u.telegram_chat_id FROM users u JOIN user_sessions s
                ON s.username=u.username WHERE s.token=? AND s.revoked_at IS NULL AND s.expires_at>?
                AND u.status='active' AND u.must_change_password=0""",
                (session_token, now.isoformat()),
            ).fetchone()
            if not user:
                return {"ok": False, "reason": "unauthorized"}
            if user["telegram_chat_id"]:
                return {"ok": False, "reason": "already_linked"}
            token = self._new_token()
            expires = (now + timedelta(minutes=min(30, max(1, ttl_minutes)))).isoformat()
            conn.execute(
                "UPDATE telegram_tokens SET status='expired' WHERE username=? AND purpose='link' AND status='pending'",
                (user["username"],),
            )
            conn.execute(
                """INSERT INTO telegram_tokens(token,purpose,username,source_session,created_at,expires_at)
                            VALUES(?,'link',?,?,?,?)""",
                (token_hash(token), user["username"], session_token, now.isoformat(), expires),
            )
            return {"ok": True, "purpose": "link", "token": token, "expires_at": expires}

    def request_telegram_link(self, *, username, password, ttl_minutes=10):
        user = self.login(username=username, password=password, update_login=False)
        if not user or user["must_change_password"]:
            return {"ok": False, "reason": "bad_credentials"}
        session = self.create_session(username=user["username"], ttl_days=1)
        return self.create_telegram_link_token(session_token=session, ttl_minutes=ttl_minutes)

    def create_telegram_login_challenge(self, *, target="web", ttl_minutes=5):
        now = datetime.now(timezone.utc)
        token, verifier = self._new_token(), self._new_token()
        expires = (now + timedelta(minutes=min(5, max(1, ttl_minutes)))).isoformat()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO telegram_tokens(token,purpose,verifier_hash,target,created_at,expires_at)
                         VALUES(?,'login',?,?,?,?)""",
                (token_hash(token), token_hash(verifier), target, now.isoformat(), expires),
            )
        return {"ok": True, "token": token, "verifier": verifier, "expires_at": expires}

    def confirm_telegram_login(self, *, token, telegram_chat_id):
        chat = normalize_sender_id(telegram_chat_id)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            user = conn.execute(
                """SELECT username FROM users WHERE telegram_chat_id=? AND telegram_chat_id<>''
                    AND status='active' AND must_change_password=0""",
                (chat,),
            ).fetchone()
            if not user:
                return {"ok": False, "reason": "not_linked"}
            approved = conn.execute(
                """UPDATE telegram_tokens SET status='confirmed',username=?,telegram_chat_id=?
                WHERE token=? AND purpose='login' AND status='pending' AND verifier_hash<>'' AND expires_at>?""",
                (user["username"], chat, token_hash(token), _utc_now()),
            )
            if not approved.rowcount:
                return {"ok": False, "reason": "invalid_token"}
            return {"ok": True, "username": user["username"]}

    def consume_confirmed_telegram_login(self, *, verifier, browser_session=False):
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT * FROM telegram_tokens WHERE verifier_hash=? AND verifier_hash<>''
                               AND purpose='login' """,
                (token_hash(verifier),),
            ).fetchone()
            if not row or row["expires_at"] <= _utc_now() or (browser_session and row["target"] != "web"):
                return {"ok": False, "reason": "expired"}
            if browser_session and row["status"] == "used" and row["source_session"]:
                # The original browser can retry after losing the HTTP response.
                session = conn.execute(
                    """SELECT s.token FROM user_sessions s JOIN users u ON u.username=s.username
                       WHERE s.token=? AND s.username=? AND s.revoked_at IS NULL AND s.expires_at>?
                       AND u.status='active' AND u.must_change_password=0 AND u.telegram_chat_id=?""",
                    (row["source_session"], row["username"], _utc_now(), row["telegram_chat_id"]),
                ).fetchone()
                if session:
                    return {"ok": True, "session_token": session["token"]}
            if row["status"] not in {"pending", "confirmed"}:
                return {"ok": False, "reason": "expired"}
            if row["status"] == "pending":
                return {"ok": False, "reason": "pending"}
            conn.execute(
                "UPDATE telegram_tokens SET status='used',used_at=? WHERE id=?", (_utc_now(), row["id"])
            )
            user = conn.execute(
                """SELECT username,display_name,telegram_chat_id,telegram_username,role,status,
                must_change_password FROM users WHERE username=? AND telegram_chat_id=? AND status='active'
                AND must_change_password=0""",
                (row["username"], row["telegram_chat_id"]),
            ).fetchone()
            if not user:
                return {"ok": False, "reason": "not_linked"}
            if browser_session:
                token = self._insert_session(conn, user["username"], self._session_ttl_days(conn))
                conn.execute("UPDATE telegram_tokens SET source_session=? WHERE id=?", (token, row["id"]))
                conn.execute("""INSERT INTO auth_events (ts, username, event_type, ok, ip, user_agent, error)
                    VALUES (?, ?, 'telegram_web_login', 1, '', '', '')""", (_utc_now(), user["username"]))
                return {"ok": True, "session_token": token}
            return {"ok": True, "username": user["username"], "user": self._with_roles(conn, dict(user))}

    def create_admin_invite(
        self,
        *,
        created_by,
        username,
        display_name="",
        telegram_username="",
        password="",
        role="driver",
        ttl_minutes=4320,
    ):
        return self.create_employee_invite(
            admin_username=created_by,
            username=username,
            display_name=display_name,
            roles=[role],
            provider="telegram",
        )

    def _normalize_telegram_username(self, value: str) -> str:
        return str(value or "").strip().lstrip("@").lower()

    def consume_telegram_start_token(
        self, *, token, telegram_chat_id, telegram_username="", display_name="", expected_purpose="link"
    ):
        chat = normalize_sender_id(telegram_chat_id)
        if not chat or expected_purpose != "link":
            return {"ok": False, "reason": "invalid_token"}
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _utc_now()
            row = conn.execute(
                """SELECT t.id,u.username,u.telegram_chat_id FROM telegram_tokens t
                JOIN user_sessions s ON s.token=t.source_session AND s.username=t.username
                JOIN users u ON u.username=t.username WHERE t.token=? AND t.purpose='link' AND t.status='pending'
                AND t.expires_at>? AND s.revoked_at IS NULL AND s.expires_at>? AND u.status='active'
                AND u.must_change_password=0""",
                (token_hash(token), now, now),
            ).fetchone()
            if not row:
                return {"ok": False, "reason": "invalid_token"}
            if row["telegram_chat_id"] or _binding(conn, "telegram", chat):
                return {"ok": False, "reason": "already_linked"}
            conn.execute(
                "UPDATE users SET telegram_chat_id=?,telegram_username=?,telegram_linked_at=? WHERE username=?",
                (chat, self._normalize_telegram_username(telegram_username), now, row["username"]),
            )
            conn.execute("UPDATE telegram_tokens SET status='used',used_at=? WHERE id=?", (now, row["id"]))
            finish_employee_referral(conn, "telegram", chat, row["username"])
            return {"ok": True, "username": row["username"], "purpose": "link"}

    def create_registration_request(
        self,
        *,
        username="",
        display_name="",
        telegram_chat_id="",
        telegram_username="",
        source="web",
        organization_id=DEFAULT_ORG,
        phone="",
        note="",
        _conn=None,
    ):
        provider = "telegram" if source in {"telegram", "telegram_link"} else "web"
        return self.submit_employee_request(
            provider=provider,
            sender_id=telegram_chat_id,
            username=username,
            display_name=display_name or username,
            phone=phone,
            organization_id=organization_id,
        )

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

    def review_registration_request(self, *, request_id, reviewed_by, decision, role="driver", password=""):
        if decision not in {"approved", "rejected"}:
            return {"ok": False, "reason": "bad_decision"}
        approve = decision == "approved"
        return self.review_employee_request(
            admin_username=reviewed_by, request_id=request_id, approve=approve, roles=[role]
        )

    def confirm_verification(self, *, telegram_chat_id: str, code: str) -> Dict[str, Any]:
        chat = normalize_sender_id(telegram_chat_id)
        token = (code or "").strip()
        if not chat or not token:
            raise ValueError("telegram_chat_id и code обязательны")

        if token_hash(token):
            return self.consume_telegram_start_token(
                token=token, telegram_chat_id=chat, expected_purpose="link"
            )

        with self._lock:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """
                    SELECT id, username, expires_at
                    FROM verification_tokens
                    WHERE code=? AND status='pending'
                      AND telegram_chat_id=?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (token, chat),
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
                owner = conn.execute(
                    "SELECT telegram_chat_id,status,must_change_password FROM users WHERE username=?",
                    (username,),
                ).fetchone()
                if (
                    not owner
                    or owner["status"] != "active"
                    or owner["must_change_password"]
                    or owner["telegram_chat_id"]
                    or _binding(conn, "telegram", chat)
                ):
                    return {"ok": False, "reason": "not_eligible"}
                now = _utc_now()
                conn.execute(
                    "UPDATE verification_tokens SET status='confirmed', confirmed_at=? WHERE id=?",
                    (now, int(row["id"])),
                )
                conn.execute(
                    """
                    UPDATE users
                    SET telegram_chat_id=?,telegram_linked_at=?
                    WHERE username=?
                    """,
                    (chat, now, username),
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
                conn.execute("BEGIN IMMEDIATE")
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
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """
                    SELECT t.id FROM password_reset_tokens t JOIN users u ON u.username=t.username
                    WHERE t.username=? AND t.code=? AND t.status='confirmed' AND t.expires_at>?
                      AND u.status='active' AND u.telegram_chat_id=t.telegram_chat_id
                    ORDER BY t.id DESC
                    LIMIT 1
                    """,
                    (usr, token, _utc_now()),
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
        if not new_password:
            return False
        usr = (username or "").strip().lower()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT password_hash FROM users WHERE username=? AND status='active'", (usr,)).fetchone()
            if not row or not _verify_password(old_password, row["password_hash"]):
                return False
            conn.execute("UPDATE users SET password_hash=?,must_change_password=0 WHERE username=?",
                         (_hash_password(new_password), usr))
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
                data = self._with_roles(conn, dict(row))
                if not _verify_password(password, str(data.get("password_hash", ""))):
                    return None
                if update_login:
                    conn.execute(
                        "UPDATE users SET last_login_at=? WHERE username=?",
                        (_utc_now(), usr),
                    )
                data.pop("password_hash", None)
                data["groups"] = self._list_user_groups_conn(conn, usr)
                data["group_ids"] = [str(group["id"]) for group in data["groups"]]
                return data

    def login_with_reason(
        self,
        *,
        username: str,
        password: str = "",
    ) -> Dict[str, Any]:
        """Like login() but returns {user, reason} where reason is one of:
        'ok', 'invalid_credentials', 'pending', 'blocked', 'not_found'."""
        usr = (username or "").strip().lower()
        if not usr:
            return {"user": None, "reason": "not_found"}
        throttle = self.login_throttle_status(username=usr)
        if throttle["limited"]:
            return {
                "user": None,
                "reason": "rate_limited",
                "retry_after_seconds": throttle["retry_after_seconds"],
            }
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM users WHERE username=?", (usr,)
                ).fetchone()
        if row is None:
            return {"user": None, "reason": "not_found"}
        status = str(row["status"] or "")
        if status == "pending":
            return {"user": None, "reason": "pending"}
        if status not in ("active",):
            return {"user": None, "reason": "blocked"}
        if not _verify_password(password, str(row["password_hash"] or "")):
            return {"user": None, "reason": "invalid_credentials"}
        data = dict(row)
        data.pop("password_hash", None)
        with self._lock:
            with self._connect() as conn:
                data = self._with_roles(conn, data)
                conn.execute("UPDATE users SET last_login_at=? WHERE username=?", (_utc_now(), usr))
                data["groups"] = self._list_user_groups_conn(conn, usr)
                data["group_ids"] = [str(group["id"]) for group in data["groups"]]
        return {"user": data, "reason": "ok"}

    def login_throttle_status(
        self,
        *,
        username: str,
        max_failures: int = LOGIN_MAX_FAILURES,
        window_seconds: int = LOGIN_FAILURE_WINDOW_SECONDS,
        lockout_seconds: int = LOGIN_LOCKOUT_SECONDS,
    ) -> Dict[str, Any]:
        usr = (username or "").strip().lower()
        if not usr:
            return {"limited": False, "retry_after_seconds": 0, "failures": 0}
        now = datetime.now(timezone.utc)
        window_start = (now - timedelta(seconds=max(1, int(window_seconds)))).isoformat()
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT ts, event_type, ok
                    FROM auth_events
                    WHERE username=? AND ts>=? AND event_type IN ('login', 'login_failed')
                    ORDER BY id DESC
                    """,
                    (usr, window_start),
                ).fetchall()
        failures: list[datetime] = []
        for row in rows:
            if bool(row["ok"]) and str(row["event_type"]) == "login":
                break
            if not bool(row["ok"]) and str(row["event_type"]) == "login_failed":
                try:
                    failures.append(datetime.fromisoformat(str(row["ts"])))
                except ValueError:
                    continue
        required = max(1, int(max_failures))
        if len(failures) < required:
            return {"limited": False, "retry_after_seconds": 0, "failures": len(failures)}
        latest = max(failures)
        retry_after = max(0, int((latest + timedelta(seconds=max(1, int(lockout_seconds))) - now).total_seconds()))
        return {
            "limited": retry_after > 0,
            "retry_after_seconds": retry_after,
            "failures": len(failures),
        }

    def has_default_admin_password(self) -> bool:
        """Returns True if 'admin' user exists and still uses the default 'admin' password."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT password_hash FROM users WHERE username='admin'"
            ).fetchone()
        if row is None:
            return False
        return _verify_password("admin", str(row["password_hash"] or ""))

    def _normalize_session_ttl_days(self, value: Any) -> int:
        try:
            days = int(value)
        except (TypeError, ValueError):
            days = DEFAULT_SESSION_TTL_DAYS
        return max(MIN_SESSION_TTL_DAYS, min(MAX_SESSION_TTL_DAYS, days))

    def get_session_ttl_days(self) -> int:
        with self._lock:
            with self._connect() as conn:
                return self._session_ttl_days(conn)

    def _session_ttl_days(self, conn) -> int:
        row = conn.execute("SELECT value FROM app_settings WHERE key=?", (SESSION_TTL_SETTING_KEY,)).fetchone()
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
        with self._lock:
            with self._connect() as conn:
                ttl = self._normalize_session_ttl_days(ttl_days) if ttl_days is not None else self._session_ttl_days(conn)
                return self._insert_session(conn, usr, ttl)

    @staticmethod
    def _insert_session(conn, username, ttl_days):
        token = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        expires = now + timedelta(days=ttl_days)
        conn.execute(
            """INSERT INTO user_sessions (token, username, created_at, last_seen_at, expires_at, revoked_at)
               VALUES (?, ?, ?, ?, ?, NULL)""",
            (token, username, now.isoformat(), now.isoformat(), expires.isoformat()),
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
                user = self._with_roles(conn, dict(row))
                user["groups"] = self._list_user_groups_conn(conn, str(user.get("username") or ""))
                user["group_ids"] = [str(group["id"]) for group in user["groups"]]
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
                if row is None:
                    return None
                user = self._with_roles(conn, dict(row))
                user["groups"] = self._list_user_groups_conn(conn, usr)
                user["group_ids"] = [str(group["id"]) for group in user["groups"]]
                return user

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
                    WHERE telegram_chat_id=? AND status='active' AND must_change_password=0
                    LIMIT 1
                    """,
                    (chat,),
                ).fetchone()
                if row is None:
                    return None
                user = self._with_roles(conn, dict(row))
                user["groups"] = self._list_user_groups_conn(conn, str(user.get("username") or ""))
                user["group_ids"] = [str(group["id"]) for group in user["groups"]]
                return user

    def _max_employee(self, conn: sqlite3.Connection, sender: str) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            """SELECT u.username, u.display_name, u.telegram_chat_id, u.telegram_username,
                      u.role, u.roles_json, u.must_change_password, u.status, u.verified_at,
                      u.created_at, u.last_login_at, m.max_user_id, m.linked_at AS max_linked_at
               FROM max_bindings m JOIN users u ON m.user_id=u.id
               WHERE m.max_user_id=? AND u.status='active' AND u.must_change_password=0""",
            (sender,),
        ).fetchone()
        if row is None:
            return None
        user = dict(row)
        user["roles"] = sorted(user_roles(user))
        user["groups"] = self._list_user_groups_conn(conn, user["username"])
        user["group_ids"] = [group["id"] for group in user["groups"]]
        return with_organizations(conn, user)

    def get_user_by_max_user_id(self, max_user_id: str | int) -> Optional[Dict[str, Any]]:
        """The ID must come from the authenticated MAX event sender in a private chat."""
        with self._connect() as conn:
            return self._max_employee(conn, normalize_sender_id(max_user_id))

    def create_max_link_token(self, *, session_token: str) -> Dict[str, Any]:
        """Only the current holder of an active catalog session may start linking it."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = datetime.now(timezone.utc)
            row = conn.execute(
                """SELECT u.id, m.max_user_id FROM user_sessions s
                   JOIN users u ON u.username=s.username LEFT JOIN max_bindings m ON m.user_id=u.id
                   WHERE s.token=? AND s.revoked_at IS NULL AND s.expires_at>?
                     AND u.status='active' AND u.must_change_password=0""",
                (session_token, now.isoformat()),
            ).fetchone()
            if row is None:
                return {"ok": False, "reason": "invalid_session"}
            if row["max_user_id"] is not None:
                return {"ok": False, "reason": "already_linked"}
            token = self._new_token()
            expires = (now + timedelta(minutes=10)).isoformat()
            conn.execute(
                """INSERT INTO max_link_tokens(user_id, token_hash, session_token, expires_at)
                   VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET token_hash=excluded.token_hash,
                       session_token=excluded.session_token, expires_at=excluded.expires_at""",
                (row["id"], token_hash(token), session_token, expires),
            )
        return {"ok": True, "token": token, "expires_at": expires}

    def consume_max_link_token(self, *, token: str, max_user_id: str | int) -> Dict[str, Any]:
        sender = normalize_sender_id(max_user_id)
        digest = token_hash(token)
        if not sender or not digest:
            return {"ok": False, "reason": "invalid_token"}
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _utc_now()
            row = conn.execute(
                """SELECT u.id, u.username FROM max_link_tokens t JOIN users u ON u.id=t.user_id
                   JOIN user_sessions s ON s.token=t.session_token AND s.username=u.username
                   WHERE t.token_hash=? AND t.expires_at>? AND s.expires_at>? AND s.revoked_at IS NULL
                     AND u.status='active' AND u.must_change_password=0""",
                (digest, now, now),
            ).fetchone()
            if row is None:
                return {"ok": False, "reason": "invalid_token"}
            if conn.execute("SELECT 1 FROM max_bindings WHERE max_user_id=? OR user_id=?",
                            (sender, row["id"])).fetchone():
                return {"ok": False, "reason": "already_linked"}
            conn.execute("INSERT INTO max_bindings(user_id,max_user_id,linked_at) VALUES (?,?,?)",
                         (row["id"], sender, now))
            conn.execute("DELETE FROM max_link_tokens WHERE user_id=?", (row["id"],))
            conn.execute("INSERT INTO auth_events(ts,username,event_type) VALUES (?,?,'max_link')",
                         (now, row["username"]))
            finish_employee_referral(conn, "max", sender, row["username"])
        return {"ok": True, "username": row["username"]}

    @staticmethod
    def _remove_max_binding(conn: sqlite3.Connection, user_id: int) -> None:
        conn.execute("""DELETE FROM max_login_challenges WHERE max_user_id IN
                        (SELECT max_user_id FROM max_bindings WHERE user_id=?)""", (user_id,))
        conn.execute("DELETE FROM max_link_tokens WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM max_bindings WHERE user_id=?", (user_id,))

    def unlink_max_user(self, max_user_id: str | int) -> Optional[str]:
        """Detach the sender's own MAX identity."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT u.id, u.username FROM max_bindings m JOIN users u ON u.id=m.user_id
                   WHERE m.max_user_id=?""", (normalize_sender_id(max_user_id),),
            ).fetchone()
            if row is None:
                return None
            self._revoke_employee_channel(conn, row["username"], "max")
            conn.execute("INSERT INTO auth_events(ts,username,event_type) VALUES (?,?,'max_unlink')",
                         (_utc_now(), row["username"]))
            return row["username"]

    def unlink_max_for_session(self, *, session_token: str) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = _utc_now()
            row = conn.execute(
                """SELECT u.id, u.username FROM user_sessions s JOIN users u ON u.username=s.username
                   WHERE s.token=? AND s.revoked_at IS NULL AND s.expires_at>?
                     AND u.status='active' AND u.must_change_password=0""", (session_token, now),
            ).fetchone()
            if row is None:
                return False
            self._revoke_employee_channel(conn, row["username"], "max")
            conn.execute("INSERT INTO auth_events(ts,username,event_type) VALUES (?,?,'max_unlink')",
                         (now, row["username"]))
            return True

    def create_max_login_challenge(self) -> Dict[str, Any]:
        """Only the bot token leaves the web UI. Its independent verifier redeems the login."""
        token, verifier = self._new_token(), self._new_token()
        now = datetime.now(timezone.utc)
        expires = (now + timedelta(minutes=5)).isoformat()
        with self._connect() as conn:
            conn.execute("DELETE FROM max_login_challenges WHERE expires_at<=?", (now.isoformat(),))
            conn.execute(
                """INSERT INTO max_login_challenges(token_hash,verifier_hash,expires_at)
                   VALUES (?,?,?)""", (token_hash(token), token_hash(verifier), expires),
            )
        return {"ok": True, "token": token, "verifier": verifier, "expires_at": expires}

    def confirm_max_login(self, *, token: str, max_user_id: str | int) -> Dict[str, Any]:
        """Call only after the linked user explicitly confirms the browser login in MAX."""
        sender = normalize_sender_id(max_user_id)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            user = self._max_employee(conn, sender)
            if user is None:
                return {"ok": False, "reason": "not_linked"}
            changed = conn.execute(
                """UPDATE max_login_challenges SET max_user_id=?
                   WHERE token_hash=? AND max_user_id IS NULL AND expires_at>?""",
                (sender, token_hash(token), _utc_now()),
            ).rowcount
            if not changed:
                return {"ok": False, "reason": "invalid_token"}
            return {"ok": True, "username": user["username"]}

    def consume_confirmed_max_login(self, *, verifier: str) -> Dict[str, Any]:
        verifier_hash = token_hash(verifier)
        if not verifier_hash:
            return {"ok": False, "reason": "invalid_token"}
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT max_user_id,expires_at FROM max_login_challenges WHERE verifier_hash=?",
                (verifier_hash,),
            ).fetchone()
            if row is None:
                return {"ok": False, "reason": "invalid_token"}
            if row["expires_at"] <= _utc_now():
                conn.execute("DELETE FROM max_login_challenges WHERE verifier_hash=?", (verifier_hash,))
                return {"ok": False, "reason": "expired"}
            if row["max_user_id"] is None:
                return {"ok": False, "reason": "pending"}
            user = self._max_employee(conn, row["max_user_id"])
            conn.execute("DELETE FROM max_login_challenges WHERE verifier_hash=?", (verifier_hash,))
            if user is None:
                return {"ok": False, "reason": "invalid_token"}
            return {"ok": True, "username": user["username"], "user": user}

    def upsert_user_from_telegram_contact(
        self, *, telegram_chat_id, display_name="", telegram_username="", created_by="", **_
    ):
        return self.create_employee_invite(
            admin_username=created_by,
            username="",
            display_name=display_name,
            provider="telegram",
            expected_sender_id=telegram_chat_id,
        )

    def unlink_telegram_chat_id(self, telegram_chat_id):
        chat = normalize_sender_id(telegram_chat_id)
        if not chat:
            return None
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = _binding(conn, "telegram", chat)
            if not row:
                return None
            self._revoke_employee_channel(conn, row["username"], "telegram")
            return row["username"]

    def unlink_telegram_for_session(self, *, session_token):
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT u.username FROM user_sessions s JOIN users u ON u.username=s.username
                WHERE s.token=? AND s.revoked_at IS NULL AND s.expires_at>? AND u.status='active'
                AND u.must_change_password=0""",
                (session_token, _utc_now()),
            ).fetchone()
            if not row:
                return False
            self._revoke_employee_channel(conn, row["username"], "telegram")
            return True

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
                users = [self._with_roles(conn, dict(row)) for row in rows]
                for user in users:
                    username = str(user.get("username") or "")
                    user["groups"] = self._list_user_groups_conn(conn, username, active_only=False)
                    user["group_ids"] = [str(group["id"]) for group in user["groups"] if group.get("status") == "active"]
                return users

    @staticmethod
    def _list_user_groups_conn(
        conn: sqlite3.Connection,
        username: str,
        *,
        active_only: bool = True,
    ) -> list[Dict[str, Any]]:
        usr = str(username or "").strip().lower()
        if not usr:
            return []
        status_clause = "AND g.status='active'" if active_only else ""
        rows = conn.execute(
            f"""
            SELECT g.id, g.name, g.description, g.status, g.created_by, g.created_at, g.updated_at,
                   m.added_by, m.created_at AS member_since
            FROM user_group_members m
            JOIN user_groups g ON g.id=m.group_id
            WHERE m.username=? {status_clause}
            ORDER BY lower(g.name)
            """,
            (usr,),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_user_groups(self, *, username: str, active_only: bool = True) -> list[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                return self._list_user_groups_conn(conn, username, active_only=active_only)

    def get_group(self, group_id: str) -> Optional[Dict[str, Any]]:
        clean_id = str(group_id or "").strip().lower()
        if not clean_id:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute("SELECT * FROM user_groups WHERE lower(id)=?", (clean_id,)).fetchone()
                if row is None:
                    return None
                group = dict(row)
                group["members"] = [
                    str(member["username"])
                    for member in conn.execute(
                        "SELECT username FROM user_group_members WHERE group_id=? ORDER BY lower(username)",
                        (str(row["id"]),),
                    ).fetchall()
                ]
                group["member_count"] = len(group["members"])
                return group

    def list_groups(self, *, include_archived: bool = True) -> list[Dict[str, Any]]:
        where = "" if include_archived else "WHERE g.status='active'"
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT g.*, COUNT(m.username) AS member_count
                    FROM user_groups g
                    LEFT JOIN user_group_members m ON m.group_id=g.id
                    {where}
                    GROUP BY g.id
                    ORDER BY g.status!='active', lower(g.name)
                    """
                ).fetchall()
                groups = [dict(row) for row in rows]
                members = conn.execute(
                    "SELECT group_id, username FROM user_group_members ORDER BY lower(username)"
                ).fetchall()
        members_by_group: Dict[str, list[str]] = {}
        for member in members:
            members_by_group.setdefault(str(member["group_id"]), []).append(str(member["username"]))
        for group in groups:
            group["members"] = members_by_group.get(str(group["id"]), [])
        return groups

    def create_group(
        self,
        *,
        name: str,
        description: str = "",
        created_by: str = "",
    ) -> Dict[str, Any]:
        clean_name = str(name or "").strip()
        if not clean_name:
            raise RuntimeError("Укажите название группы.")
        if len(clean_name) > 120:
            raise RuntimeError("Название группы не должно превышать 120 символов.")
        group_id = secrets.token_hex(16)
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                try:
                    conn.execute(
                        """
                        INSERT INTO user_groups (id, name, description, status, created_by, created_at, updated_at)
                        VALUES (?, ?, ?, 'active', ?, ?, ?)
                        """,
                        (
                            group_id,
                            clean_name,
                            str(description or "").strip(),
                            str(created_by or "").strip().lower(),
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise RuntimeError("Группа с таким названием уже существует.") from exc
        return self.get_group(group_id) or {}

    def update_group(
        self,
        *,
        group_id: str,
        name: str,
        description: str = "",
        status: str = "active",
    ) -> Optional[Dict[str, Any]]:
        clean_id = str(group_id or "").strip()
        clean_name = str(name or "").strip()
        clean_status = str(status or "active").strip().lower()
        if not clean_id or not clean_name:
            raise RuntimeError("Не заданы группа или название.")
        if len(clean_name) > 120:
            raise RuntimeError("Название группы не должно превышать 120 символов.")
        if clean_status not in {"active", "archived"}:
            raise RuntimeError("Недопустимый статус группы.")
        with self._lock:
            with self._connect() as conn:
                try:
                    cur = conn.execute(
                        """
                        UPDATE user_groups
                        SET name=?, description=?, status=?, updated_at=?
                        WHERE id=?
                        """,
                        (clean_name, str(description or "").strip(), clean_status, _utc_now(), clean_id),
                    )
                except sqlite3.IntegrityError as exc:
                    raise RuntimeError("Группа с таким названием уже существует.") from exc
                if cur.rowcount <= 0:
                    return None
        return self.get_group(clean_id)

    def add_group_member(self, *, group_id: str, username: str, added_by: str = "") -> bool:
        clean_id = str(group_id or "").strip()
        usr = str(username or "").strip().lower()
        if not clean_id or not usr:
            return False
        with self._lock:
            with self._connect() as conn:
                group = conn.execute("SELECT status FROM user_groups WHERE id=?", (clean_id,)).fetchone()
                user = conn.execute("SELECT status FROM users WHERE username=?", (usr,)).fetchone()
                if group is None:
                    raise RuntimeError("Группа не найдена.")
                if str(group["status"] or "") != "active":
                    raise RuntimeError("Нельзя добавлять участников в архивную группу.")
                if user is None:
                    raise RuntimeError("Пользователь не найден.")
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO user_group_members (group_id, username, added_by, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (clean_id, usr, str(added_by or "").strip().lower(), _utc_now()),
                )
                return cur.rowcount > 0

    def remove_group_member(self, *, group_id: str, username: str) -> bool:
        clean_id = str(group_id or "").strip()
        usr = str(username or "").strip().lower()
        if not clean_id or not usr:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM user_group_members WHERE group_id=? AND username=?",
                    (clean_id, usr),
                )
                return cur.rowcount > 0

    def update_profile(self, *, username, display_name, telegram_chat_id="", telegram_username=""):
        with self._connect() as conn:
            return bool(
                conn.execute(
                    "UPDATE users SET display_name=? WHERE username=?",
                    (str(display_name or "").strip(), username),
                ).rowcount
            )

    def save_driver(
        self,
        token: str,
        *,
        username: str,
        display_name: str,
        password: str = "",
        create: bool = False,
        archived: bool = False,
        organization_id: str = DEFAULT_ORG,
    ) -> None:
        """Dispatcher-scoped account changes; authorization and mutation share a transaction."""
        usr, name = str(username).strip().lower(), str(display_name).strip()
        if not usr or len(usr) > 100 or any(c.isspace() for c in usr) or not name or len(name) > 200:
            raise ValueError("Укажите логин без пробелов и имя сотрудника")
        if (create or password) and len(password) < 12:
            raise ValueError("Временный пароль должен содержать не менее 12 символов")
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                actor_row = conn.execute("""SELECT u.* FROM users u JOIN user_sessions s ON s.username=u.username
                    WHERE s.token=? AND s.revoked_at IS NULL AND s.expires_at>?""", (token, now)).fetchone()
                actor = dict(actor_row) if actor_row else {}
                if (
                    actor.get("status") != "active"
                    or actor.get("must_change_password")
                    or not actor.get("username")
                ):
                    raise PermissionError("Недостаточно прав для изменения сотрудников")
                managed = _scope(conn, actor["username"]) | {
                    m["id"] for m in _memberships(conn, actor["username"]) if "dispatcher" in m["roles"]
                }
                if organization_id not in managed:
                    raise PermissionError("Недостаточно прав в организации")
                target = conn.execute("SELECT * FROM users WHERE username=?", (usr,)).fetchone()
                if create:
                    if target:
                        raise ValueError("Этот логин уже занят")
                    conn.execute("""INSERT INTO users(username,display_name,password_hash,role,roles_json,
                        status,must_change_password,created_at,verified_at) VALUES(?,?,?,?,?,?,1,?,?)""",
                        (usr, name, _hash_password(password), "driver", '["driver"]', "active", now, now))
                    assign_new_membership(conn, usr, organization_id, ["driver"])
                else:
                    if not target:
                        raise ValueError("Сотрудник не найден")
                    memberships = _memberships(conn, usr)
                    if (
                        organization_id not in {m["id"] for m in memberships}
                        or any(m["id"] not in managed or set(m["roles"]) != {"driver"} for m in memberships)
                        or user_roles(dict(target)) != {"driver"}
                        or target["status"] == "pending"
                    ):
                        raise PermissionError("Учётную запись с дополнительными ролями изменяет администратор в настройках")
                    conn.execute("UPDATE users SET display_name=?,status=? WHERE username=?",
                                 (name, "blocked" if archived else "active", usr))
                    if password:
                        conn.execute("UPDATE users SET password_hash=?,must_change_password=1 WHERE username=?",
                                     (_hash_password(password), usr))
                        for provider in ("telegram", "max"):
                            self._revoke_employee_channel(conn, usr, provider)
                    if archived or password:
                        conn.execute("UPDATE user_sessions SET revoked_at=? WHERE username=? AND revoked_at IS NULL", (now, usr))
                conn.execute("INSERT INTO auth_events(ts,username,event_type,ok,error) VALUES(?,?,?,1,?)",
                             (now, actor["username"], "driver_create" if create else "driver_update",
                              json.dumps({"target": usr, "display_name": name, "archived": archived}, ensure_ascii=False)))

    def admin_update_user(
        self,
        *,
        username,
        display_name,
        telegram_chat_id="",
        telegram_username="",
        role,
        status,
        must_change_password,
        roles=None,
    ):
        """Trusted platform settings. Per-organization grants use set_employee_org_roles."""
        usr = str(username or "").strip().lower()
        values = normalize_roles([role] if roles is None else roles)
        if status not in {"pending", "active", "blocked"}:
            return False
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM users WHERE username=?", (usr,)).fetchone()
            if not row:
                return False
            if row["status"] == "pending" and not row["password_hash"] and status == "active":
                return False
            if set(values) != user_roles(dict(row)):
                memberships = _memberships(conn, usr)
                if len(memberships) != 1:
                    return False
                if "admin" in user_roles(dict(row)) and "admin" not in values:
                    conn.execute("DELETE FROM employee_platform_admins WHERE username=?", (usr,))
                conn.execute(
                    "UPDATE employee_memberships SET roles_json=? WHERE username=?", (json.dumps(values), usr)
                )
                project_roles(conn, usr)
            conn.execute(
                "UPDATE users SET display_name=?,status=?,must_change_password=? WHERE username=?",
                (str(display_name or "").strip(), status, int(bool(must_change_password)), usr),
            )
            if status != "active":
                conn.execute(
                    "UPDATE user_sessions SET revoked_at=? WHERE username=? AND revoked_at IS NULL",
                    (_utc_now(), usr),
                )
            return True

    def _revoke_employee_channel(self, conn, username, provider):
        user = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
        if provider == "max":
            self._remove_max_binding(conn, user["id"])
        else:
            conn.execute(
                "UPDATE users SET telegram_chat_id='',telegram_username='',telegram_linked_at='' WHERE username=?", (username,)
            )
            for table in ("telegram_tokens", "verification_tokens", "password_reset_tokens"):
                conn.execute(
                    f"UPDATE {table} SET status='expired' WHERE username=? AND status IN ('pending','confirmed')",
                    (username,),
                )
        conn.execute("DELETE FROM employee_invites WHERE username=? AND provider=?", (username, provider))
        conn.execute(
            "DELETE FROM account_activations WHERE username=? AND bound_provider=? AND used_at IS NULL",
            (username, provider),
        )

    def reset_employee_channel(self, *, admin_username, username, provider):
        if provider not in {"max", "telegram"}:
            return {"ok": False, "reason": "invalid_provider"}
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if not _all_in_scope(conn, admin_username, username):
                return {"ok": False, "reason": "not_admin"}
            self._revoke_employee_channel(conn, username, provider)
            conn.execute(
                "UPDATE user_sessions SET revoked_at=? WHERE username=? AND revoked_at IS NULL",
                (_utc_now(), username),
            )
            _audit(conn, admin_username, "employee_channel_reset", target=username, provider=provider)
            return {"ok": True}

    def create_account_activation(
        self,
        *,
        username,
        display_name,
        created_by,
        ttl_hours=72,
        reset_existing=False,
        roles=None,
        renew_pending=False,
        organization_id=DEFAULT_ORG,
    ):
        usr = str(username or "").strip().lower()
        if not re.fullmatch(r"[a-z][a-z0-9._-]{2,63}", usr):
            raise ValueError("Use a Latin login of 3 to 64 characters")
        if not 1 <= ttl_hours <= 168 or (renew_pending and reset_existing):
            raise ValueError("Invalid invitation operation")
        values = normalize_roles(["driver"] if roles is None else roles)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if organization_id not in _scope(conn, created_by):
                return {"ok": False, "reason": "not_admin"}
            row = conn.execute("SELECT id,status FROM users WHERE username=?", (usr,)).fetchone()
            if (renew_pending or reset_existing) and row:
                if not _all_in_scope(conn, created_by, usr) or organization_id not in {
                    m["id"] for m in _memberships(conn, usr)
                }:
                    return {"ok": False, "reason": "not_admin"}
            if renew_pending:
                pending = conn.execute(
                    """SELECT 1 FROM account_activations a JOIN users u ON u.username=a.username
                              WHERE a.username=? AND a.used_at IS NULL AND u.status='pending' AND u.password_hash=''
                              AND u.must_change_password=1 AND a.bound_provider IN ('','web')""",
                    (usr,),
                ).fetchone()
                if not pending:
                    return {"ok": False, "reason": "not_pending"}
                event = "account_invite_renewed"
            elif reset_existing:
                if not row:
                    return {"ok": False, "reason": "not_found"}
                if row["status"] not in {"pending", "active"}:
                    return {"ok": False, "reason": "blocked"}
                for provider in ("max", "telegram"):
                    self._revoke_employee_channel(conn, usr, provider)
                conn.execute(
                    "UPDATE users SET status='pending',must_change_password=1,password_hash='' WHERE id=?",
                    (row["id"],),
                )
                conn.execute(
                    "UPDATE user_sessions SET revoked_at=? WHERE username=? AND revoked_at IS NULL",
                    (_utc_now(), usr),
                )
                event = "account_password_reset_link"
            else:
                user = create_pending(conn, usr, display_name, organization_id, values)
                if not user["ok"]:
                    return user
                event = "account_invited"
            result = issue_activation(conn, usr, created_by, ttl_hours=ttl_hours)
            _audit(conn, created_by, event, target=usr)
            return {"ok": True, **result}

    def activate_account(self, *, token: str, new_password: str) -> Dict[str, Any]:
        digest = token_hash(token)
        if not digest:
            return {"ok": False, "reason": "invalid_activation"}
        if not isinstance(new_password, str) or not 8 <= len(new_password) <= 128 or not new_password.strip():
            return {"ok": False, "reason": "invalid_password"}
        now = _utc_now()
        with self._lock, closing(self._connect()) as conn, conn:
            # Serialize consumption across web workers and independent DB instances.
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT a.username FROM account_activations a JOIN users u ON u.username=a.username
                   WHERE a.token_hash=? AND a.used_at IS NULL AND a.expires_at>?
                     AND u.status='pending' AND u.password_hash='' AND u.must_change_password=1""",
                (digest, now),
            ).fetchone()
            if row is None:
                return {"ok": False, "reason": "invalid_activation"}
            username = str(row["username"])
            conn.execute(
                """UPDATE users SET password_hash=?, status='active', must_change_password=0,
                   verified_at=? WHERE username=?""",
                (_hash_password(new_password), now, username),
            )
            conn.execute("UPDATE account_activations SET used_at=? WHERE token_hash=?", (now, digest))
            conn.execute(
                "UPDATE user_sessions SET revoked_at=? WHERE username=? AND revoked_at IS NULL", (now, username),
            )
            conn.execute(
                "INSERT INTO auth_events(ts, username, event_type, ok) VALUES (?, ?, 'account_activated', 1)",
                (now, username),
            )
        return {"ok": True, "username": username}

    def admin_set_password(self, *, username: str, new_password: str, must_change_password: bool = True) -> bool:
        usr = (username or "").strip().lower()
        if not usr or not isinstance(new_password, str) or not 8 <= len(new_password) <= 128 or not new_password.strip():
            return False
        password_hash = _hash_password(new_password)
        with self._lock:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                now = _utc_now()
                # A verified employee awaiting web recovery can switch to a temporary
                # password. Do not approve a new registration or reactivate a blocked user.
                cur = conn.execute(
                    """UPDATE users SET password_hash=?,
                       must_change_password=CASE WHEN status='pending' THEN 1 ELSE ? END, status='active'
                       WHERE username=? AND (status='active' OR
                           (status='pending' AND verified_at IS NOT NULL AND verified_at<>''
                            AND must_change_password=1 AND EXISTS (
                                SELECT 1 FROM account_activations a WHERE a.username=users.username
                                AND a.used_at IS NULL AND a.expires_at>? AND a.bound_provider='')))""",
                    (password_hash, int(bool(must_change_password)), usr, now),
                )
                if not cur.rowcount:
                    return False
                conn.execute("DELETE FROM account_activations WHERE username=? AND used_at IS NULL", (usr,))
                for provider in ("telegram", "max"):
                    self._revoke_employee_channel(conn, usr, provider)
                conn.execute("UPDATE user_sessions SET revoked_at=? WHERE username=? AND revoked_at IS NULL",
                             (now, usr))
                return True

    def admin_create_user(
        self,
        *,
        username: str,
        display_name: str = "",
        telegram_chat_id: str = "",
        telegram_username: str = "",
        password: str,
        role: str = "driver",
        organization_id: str = DEFAULT_ORG,
        platform_admin: Optional[bool] = None,
        status: str = "active",
        must_change_password: bool = True,
        roles: Optional[list[str]] = None,
    ) -> bool:
        """Trusted provisioning. The legacy explicit role='admin' is a platform bootstrap.

        New employee forms pass organization roles and platform_admin=False.
        """
        usr = (username or "").strip().lower()
        role_values = normalize_roles(roles if roles is not None else [role])
        role_value = primary_role(role_values)
        status_value = str(status or "").strip().lower()
        if status_value not in {"active", "pending", "blocked"}:
            status_value = "active"
        if not usr or not password:
            return False
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if not conn.execute(
                    "SELECT 1 FROM employee_organizations WHERE id=?", (organization_id,)
                ).fetchone():
                    return False
                if telegram_chat_id and not normalize_sender_id(telegram_chat_id):
                    return False
                try:
                    conn.execute(
                        """
                        INSERT INTO users (
                            username, display_name, telegram_chat_id, telegram_username, password_hash,
                            role, must_change_password, status, created_at,
                            verified_at, roles_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                            json.dumps(role_values),
                        ),
                    )
                    if "admin" in role_values and (
                        platform_admin is True
                        or (platform_admin is None and role == "admin" and roles is None)
                    ):
                        conn.execute("INSERT INTO employee_platform_admins VALUES(?)", (usr,))
                    if telegram_chat_id:
                        conn.execute("UPDATE users SET telegram_linked_at=? WHERE username=?", (now, usr))
                    assign_new_membership(conn, usr, organization_id, role_values)
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

    # ── Saved searches ────────────────────────────────────────────────────────

    def list_saved_searches(self, *, username: str) -> list[Dict[str, Any]]:
        usr = (username or "").strip().lower()
        if not usr:
            return []
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM saved_searches WHERE username=? ORDER BY created_at DESC",
                    (usr,),
                ).fetchall()
                return [dict(row) for row in rows]

    def add_saved_search(self, *, username: str, query: str, label: str = "") -> bool:
        usr = (username or "").strip().lower()
        q = (query or "").strip()
        if not usr or not q:
            return False
        with self._lock:
            with self._connect() as conn:
                try:
                    conn.execute(
                        "INSERT OR REPLACE INTO saved_searches (username, query, label, created_at) VALUES (?,?,?,?)",
                        (usr, q, (label or q).strip(), _utc_now()),
                    )
                    return True
                except Exception:
                    return False

    def remove_saved_search(self, *, username: str, query: str) -> bool:
        usr = (username or "").strip().lower()
        q = (query or "").strip()
        if not usr or not q:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM saved_searches WHERE username=? AND query=?",
                    (usr, q),
                )
                return cur.rowcount > 0

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
