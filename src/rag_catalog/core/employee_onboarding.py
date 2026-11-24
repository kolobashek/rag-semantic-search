"""Organization-scoped employee onboarding for web and messenger entry points."""

import hashlib
import json
import re
import secrets
from datetime import datetime, timedelta, timezone

from .roles import normalize_roles, primary_role, user_roles

DEFAULT_ORG = "default"
PROVIDERS = {"telegram", "max"}


def _now():
    return datetime.now(timezone.utc).isoformat()


def token_hash(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{43}", value):
        return ""
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def normalize_sender_id(value):
    text = str(value)
    return text if re.fullmatch(r"[1-9][0-9]{0,18}", text) and int(text) < 2**63 else ""


def _audit(conn, actor, event, **data):
    conn.execute(
        "INSERT INTO auth_events(ts,username,event_type,ok,error) VALUES(?,?,?,1,?)",
        (_now(), actor, event, json.dumps(data, ensure_ascii=False)),
    )


def _memberships(conn, username):
    return [
        dict(id=r["id"], name=r["name"], roles=json.loads(r["roles_json"]))
        for r in conn.execute(
            """SELECT o.id,o.name,m.roles_json FROM employee_memberships m JOIN employee_organizations o
           ON o.id=m.organization_id WHERE m.username=? ORDER BY o.name,o.id""",
            (username,),
        )
    ]


def with_organizations(conn, user):
    groups = _memberships(conn, user["username"])
    user["organizations"] = groups
    user["organization_roles"] = {g["id"]: g["roles"] for g in groups}
    user["organization_ids"] = [g["id"] for g in groups]
    user["is_platform_admin"] = bool(
        conn.execute(
            "SELECT 1 FROM employee_platform_admins WHERE username=?", (user["username"],)
        ).fetchone()
    )
    return user


def _scope(conn, actor):
    valid = conn.execute(
        "SELECT 1 FROM users WHERE username=? AND status='active' AND must_change_password=0", (actor,)
    ).fetchone()
    if not valid:
        return set()
    if conn.execute("SELECT 1 FROM employee_platform_admins WHERE username=?", (actor,)).fetchone():
        return {row["id"] for row in conn.execute("SELECT id FROM employee_organizations")}
    return {m["id"] for m in _memberships(conn, actor) if "admin" in m["roles"]}


def _all_in_scope(conn, actor, username):
    if not _may_edit_platform_admin(conn, actor, username):
        return False
    memberships = {m["id"] for m in _memberships(conn, username)}
    return bool(memberships and memberships <= _scope(conn, actor))


def _may_edit_platform_admin(conn, actor, target):
    def platform(name):
        return conn.execute("SELECT 1 FROM employee_platform_admins WHERE username=?", (name,)).fetchone()

    return not platform(target) or bool(platform(actor))


def _binding(conn, provider, sender):
    if provider == "telegram":
        return conn.execute(
            "SELECT id,username,status FROM users WHERE telegram_chat_id=?", (sender,)
        ).fetchone()
    return conn.execute(
        """SELECT u.id,u.username,u.status FROM users u JOIN max_bindings b ON b.user_id=u.id
                           WHERE b.max_user_id=?""",
        (sender,),
    ).fetchone()


def _attach(conn, provider, sender, user):
    if provider == "telegram":
        conn.execute(
            "UPDATE users SET telegram_chat_id=?,telegram_linked_at=? WHERE id=?",
            (sender, _now(), user["id"]),
        )
    else:
        conn.execute(
            "INSERT INTO max_bindings(user_id,max_user_id,linked_at) VALUES(?,?,?)",
            (user["id"], sender, _now()),
        )


def issue_activation(conn, username, actor, *, ttl_hours=72, provider="", sender=""):
    now = datetime.now(timezone.utc)
    expires = (now + timedelta(hours=ttl_hours)).isoformat()
    token = secrets.token_urlsafe(32)
    conn.execute(
        """INSERT INTO account_activations
        (username,token_hash,created_by,created_at,expires_at,bound_provider,bound_sender_id)
        VALUES(?,?,?,?,?,?,?) ON CONFLICT(username) DO UPDATE SET token_hash=excluded.token_hash,
        created_by=excluded.created_by,created_at=excluded.created_at,expires_at=excluded.expires_at,
        bound_provider=excluded.bound_provider,bound_sender_id=excluded.bound_sender_id,used_at=NULL""",
        (username, token_hash(token), actor, now.isoformat(), expires, provider, sender),
    )
    return {"token": token, "username": username, "expires_at": expires}


def project_roles(conn, username):
    rows = _memberships(conn, username)
    roles = {role for row in rows for role in row["roles"]} - {"admin"}
    platform = conn.execute("SELECT 1 FROM employee_platform_admins WHERE username=?", (username,)).fetchone()
    if platform:
        roles.add("admin")
    if not roles and rows:
        roles.add("driver")
    conn.execute(
        "UPDATE users SET role=?,roles_json=? WHERE username=?",
        (primary_role(roles) if roles else "driver", json.dumps(sorted(roles)), username),
    )


def assign_new_membership(conn, username, organization_id, roles):
    # Old insert paths get a default membership via the trigger; replace it within this transaction.
    conn.execute("DELETE FROM employee_memberships WHERE username=?", (username,))
    conn.execute(
        "INSERT INTO employee_memberships(username,organization_id,roles_json) VALUES(?,?,?)",
        (username, organization_id, json.dumps(roles)),
    )
    project_roles(conn, username)


def create_pending(conn, requested, name, org, roles):
    username = str(requested or "").strip().lower()
    if username and not re.fullmatch(r"[a-z][a-z0-9._-]{2,63}", username):
        return {"ok": False, "reason": "invalid_username"}
    if not username:
        username = "staff_" + secrets.token_hex(8)
    if conn.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone():
        return {"ok": False, "reason": "already_exists"}
    conn.execute(
        """INSERT INTO users(username,display_name,password_hash,role,roles_json,status,
                 must_change_password,created_at) VALUES(?,?,'','driver','["driver"]','pending',1,?)""",
        (username, str(name or username).strip()[:200], _now()),
    )
    assign_new_membership(conn, username, org, roles)
    return {
        "ok": True,
        **dict(conn.execute("SELECT id,username FROM users WHERE username=?", (username,)).fetchone()),
    }


def migrate_employee_onboarding(conn):
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    initialized = conn.execute("SELECT 1 FROM sqlite_master WHERE name='employee_memberships'").fetchone()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS employee_organizations(id TEXT PRIMARY KEY,name TEXT NOT NULL UNIQUE)"
    )
    conn.execute("INSERT OR IGNORE INTO employee_organizations VALUES('default','Основная организация')")
    conn.execute("""CREATE TABLE IF NOT EXISTS employee_memberships(username TEXT NOT NULL, organization_id TEXT NOT NULL,
                 roles_json TEXT NOT NULL, PRIMARY KEY(username,organization_id))""")
    conn.execute("CREATE TABLE IF NOT EXISTS employee_platform_admins(username TEXT PRIMARY KEY)")
    if not initialized:
        for row in conn.execute("SELECT username,role,roles_json,status FROM users").fetchall():
            roles = sorted(user_roles(dict(row)))
            conn.execute(
                "INSERT INTO employee_memberships VALUES(?,'default',?)", (row["username"], json.dumps(roles))
            )
            if "admin" in roles:
                conn.execute("INSERT INTO employee_platform_admins VALUES(?)", (row["username"],))
    conn.execute("""CREATE TRIGGER IF NOT EXISTS employee_membership_after_insert AFTER INSERT ON users BEGIN
                 INSERT INTO employee_memberships VALUES(NEW.username,'default',
                 CASE WHEN NEW.roles_json!='' THEN NEW.roles_json ELSE json_array(NEW.role) END); END""")
    for table, columns in {
        "users": {"telegram_linked_at": "''"},
        "registration_requests": {
            "provider": "'web'",
            "sender_id": "''",
            "organization_id": "'default'",
            "phone": "''",
        },
        "account_activations": {"bound_provider": "''", "bound_sender_id": "''"},
        "telegram_tokens": {"verifier_hash": "''", "source_session": "''"},
    }.items():
        existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        for name, default in columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} TEXT NOT NULL DEFAULT {default}")
    if not initialized:
        conn.execute("""UPDATE registration_requests SET provider='telegram',sender_id=telegram_chat_id
                        WHERE source IN ('telegram','telegram_link') AND telegram_chat_id<>''""")
        # No account can securely claim a shared historical binding. Both owners may re-link from their session.
        for row in conn.execute(
            "SELECT id,telegram_chat_id FROM users WHERE telegram_chat_id<>''"
        ).fetchall():
            if not normalize_sender_id(row["telegram_chat_id"]):
                conn.execute(
                    "UPDATE users SET telegram_chat_id='',telegram_username='' WHERE id=?", (row["id"],)
                )
        conn.execute("UPDATE verification_tokens SET status='expired' WHERE status='pending'")
        conn.execute(
            "UPDATE password_reset_tokens SET status='expired' WHERE status IN ('pending','confirmed')"
        )
        ambiguous = [
            r[0]
            for r in conn.execute("""SELECT telegram_chat_id FROM users WHERE telegram_chat_id<>''
                                                   GROUP BY telegram_chat_id HAVING count(*)>1""")
        ]
        for chat in ambiguous:
            conn.execute(
                "UPDATE users SET telegram_chat_id='',telegram_username='' WHERE telegram_chat_id=?", (chat,)
            )
        conn.execute("UPDATE telegram_tokens SET status='expired' WHERE status IN ('pending','confirmed')")
        conn.execute("UPDATE telegram_tokens SET payload_json='{}' WHERE purpose='invite'")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS users_tg_identity ON users(telegram_chat_id) WHERE telegram_chat_id<>''"
    )
    conn.execute(
        "UPDATE users SET telegram_linked_at=? WHERE telegram_chat_id<>'' AND telegram_linked_at=''",
        (_now(),),
    )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS employee_request_sender ON registration_requests(provider,sender_id,id)"""
    )
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS telegram_login_verifier ON telegram_tokens(verifier_hash)
                    WHERE verifier_hash<>''""")
    conn.execute("""CREATE TABLE IF NOT EXISTS employee_invites(
                 username TEXT PRIMARY KEY, organization_id TEXT NOT NULL, provider TEXT NOT NULL,
                 expected_sender_id TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE, expires_at TEXT NOT NULL,
                 created_by TEXT NOT NULL, request_key TEXT NOT NULL, claimed_sender_id TEXT NOT NULL DEFAULT '')""")
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS employee_invite_event ON employee_invites(provider,created_by,request_key)
                    WHERE request_key<>''""")


class EmployeeOnboarding:
    def list_employee_organizations(self, *, admin_username=None):
        with self._connect() as conn:
            scope = _scope(conn, admin_username) if admin_username is not None else None
            return [
                dict(row)
                for row in conn.execute("SELECT id,name FROM employee_organizations ORDER BY name,id")
                if scope is None or row["id"] in scope
            ]

    def create_employee_organization(self, *, admin_username, name, organization_id=""):
        name = str(name or "").strip()
        org = organization_id or "org_" + secrets.token_hex(8)
        if not name or len(name) > 120 or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", org):
            return {"ok": False, "reason": "invalid_organization"}
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            actor = conn.execute(
                """SELECT 1 FROM employee_platform_admins a JOIN users u ON u.username=a.username
                                   WHERE a.username=? AND u.status='active' AND u.must_change_password=0""",
                (admin_username,),
            ).fetchone()
            if not actor:
                return {"ok": False, "reason": "not_admin"}
            if not conn.execute(
                "INSERT OR IGNORE INTO employee_organizations VALUES(?,?)", (org, name)
            ).rowcount:
                return {"ok": False, "reason": "already_exists"}
            conn.execute("INSERT INTO employee_memberships VALUES(?,?,?)", (admin_username, org, '["admin"]'))
            _audit(conn, admin_username, "organization_created", organization_id=org)
            return {"ok": True, "id": org, "name": name}

    def can_manage_employee(self, *, admin_username, username):
        with self._connect() as conn:
            return _all_in_scope(conn, admin_username, username)

    def list_managed_employees(self, *, admin_username):
        with self._connect() as conn:
            conn.execute("BEGIN")
            scope = _scope(conn, admin_username)
            if not scope:
                return []
            rows = conn.execute("""SELECT u.username,u.display_name,u.status,u.telegram_chat_id,b.max_user_id
                FROM users u LEFT JOIN max_bindings b ON b.user_id=u.id ORDER BY u.username""")
            employees = []
            for row in rows:
                memberships = [m for m in _memberships(conn, row["username"]) if m["id"] in scope]
                if memberships:
                    employees.append(
                        {
                            **dict(row),
                            "organization_ids": [m["id"] for m in memberships],
                            "organization_roles": {m["id"]: m["roles"] for m in memberships},
                            "organizations": memberships,
                        }
                    )
            return employees

    def set_employee_org_roles(self, *, admin_username, username, organization_id, roles):
        values = normalize_roles(roles) if roles else []
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            scope = _scope(conn, admin_username)
            target_orgs = {m["id"] for m in _memberships(conn, username)}
            if (
                organization_id not in scope
                or not target_orgs.intersection(scope)
                or not _may_edit_platform_admin(conn, admin_username, username)
            ):
                return {"ok": False, "reason": "not_admin"}
            if not values and target_orgs == {organization_id}:
                return {"ok": False, "reason": "last_organization"}
            if username == admin_username and "admin" not in values:
                return {"ok": False, "reason": "self_admin"}
            if values:
                conn.execute(
                    """INSERT INTO employee_memberships VALUES(?,?,?) ON CONFLICT(username,organization_id)
                                DO UPDATE SET roles_json=excluded.roles_json""",
                    (username, organization_id, json.dumps(values)),
                )
            else:
                conn.execute(
                    "DELETE FROM employee_memberships WHERE username=? AND organization_id=?",
                    (username, organization_id),
                )
            project_roles(conn, username)
            _audit(
                conn,
                admin_username,
                "employee_org_roles",
                target=username,
                organization_id=organization_id,
                roles=values,
            )
            return {"ok": True}

    def submit_employee_request(
        self, *, provider, sender_id, display_name, phone="", organization_id=DEFAULT_ORG, username=""
    ):
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self._submit_request(
                conn, provider, sender_id, display_name, phone, organization_id, username
            )

    def _submit_request(self, conn, provider, sender, display_name, phone, org, username):
        sender = normalize_sender_id(sender) if provider in PROVIDERS else ""
        name = str(display_name or "").strip()
        username = str(username or "").strip().lower()
        if (
            provider not in (PROVIDERS | {"web"})
            or (provider != "web" and not sender)
            or not name
            or len(name) > 200
        ):
            return {"ok": False, "reason": "invalid_request"}
        if username and not re.fullmatch(r"[a-z][a-z0-9._-]{2,63}", username):
            return {"ok": False, "reason": "invalid_username"}
        if provider == "web" and not username:
            return {"ok": False, "reason": "missing_username"}
        if not conn.execute("SELECT 1 FROM employee_organizations WHERE id=?", (org,)).fetchone():
            return {"ok": False, "reason": "invalid_organization"}
        if sender:
            if _binding(conn, provider, sender):
                return {"ok": False, "reason": "already_linked"}
            previous = conn.execute(
                """SELECT id,status,organization_id FROM registration_requests WHERE
                provider=? AND sender_id=? AND status='pending' ORDER BY id DESC LIMIT 1""",
                (provider, sender),
            ).fetchone()
        else:
            previous = conn.execute(
                """SELECT id,status,organization_id FROM registration_requests WHERE
                provider='web' AND username=? AND status='pending' ORDER BY id DESC LIMIT 1""",
                (username,),
            ).fetchone()
        if previous:
            return {"ok": True, "created": False, **dict(previous)}
        if username and conn.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone():
            return {"ok": False, "reason": "already_exists"}
        row = conn.execute(
            """INSERT INTO registration_requests
            (username,display_name,phone,organization_id,provider,sender_id,telegram_chat_id,source,status,requested_at)
            VALUES(?,?,?,?,?,?,?,?,'pending',?)""",
            (
                username,
                name,
                str(phone or "").strip()[:64],
                org,
                provider,
                sender,
                sender if provider == "telegram" else "",
                provider,
                _now(),
            ),
        )
        return {"ok": True, "created": True, "id": row.lastrowid, "status": "pending", "organization_id": org}

    def list_employee_requests(self, *, admin_username, status="pending"):
        with self._connect() as conn:
            scope = _scope(conn, admin_username)
            return [
                dict(r)
                for r in conn.execute(
                    """SELECT r.*,o.name AS organization_name FROM registration_requests r
                JOIN employee_organizations o ON r.organization_id=o.id WHERE (?='all' OR r.status=?) ORDER BY r.id DESC""",
                    (status, status),
                )
                if r["organization_id"] in scope
            ]

    def employee_request_status(self, *, provider, sender_id):
        sender = normalize_sender_id(sender_id)
        if provider not in PROVIDERS or not sender:
            return {"ok": False, "reason": "invalid_sender"}
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT id,status,username,organization_id FROM registration_requests
                WHERE provider=? AND sender_id=? ORDER BY id DESC LIMIT 1""",
                (provider, sender),
            ).fetchone()
            user = _binding(conn, provider, sender)
            if not user:
                return {"ok": True, **(dict(row) if row else {"status": "none"})}
            if user["status"] != "pending":
                return {
                    "ok": True,
                    "status": "linked" if user["status"] == "active" else "blocked",
                    "username": user["username"],
                }
            existing = conn.execute(
                """SELECT created_by FROM account_activations WHERE username=? AND bound_provider=?
                    AND bound_sender_id=? AND used_at IS NULL""",
                (user["username"], provider, sender),
            ).fetchone()
            if not existing:
                return {"ok": True, "status": "pending"}
            activation = issue_activation(
                conn, user["username"], existing["created_by"], provider=provider, sender=sender
            )
            return {
                "ok": True,
                **(dict(row) if row else {}),
                "status": "approved",
                "username": user["username"],
                "activation_token": activation["token"],
            }

    def refer_employee_request(self, *, admin_username, request_id, username):
        """Close the request and direct the requester to prove ownership in the existing cabinet."""
        target = str(username or "").strip().lower()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM registration_requests WHERE id=?", (request_id,)).fetchone()
            if not row or row["organization_id"] not in _scope(conn, admin_username):
                return {"ok": False, "reason": "not_admin"}
            if row["status"] != "pending":
                return {"ok": False, "reason": "already_reviewed"}
            member = conn.execute(
                """SELECT 1 FROM users u JOIN employee_memberships m ON m.username=u.username
                   WHERE u.username=? AND u.status='active' AND m.organization_id=?""",
                (target, row["organization_id"]),
            ).fetchone()
            if not member:
                return {"ok": False, "reason": "not_found"}
            conn.execute(
                """UPDATE registration_requests SET status='use_existing',reviewed_at=?,reviewed_by=?
                   WHERE id=?""",
                (_now(), admin_username, request_id),
            )
            _audit(conn, admin_username, "employee_request_referred", request_id=request_id, target=target)
            return {
                "ok": True,
                "status": "use_existing",
                "provider": row["provider"],
                "sender_id": row["sender_id"],
            }

    def review_employee_request(self, *, admin_username, request_id, approve, roles=None):
        values = normalize_roles(["driver"] if roles is None else roles) if approve else []
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM registration_requests WHERE id=?", (request_id,)).fetchone()
            if row is None or row["organization_id"] not in _scope(conn, admin_username):
                return {"ok": False, "reason": "not_admin"}
            if row["status"] != "pending":
                return {"ok": False, "reason": "already_reviewed"}
            status = "approved" if approve else "rejected"
            result = {
                "ok": True,
                "id": row["id"],
                "status": status,
                "provider": row["provider"],
                "sender_id": row["sender_id"],
            }
            username = row["username"]
            if approve:
                provider, sender = row["provider"], row["sender_id"]
                if provider in PROVIDERS and (
                    not normalize_sender_id(sender) or _binding(conn, provider, sender)
                ):
                    return {"ok": False, "reason": "already_linked"}
                user = create_pending(conn, username, row["display_name"], row["organization_id"], values)
                if not user["ok"]:
                    return user
                username = user["username"]
                if provider in PROVIDERS:
                    _attach(conn, provider, sender, user)
                activation = issue_activation(
                    conn, username, admin_username, provider=provider, sender=sender
                )
                result.update(username=username, activation_token=activation["token"])
            conn.execute(
                "UPDATE registration_requests SET username=?,status=?,reviewed_at=?,reviewed_by=? WHERE id=?",
                (username, status, _now(), admin_username, request_id),
            )
            _audit(
                conn, admin_username, "employee_request_" + status, request_id=request_id, username=username
            )
            return result

    def create_employee_invite(
        self,
        *,
        admin_username,
        username,
        provider,
        display_name="",
        expected_sender_id="",
        organization_id=DEFAULT_ORG,
        roles=None,
        renew_pending=False,
        request_key="",
    ):
        if provider not in PROVIDERS or (expected_sender_id and not normalize_sender_id(expected_sender_id)):
            return {"ok": False, "reason": "invalid_sender"}
        username, expected = str(username or "").lower().strip(), normalize_sender_id(expected_sender_id)
        values = normalize_roles(["driver"] if roles is None else roles)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            scope = _scope(conn, admin_username)
            if organization_id not in scope:
                return {"ok": False, "reason": "not_admin"}
            existing = None
            if request_key:
                existing = conn.execute(
                    """SELECT * FROM employee_invites WHERE provider=? AND created_by=?
                                          AND request_key=?""",
                    (provider, admin_username, request_key),
                ).fetchone()
            if renew_pending and not existing:
                existing = conn.execute(
                    "SELECT * FROM employee_invites WHERE username=? AND provider=?", (username, provider)
                ).fetchone()
                if not existing:
                    return {"ok": False, "reason": "not_pending"}
            if existing:
                if organization_id != existing["organization_id"]:
                    return {"ok": False, "reason": "wrong_organization"}
                if not _all_in_scope(conn, admin_username, existing["username"]):
                    return {"ok": False, "reason": "not_admin"}
                username, organization_id, expected = (
                    existing["username"],
                    existing["organization_id"],
                    existing["expected_sender_id"],
                )
                if existing["claimed_sender_id"]:
                    return {"ok": False, "reason": "already_claimed"}
            if expected and _binding(conn, provider, expected):
                return {"ok": False, "reason": "already_linked"}
            if not existing:
                user = create_pending(conn, username, display_name, organization_id, values)
                if not user["ok"]:
                    return user
                username = user["username"]
            elif not conn.execute(
                """SELECT 1 FROM users WHERE username=? AND status='pending' AND password_hash=''""",
                (username,),
            ).fetchone():
                return {"ok": False, "reason": "not_pending"}
            token = secrets.token_urlsafe(32)
            expires = (datetime.now(timezone.utc) + timedelta(hours=72)).isoformat()
            conn.execute(
                """INSERT INTO employee_invites(username,organization_id,provider,expected_sender_id,
                            token_hash,expires_at,created_by,request_key) VALUES(?,?,?,?,?,?,?,?)
                            ON CONFLICT(username) DO UPDATE SET token_hash=excluded.token_hash,expires_at=excluded.expires_at""",
                (
                    username,
                    organization_id,
                    provider,
                    expected,
                    token_hash(token),
                    expires,
                    admin_username,
                    request_key,
                ),
            )
            _audit(conn, admin_username, "employee_invited", target=username, organization_id=organization_id)
            return {"ok": True, "username": username, "token": token, "expires_at": expires}

    def claim_employee_invite(self, *, provider, sender_id, token):
        sender = normalize_sender_id(sender_id)
        if provider not in PROVIDERS or not sender:
            return {"ok": False, "reason": "invalid_sender"}
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT i.*,u.id FROM employee_invites i JOIN users u ON u.username=i.username
                WHERE i.token_hash=? AND i.provider=? AND i.expires_at>? AND u.status='pending'
                AND u.must_change_password=1 AND u.password_hash=''""",
                (token_hash(token), provider, _now()),
            ).fetchone()
            if not row or (row["expected_sender_id"] and row["expected_sender_id"] != sender):
                return {"ok": False, "reason": "invalid_invite"}
            if row["claimed_sender_id"]:
                return {"ok": False, "reason": "already_claimed"}
            if _binding(conn, provider, sender):
                return {"ok": False, "reason": "already_linked"}
            _attach(conn, provider, sender, row)
            conn.execute(
                "UPDATE employee_invites SET claimed_sender_id=? WHERE username=?", (sender, row["username"])
            )
            activation = issue_activation(
                conn, row["username"], row["created_by"], provider=provider, sender=sender
            )
            return {"ok": True, "username": row["username"], "activation_token": activation["token"]}
