"""Work shift journal, isolated from catalog storage and authenticated per operation."""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .roles import can_manage_shifts, can_use_shifts, user_roles
from .sqlite_runtime import prepare_sqlite_connection

STATUS_LABELS = {"draft": "Черновик", "submitted": "На проверке", "approved": "Подтверждено"}
TEXT_FIELDS = ("organization", "equipment", "workplace", "partner", "waybill_number", "comment")
REFERENCE_LABELS = {"organization": "Организации", "equipment": "Техника", "workplace": "Объекты", "partner": "Контрагенты"}
AUDIT_FIELDS = ("employee", "employee_name", "work_date", "shift_number", *TEXT_FIELDS,
                "work_minutes", "break_minutes", "status", "deleted")


def _org_roles(user):
    return user.get("organization_roles", {"default": user_roles(user)})


def _manager(user, organization):
    return bool(user.get("is_platform_admin") or
                {"admin", "dispatcher"}.intersection(_org_roles(user).get(organization, ())))


def _shift_access(user, organization):
    return _manager(user, organization) or "driver" in _org_roles(user).get(organization, ())


def _scope_sql(user, *, management=False):
    if user.get("is_platform_admin"):
        return "1", []
    roles = _org_roles(user)
    managed = [key for key in roles if _manager(user, key)]
    condition = "organization_id IN (SELECT value FROM json_each(?))"
    params = [json.dumps(managed)]
    if not management:
        condition += " OR (employee=? AND organization_id IN (SELECT value FROM json_each(?)))"
        params.extend([user["username"], json.dumps([key for key in roles if _shift_access(user, key)])])
    return "(" + condition + ")", params


def _day(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError
        return value
    except (ValueError, TypeError):
        raise ValueError("Укажите дату в формате ГГГГ-ММ-ДД") from None


def _minutes(value: Any) -> int:
    try:
        hours = Decimal(str(value))
        minutes = hours * 60
        if not hours.is_finite() or hours < 0 or hours > 24 or minutes != minutes.to_integral_value():
            raise ValueError
        return int(minutes)
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("Часы должны быть от 0 до 24 с точностью до минуты") from None


def _payload(data: dict) -> dict:
    result = {key: str(data.get(key) or "").strip() for key in TEXT_FIELDS}
    for key, value in result.items():
        if len(value) > (2000 if key == "comment" else 200):
            raise ValueError("Слишком длинное значение поля")
    if not all(result[key] for key in ("organization", "equipment", "workplace")):
        raise ValueError("Заполните организацию, технику и объект")
    result["work_date"] = _day(data.get("work_date", ""))
    shift = data.get("shift_number")
    if isinstance(shift, bool) or shift not in (1, 2, 3):
        raise ValueError("Номер смены должен быть 1, 2 или 3")
    result["shift_number"] = int(shift)
    result["work_minutes"] = _minutes(data.get("hours", 0))
    result["break_minutes"] = _minutes(data.get("breaks", 0))
    if result["work_minutes"] + result["break_minutes"] > 1440:
        raise ValueError("Работа и перерывы вместе не могут превышать 24 часа")
    result["equipment_key"] = result["equipment"].casefold()
    return result


class WorkShiftJournal:
    def __init__(self, path: str | Path, auth_db: Any):
        self.path = Path(path)
        self.auth = auth_db
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as conn:
            seed_references = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='shift_references'").fetchone() is None
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS shifts (
                    id INTEGER PRIMARY KEY,
                    employee TEXT NOT NULL,
                    employee_name TEXT NOT NULL,
                    work_date TEXT NOT NULL,
                    shift_number INTEGER NOT NULL CHECK(shift_number BETWEEN 1 AND 3),
                    organization TEXT NOT NULL,
                    equipment TEXT NOT NULL,
                    equipment_key TEXT NOT NULL,
                    workplace TEXT NOT NULL,
                    partner TEXT NOT NULL,
                    waybill_number TEXT NOT NULL,
                    work_minutes INTEGER NOT NULL CHECK(work_minutes BETWEEN 0 AND 1440),
                    break_minutes INTEGER NOT NULL CHECK(break_minutes BETWEEN 0 AND 1440),
                    comment TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','submitted','approved')),
                    revision INTEGER NOT NULL DEFAULT 1,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    CHECK(work_minutes + break_minutes <= 1440)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS shifts_unique_active
                    ON shifts(employee, work_date, shift_number, equipment_key) WHERE deleted=0;
                CREATE INDEX IF NOT EXISTS shifts_date ON shifts(work_date, id);
                CREATE TABLE IF NOT EXISTS shift_events (
                    id INTEGER PRIMARY KEY,
                    shift_id INTEGER NOT NULL REFERENCES shifts(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    snapshot TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS shift_events_record ON shift_events(shift_id, id);
                CREATE TABLE IF NOT EXISTS shift_create_requests (
                    source_event_id TEXT PRIMARY KEY,
                    actor TEXT NOT NULL,
                    shift_id INTEGER NOT NULL REFERENCES shifts(id)
                );
                CREATE TABLE IF NOT EXISTS shift_references (
                    id INTEGER PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL,
                    name_key TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(kind, name_key)
                );
            """)
            conn.execute("BEGIN IMMEDIATE")
            if "organization_id" not in {r["name"] for r in conn.execute("PRAGMA table_info(shifts)")}:
                conn.execute("ALTER TABLE shifts ADD COLUMN organization_id TEXT NOT NULL DEFAULT 'default'")
            if "organization_id" not in {r["name"] for r in conn.execute("PRAGMA table_info(shift_references)")}:
                conn.execute("ALTER TABLE shift_references RENAME TO legacy_shift_references")
                conn.execute("""CREATE TABLE shift_references (
                    id INTEGER PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL, name_key TEXT NOT NULL,
                    archived INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 1,
                    organization_id TEXT NOT NULL DEFAULT 'default', UNIQUE(organization_id, kind, name_key))""")
                conn.execute("""INSERT INTO shift_references(id,kind,name,name_key,archived,revision)
                    SELECT id,kind,name,name_key,archived,revision FROM legacy_shift_references""")
                conn.execute("DROP TABLE legacy_shift_references")
            for kind in (REFERENCE_LABELS if seed_references else ()):
                for org, value in conn.execute(f"SELECT DISTINCT organization_id,{kind} FROM shifts WHERE {kind}<>''"):
                    conn.execute("INSERT OR IGNORE INTO shift_references(organization_id,kind,name,name_key) VALUES(?,?,?,?)",
                                 (org, kind, value, value.casefold()))

    @contextmanager
    def _connection(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            prepare_sqlite_connection(conn)
            conn.execute("PRAGMA foreign_keys=ON")
            with conn:
                yield conn
        finally:
            conn.close()

    def _user(self, token: str) -> dict:
        user = self.auth.get_user_by_session(token)
        if not user or user.get("status") != "active" or user.get("must_change_password"):
            raise PermissionError("Сессия истекла или требуется сменить пароль. Войдите заново.")
        if not can_use_shifts(user):
            raise PermissionError("Нужна роль Водитель или Диспетчер")
        return user

    def users(self, token: str, *, organization_id: str = "") -> dict[str, str]:
        user = self._user(token)
        if organization_id and (
            not _shift_access(user, organization_id) or organization_id not in {row["id"] for row in self._directory(user)}
        ):
            raise PermissionError("Организация недоступна")
        users = self.auth.list_users() if can_manage_shifts(user) else [user]
        result = {}
        for employee in users:
            if employee["status"] != "active":
                continue
            memberships = _org_roles(employee)
            if organization_id and organization_id not in memberships and not (
                employee["username"] == user["username"] and user.get("is_platform_admin")
            ):
                continue
            choices = [organization_id] if organization_id else memberships
            if employee["username"] == user["username"] or any(_manager(user, key) for key in choices):
                result[employee["username"]] = employee.get("display_name") or employee["username"]
        return result

    def _directory(self, user):
        if hasattr(self.auth, "list_employee_organizations"):
            return self.auth.list_employee_organizations()
        return user.get("organizations", [{"id": "default", "name": "Основная организация"}])

    def organization_for(self, user, name, org_id="", old=None):
        # Older journals used arbitrary labels. Preserve the stored scope when editing
        # an unchanged label, even after a new organization adopts the same name.
        if old and name == old["organization"]:
            inferred = old["organization_id"]
        else:
            matches = {row["id"] for row in self._directory(user) if row["name"].casefold() == name.casefold()}
            if len(matches) > 1 and org_id not in matches:
                raise ValueError("Укажите организацию из справочника")
            inferred = org_id if org_id in matches else next(iter(matches), "default")
        if org_id and org_id != inferred:
            raise PermissionError("Выберите название организации из списка")
        if old and old["organization_id"] != inferred:
            raise PermissionError("Смена относится к другой организации")
        if not _shift_access(user, inferred):
            raise PermissionError("Организация недоступна")
        return inferred

    def organizations(self, token, *, manage=False):
        user = self._user(token)
        rows = self._directory(user)
        permitted = _manager if manage else _shift_access
        return {row["id"]: row["name"] for row in rows if permitted(user, row["id"])}

    @staticmethod
    def _row(conn, user, shift_id, revision=None) -> dict:
        row = conn.execute("SELECT * FROM shifts WHERE id=? AND deleted=0", (shift_id,)).fetchone()
        if row is None or not _shift_access(user, row["organization_id"]) or (
                not _manager(user, row["organization_id"]) and row["employee"] != user["username"]):
            raise PermissionError("Смена недоступна")
        if revision is not None and revision != row["revision"]:
            raise ValueError("Смена уже изменена. Обновите журнал и откройте её заново.")
        return dict(row)

    def get(self, token: str, shift_id: int) -> dict:
        user = self._user(token)
        with self._connection() as conn:
            return self._row(conn, user, shift_id)

    @staticmethod
    def _event(conn, shift_id, user, action, reason=""):
        row = dict(conn.execute("SELECT * FROM shifts WHERE id=?", (shift_id,)).fetchone())
        conn.execute(
            "INSERT INTO shift_events(shift_id,actor,action,reason,snapshot,created_at) VALUES(?,?,?,?,?,?)",
            (shift_id, user["username"], action, reason, json.dumps(row, ensure_ascii=False), row["updated_at"]),
        )
        return row

    def save(self, token: str, data: dict, *, shift_id: int | None = None, revision: int | None = None,
             source_event_id: str = "") -> dict:
        user = self._user(token)
        if source_event_id and (shift_id is not None or len(source_event_id) > 256):
            raise ValueError("Некорректный идентификатор запроса создания смены")
        values = _payload(data)
        employee = str(data.get("employee") or user["username"]).strip().lower()
        try:
            with self._connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if source_event_id:
                    previous = conn.execute(
                        "SELECT actor, shift_id FROM shift_create_requests WHERE source_event_id=?", (source_event_id,)
                    ).fetchone()
                    if previous:
                        if previous["actor"] != user["username"]:
                            raise PermissionError("Смена недоступна")
                        return self._row(conn, user, previous["shift_id"])
                old = {}
                if shift_id is not None:
                    if revision is None:
                        raise ValueError("Не указана версия смены")
                    old = self._row(conn, user, shift_id, revision)
                org = self.organization_for(user, values["organization"], data.get("organization_id", ""), old)
                manager = _manager(user, org)
                if not manager and employee != user["username"]:
                    raise PermissionError("Нельзя создавать смены другого сотрудника")
                target = self.auth.get_user(username=employee)
                platform_self = employee == user["username"] and user.get("is_platform_admin")
                if not target or (org not in _org_roles(target) and not platform_self):
                    raise ValueError("Сотрудник недоступен в организации")
                if target.get("status") != "active" and not (manager and old.get("employee") == employee):
                    raise ValueError("Нельзя назначить смену отключённому сотруднику")
                values.update(organization_id=org, employee=employee, employee_name=target.get("display_name") or employee,
                              updated_at=datetime.now(timezone.utc).isoformat(), status="draft" if manager else "submitted")
                if manager:
                    for kind in REFERENCE_LABELS:
                        if values[kind] and values[kind] != old.get(kind):
                            conn.execute("INSERT OR IGNORE INTO shift_references(organization_id,kind,name,name_key) VALUES(?,?,?,?)",
                                         (org, kind, values[kind], values[kind].casefold()))
                if shift_id is None:
                    values["created_at"] = values["updated_at"]
                    fields = ",".join(values)
                    placeholders = ",".join("?" for _ in values)
                    shift_id = conn.execute(
                        f"INSERT INTO shifts({fields}) VALUES({placeholders})", tuple(values.values()),
                    ).lastrowid
                    if source_event_id:
                        conn.execute("INSERT INTO shift_create_requests(source_event_id, actor, shift_id) VALUES(?,?,?)",
                                     (source_event_id, user["username"], shift_id))
                    action = "create"
                else:
                    if old["status"] != "draft":
                        values["status"] = "submitted"
                    setters = ",".join(f"{key}=?" for key in values)
                    conn.execute(
                        f"UPDATE shifts SET {setters}, revision=revision+1 WHERE id=?",
                        (*values.values(), shift_id),
                    )
                    action = "edit"
                return self._event(conn, shift_id, user, action)
        except sqlite3.IntegrityError:
            raise ValueError("Для сотрудника, даты, номера смены и техники уже существует запись") from None

    def transition(self, token: str, shift_id: int, revision: int, status: str, reason: str = "") -> dict:
        user = self._user(token)
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise ValueError("Не указана версия смены")
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._row(conn, user, shift_id, revision)
            edge = (row["status"], status)
            allowed = set()
            if _manager(user, row["organization_id"]):
                allowed = {(before, after) for before in STATUS_LABELS for after in STATUS_LABELS if before != after}
            if edge not in allowed:
                raise PermissionError("Этот переход статуса недоступен")
            if status in {"submitted", "approved"} and row["work_minutes"] == 0:
                raise ValueError("Перед отправкой на проверку укажите отработанные часы")
            reason = str(reason).strip()
            if len(reason) > 2000 or (row["status"] == "approved" and not reason):
                raise ValueError("Укажите причину исправления (до 2000 символов)")
            conn.execute(
                "UPDATE shifts SET status=?, revision=revision+1, updated_at=? WHERE id=?",
                (status, datetime.now(timezone.utc).isoformat(), shift_id),
            )
            return self._event(conn, shift_id, user, status, reason)

    def delete(self, token: str, shift_id: int, revision: int) -> None:
        user = self._user(token)
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise ValueError("Не указана версия смены")
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._row(conn, user, shift_id, revision)
            if not _manager(user, row["organization_id"]):
                raise PermissionError("Удаление доступно диспетчеру")
            conn.execute(
                "UPDATE shifts SET deleted=1, revision=revision+1, updated_at=? WHERE id=?",
                (datetime.now(timezone.utc).isoformat(), shift_id),
            )
            self._event(conn, shift_id, user, "delete")

    def history(self, token: str, shift_id: int) -> list[dict]:
        user = self._user(token)
        with self._connection() as conn:
            row = conn.execute("SELECT employee, organization_id, deleted FROM shifts WHERE id=?", (shift_id,)).fetchone()
            if not row or not _manager(user, row["organization_id"]):
                self._row(conn, user, shift_id)
            events = [dict(row) for row in conn.execute(
                "SELECT actor,action,reason,snapshot,created_at FROM shift_events WHERE shift_id=? ORDER BY id",
                (shift_id,),
            )]
            previous = {}
            for event in events:
                snapshot = json.loads(event["snapshot"])
                event["changes"] = {key: [previous.get(key), snapshot.get(key)] for key in AUDIT_FIELDS
                                    if previous.get(key) != snapshot.get(key)}
                previous = snapshot
            return events

    def audit(self, token: str, *, offset: int = 0, limit: int = 50) -> dict:
        user = self._user(token)
        if not can_manage_shifts(user):
            raise PermissionError("История всех путевых доступна диспетчеру")
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("Некорректная страница истории")
        with self._connection() as conn:
            conn.execute("BEGIN")
            scope, params = _scope_sql(user, management=True)
            joined = "FROM shift_events e JOIN shifts s ON s.id=e.shift_id WHERE " + scope
            total = conn.execute("SELECT count(*) " + joined, params).fetchone()[0]
            rows = [dict(row) for row in conn.execute("""
                SELECT e.*, (SELECT p.snapshot FROM shift_events p
                  WHERE p.shift_id=e.shift_id AND p.id<e.id ORDER BY p.id DESC LIMIT 1) AS previous
            """ + joined + " ORDER BY e.id DESC LIMIT ? OFFSET ?", (*params, limit, offset))]
            for row in rows:
                before, after = json.loads(row.pop("previous") or "{}"), json.loads(row["snapshot"])
                row["changes"] = {key: [before.get(key), after.get(key)] for key in AUDIT_FIELDS
                                  if before.get(key) != after.get(key)}
            return {"count": total, "rows": rows}

    def references(self, token: str, *, archived: bool = False) -> list[dict]:
        user = self._user(token)
        if archived and not can_manage_shifts(user):
            raise PermissionError("Недостаточно прав")
        with self._connection() as conn:
            permitted = _manager if archived else _shift_access
            keys = [key for key in _org_roles(user) if permitted(user, key)]
            return [dict(row) for row in conn.execute("""SELECT * FROM shift_references
                WHERE (archived=0 OR ?) AND (? OR organization_id IN (SELECT value FROM json_each(?)))
                ORDER BY kind,name""", (int(archived), bool(user.get("is_platform_admin")), json.dumps(keys)))]

    def employees(self, token: str) -> list[dict]:
        user = self._user(token)
        if not can_manage_shifts(user):
            raise PermissionError("Сотрудниками управляет диспетчер")
        result = []
        for employee in self.auth.list_users():
            memberships = _org_roles(employee)
            managed = [key for key in memberships if _manager(user, key)]
            if not managed:
                continue
            editable = (employee["status"] != "pending" and user_roles(employee) == {"driver"}
                        and len(managed) == len(memberships)
                        and all(set(roles) == {"driver"} for roles in memberships.values()))
            result.append({"username": employee["username"], "display_name": employee.get("display_name") or employee["username"],
                           "status": employee["status"], "editable": editable, "organization_ids": managed})
        return result

    def save_employee(self, token: str, **data) -> None:
        self._user(token)
        self.auth.save_driver(token, **data)

    def save_reference(self, token: str, kind: str, name: str, *, record_id: int | None = None,
                       revision: int | None = None, archived: bool = False, organization_id: str = "default") -> None:
        if organization_id not in self.organizations(token, manage=True):
            raise PermissionError("Справочники изменяет диспетчер")
        name = str(name).strip()
        if kind not in REFERENCE_LABELS or not name or len(name) > 200:
            raise ValueError("Укажите тип и название (до 200 символов)")
        try:
            with self._connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if record_id is None:
                    conn.execute("INSERT INTO shift_references(organization_id,kind,name,name_key) VALUES(?,?,?,?)",
                                 (organization_id, kind, name, name.casefold()))
                else:
                    result = conn.execute("""UPDATE shift_references SET name=?,name_key=?,archived=?,revision=revision+1
                        WHERE id=? AND organization_id=? AND kind=? AND revision=?""",
                        (name, name.casefold(), int(archived), record_id, organization_id, kind, revision))
                    if not result.rowcount:
                        raise ValueError("Справочник изменён. Обновите список.")
        except sqlite3.IntegrityError:
            raise ValueError("Такое название уже есть, в том числе в архиве") from None

    def list(self, token: str, *, date_from: str, date_to: str, employee: str = "", status: str = "",
             offset: int = 0, limit: int = 50) -> dict:
        user = self._user(token)
        _day(date_from)
        _day(date_to)
        if date_from > date_to or offset < 0 or not 1 <= limit <= 10001 or status not in ("", *STATUS_LABELS):
            raise ValueError("Некорректные параметры журнала")
        conditions = ["deleted=0", "work_date>=?", "work_date<=?"]
        params = [date_from, date_to]
        scope, scope_params = _scope_sql(user)
        conditions.append(scope)
        params.extend(scope_params)
        if not can_manage_shifts(user):
            employee = user["username"]
        if employee:
            conditions.append("employee=?")
            params.append(employee)
        if status:
            conditions.append("status=?")
            params.append(status)
        where = " AND ".join(conditions)
        with self._connection() as conn:
            conn.execute("BEGIN")
            summary = dict(conn.execute(
                f"SELECT count(*) AS count, coalesce(sum(work_minutes),0) AS work_minutes, "
                f"coalesce(sum(break_minutes),0) AS break_minutes FROM shifts WHERE {where}", params,
            ).fetchone())
            summary["rows"] = [dict(row) for row in conn.execute(
                f"SELECT * FROM shifts WHERE {where} ORDER BY work_date DESC,shift_number,id DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            )]
            return summary

    def export_csv(self, token: str, **filters) -> bytes:
        data = self.list(token, **filters, limit=10001)
        if data["count"] > 10000:
            raise ValueError("В выгрузке больше 10 000 смен. Сократите период.")
        stream = io.StringIO(newline="")
        writer = csv.writer(stream, delimiter=";")
        fields = ("id", "work_date", "shift_number", "employee_name", "organization", "equipment", "workplace",
                  "partner", "waybill_number", "hours", "breaks", "status", "comment")
        writer.writerow(("ID", "Дата", "Смена", "Сотрудник", "Организация", "Техника", "Объект", "Контрагент",
                         "Путевой лист", "Отработано, ч", "Перерывы, ч", "Статус", "Комментарий"))
        for row in data["rows"]:
            row.update(hours=row["work_minutes"] / 60, breaks=row["break_minutes"] / 60,
                       status=STATUS_LABELS[row["status"]])
            # Quoting does not stop spreadsheet formula evaluation.
            values = [row[key] for key in fields]
            writer.writerow([
                "'" + value if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")) else value
                for value in values
            ])
        return stream.getvalue().encode("utf-8-sig")
