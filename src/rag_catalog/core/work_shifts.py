"""Work shift journal, isolated from catalog storage and authenticated per operation."""

from __future__ import annotations

import csv
import calendar
import io
import json
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from uuid import uuid4

from . import shift_operations as ops
from . import shift_payroll
from .roles import can_manage_shifts, can_use_shifts, user_roles
from .sqlite_runtime import prepare_sqlite_connection

STATUS_LABELS = {"draft": "Черновик", "submitted": "На проверке", "approved": "Подтверждено"}
TEXT_FIELDS = ("organization", "equipment", "workplace", "partner", "waybill_number", "comment")
REFERENCE_LABELS = {"organization": "Организации", "equipment": "Техника", "workplace": "Объекты", "partner": "Контрагенты"}
AUDIT_FIELDS = ("employee", "employee_name", "work_date", "shift_number", *TEXT_FIELDS,
                "work_minutes", "break_minutes", "status", "deleted", *ops.NEW_FIELDS)


def _journal_row(row):
    """The operational journal never exports the private pricing snapshot."""
    result = dict(row)
    if "work_lines" in result:
        result["work_lines"] = json.dumps(
            [{k: line[k] for k in ("rate_id", "work", "unit", "quantity")} for line in json.loads(result["work_lines"])],
            ensure_ascii=False,
        )
    return result


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
    result["entry_type"] = data.get("entry_type", "vehicle")
    if result["entry_type"] not in ("vehicle","attendance"):
        raise ValueError("Укажите вид записи")
    if result["entry_type"] == "attendance":
        if not result["organization"]:
            raise ValueError("Заполните организацию")
        if result["equipment"] or result["waybill_number"]:
            raise ValueError("Явку оформляют без техники и путевого листа")
    elif not all(result[key] for key in ("organization", "equipment", "workplace")):
        raise ValueError("Заполните организацию, технику и объект")
    result["work_date"] = _day(data.get("work_date", ""))
    shift = data.get("shift_number")
    if isinstance(shift, bool) or shift not in (1, 2, 3):
        raise ValueError("Номер смены должен быть 1, 2 или 3")
    result["shift_number"] = int(shift)
    result["work_minutes"] = _minutes(data.get("hours", 0))
    if result["entry_type"] == "attendance" and not result["work_minutes"]:
        raise ValueError("Укажите фактически отработанные часы")
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
            ref_columns = {r["name"] for r in conn.execute("PRAGMA table_info(shift_references)")}
            for column in ("equipment_name", "registration_number", "fleet_number", "equipment_category","licence_kind","licence_mark"):
                if column not in ref_columns:
                    conn.execute(f"ALTER TABLE shift_references ADD COLUMN {column} TEXT NOT NULL DEFAULT ''")
            conn.execute("""CREATE TABLE IF NOT EXISTS shift_staff (
                id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, name TEXT NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0)""")
            ops.migrate(conn)
            shift_payroll.migrate(conn)
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

    def users(self, token: str, *, organization_id: str = "", category: str = "") -> dict[str, str]:
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
        if can_manage_shifts(user):
            with self._connection() as conn:
                for person in conn.execute("SELECT * FROM shift_staff WHERE archived=0 AND (?='' OR organization_id=?)",
                                           (organization_id, organization_id)):
                    if _manager(user, person["organization_id"]):
                        result[person["id"]] = person["name"] + " (без входа)"
        if category:
            with self._connection() as conn:
                members = {r[0] for r in conn.execute("SELECT employee FROM shift_staff_categories WHERE organization_id=? AND category=?",
                                                      (organization_id,category))}
            result = {k:v for k,v in result.items() if k in members}
        return result

    def _employee_name(self, conn, user, org, employee, *, previous=""):
        manager = _manager(user, org)
        if employee.startswith("staff:"):
            person = conn.execute("SELECT name,archived FROM shift_staff WHERE id=? AND organization_id=?",
                                  (employee, org)).fetchone()
            if manager and person and (not person["archived"] or previous == employee):
                return person["name"]
            raise ValueError("Сотрудник недоступен в организации")
        target = self.auth.get_user(username=employee)
        platform_self = employee == user["username"] and user.get("is_platform_admin")
        if not target or (org not in _org_roles(target) and not platform_self):
            raise ValueError("Сотрудник недоступен в организации")
        if target.get("status") != "active" and not (manager and previous == employee):
            raise ValueError("Нельзя назначить смену отключённому сотруднику")
        return target.get("display_name") or employee

    def qualifications(self, token, organization_id, *, work_date=None):
        user = self._user(token)
        if organization_id not in self.organizations(token):
            raise PermissionError("Организация недоступна")
        with self._connection() as conn:
            people = {}
            for row in conn.execute("""SELECT employee,category FROM shift_staff_categories WHERE organization_id=?
                AND (? OR employee=?)""",(organization_id,_manager(user,organization_id),user["username"])):
                people.setdefault(row["employee"],[]).append(row["category"])
            if work_date:
                for row in conn.execute("""SELECT employee,kind,mark FROM shift_staff_licences WHERE org=? AND
                    (? OR employee=?) AND ? BETWEEN since AND until_day""",
                    (organization_id,_manager(user,organization_id),user["username"],_day(work_date))):
                    people.setdefault(row["employee"],[]).append(row["kind"]+":"+row["mark"])
            return people

    def staff_licences(self,token,organization_id,employee):
        if organization_id not in self.organizations(token,manage=True):
            raise PermissionError("Удостоверения ведёт диспетчер организации")
        with self._connection() as conn:
            docs = {}
            for r in conn.execute("SELECT * FROM shift_staff_licences WHERE org=? AND employee=?",(organization_id,employee)):
                docs.setdefault(r["kind"],dict(marks=[],since=r["since"],until=r["until_day"]))["marks"].append(r["mark"])
            return docs

    def set_categories(self, token, organization_id, employee, categories, *, licences=None):
        if organization_id not in self.organizations(token,manage=True):
            raise PermissionError("Допуски указывает диспетчер организации")
        user = self._user(token)
        if not isinstance(categories,list) or any(c not in ops.CATEGORIES for c in categories):
            raise ValueError("Неизвестная категория сотрудника")
        records = ops.licence_rows(licences) if licences is not None else None
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._employee_name(conn,user,organization_id,employee)
            conn.execute("DELETE FROM shift_staff_categories WHERE organization_id=? AND employee=?",(organization_id,employee))
            conn.executemany("INSERT INTO shift_staff_categories VALUES(?,?,?)",[(organization_id,employee,c) for c in set(categories)])
            ops.replace_licences(conn,organization_id,employee,records)

    @staticmethod
    def _check_assignment(conn,org,employee,equipment_key,work_date,strict):
        ref = conn.execute("""SELECT equipment_category,licence_kind,licence_mark FROM shift_references
            WHERE organization_id=? AND kind='equipment' AND name_key=? AND archived=0""",(org,equipment_key)).fetchone()
        if strict and (not ref or not all(ref)):
            raise ValueError("Укажите категорию техники и требуемое удостоверение в справочнике")
        if not ref:
            return
        if ref[0] and not conn.execute("""SELECT 1 FROM shift_staff_categories WHERE organization_id=?
            AND employee=? AND category=?""",(org,employee,ref[0])).fetchone():
            raise ValueError("У сотрудника нет допуска к выбранной категории техники")
        if ref[1] and not conn.execute("""SELECT 1 FROM shift_staff_licences WHERE org=? AND employee=?
            AND kind=? AND mark=? AND ? BETWEEN since AND until_day""",(org,employee,ref[1],ref[2],work_date)).fetchone():
            raise ValueError("На дату смены нет действующего удостоверения с требуемой категорией")

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
            return _journal_row(self._row(conn, user, shift_id))

    @staticmethod
    def _event(conn, shift_id, user, action, reason=""):
        row = dict(conn.execute("SELECT * FROM shifts WHERE id=?", (shift_id,)).fetchone())
        conn.execute(
            "INSERT INTO shift_events(shift_id,actor,action,reason,snapshot,created_at) VALUES(?,?,?,?,?,?)",
            (shift_id, user["username"], action, reason, json.dumps(row, ensure_ascii=False), row["updated_at"]),
        )
        return _journal_row(row)

    def save(self, token: str, data: dict, *, shift_id: int | None = None, revision: int | None = None,
             source_event_id: str = "", require_equipment_category=False) -> dict:
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
                        return _journal_row(self._row(conn, user, previous["shift_id"]))
                old = {}
                if shift_id is not None:
                    if revision is None:
                        raise ValueError("Не указана версия смены")
                    old = self._row(conn, user, shift_id, revision)
                org = self.organization_for(user, values["organization"], data.get("organization_id", ""), old)
                manager = _manager(user, org)
                if not manager and employee != user["username"]:
                    raise PermissionError("Нельзя создавать смены другого сотрудника")
                values.update(organization_id=org, employee=employee, employee_name=self._employee_name(
                    conn, user, org, employee, previous=old.get("employee")),
                              updated_at=datetime.now(timezone.utc).isoformat(), status="draft" if manager else "submitted")
                if values["entry_type"]=="vehicle" and (old.get("employee"),old.get("equipment"),old.get("work_date")) != (
                        employee,values["equipment"],values["work_date"]):
                    self._check_assignment(conn,org,employee,values["equipment_key"],values["work_date"],require_equipment_category)
                for key in ("assigned_employee", "assigned_equipment"):
                    value = str(data.get(key, old.get(key)) or "").strip()
                    if not manager and value != old.get(key, ""):
                        raise PermissionError("Назначение изменяет диспетчер")
                    values[key] = value
                assigned = values["assigned_employee"]
                if assigned == old.get("assigned_employee"):
                    values["assigned_employee_name"] = old["assigned_employee_name"]
                else:
                    values["assigned_employee_name"] = self._employee_name(conn, user, org, assigned) if assigned else ""
                if values["entry_type"] == "attendance" and values["assigned_equipment"]:
                    raise ValueError("В табеле явки не указывают технику")
                for key, plate in (("equipment","registration_number"),("assigned_equipment","assigned_registration_number")):
                    if old and old[key] == values[key]:
                        values[plate] = old[plate]
                        continue
                    ref = conn.execute("""SELECT registration_number FROM shift_references WHERE kind='equipment'
                        AND organization_id=? AND name_key=? AND archived=0""", (org, values[key].casefold())).fetchone()
                    if key == "assigned_equipment" and values[key] and not ref:
                        raise ValueError("Назначенная техника отсутствует в справочнике")
                    values[plate] = ref[0] if ref else ""
                values["time_from"], values["time_to"] = ops.shift_times(
                    data.get("time_from",old.get("time_from")), data.get("time_to",old.get("time_to")))
                values["work_lines"] = ops.price_lines(conn, values, old, data.get("work_lines", json.loads(old.get("work_lines","[]"))))
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
            if status=="approved" and row["entry_type"]=="vehicle":
                self._check_assignment(conn,row["organization_id"],row["employee"],row["equipment_key"],row["work_date"],False)
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
            visible = []
            for event in events:
                snapshot = _journal_row(json.loads(event["snapshot"]))
                if not _manager(user, row["organization_id"]) and snapshot.get("employee") != user["username"]:
                    previous = {}
                    continue
                event["snapshot"] = json.dumps(snapshot, ensure_ascii=False)
                event["changes"] = {key: [previous.get(key), snapshot.get(key)] for key in AUDIT_FIELDS
                                    if previous.get(key) != snapshot.get(key)}
                previous = snapshot
                visible.append(event)
            return visible

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
                before = _journal_row(json.loads(row.pop("previous") or "{}"))
                after = _journal_row(json.loads(row["snapshot"]))
                row["snapshot"] = json.dumps(after, ensure_ascii=False)
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
        with self._connection() as conn:
            for person in conn.execute("SELECT * FROM shift_staff ORDER BY name"):
                if _manager(user, person["organization_id"]):
                    result.append(dict(username=person["id"], display_name=person["name"], offline=True,
                                       status="blocked" if person["archived"] else "active", editable=True,
                                       organization_ids=[person["organization_id"]]))
            cats = {}
            for item in conn.execute("SELECT * FROM shift_staff_categories"):
                cats.setdefault((item["organization_id"],item["employee"]), []).append(item["category"])
            for person in result:
                person["categories"] = cats.get((person["organization_ids"][0],person["username"]), [])
        return result

    def save_employee(self, token: str, *, offline=False, categories=None, licences=None, **data) -> str:
        self._user(token)
        org = data.get("organization_id", "default")
        if categories is not None and (not isinstance(categories,list) or any(c not in ops.CATEGORIES for c in categories)):
            raise ValueError("Неизвестная категория сотрудника")
        records = ops.licence_rows(licences) if licences is not None else None

        def set_categories(conn, key):
            ops.replace_licences(conn,org,key,records)
            if categories is not None:
                conn.execute("DELETE FROM shift_staff_categories WHERE organization_id=? AND employee=?", (org,key))
                conn.executemany("INSERT INTO shift_staff_categories VALUES(?,?,?)", [(org,key,c) for c in set(categories)])

        if offline:
            if org not in self.organizations(token, manage=True):
                raise PermissionError("Сотрудниками управляет диспетчер организации")
            name = str(data.get("display_name") or "").strip()
            if not name or len(name) > 200:
                raise ValueError("Укажите ФИО сотрудника (до 200 символов)")
            with self._connection() as conn:
                if data.get("create"):
                    key = "staff:" + uuid4().hex
                    conn.execute("INSERT INTO shift_staff(id,organization_id,name) VALUES(?,?,?)",(key,org,name))
                else:
                    key = data.get("username")
                    if not conn.execute("UPDATE shift_staff SET name=?,archived=? WHERE id=? AND organization_id=?",
                                        (name, int(bool(data.get("archived"))), key, org)).rowcount:
                        raise ValueError("Сотрудник не найден")
                set_categories(conn,key)
            return key
        self.auth.save_driver(token, **data)
        key = data["username"].strip().lower()
        with self._connection() as conn:
            set_categories(conn,key)
        return key

    def rates(self, token, *, organization_id, work_date, equipment="", workplace=""):
        if organization_id not in self.organizations(token):
            raise PermissionError("Организация недоступна")
        with self._connection() as conn:
            rows = ops.applicable_rates(conn, organization_id, _day(work_date), equipment or "", workplace or "")
            return [{k: r[k] for k in ("id", "work", "unit")} for r in rows]

    def add_rate(self, token, *, organization_id, work, unit, price, since, equipment="", workplace=""):
        user = shift_payroll.MonthlyPayroll(self)._manager(token, organization_id)
        work = (work or "").strip()
        if not work or len(work)>200 or unit not in ops.UNITS:
            raise ValueError("Укажите вид работы и единицу расчёта")
        cents = int(100 * ops.number(price, 2, 9999999, "Укажите положительную ставку с точностью до копейки"))
        with self._connection() as conn:
            for kind, value in (("equipment",equipment),("workplace",workplace)):
                if value and not conn.execute("""SELECT 1 FROM shift_references WHERE organization_id=? AND kind=?
                    AND name=? AND archived=0""",(organization_id,kind,value)).fetchone():
                    raise ValueError("Техника или объект отсутствуют в справочнике организации")
            try:
                return conn.execute("""INSERT INTO shift_rates(organization_id,work,work_key,unit,equipment,workplace,
                    since,kopecks,created_by) VALUES(?,?,?,?,?,?,?,?,?)""",(organization_id,work,work.casefold(),unit,
                        equipment or "",workplace or "",_day(since),cents,user["username"])).lastrowid
            except sqlite3.IntegrityError:
                raise ValueError("Такой тариф уже задан на эту дату") from None

    def save_reference(self, token: str, kind: str, name: str, *, record_id: int | None = None,
                       revision: int | None = None, archived: bool = False, organization_id: str = "default",
                       registration_number: str = "", fleet_number: str = "", equipment_category: str = "",
                       licence_kind: str = "", licence_mark: str = "") -> str:
        if organization_id not in self.organizations(token, manage=True):
            raise PermissionError("Справочники изменяет диспетчер")
        name = str(name).strip()
        if kind not in REFERENCE_LABELS or not name or len(name) > 200:
            raise ValueError("Укажите тип и название (до 200 символов)")
        equipment_name = name if kind == "equipment" else ""
        if equipment_category and (kind!="equipment" or equipment_category not in ops.VEHICLE_CATEGORIES):
            raise ValueError("Неизвестная категория техники")
        if (licence_kind or licence_mark) and (not equipment_category or licence_mark not in ops.MARKS.get(licence_kind,())):
            raise ValueError("Укажите тип и категорию требуемого удостоверения")
        registration_number, fleet_number = registration_number.strip(), fleet_number.strip()
        if registration_number or fleet_number:
            if kind != "equipment" or max(len(registration_number), len(fleet_number)) > 32:
                raise ValueError("Номера техники должны содержать не более 32 символов")
            # Shifts and waybills keep a readable snapshot, including for identical models.
            name = " · ".join(filter(None, (name, f"гос. {registration_number}" if registration_number else "",
                                            f"борт {fleet_number}" if fleet_number else "")))
            if len(name) > 200:
                raise ValueError("Название техники вместе с номерами должно укладываться в 200 символов")
        try:
            with self._connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if record_id is None:
                    conn.execute("""INSERT INTO shift_references(organization_id,kind,name,name_key,
                        equipment_name,registration_number,fleet_number,equipment_category,licence_kind,licence_mark)
                        VALUES(?,?,?,?,?,?,?,?,?,?)""",(organization_id,kind,name,name.casefold(),equipment_name,
                                                        registration_number,fleet_number,equipment_category,licence_kind,licence_mark))
                else:
                    result = conn.execute("""UPDATE shift_references SET name=?,name_key=?,archived=?,revision=revision+1,
                        equipment_name=?,registration_number=?,fleet_number=?,equipment_category=?,licence_kind=?,licence_mark=?
                        WHERE id=? AND organization_id=? AND kind=? AND revision=?""",
                        (name, name.casefold(), int(archived), equipment_name, registration_number, fleet_number,equipment_category,licence_kind,licence_mark,
                         record_id, organization_id, kind, revision))
                    if not result.rowcount:
                        raise ValueError("Справочник изменён. Обновите список.")
        except sqlite3.IntegrityError:
            raise ValueError("Такое название уже есть, в том числе в архиве") from None
        return name

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
            summary["rows"] = [_journal_row(row) for row in conn.execute(
                f"SELECT * FROM shifts WHERE {where} ORDER BY work_date DESC,shift_number,id DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            )]
            ops.conflicts(conn, summary["rows"], scope, scope_params)
            return summary

    def month_sheet(self, token, month, *, attendance_only=False):
        user = self._user(token)
        if not can_manage_shifts(user):
            raise PermissionError("Табель доступен диспетчеру")
        first = date.fromisoformat(_day(f"{month}-01"))
        end = first.replace(day=calendar.monthrange(first.year, first.month)[1])
        scope, params = _scope_sql(user, management=True)
        with self._connection() as conn:
            rows = conn.execute(f"""SELECT employee,employee_name,work_date,status,work_minutes
                FROM shifts WHERE deleted=0 AND work_date BETWEEN ? AND ? AND {scope}
                AND (NOT ? OR entry_type='attendance') ORDER BY work_date,employee_name,id""",
                (str(first),str(end),*params,int(attendance_only)))
            people = {}
            for r in rows:
                p = people.setdefault(r["employee"], dict(name=r["employee_name"],days={},minutes=0,approved=0))
                day = int(r["work_date"][-2:])
                p["days"][day] = p["days"].get(day,0) + r["work_minutes"]
                p["minutes"] += r["work_minutes"]
                if r["status"] == "approved":
                    p["approved"] += r["work_minutes"]
            return people

    def conflict_preview(self, token, *, organization_id, work_date, shift_number, shift_id=None,
                         employee="", equipment="", assigned_employee="", assigned_equipment="", time_from="", time_to=""):
        user = self._user(token)
        if organization_id not in self.organizations(token, manage=True):
            raise PermissionError("Проверка назначений доступна диспетчеру организации")
        start,end = ops.shift_times(time_from,time_to)
        item = dict(id=shift_id, work_date=_day(work_date), shift_number=shift_number, employee=employee,
                    equipment=equipment, assigned_employee=assigned_employee, assigned_equipment=assigned_equipment,
                    time_from=start,time_to=end)
        with self._connection() as conn:
            for key, plate in (("equipment","registration_number"),("assigned_equipment","assigned_registration_number")):
                ref = conn.execute("""SELECT registration_number FROM shift_references
                    WHERE organization_id=? AND kind='equipment' AND name_key=?""",
                    (organization_id,(item[key] or "").casefold())).fetchone()
                item[plate] = ref[0] if ref else ""
            scope,params = _scope_sql(user, management=True)
            ops.conflicts(conn,[item],scope,params)
        return item["conflict"]

    def export_csv(self, token: str, **filters) -> bytes:
        data = self.list(token, **filters, limit=10001)
        if data["count"] > 10000:
            raise ValueError("В выгрузке больше 10 000 смен. Сократите период.")
        stream = io.StringIO(newline="")
        writer = csv.writer(stream, delimiter=";")
        fields = ("id", "work_date", "shift_number", "employee_name", "organization", "equipment", "workplace",
                  "partner", "waybill_number", "hours", "breaks", "status", "comment",
                  "assigned_employee_name","assigned_equipment","registration_number","assigned_registration_number","time_from","time_to")
        writer.writerow(("ID", "Дата", "Смена", "Фактический исполнитель", "Организация", "Фактическая техника", "Объект", "Контрагент",
                         "Путевой лист", "Отработано, ч", "Перерывы, ч", "Статус", "Комментарий",
                         "Сотрудник по назначению","Техника по назначению","Госномер фактической техники","Госномер по назначению",
                         "Начало","Окончание"))
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
