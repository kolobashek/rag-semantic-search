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

from .roles import can_manage_shifts, can_use_shifts
from .sqlite_runtime import prepare_sqlite_connection

STATUS_LABELS = {"draft": "Черновик", "submitted": "На проверке", "approved": "Подтверждено"}
TEXT_FIELDS = ("organization", "equipment", "workplace", "partner", "waybill_number", "comment")
REFERENCE_LABELS = {"organization": "Организации", "equipment": "Техника", "workplace": "Объекты", "partner": "Контрагенты"}
AUDIT_FIELDS = ("employee", "employee_name", "work_date", "shift_number", *TEXT_FIELDS,
                "work_minutes", "break_minutes", "status", "deleted")


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
                CREATE TABLE IF NOT EXISTS shift_references (
                    id INTEGER PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL,
                    name_key TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(kind, name_key)
                );
            """)
            for kind in (REFERENCE_LABELS if seed_references else ()):
                for row in conn.execute(f"SELECT DISTINCT {kind} FROM shifts WHERE {kind}<>''").fetchall():
                    conn.execute("INSERT OR IGNORE INTO shift_references(kind,name,name_key) VALUES(?,?,?)",
                                 (kind, row[0], row[0].casefold()))

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

    def users(self, token: str) -> dict[str, str]:
        user = self._user(token)
        users = self.auth.list_users() if can_manage_shifts(user) else [user]
        return {u["username"]: u.get("display_name") or u["username"] for u in users if u["status"] == "active"}

    @staticmethod
    def _row(conn, user, shift_id, revision=None) -> dict:
        row = conn.execute("SELECT * FROM shifts WHERE id=? AND deleted=0", (shift_id,)).fetchone()
        if row is None or (not can_manage_shifts(user) and row["employee"] != user["username"]):
            raise PermissionError("Смена недоступна")
        if revision is not None and revision != row["revision"]:
            raise ValueError("Смена уже изменена. Обновите журнал и откройте её заново.")
        return dict(row)

    @staticmethod
    def _event(conn, shift_id, user, action, reason=""):
        row = dict(conn.execute("SELECT * FROM shifts WHERE id=?", (shift_id,)).fetchone())
        conn.execute(
            "INSERT INTO shift_events(shift_id,actor,action,reason,snapshot,created_at) VALUES(?,?,?,?,?,?)",
            (shift_id, user["username"], action, reason, json.dumps(row, ensure_ascii=False), row["updated_at"]),
        )
        return row

    def save(self, token: str, data: dict, *, shift_id: int | None = None, revision: int | None = None) -> dict:
        user = self._user(token)
        values = _payload(data)
        employee = str(data.get("employee") or user["username"]).strip().lower()
        if not can_manage_shifts(user) and employee != user["username"]:
            raise PermissionError("Нельзя создавать смены другого сотрудника")
        target = self.auth.get_user(username=employee)
        if not target or (target.get("status") != "active" and (not can_manage_shifts(user) or shift_id is None)):
            raise ValueError("Сотрудник не найден или отключён")
        values.update(employee=employee, employee_name=target.get("display_name") or employee)
        values["updated_at"] = datetime.now(timezone.utc).isoformat()
        values["status"] = "draft" if can_manage_shifts(user) else "submitted"
        try:
            with self._connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                old = {}
                if shift_id is not None:
                    if revision is None:
                        raise ValueError("Не указана версия смены")
                    old = self._row(conn, user, shift_id, revision)
                    if target.get("status") != "active" and employee != old["employee"]:
                        raise ValueError("Нельзя назначить смену отключённому сотруднику")
                if can_manage_shifts(user):
                    for kind in REFERENCE_LABELS:
                        if values[kind] and values[kind] != old.get(kind):
                            conn.execute("INSERT OR IGNORE INTO shift_references(kind,name,name_key) VALUES(?,?,?)",
                                         (kind, values[kind], values[kind].casefold()))
                if shift_id is None:
                    values["created_at"] = values["updated_at"]
                    fields = ",".join(values)
                    placeholders = ",".join("?" for _ in values)
                    shift_id = conn.execute(
                        f"INSERT INTO shifts({fields}) VALUES({placeholders})", tuple(values.values()),
                    ).lastrowid
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
            if can_manage_shifts(user):
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
            self._row(conn, user, shift_id, revision)
            if not can_manage_shifts(user):
                raise PermissionError("Удаление доступно диспетчеру")
            conn.execute(
                "UPDATE shifts SET deleted=1, revision=revision+1, updated_at=? WHERE id=?",
                (datetime.now(timezone.utc).isoformat(), shift_id),
            )
            self._event(conn, shift_id, user, "delete")

    def history(self, token: str, shift_id: int) -> list[dict]:
        user = self._user(token)
        with self._connection() as conn:
            if not can_manage_shifts(user):
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
        if not can_manage_shifts(self._user(token)):
            raise PermissionError("История всех путевых доступна диспетчеру")
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("Некорректная страница истории")
        with self._connection() as conn:
            conn.execute("BEGIN")
            total = conn.execute("SELECT count(*) FROM shift_events").fetchone()[0]
            rows = [dict(row) for row in conn.execute("""
                SELECT e.*, (SELECT p.snapshot FROM shift_events p
                  WHERE p.shift_id=e.shift_id AND p.id<e.id ORDER BY p.id DESC LIMIT 1) AS previous
                FROM shift_events e ORDER BY e.id DESC LIMIT ? OFFSET ?
            """, (limit, offset))]
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
            return [dict(row) for row in conn.execute(
                "SELECT * FROM shift_references WHERE archived=0 OR ? ORDER BY kind,name", (int(archived),))]

    def employees(self, token: str) -> list[dict]:
        if not can_manage_shifts(self._user(token)):
            raise PermissionError("Сотрудниками управляет диспетчер")
        from .roles import user_roles
        return [{"username": u["username"], "display_name": u.get("display_name") or u["username"],
                 "status": u["status"], "editable": user_roles(u) == {"driver"}}
                for u in self.auth.list_users()]

    def save_employee(self, token: str, **data) -> None:
        self._user(token)
        self.auth.save_driver(token, **data)

    def save_reference(self, token: str, kind: str, name: str, *, record_id: int | None = None,
                       revision: int | None = None, archived: bool = False) -> None:
        if not can_manage_shifts(self._user(token)):
            raise PermissionError("Справочники изменяет диспетчер")
        name = str(name).strip()
        if kind not in REFERENCE_LABELS or not name or len(name) > 200:
            raise ValueError("Укажите тип и название (до 200 символов)")
        try:
            with self._connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if record_id is None:
                    conn.execute("INSERT INTO shift_references(kind,name,name_key) VALUES(?,?,?)",
                                 (kind, name, name.casefold()))
                else:
                    result = conn.execute("""UPDATE shift_references SET name=?,name_key=?,archived=?,revision=revision+1
                        WHERE id=? AND kind=? AND revision=?""",
                        (name, name.casefold(), int(archived), record_id, kind, revision))
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
