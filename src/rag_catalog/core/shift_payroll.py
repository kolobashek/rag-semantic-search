"""Organization-scoped monthly management estimates; no transfers or payroll posting."""

import calendar
import json
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from uuid import uuid4

from .shift_operations import number

GROUPS = {"credit": "Начисление", "debit": "Удержание", "voluntary": "Добровольный взнос", "memo": "Справочно"}
DEFAULT_TYPES = {
    "bonus": ("Премия", "credit"),
    "extra": ("Доплата", "credit"),
    "advance": ("Ранее выплаченный аванс", "debit"),
    "deduction": ("Удержание по документу", "debit"),
    "contribution": ("Взнос по заявлению", "voluntary"),
    "union": ("Профсоюзный взнос", "voluntary"),
    "note": ("Замечание без удержания", "memo"),
}


def migrate(conn):
    # No executescript: the caller owns the migration transaction.
    for ddl in (
        """CREATE TABLE IF NOT EXISTS shift_pay_norms (id INTEGER PRIMARY KEY, org TEXT NOT NULL,
           month TEXT NOT NULL, days TEXT NOT NULL, actor TEXT NOT NULL, stamp TEXT DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE INDEX IF NOT EXISTS shift_norm_period ON shift_pay_norms(org,month,id)""",
        """CREATE TABLE IF NOT EXISTS shift_salaries (id INTEGER PRIMARY KEY, org TEXT NOT NULL,
           employee TEXT NOT NULL, name TEXT NOT NULL, since TEXT NOT NULL, cents INTEGER NOT NULL,
           actor TEXT NOT NULL, stamp TEXT DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE INDEX IF NOT EXISTS shift_salary_period ON shift_salaries(org,employee,since,id)""",
        """CREATE TABLE IF NOT EXISTS shift_pay_types (id TEXT PRIMARY KEY, org TEXT NOT NULL,
           label TEXT NOT NULL, label_key TEXT NOT NULL, kind TEXT NOT NULL, actor TEXT NOT NULL, UNIQUE(org,label_key))""",
        """CREATE TABLE IF NOT EXISTS shift_pay_items (id INTEGER PRIMARY KEY, org TEXT NOT NULL, employee TEXT NOT NULL,
           name TEXT NOT NULL, type_id TEXT NOT NULL, label TEXT NOT NULL, kind TEXT NOT NULL, cents INTEGER NOT NULL,
           since TEXT NOT NULL, until_month TEXT NOT NULL, basis TEXT NOT NULL, consent INTEGER NOT NULL,
           actor TEXT NOT NULL, stamp TEXT DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE INDEX IF NOT EXISTS shift_pay_period ON shift_pay_items(org,since)""",
        """CREATE TABLE IF NOT EXISTS shift_pay_stops (item INTEGER NOT NULL REFERENCES shift_pay_items(id),
           since TEXT NOT NULL, actor TEXT NOT NULL, stamp TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(item,since))""",
    ):
        conn.execute(ddl)
    if "basis_points" not in {r["name"] for r in conn.execute("PRAGMA table_info(shift_pay_items)")}:
        conn.execute("ALTER TABLE shift_pay_items ADD COLUMN basis_points INTEGER NOT NULL DEFAULT 0")


def period(value):
    try:
        first = date.fromisoformat(f"{value}-01")
        if first.isoformat()[:7] != value:
            raise ValueError
        return first
    except (TypeError, ValueError):
        raise ValueError("Выберите месяц") from None


def _cents(value):
    return int(100 * number(value, 2, 999999999, "Укажите положительную сумму в рублях с точностью до копейки"))


class MonthlyPayroll:
    def __init__(self, journal):
        self.journal = journal

    def _owner(self, token):
        user = self.journal.auth.get_user_by_session(token)
        if not user or user.get("status") != "active" or user.get("must_change_password") or not user.get("is_payroll_owner"):
            raise PermissionError("Нет доступа к расчёту зарплаты")
        return user

    def organizations(self, token):
        return {r["id"]: r["name"] for r in self.journal._directory(self._owner(token))}

    def _manager(self, token, org):
        user = self._owner(token)
        if org not in {r["id"] for r in self.journal._directory(user)}:
            raise PermissionError("Организация недоступна")
        return user

    def employees(self, token, org):
        self._manager(token, org)
        people = {u["username"]: u.get("display_name") or u["username"] for u in self.journal.auth.list_users()
                  if org in u["organization_roles"] and u["status"] == "active"}
        with self.journal._connection() as conn:
            people.update(conn.execute("SELECT id,name FROM shift_staff WHERE organization_id=? AND archived=0", (org,)))
        return people

    def rate_options(self, token, org):
        self._manager(token, org)
        with self.journal._connection() as conn:
            return [dict(r) for r in conn.execute("""SELECT kind,name FROM shift_references WHERE organization_id=?
                AND archived=0 AND kind IN ('equipment','workplace') ORDER BY name""", (org,))]

    def _name(self, conn, org, employee, *, past=False):
        if employee.startswith("staff:"):
            row = conn.execute("SELECT name FROM shift_staff WHERE id=? AND organization_id=? AND (? OR archived=0)",
                               (employee, org, past)).fetchone()
            if row:
                return row[0]
        else:
            user = self.journal.auth.get_user(username=employee)
            if user and org in user["organization_roles"] and (past or user["status"] == "active"):
                return user.get("display_name") or employee
        raise ValueError("Сотрудник недоступен в организации")

    def set_norm(self, token, org, month, day_minutes, *, revision):
        actor = self._manager(token, org)["username"]
        first = period(month)
        if (
            not isinstance(day_minutes, list)
            or len(day_minutes) != calendar.monthrange(first.year, first.month)[1]
            or any(type(v) is not int or not 0 <= v <= 1440 for v in day_minutes)
            or not sum(day_minutes)
        ):
            raise ValueError("Задайте положительную месячную норму, до 24 часов на день")
        with self.journal._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT max(id) FROM shift_pay_norms WHERE org=? AND month=?", (org, month)
            ).fetchone()[0]
            if current != revision:
                raise ValueError("Норма уже изменена. Откройте календарь заново")
            conn.execute(
                "INSERT INTO shift_pay_norms(org,month,days,actor) VALUES(?,?,?,?)",
                (org, month, json.dumps(day_minutes), actor),
            )

    def set_salary(self, token, org, month, employee, amount):
        user = self._manager(token, org)
        period(month)
        # An explicit zero ends salary-based calculation from the chosen month.
        cents = 0 if str(amount) in ("0", "0.0", "0.00") else _cents(amount)
        with self.journal._connection() as conn:
            name = self._name(conn, org, employee)
            conn.execute(
                "INSERT INTO shift_salaries(org,employee,name,since,cents,actor) VALUES(?,?,?,?,?,?)",
                (org, employee, name, month, cents, user["username"]),
            )

    def types(self, conn, org):
        result = DEFAULT_TYPES.copy()
        result.update(
            {
                r["id"]: (r["label"], r["kind"])
                for r in conn.execute(
                    "SELECT id,label,kind FROM shift_pay_types WHERE org=? ORDER BY label", (org,)
                )
            }
        )
        return result

    def add_type(self, token, org, label, kind):
        actor = self._manager(token, org)["username"]
        label = str(label or "").strip()
        if not label or len(label) > 100 or kind not in GROUPS:
            raise ValueError("Укажите название и вид операции")
        with self.journal._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if any(label.casefold() == existing.casefold() for existing, _ in self.types(conn, org).values()):
                raise ValueError("Такой вид уже существует")
            key = uuid4().hex
            conn.execute(
                "INSERT INTO shift_pay_types VALUES(?,?,?,?,?,?)",
                (key, org, label, label.casefold(), kind, actor),
            )
            return key

    def add_item(
        self,
        token,
        org,
        month,
        employee,
        type_id,
        amount,
        basis,
        *,
        monthly=False,
        until="",
        consent=False,
        percent=None,
    ):
        user = self._manager(token, org)
        period(month)
        end = until if monthly else month
        if end and period(end) < period(month):
            raise ValueError("Конец периода раньше начала")
        bps = 0 if percent is None else int(100 * number(percent, 2, 100, "Укажите процент от 0,01 до 100"))
        cents = 0 if bps else _cents(amount)
        basis = str(basis or "").strip()
        if not basis or len(basis) > 300:
            raise ValueError("Укажите основание операции (до 300 символов)")
        with self.journal._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            name = self._name(conn, org, employee, past=True)
            options = self.types(conn, org)
            if type_id not in options:
                raise ValueError("Вид операции не найден")
            label, kind = options[type_id]
            if bps and kind not in ("debit", "voluntary"):
                raise ValueError("Процент от начисленного заработка применяется только к удержанию или взносу")
            if kind == "voluntary" and consent is not True:
                raise ValueError("Для добровольного взноса требуется заявление сотрудника")
            if (
                type_id == "union"
                and conn.execute(
                    """SELECT 1 FROM shift_pay_items i WHERE org=? AND employee=?
                AND type_id='union' AND since<=? AND (until_month='' OR until_month>=?)
                AND NOT EXISTS(SELECT 1 FROM shift_pay_stops s WHERE s.item=i.id AND s.since<=max(i.since,?))""",
                    (org, employee, end or "9999-12", month, month),
                ).fetchone()
            ):
                raise ValueError("У сотрудника уже есть профсоюзный взнос на этот период")
            return conn.execute(
                """INSERT INTO shift_pay_items(org,employee,name,type_id,label,kind,cents,since,
                until_month,basis,consent,actor,basis_points) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    org,
                    employee,
                    name,
                    type_id,
                    label,
                    kind,
                    cents,
                    month,
                    end,
                    basis,
                    int(kind == "voluntary" and consent),
                    user["username"],
                    bps,
                ),
            ).lastrowid

    def stop_item(self, token, org, month, item):
        actor = self._manager(token, org)["username"]
        period(month)
        with self.journal._connection() as conn:
            if not conn.execute(
                """SELECT 1 FROM shift_pay_items WHERE id=? AND org=? AND since<=?
                AND (until_month='' OR until_month>=?)""",
                (item, org, month, month),
            ).fetchone():
                raise ValueError("Операция не действует в выбранном месяце")
            conn.execute(
                "INSERT OR IGNORE INTO shift_pay_stops(item,since,actor) VALUES(?,?,?)", (item, month, actor)
            )

    def sheet(self, token, org, month):
        self._manager(token, org)
        first = period(month)
        days_count = calendar.monthrange(first.year, first.month)[1]
        people = {}

        def person(key, name):
            return people.setdefault(
                key,
                dict(
                    name=name, minutes=0, pending=0, piece=0, salary=0, credit=0, debit=0, voluntary=0, memo=0
                ),
            )

        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            norm = conn.execute(
                "SELECT id,days FROM shift_pay_norms WHERE org=? AND month=? ORDER BY id DESC LIMIT 1",
                (org, month),
            ).fetchone()
            days = (
                json.loads(norm["days"])
                if norm
                else [480 if first.replace(day=d).weekday() < 5 else 0 for d in range(1, days_count + 1)]
            )
            for r in conn.execute(
                """SELECT * FROM shift_salaries WHERE org=? AND since<=? ORDER BY since,id""", (org, month)
            ):
                person(r["employee"], r["name"])["salary"] = r["cents"]
            for r in conn.execute(
                """SELECT employee,employee_name,status,work_minutes,work_lines FROM shifts
                WHERE deleted=0 AND organization_id=? AND work_date BETWEEN ? AND ?""",
                (org, str(first), str(first.replace(day=days_count))),
            ):
                p = person(r["employee"], r["employee_name"])
                if r["status"] == "approved":
                    p["minutes"] += r["work_minutes"]
                    p["piece"] += sum(line["amount"] for line in json.loads(r["work_lines"]))
                else:
                    p["pending"] += r["work_minutes"]
            items = [
                dict(r)
                for r in conn.execute(
                    """SELECT i.*,EXISTS(SELECT 1 FROM shift_pay_stops s
                WHERE s.item=i.id AND s.since<=?) AS stopped FROM shift_pay_items i
                WHERE org=? AND since<=? AND (until_month='' OR until_month>=?) ORDER BY id""",
                    (month, org, month, month),
                )
            ]
            for r in items:
                if not r["stopped"]:
                    person(r["employee"], r["name"])[r["kind"]] += r["cents"]
            types = self.types(conn, org)
            rates = [dict(r) for r in conn.execute("""SELECT work,unit,equipment,workplace,since,kopecks FROM shift_rates
                WHERE organization_id=? AND since<=? ORDER BY work,since DESC,id DESC""",
                (org, str(first.replace(day=days_count))))]
        for p in people.values():
            p["base"] = p["piece"]
            p["extra_minutes"] = 0
            p["extra_cents"] = 0
            if p["salary"]:
                p["base"] = None
                if norm:
                    n = sum(days)
                    # Round the total once so a full norm pays exactly the monthly salary.
                    p["base"] = int(
                        (Decimal(p["salary"]) * p["minutes"] / n).quantize(Decimal(1), rounding=ROUND_HALF_UP)
                    )
                    p["extra_minutes"] = max(0, p["minutes"] - n)
                    p["extra_cents"] = max(0, p["base"] - p["salary"])
        for r in items:
            r["calculated"] = r["cents"]
            r["earnings"] = None
            if r["basis_points"]:
                p = people.get(r["employee"])
                r["calculated"] = 0 if r["stopped"] else None
                if not r["stopped"] and p["base"] is not None:
                    r["earnings"] = p["base"] + p["credit"]
                    r["calculated"] = (r["earnings"] * r["basis_points"] + 5000) // 10000
                    p[r["kind"]] += r["calculated"]
        for p in people.values():
            p["estimate"] = None if p["base"] is None else p["base"] + p["credit"] - p["debit"] - p["voluntary"]
        return dict(people=people, items=items, days=days, norm_id=norm["id"] if norm else None, types=types, rates=rates)
