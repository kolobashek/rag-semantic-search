"""Shift resource conflicts and versioned piecework rates. Monetary amounts are kopecks."""

import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import escape

UNITS = {"hour": "ч", "km": "км", "trip": "ходка", "ton": "т"}
CATEGORIES = {
    "truck": "Грузовики",
    "loader": "Погрузчики",
    "grader": "Грейдеры",
    "dozer": "Бульдозеры",
    "excavator": "Экскаваторы",
    "car": "Легковые",
    "builder": "Строители",
    "engineer": "ИТР",
    "office": "Офис",
}
VEHICLE_CATEGORIES = {k: CATEGORIES[k] for k in ("truck", "loader", "grader", "dozer", "excavator", "car")}
LICENCES = {"road": "Водительское удостоверение", "tractor": "Удостоверение тракториста-машиниста"}
MARKS = {
    "road": "M A A1 B B1 BE C C1 CE C1E D D1 DE D1E Tm Tb".split(),
    "tractor": "AI AII AIII AIV B C D E F".split(),
}


def licence_rows(licences):
    result = []
    if not isinstance(licences, dict) or set(licences) - LICENCES.keys():
        raise ValueError("Неизвестное удостоверение")
    for kind, doc in licences.items():
        if (
            not isinstance(doc, dict)
            or not isinstance(doc.get("marks"), list)
            or any(m not in MARKS[kind] for m in doc["marks"])
        ):
            raise ValueError("Проверьте открытые категории удостоверения")
        if doc["marks"]:
            try:
                start, end = date.fromisoformat(doc["since"]), date.fromisoformat(doc["until"])
                if start > end:
                    raise ValueError
            except (KeyError, TypeError, ValueError):
                raise ValueError(
                    "Для удостоверения с открытыми категориями укажите даты выдачи и окончания"
                ) from None
            result.extend((kind, m, str(start), str(end)) for m in set(doc["marks"]))
    return result


def replace_licences(conn, org, employee, rows):
    if rows is not None:
        conn.execute("DELETE FROM shift_staff_licences WHERE org=? AND employee=?", (org, employee))
        conn.executemany(
            "INSERT INTO shift_staff_licences VALUES(?,?,?,?,?,?)", [(org, employee, *r) for r in rows]
        )


NEW_FIELDS = {
    "assigned_employee": "",
    "assigned_employee_name": "",
    "assigned_equipment": "",
    "registration_number": "",
    "assigned_registration_number": "",
    "time_from": "",
    "time_to": "",
    "work_lines": "[]",
    "entry_type": "vehicle",
}


def migrate(conn):
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(shifts)")}
    for key, default in NEW_FIELDS.items():
        if key not in cols:
            conn.execute(f"ALTER TABLE shifts ADD COLUMN {key} TEXT NOT NULL DEFAULT '{default}'")
    conn.execute("""CREATE TABLE IF NOT EXISTS shift_rates (
        id INTEGER PRIMARY KEY, organization_id TEXT NOT NULL, work TEXT NOT NULL, work_key TEXT NOT NULL,
        unit TEXT NOT NULL, equipment TEXT NOT NULL, workplace TEXT NOT NULL, since TEXT NOT NULL,
        kopecks INTEGER NOT NULL, created_by TEXT NOT NULL,
        UNIQUE(organization_id,work_key,unit,equipment,workplace,since))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS shift_staff_categories (
        organization_id TEXT NOT NULL, employee TEXT NOT NULL, category TEXT NOT NULL,
        PRIMARY KEY(organization_id,employee,category))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS shift_staff_licences (
        org TEXT NOT NULL,employee TEXT NOT NULL,kind TEXT NOT NULL,mark TEXT NOT NULL,
        since TEXT NOT NULL,until_day TEXT NOT NULL,PRIMARY KEY(org,employee,kind,mark))""")


def number(value, places, maximum, message):
    try:
        d = Decimal(str(value).replace(",", "."))
        if not d.is_finite() or not 0 < d <= maximum or d != d.quantize(Decimal(1).scaleb(-places)):
            raise ValueError
        return d
    except (InvalidOperation, ValueError):
        raise ValueError(message) from None


def shift_times(start, end):
    if not start and not end:
        return "", ""
    try:
        if start == end or any(time.fromisoformat(v).strftime("%H:%M") != v for v in (start, end)):
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError("Укажите начало и окончание смены в формате ЧЧ:ММ") from None
    return start, end


def applicable_rates(conn, org, day, equipment, workplace):
    rows = conn.execute(
        """SELECT * FROM shift_rates WHERE organization_id=? AND since<=?
        AND (equipment='' OR equipment=?) AND (workplace='' OR workplace=?)
        ORDER BY work_key,unit,(equipment<>'')+(workplace<>'') DESC,(equipment<>'') DESC,since DESC,id DESC""",
        (org, day, equipment, workplace),
    )
    selected = {}
    for row in rows:
        selected.setdefault((row["work_key"], row["unit"]), dict(row))
    return list(selected.values())


def price_lines(conn, values, old, items):
    if not isinstance(items, list) or len(items) > 100:
        raise ValueError("Допускается до 100 строк работ")
    rates = {
        r["id"]: r
        for r in applicable_rates(
            conn, values["organization_id"], values["work_date"], values["equipment"], values["workplace"]
        )
    }
    if old and all(old[k] == values[k] for k in ("organization_id", "work_date", "equipment", "workplace")):
        for line in json.loads(old["work_lines"]):
            rates[line["rate_id"]] = line
    result = []
    for item in items:
        if not isinstance(item, dict) or type(item.get("rate_id")) is not int or item["rate_id"] not in rates:
            raise ValueError("Выберите действующий тариф для даты, техники и объекта")
        rate = rates[item["rate_id"]]
        qty = number(
            item.get("quantity"),
            0 if rate["unit"] == "trip" else 3,
            9999999,
            "Укажите положительный объём: ходки целиком, остальные единицы до трёх знаков",
        )
        result.append(
            dict(
                rate_id=item["rate_id"],
                work=rate["work"],
                unit=rate["unit"],
                quantity=format(qty, "f"),
                kopecks=rate["kopecks"],
                amount=int((qty * rate["kopecks"]).quantize(Decimal(1), rounding=ROUND_HALF_UP)),
            )
        )
    return json.dumps(result, ensure_ascii=False)


def conflicts(conn, rows, scope, params):
    if not rows:
        return
    days = {date.fromisoformat(r["work_date"]) for r in rows}
    neighbors = {
        str(d + timedelta(days=i))
        for d in days
        for i in (-1, 0, 1)
        if 0 < d.toordinal() + i <= date.max.toordinal()
    }
    others = conn.execute(
        f"""SELECT id,work_date,shift_number,time_from,time_to,employee,assigned_employee,
        equipment,assigned_equipment,registration_number,assigned_registration_number FROM shifts
        WHERE deleted=0 AND work_date IN (SELECT value FROM json_each(?)) AND {scope}""",
        (json.dumps(sorted(neighbors)), *params),
    )

    def resources(r):
        out = set()
        for kind, fields in (
            ("сотрудник", ("employee", "assigned_employee")),
            ("техника", ("equipment", "assigned_equipment")),
            ("госномер", ("registration_number", "assigned_registration_number")),
        ):
            for field in fields:
                if v := r.get(field):
                    value = v.casefold()
                    out.add((kind, "".join(c for c in value if c.isalnum()) if kind == "госномер" else value))
        return out

    def span(r):
        a = datetime.fromisoformat(r["work_date"] + "T" + r["time_from"])
        b = datetime.fromisoformat(r["work_date"] + "T" + r["time_to"])
        return (a, b if b > a else b + timedelta(days=1))

    by_resource = {}
    for other in map(dict, others):
        for key in resources(other):
            by_resource.setdefault(key, []).append(other)
    for row in rows:
        warnings = set()
        for key in resources(row):
            for other in by_resource.get(key, ()):
                if row.get("id") == other["id"]:
                    continue
                if row.get("time_from") and other["time_from"]:
                    a, b = span(row)
                    x, y = span(other)
                    overlap = a < y and x < b
                else:
                    overlap = (
                        row["work_date"] == other["work_date"] and row["shift_number"] == other["shift_number"]
                    )
                if overlap:
                    warnings.add(key[0])
                    break
        row["conflict"] = ", ".join(sorted(warnings))


def printable_shift(row, status):
    fields = [
        ("Статус", status),
        ("Дата", row["work_date"]),
        ("Номер смены", row["shift_number"]),
        ("Организация", row["organization"]),
        ("Объект", row["workplace"]),
        ("Контрагент", row["partner"]),
        ("Путевой лист", row["waybill_number"]),
        ("Фактически работал", row["employee_name"]),
        ("Фактическая техника", row["equipment"]),
        ("Госномер фактической техники", row["registration_number"]),
        ("Первоначально назначен", row["assigned_employee_name"]),
        ("Техника по назначению", row["assigned_equipment"]),
        ("Госномер по назначению", row["assigned_registration_number"]),
        ("Начало", row["time_from"]),
        ("Окончание", row["time_to"]),
        ("Отработано, ч", f"{row['work_minutes'] / 60:g}"),
        ("Комментарий", row["comment"]),
    ]
    pairs = "".join(f"<tr><th>{key}</th><td>{escape(str(value))}</td></tr>" for key, value in fields if value)
    work = "".join(
        f"<li>{escape(item['work'])}: {escape(item['quantity'])} {UNITS[item['unit']]}</li>"
        for item in json.loads(row["work_lines"])
    )
    return (
        f'<!doctype html><html lang="ru"><meta charset="utf-8"><title>Карточка смены №{row["id"]}</title>'
        "<style>body{font:16px system-ui;max-width:48rem;margin:2rem auto}table{width:100%;border-collapse:collapse}"
        "th,td{text-align:left;padding:.4rem;border:1px solid #aaa;overflow-wrap:anywhere}th{width:40%}</style>"
        f"<h1>Карточка смены №{row['id']}</h1><table>{pairs}</table><ul>{work}</ul></html>"
    ).encode("utf-8")
