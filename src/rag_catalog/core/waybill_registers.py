"""Printable monthly statements of vehicle services agreed with a counterparty."""

import calendar
import hashlib
import json
import math
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from io import BytesIO
from itertools import groupby

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.properties import PageSetupProperties

from .shift_operations import describe_work_lines
from .shift_payroll import period


def migrate(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS shift_waybill_registers (
        organization_id TEXT NOT NULL, month TEXT NOT NULL, partner TEXT NOT NULL,
        organization TEXT NOT NULL, revision INTEGER NOT NULL, rows_json TEXT NOT NULL,
        PRIMARY KEY (organization_id,month,partner))""")


def _dates(month):
    first = period(month)
    return first, first.replace(day=calendar.monthrange(first.year, first.month)[1])


_ELIGIBLE = """organization_id=? AND work_date BETWEEN ? AND ?
               AND deleted=0 AND status='approved' AND entry_type='vehicle'"""

_TEXT = {"waybill_number": 200, "equipment": 200, "registration_number": 32, "workplace": 200,
         "employee_name": 200, "work": 32767}
_NUMBERS = {"hours": (24, 18), "duty_hours": (24, 18), "trips": (9999999, 0),
            "fuel_used": (9999999, 3), "fuel_filled": (9999999, 3),
            "rate_with_vat": (999999999, 2), "duty_rate_with_vat": (999999999, 2)}
_CONFLICT = "Реестр изменился. Откройте его заново перед сохранением или печатью."


def _number(key, value):
    if value in (None, "") and key != "hours":
        return None
    maximum, places = _NUMBERS[key]
    try:
        result = Decimal(str(value).replace(",", "."))
        if not result.is_finite() or not 0 <= result <= maximum or result != round(result, places):
            raise ValueError
        return int(result) if not places else float(result)
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"Проверьте значение в графе «{_LABELS[key]}».") from None


def _clean(row, month):
    try:
        day = date.fromisoformat(str(row["work_date"]))
        shift = Decimal(str(row["shift_number"]))
        if day.isoformat()[:7] != month or shift not in (1, 2, 3):
            raise ValueError
    except (KeyError, TypeError, ValueError, InvalidOperation):
        raise ValueError("Дата должна быть в выбранном месяце, номер смены — от 1 до 3.") from None
    clean = {"shift_id": row["shift_id"], "work_date": day.isoformat(), "shift_number": int(shift)}
    for key, limit in _TEXT.items():
        value = row.get(key) or ""
        if not isinstance(value, str) or len(value) > limit or any(ord(c) < 32 and c not in "\t\n\r" for c in value):
            raise ValueError(f"Проверьте значение в графе «{_LABELS[key]}».")
        clean[key] = value.strip()
    for key in _NUMBERS:
        clean[key] = _number(key, row.get(key))
    return clean


def _source(conn, org, month, partner):
    from .waybill_documents import equipment_parts, register_details

    rows = []
    issuers = set()
    for shift in conn.execute(f"""SELECT id, organization, work_date, waybill_number, equipment, registration_number,
            workplace, employee_name, shift_number, work_minutes, work_lines
            FROM shifts WHERE {_ELIGIBLE} AND partner=?
            ORDER BY equipment, registration_number, work_date, shift_number, id""",
            (org, *(day.isoformat() for day in _dates(month)), partner)):
        issuers.add(shift["organization"])
        lines = json.loads(shift["work_lines"])
        trip_lines = [int(Decimal(line["quantity"])) for line in lines if line["unit"] == "trip"]
        worked = describe_work_lines(lines)
        details = register_details(conn, shift["id"])
        model, fleet = equipment_parts(shift["equipment"], shift["registration_number"])
        row = {key: shift[key] for key in ("work_date", "waybill_number", "registration_number",
                                          "workplace", "employee_name", "shift_number")}
        row.update(shift_id=shift["id"], equipment=f"{model} · борт {fleet}" if fleet else model,
                   hours=shift["work_minutes"] / 60, work=details.get("work") or worked,
                   trips=sum(trip_lines) if trip_lines else None, duty_hours=None, fuel_used=None,
                   fuel_filled=None, rate_with_vat=None, duty_rate_with_vat=None)
        for key in ("fuel_used", "fuel_filled", "trips"):
            if details.get(key) not in (None, "") and row[key] is None:
                try:
                    row[key] = _number(key, details[key])
                except ValueError:
                    pass  # Free-text fields on a printed form may contain units or explanatory notes.
        projection = json.dumps([shift["organization"], row], ensure_ascii=False, sort_keys=True)
        row["origin_revision"] = hashlib.sha256(projection.encode()).hexdigest()
        rows.append(row)
    if len({name.casefold() for name in issuers}) > 1:
        raise ValueError("Смены оформлены от разных исполнителей. Уточните организацию в журнале смен.")
    return next(iter(issuers), ""), rows


class WaybillRegisters:
    def __init__(self, journal):
        self.journal = journal

    def _authorize(self, token, org):
        if org not in self.journal.organizations(token, manage=True):
            raise PermissionError("Для реестра нужен доступ диспетчера к организации.")

    def partners(self, token, organization_id, month):
        self._authorize(token, organization_id)
        with self.journal._connection() as conn:
            return self._partners(conn, organization_id, month)

    @staticmethod
    def _partners(conn, org, month):
        partners = conn.execute(f"""SELECT partner FROM shifts WHERE {_ELIGIBLE} AND partner<>''
            UNION SELECT partner FROM shift_waybill_registers WHERE organization_id=? AND month=?""",
            (org, *(day.isoformat() for day in _dates(month)), org, month))
        return sorted((r[0] for r in partners), key=str.casefold)

    @staticmethod
    def _read(conn, org, month, partner):
        _dates(month)
        if not isinstance(partner, str) or not partner.strip() or len(partner) > 200:
            raise ValueError("Выберите контрагента.")
        saved = conn.execute("""SELECT organization, revision, rows_json FROM shift_waybill_registers
            WHERE organization_id=? AND month=? AND partner=?""", (org, month, partner)).fetchone()
        if saved:
            issuer, revision, raw = saved
            rows = json.loads(raw)
        else:
            issuer, rows = _source(conn, org, month, partner)
            revision = 0
            if not rows:
                raise ValueError("У контрагента за этот месяц нет подтверждённых смен с техникой.")
        return dict(organization_id=org, month=month, organization=issuer, partner=partner, revision=revision, rows=rows)

    def open(self, token, organization_id, month, partner):
        self._authorize(token, organization_id)
        with self.journal._connection() as conn:
            return self._read(conn, organization_id, month, partner)

    @staticmethod
    def _write(conn, doc, rows):
        revision = doc["revision"] + 1
        conn.execute("""INSERT INTO shift_waybill_registers(organization_id,month,partner,organization,revision,rows_json)
            VALUES(?,?,?,?,?,?) ON CONFLICT(organization_id,month,partner) DO UPDATE SET
                revision=excluded.revision, rows_json=excluded.rows_json""",
            (doc["organization_id"], doc["month"], doc["partner"], doc["organization"], revision,
             json.dumps(rows, ensure_ascii=False, allow_nan=False)))
        return dict(doc, revision=revision, rows=rows)

    def save(self, token, organization_id, month, partner, revision, rows):
        self._authorize(token, organization_id)
        with self.journal._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            doc = self._read(conn, organization_id, month, partner)
            if type(revision) is not int or revision != doc["revision"]:
                raise ValueError(_CONFLICT)
            if not isinstance(rows, list) or any(not isinstance(r, dict) or type(r.get("shift_id")) is not int for r in rows):
                raise ValueError("Некорректные строки реестра.")
            if sorted(r["shift_id"] for r in rows) != sorted(r["shift_id"] for r in doc["rows"]):
                raise ValueError("Состав смен изменился. Откройте реестр заново.")
            sources = {r["shift_id"]: r.get("origin_revision") for r in doc["rows"]}
            if not revision and any(row.get("origin_revision") != sources[row["shift_id"]] for row in rows):
                raise ValueError("Исходные путевые изменились. Откройте реестр заново.")
            cleaned = [dict(_clean(row, month), origin_revision=sources[row["shift_id"]]) for row in rows]
            return self._write(conn, doc, cleaned)

    def refresh(self, token, organization_id, month, partner, revision):
        self._authorize(token, organization_id)
        with self.journal._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            doc = self._read(conn, organization_id, month, partner)
            if type(revision) is not int or revision != doc["revision"]:
                raise ValueError(_CONFLICT)
            issuer, source = _source(conn, organization_id, month, partner)
            if source and issuer.casefold() != doc["organization"].casefold():
                raise ValueError("Организация новых смен отличается от исполнителя сохранённого реестра.")
            known = {r["shift_id"] for r in doc["rows"]}
            additional = [r for r in source if r["shift_id"] not in known]
            if revision and not additional:
                return doc
            return self._write(conn, doc, doc["rows"] + additional)

    def xlsx(self, token, organization_id, month, partner, *, revision=None):
        doc = self.open(token, organization_id, month, partner)
        if revision is not None and revision != doc["revision"]:
            raise ValueError(_CONFLICT)
        return _workbook([doc], monthly=False)

    def month_book(self, token, organization_id, month):
        self._authorize(token, organization_id)
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            documents = [self._read(conn, organization_id, month, partner)
                         for partner in self._partners(conn, organization_id, month)]
        if not documents:
            raise ValueError("За этот месяц нет реестров и подтверждённых смен с контрагентом.")
        if len({doc["organization"].casefold() for doc in documents}) > 1:
            raise ValueError("Смены оформлены от разных исполнителей. Уточните организацию в журнале смен.")
        return _workbook(documents, monthly=True)


_LAYOUT = [
    ("work_date", "Дата п/л", 13), ("waybill_number", "№ п/л", 12), ("shift_number", "Смена", 8),
    ("equipment", "Марка техники", 24), ("registration_number", "Госномер", 14),
    ("employee_name", "Водитель", 22), ("workplace", "Объект", 22), ("work", "Выполненная работа", 30),
    ("hours", "Отработано, ч", 13), ("duty_hours", "Техдежурство, ч", 14),
    ("trips", "Рейсы", 9), ("fuel_used", "Расход топлива, л", 13), ("fuel_filled", "Заправка, л", 13),
    ("rate_with_vat", "Тариф за час с НДС, руб.", 15), ("work_amount", "Работа с НДС, руб.", 15),
    ("duty_rate_with_vat", "Тариф техдежурства за час с НДС, руб.", 15), ("duty_amount", "Техдежурство с НДС, руб.", 15),
    ("amount", "Стоимость с НДС, руб.", 16),
]
_LABELS = {key: title for key, title, _ in _LAYOUT}
_AMOUNTS = {"amount", "work_amount", "duty_amount"}


def _literal(cell, value):
    cell.value = ILLEGAL_CHARACTERS_RE.sub(" ", value) if isinstance(value, str) else value
    if isinstance(value, str):
        cell.data_type = "s"  # Codes and user-entered labels are always literal Excel text.


def _summary_row(ws, row, title, start, end, layout, letters):
    first_quantity = next(i for i, (key, _, _) in enumerate(layout, 1) if key == "hours")
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=first_quantity-1)
    _literal(ws.cell(row, 1), title)
    ws.cell(row, 1).alignment = Alignment(wrap_text=True, vertical="center")
    dates = f'{letters["work_date"]}{start}:{letters["work_date"]}{end}'
    for i, (key, _, _) in enumerate(layout, 1):
        cell = ws.cell(row, i)
        if key in {"hours", "duty_hours", "trips", "fuel_used", "fuel_filled"} | _AMOUNTS:
            span = f"{letters[key]}{start}:{letters[key]}{end}"
            # Dates are numeric on detail rows. Subtotal labels are text and cannot be counted twice.
            summed = f'SUMIF({dates},">0",{span})'
            cell.value = f'=IF(COUNTBLANK({span}),"",{summed})' if key in _AMOUNTS else f'=IF(COUNT({span}),{summed},"")'
            cell.number_format = '#,##0.00' if key in _AMOUNTS else "0.###"
        cell.font = Font(name="Arial", size=10, bold=True)
        cell.fill = PatternFill("solid", fgColor="DCE7F0")
    ws.row_dimensions[row].height = 25


def _charge(hours, rate):
    return f'=IF({hours}=0,0,IF({rate}="","",ROUND({hours}*{rate},2)))'


def _sheet_title(partner, used):
    base = re.sub(r"[\\/:?*\[\]\x00-\x1f]+", " ", partner).strip(" '") or "Контрагент"
    i = 1
    while True:
        suffix = "" if i == 1 else f" ({i})"
        # Excel limits sheet names to 31 UTF-16 units, including any collision suffix.
        head = base.encode("utf-16-le")[:(31-len(suffix))*2].decode("utf-16-le", errors="ignore").rstrip(" '")
        name = head + suffix
        if name.casefold() not in used:
            used.add(name.casefold())
            return name
        i += 1


def _workbook(documents, *, monthly):
    book = Workbook()
    book.remove(book.active)
    used = {"history"}  # Reserved worksheet name in Excel.
    for doc in documents:
        title = _sheet_title(doc["partner"], used) if monthly else "Реестр"
        _write_sheet(book.create_sheet(title), doc)
    out = BytesIO()
    book.save(out)
    return out.getvalue()


def _write_sheet(ws, doc):
    rows = sorted(doc["rows"], key=lambda r: (r["equipment"].casefold(), r["registration_number"].casefold(),
                                               r["work_date"], r["shift_number"], r["shift_id"]))
    required = {"work_date", "waybill_number", "shift_number", "equipment", "employee_name", "hours"}
    included = {key for key, _, _ in _LAYOUT if key in required or any(r.get(key) not in ("", None) for r in rows)}
    if included & {"rate_with_vat", "duty_rate_with_vat"}:
        included.update({"rate_with_vat", "amount"})
        if included & {"duty_hours", "duty_rate_with_vat"}:
            included.update({"duty_hours", "duty_rate_with_vat", "work_amount", "duty_amount"})
    layout = [column for column in _LAYOUT if column[0] in included]
    letters = {key: get_column_letter(i) for i, (key, _, _) in enumerate(layout, 1)}
    width = len(layout)
    first, last = _dates(doc["month"])
    for i, title in enumerate((f"Реестр путевых листов с {first:%d.%m.%Y} по {last:%d.%m.%Y}",
                               f'Исполнитель: {doc["organization"]}', f'Заказчик: {doc["partner"]}'), 1):
        ws.merge_cells(start_row=i, start_column=1, end_row=i, end_column=width)
        _literal(ws.cell(i, 1), title)
        ws.cell(i, 1).font = Font(name="Arial", size=15 if i == 1 else 11, bold=i == 1)
        ws.cell(i, 1).alignment = Alignment(vertical="center", wrap_text=True)
        ws.row_dimensions[i].height = 30
    for col, (key, title, size) in enumerate(layout, 1):
        cell = ws.cell(5, col, title)
        cell.font = Font(name="Arial", size=10, bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.fill = PatternFill("solid", fgColor="DCE7F0")
        ws.column_dimensions[letters[key]].width = size
    ws.row_dimensions[5].height = 60 if "duty_rate_with_vat" in included else 45
    rule = Side(style="thin", color="A7B2BC")
    groups = [list(items) for _, items in groupby(rows, lambda r: (r["equipment"].casefold(), r["registration_number"].casefold()))]
    next_row = 6
    positions = []
    for group in groups:
        start = next_row
        positions.extend(enumerate(group, start))
        next_row += len(group)
        if len(groups) > 1:
            machine = " · ".join(filter(None, (group[0]["equipment"], group[0]["registration_number"])))
            _summary_row(ws, next_row, f"Итого по технике: {machine}", start, next_row-1, layout, letters)
            next_row += 1
    for i, row in positions:
        height = 22
        for col, (key, _, size) in enumerate(layout, 1):
            cell = ws.cell(i, col)
            if key == "amount" and "duty_amount" in included:
                paid, duty = letters["work_amount"]+str(i), letters["duty_amount"]+str(i)
                cell.value = f'=IF(COUNT({paid},{duty})=2,{paid}+{duty},"")'
            elif key in {"amount", "work_amount"}:
                cell.value = _charge(letters["hours"]+str(i), letters["rate_with_vat"]+str(i))
            elif key == "duty_amount":
                cell.value = _charge(letters["duty_hours"]+str(i), letters["duty_rate_with_vat"]+str(i))
            elif key == "work_date":
                cell.value = date.fromisoformat(row[key])
                cell.number_format = "dd.mm.yyyy"
            else:
                value = row[key]
                _literal(cell, value)
                if isinstance(value, str):
                    lines = sum(max(1, math.ceil(len(part) / max(1, size - 2))) for part in value.split("\n"))
                    height = max(height, 15 * lines)
            if key in {"rate_with_vat", "duty_rate_with_vat"} | _AMOUNTS:
                cell.number_format = '#,##0.00'
            elif key in _NUMBERS and key != "trips":
                cell.number_format = '0.###'
            cell.border = Border(bottom=rule)
            cell.font = Font(name="Arial", size=10)
            centered = key in {"work_date", "waybill_number", "shift_number", "registration_number"}
            cell.alignment = Alignment(horizontal="center" if centered else None, vertical="center", wrap_text=True)
        ws.row_dimensions[i].height = min(409, height)
    total = next_row
    _summary_row(ws, total, "Всего", 6, total - 1, layout, letters)
    split = width // 2 + 1
    for col, end, role in ((1, split - 1, "Исполнитель"), (split, width, "Заказчик")):
        ws.merge_cells(start_row=total+2, start_column=col, end_row=total+2, end_column=end)
        ws.cell(total+2, col, f"{role}  ____________________ / ____________________").alignment = Alignment(wrap_text=True)
    ws.row_dimensions[total+2].height = 35
    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "A6"
    ws.print_title_rows = "1:5"
    ws.print_area = f"A1:{get_column_letter(width)}{total+2}"
    ws.page_setup.orientation = "landscape"
    ws.page_setup.paperSize = ws.PAPERSIZE_A3 if sum(c[2] for c in layout) > 185 else ws.PAPERSIZE_A4
    ws.sheet_properties.pageSetUpPr = PageSetupProperties(fitToPage=True)
    ws.page_setup.fitToWidth, ws.page_setup.fitToHeight = 1, 0
    ws.page_margins.left = ws.page_margins.right = 0.3
    ws.page_margins.top = ws.page_margins.bottom = 0.45
    ws.oddFooter.right.text = "Стр. &P из &N"
