"""Fill printable waybills from journal shifts and retain organisation-specific forms."""

import json
import re
import sqlite3
import subprocess
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

from openpyxl import load_workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

from . import waybill_forms as forms
from . import waybill_templates as templates
from .shift_operations import describe_work_lines
from .work_shifts import _manager

WAYBILL_SHIFT_FIELDS = {
    "work_date": "Дата", "shift_number": "Номер смены", "organization": "Организация",
    "employee_name": "Фактически работал", "equipment": "Техника", "registration_number": "Госномер",
    "workplace": "Объект", "partner": "Контрагент", "waybill_number": "Номер путевого листа",
    "time_from": "Начало смены", "time_to": "Окончание смены", "hours": "Отработано, ч",
    "breaks": "Перерывы, ч", "comment": "Комментарий",
}


def print_pdf(xlsx: bytes) -> bytes:
    from .extractors.files import _libreoffice_environment, _resolve_soffice

    executable = _resolve_soffice()
    if not executable:
        raise ValueError("Для печати PDF установите LibreOffice Calc на сервере. Бланк также можно распечатать из Excel.")
    with TemporaryDirectory(prefix="waybill-") as temp:
        folder = Path(temp).resolve()
        source = folder / "document.xlsx"
        source.write_bytes(xlsx)
        try:
            result = subprocess.run(
                [executable, "-env:UserInstallation=" + (folder / "profile").as_uri(),
                 "--headless", "--norestore", "--convert-to", "pdf:calc_pdf_Export",
                 "--outdir", str(folder), str(source)],
                cwd=folder, env=_libreoffice_environment(), capture_output=True, timeout=90,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ValueError("LibreOffice не смог подготовить PDF. Попробуйте снова или скачайте Excel.") from exc
        pdf = source.with_suffix(".pdf")
        if result.returncode or not pdf.is_file():
            raise ValueError("Не удалось подготовить PDF. Проверьте установку LibreOffice Calc.")
        return pdf.read_bytes()


def _scope(journal, token, org, *, manage=False):
    if org not in journal.organizations(token, manage=manage):
        raise PermissionError("Бланки этой организации недоступны")


def _custom_id(template_id):
    if isinstance(template_id, str) and re.fullmatch(r"custom:[1-9][0-9]{0,17}", template_id):
        return int(template_id[7:])
    raise ValueError("Выберите доступный бланк")


def _fields(conn, org, template_id):
    if template_id in forms.form_catalog():
        return forms.form_fields(template_id)
    row = conn.execute("SELECT fields FROM waybill_templates WHERE organization_id=? AND id=?",
                       (org, _custom_id(template_id))).fetchone()
    if row is None:
        raise ValueError("Выберите доступный бланк")
    return {key: WAYBILL_SHIFT_FIELDS.get(key, label) for key, label in json.loads(row[0]).items()}


def _render(conn, org, template_id, values):
    if template_id in forms.form_catalog():
        return forms.render_form(template_id, values)
    row = conn.execute("SELECT content FROM waybill_templates WHERE organization_id=? AND id=?",
                       (org, _custom_id(template_id))).fetchone()
    if row is None:
        raise ValueError("Выберите доступный бланк")
    book = load_workbook(BytesIO(row[0]))
    if values.get("work_date"):
        values = dict(values, work_date=date.fromisoformat(values["work_date"]).strftime("%d.%m.%Y"))
    templates.fill_sheet(book.active, values)
    return templates.workbook_bytes(book)


def equipment_parts(label, registration_number):
    # Only remove the suffixes written by WorkShiftJournal.save_reference.
    model, separator, fleet = label.rpartition(" · борт ")
    model = model if separator else label
    if registration_number:
        model = model.removesuffix(" · гос. " + registration_number)
    return model, fleet if separator else ""


def _suggestions(shift):
    _, fleet = equipment_parts(shift["equipment"], shift["registration_number"])
    return {"fleet_number": fleet, "work_description": describe_work_lines(json.loads(shift["work_lines"]))}


def _document_state(conn, shift, template_id):
    if shift["entry_type"] != "vehicle":
        raise ValueError("Для явки без техники путевой лист не оформляется")
    fields = _fields(conn, shift["organization_id"], template_id)
    record = conn.execute("""SELECT revision,fields FROM shift_waybill_documents
        WHERE shift_id=? AND template_id=? ORDER BY revision DESC LIMIT 1""",
                          (shift["id"], template_id)).fetchone()
    manual = json.loads(record["fields"]) if record else {}
    snapshot = {key: shift.get(key, "") for key in WAYBILL_SHIFT_FIELDS}
    snapshot["equipment"], _ = equipment_parts(shift["equipment"], shift["registration_number"])
    defaults = _suggestions(shift)
    snapshot.update(hours=f"{shift['work_minutes'] / 60:.4f}".rstrip("0").rstrip("."),
                    breaks=f"{shift['break_minutes'] / 60:.4f}".rstrip("0").rstrip("."))
    values = {key: str(snapshot.get(key, manual.get(key, defaults.get(key, "")))) for key in fields}
    return dict(values=values, revision=record["revision"] if record else 0, shift_revision=shift["revision"])


def _check_revision(expected, actual):
    if type(expected) is not int or expected != actual:
        raise ValueError("Документ уже изменился. Откройте его заново.")


def migrate(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS waybill_templates (
        id INTEGER PRIMARY KEY, organization_id TEXT NOT NULL, name TEXT NOT NULL, name_key TEXT NOT NULL,
        content BLOB NOT NULL, fields TEXT NOT NULL, UNIQUE(organization_id, name_key))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS shift_waybill_documents (
        shift_id INTEGER NOT NULL REFERENCES shifts(id), template_id TEXT NOT NULL,
        revision INTEGER NOT NULL, fields TEXT NOT NULL, updated_by TEXT NOT NULL, updated_at TEXT NOT NULL,
        PRIMARY KEY (shift_id, template_id, revision))""")


def register_details(conn, shift_id):
    """Return agreed values only, when several saved forms refer to the same shift."""
    mapping = {"work_description": "work", "fuel_used": "fuel_used", "fuel_filled": "fuel_filled", "trips": "trips"}
    candidates = {field: set() for field in mapping.values()}
    records = conn.execute("""SELECT d.fields FROM shift_waybill_documents AS d
        JOIN (SELECT template_id,MAX(revision) AS revision FROM shift_waybill_documents
              WHERE shift_id=? GROUP BY template_id) AS latest USING(template_id,revision)
        WHERE d.shift_id=?""", (shift_id, shift_id))
    for record in records:
        manual = json.loads(record[0])
        for key, target in mapping.items():
            if value := manual.get(key, "").strip():
                candidates[target].add(value)
    return {key: next(iter(values)) for key, values in candidates.items() if len(values) == 1}


class WaybillDocuments:
    def __init__(self, journal):
        self.journal = journal

    def templates(self, token, organization_id):
        _scope(self.journal, token, organization_id)
        with self.journal._connection() as conn:
            rows = conn.execute("SELECT id,name FROM waybill_templates WHERE organization_id=? ORDER BY name",
                                (organization_id,))
            return {**forms.form_catalog(), **{f"custom:{r['id']}": r["name"] for r in rows}}

    def _new_name(self, token, organization_id, name):
        _scope(self.journal, token, organization_id, manage=True)
        name = str(name or "").strip()
        if (not name or name.casefold() == "history" or name.strip("'") != name
                or re.search(r"[\\/*?:\[\]\x00-\x1f\ud800-\udfff\ufffe\uffff]", name)
                or len(name.encode("utf-16-le")) > 62):
            raise ValueError("Название листа должно содержать от 1 до 31 допустимого для Excel символа")
        builtin = load_workbook(BytesIO(forms.form_book()), read_only=True)
        try:
            if name.casefold() in {s.casefold() for s in (*builtin.sheetnames, *forms.form_catalog().values())}:
                raise ValueError("Такой бланк уже есть")
        finally:
            builtin.close()
        return name

    def create_template(self, token, organization_id, name, auto_fields, extra_labels):
        name = self._new_name(token, organization_id, name)
        if not isinstance(auto_fields, list) or any(not isinstance(k, str) or k not in WAYBILL_SHIFT_FIELDS for k in auto_fields):
            raise ValueError("Выберите поля смены из списка")
        if not isinstance(extra_labels, list) or any(not isinstance(v, str) or len(v.strip()) > 80
                                                      or ILLEGAL_CHARACTERS_RE.search(v) for v in extra_labels):
            raise ValueError("Название дополнительного поля должно содержать до 80 символов")
        fields = {k: WAYBILL_SHIFT_FIELDS[k] for k in auto_fields}
        for index, label in enumerate(extra_labels, 1):
            if label.strip():
                fields[f"field_{index}"] = label.strip()
        if not 1 <= len(fields) <= 100:
            raise ValueError("Добавьте от 1 до 100 полей")
        return self._store_template(token, organization_id, name, templates.new_template(name, fields), fields)

    def add_template(self, token, organization_id, name, content):
        name = self._new_name(token, organization_id, name)
        content, fields = templates.import_template(content, name)
        return self._store_template(token, organization_id, name, content, fields)

    def _store_template(self, token, organization_id, name, content, fields):
        _scope(self.journal, token, organization_id, manage=True)
        try:
            with self.journal._connection() as conn:
                result = conn.execute("""INSERT INTO waybill_templates(organization_id,name,name_key,content,fields)
                    VALUES(?,?,?,?,?)""", (organization_id, name, name.casefold(), content, json.dumps(fields)))
                return f"custom:{result.lastrowid}"
        except sqlite3.IntegrityError as exc:
            raise ValueError("Такой бланк уже есть") from exc

    def book(self, token, organization_id):
        _scope(self.journal, token, organization_id)
        base = forms.form_book()
        with self.journal._connection() as conn:
            rows = conn.execute("SELECT name,content FROM waybill_templates WHERE organization_id=? ORDER BY name",
                                (organization_id,)).fetchall()
        if not rows:
            return base
        book = load_workbook(BytesIO(base))
        for row in rows:
            custom = load_workbook(BytesIO(row["content"]))
            target = templates.append_sheet(book, custom.active, row["name"])
            templates.fill_sheet(target, {})
        return templates.workbook_bytes(book)

    def fields(self, token, organization_id, template_id):
        _scope(self.journal, token, organization_id)
        with self.journal._connection() as conn:
            return _fields(conn, organization_id, template_id)

    def blank(self, token, organization_id, template_id):
        _scope(self.journal, token, organization_id)
        with self.journal._connection() as conn:
            return _render(conn, organization_id, template_id, {})

    def issued_templates(self, token, shift_id):
        user = self.journal._user(token)
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            self.journal._row(conn, user, shift_id)
            return [r[0] for r in conn.execute("""SELECT template_id FROM shift_waybill_documents
                WHERE shift_id=? GROUP BY template_id ORDER BY MAX(updated_at) DESC, template_id""", (shift_id,))]

    def draft(self, token, shift_id, template_id):
        user = self.journal._user(token)
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            return _document_state(conn, self.journal._row(conn, user, shift_id), template_id)

    def save(self, token, shift_id, template_id, values, revision, shift_revision):
        user = self.journal._user(token)
        with self.journal._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            shift = self.journal._row(conn, user, shift_id)
            state = _document_state(conn, shift, template_id)
            _check_revision(shift_revision, state["shift_revision"])
            _check_revision(revision, state["revision"])
            editable = state["values"].keys() - WAYBILL_SHIFT_FIELDS.keys()
            if not isinstance(values, dict) or values.keys() - editable:
                raise ValueError("Данные из смены редактируются в журнале смен")
            manual = {key: values.get(key, state["values"][key]) for key in editable}
            if any(not isinstance(v, str) or len(v) > 2000 or ILLEGAL_CHARACTERS_RE.search(v) for v in manual.values()):
                raise ValueError("Проверьте текст полей бланка (до 2000 символов)")
            if template_id in forms.form_catalog():
                forms.validate_values(template_id, {**state["values"], **manual})
            changed = any(v != state["values"][k] for k, v in manual.items())
            if revision and not changed:
                return state
            now = datetime.now(timezone.utc).isoformat()
            defaults = _suggestions(shift)
            overrides = {k: v for k, v in manual.items() if v != defaults.get(k, "")}
            conn.execute("INSERT INTO shift_waybill_documents VALUES(?,?,?,?,?,?)",
                         (shift_id, template_id, revision + 1, json.dumps(overrides, ensure_ascii=False),
                          user["username"], now))
            if changed:
                status = "draft" if shift["status"] == "draft" and _manager(user, shift["organization_id"]) else "submitted"
                conn.execute("UPDATE shifts SET status=?,revision=revision+1,updated_at=? WHERE id=?", (status, now, shift_id))
                self.journal._event(conn, shift_id, user, "edit", "Изменены поля печатного путевого листа")
                state["shift_revision"] += 1
            state["values"].update(manual)
            state["revision"] += 1
            return state

    def export(self, token, shift_id, template_id, revision, shift_revision):
        user = self.journal._user(token)
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            shift = self.journal._row(conn, user, shift_id)
            state = _document_state(conn, shift, template_id)
            _check_revision(shift_revision, state["shift_revision"])
            _check_revision(revision, state["revision"])
            if revision == 0:
                raise ValueError("Сначала сохраните путевой лист")
            return _render(conn, shift["organization_id"], template_id, state["values"])
