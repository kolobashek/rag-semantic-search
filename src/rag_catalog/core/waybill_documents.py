"""Fill printable waybills from journal shifts and retain organisation-specific forms."""

import json
import re
import sqlite3
import subprocess
from datetime import date, datetime, timezone
from decimal import Decimal
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
WAYBILL_METRIC_FIELDS = {
    "meter_hours": "Наработка, моточасы", "distance": "Пробег, км", "trips": "Количество ездок",
    "tonnes": "Масса груза, т", "idle_hours": "Простой, ч",
    "odometer_start": "Спидометр при выезде, км", "odometer_end": "Спидометр при возвращении, км",
    "planned_trips": "Ездок по заданию", "actual_trips": "Выполнено ездок", "actual_tonnes": "Перевезено по факту, т",
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


def _template_name(conn, org, key):
    if key in forms.form_catalog():
        return forms.form_catalog()[key]
    row = conn.execute("SELECT name FROM waybill_templates WHERE id=? AND organization_id=?",
                       (_custom_id(key), org)).fetchone()
    if row:
        return row[0]
    raise ValueError("Выберите доступный бланк")


def _retired(conn, org, key):
    return conn.execute("SELECT 1 FROM waybill_template_replacements WHERE organization_id=? AND previous_id=?",
                        (org, key)).fetchone() is not None


def _current_template(conn, org, template_id):
    _template_name(conn, org, template_id)
    while next_version := conn.execute(
        "SELECT new_id FROM waybill_template_replacements WHERE organization_id=? AND previous_id=?", (org, template_id)
    ).fetchone():
        template_id = f"custom:{next_version[0]}"
    return template_id


def _original_template(conn, org, template_id):
    while template_id not in forms.form_catalog():
        row = conn.execute("SELECT source_id FROM waybill_templates WHERE organization_id=? AND id=?",
                           (org, _custom_id(template_id))).fetchone()
        if row is None:
            raise ValueError("Выберите доступный бланк")
        if not row[0]:
            break
        template_id = row[0]
    return template_id


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
    lines = json.loads(shift["work_lines"])
    result = {"fleet_number": fleet, "work_description": describe_work_lines(lines)}
    for unit, field in (("trip", "actual_trips"), ("ton", "actual_tonnes")):
        quantities = [Decimal(line["quantity"]) for line in lines if line["unit"] == unit]
        if quantities:
            result[field] = format(sum(quantities).normalize(), "f")
    return result


def _document_state(conn, shift, template_id):
    if shift["entry_type"] != "vehicle":
        raise ValueError("Для явки без техники путевой лист не оформляется")
    fields = _fields(conn, shift["organization_id"], template_id)
    record = conn.execute("""SELECT revision,fields FROM shift_waybill_documents
        WHERE shift_id=? AND template_id=? ORDER BY revision DESC LIMIT 1""",
                          (shift["id"], template_id)).fetchone()
    if record is None and _retired(conn, shift["organization_id"], template_id):
        raise ValueError("Макет обновлён. Откройте список бланков заново.")
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


def _manual_fields(conn, org, template_id, state, values):
    editable = state["values"].keys() - WAYBILL_SHIFT_FIELDS.keys()
    if not isinstance(values, dict) or values.keys() - editable:
        raise ValueError("Общие реквизиты изменяются в карточке смены")
    manual = {key: values.get(key, state["values"][key]) for key in editable}
    if any(not isinstance(v, str) or len(v) > 2000 or ILLEGAL_CHARACTERS_RE.search(v)
           or re.search(r"[\ud800-\udfff\ufffe\uffff]", v) for v in manual.values()):
        raise ValueError("Проверьте текст полей бланка (до 2000 символов)")
    source = _original_template(conn, org, template_id)
    if source in forms.form_catalog():
        forms.validate_values(source, {**state["values"], **manual})
    return manual


def migrate(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS shift_waybill_preferences (
        organization_id TEXT NOT NULL, equipment_key TEXT NOT NULL, partner_key TEXT NOT NULL,
        template_id TEXT NOT NULL, PRIMARY KEY(organization_id,equipment_key,partner_key))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS waybill_templates (
        id INTEGER PRIMARY KEY, organization_id TEXT NOT NULL, name TEXT NOT NULL, name_key TEXT NOT NULL,
        content BLOB NOT NULL, fields TEXT NOT NULL, UNIQUE(organization_id, name_key))""")
    columns = {row[1] for row in conn.execute("PRAGMA table_info(waybill_templates)")}
    if "layout_type" not in columns:
        conn.execute("ALTER TABLE waybill_templates ADD COLUMN layout_type TEXT NOT NULL DEFAULT 'legacy'")
    if "source_id" not in columns:
        conn.execute("ALTER TABLE waybill_templates ADD COLUMN source_id TEXT NOT NULL DEFAULT ''")
    conn.execute("""CREATE TABLE IF NOT EXISTS waybill_template_replacements (
        organization_id TEXT NOT NULL, previous_id TEXT NOT NULL, new_id INTEGER NOT NULL REFERENCES waybill_templates(id),
        PRIMARY KEY (organization_id, previous_id))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS shift_waybill_documents (
        shift_id INTEGER NOT NULL REFERENCES shifts(id), template_id TEXT NOT NULL,
        revision INTEGER NOT NULL, fields TEXT NOT NULL, updated_by TEXT NOT NULL, updated_at TEXT NOT NULL,
        PRIMARY KEY (shift_id, template_id, revision))""")


def register_details(conn, shift_id):
    """Return agreed values only, when several saved forms refer to the same shift."""
    mapping = {"work_description": "work", "fuel_used": "fuel_used", "fuel_filled": "fuel_filled",
               "trips": "trips", "actual_trips": "trips"}
    candidates = {field: set() for field in mapping.values()}
    records = conn.execute("""SELECT d.fields,d.template_id,s.organization_id FROM shift_waybill_documents AS d
        JOIN shifts s ON s.id=d.shift_id
        JOIN (SELECT template_id,MAX(revision) AS revision FROM shift_waybill_documents
              WHERE shift_id=? GROUP BY template_id) AS latest
                ON latest.template_id=d.template_id AND latest.revision=d.revision
        WHERE d.shift_id=?""", (shift_id, shift_id))
    for record in records:
        manual = json.loads(record[0])
        for key, target in mapping.items():
            if value := manual.get(key, "").strip():
                if key == "trips":
                    source = record[1]
                    if source.startswith("custom:"):
                        source = _original_template(conn, record[2], source)
                    if source == "truck_4s":
                        continue  # The front of 4-С records the assignment.
                candidates[target].add(value)
    return {key: next(iter(values)) for key, values in candidates.items() if len(values) == 1}


class WaybillDocuments:
    def __init__(self, journal):
        self.journal = journal

    def templates(self, token, organization_id, shift_id=None):
        _scope(self.journal, token, organization_id)
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            rows = conn.execute("SELECT id,name FROM waybill_templates WHERE organization_id=? ORDER BY name",
                                (organization_id,))
            names = {**forms.form_catalog(), **{f"custom:{r['id']}": r["name"] for r in rows}}
            retired = {r[0] for r in conn.execute(
                "SELECT previous_id FROM waybill_template_replacements WHERE organization_id=?", (organization_id,))}
            available = {key: label for key, label in names.items() if key not in retired}
            if shift_id is not None:
                shift = self.journal._row(conn, self.journal._user(token), shift_id)
                if shift["organization_id"] != organization_id:
                    raise PermissionError("Смена другой организации")
                for r in conn.execute("SELECT DISTINCT template_id FROM shift_waybill_documents WHERE shift_id=?", (shift_id,)):
                    if r[0] in retired:
                        available[r[0]] = names[r[0]] + " (прежний макет)"
            return available

    def _new_name(self, token, organization_id, name, replace=None):
        _scope(self.journal, token, organization_id, manage=True)
        name = str(name or "").strip()
        if (not name or name.casefold() == "history" or name.strip("'") != name
                or re.search(r"[\\/*?:\[\]\x00-\x1f\ud800-\udfff\ufffe\uffff]", name)
                or len(name.encode("utf-16-le")) > 62):
            raise ValueError("Название листа должно содержать от 1 до 31 допустимого для Excel символа")
        if name.casefold() in {s.casefold() for s in forms.form_catalog().values()}:
            with self.journal._connection() as conn:
                if not replace or name.casefold() != _template_name(conn, organization_id, replace).casefold():
                    raise ValueError("Такой бланк уже есть")
        return name

    @staticmethod
    def _builder_fields(auto_fields, extra_labels, metric_fields):
        if not isinstance(auto_fields, list) or any(not isinstance(k, str) or k not in WAYBILL_SHIFT_FIELDS for k in auto_fields):
            raise ValueError("Выберите поля смены из списка")
        if not isinstance(extra_labels, list) or any(not isinstance(v, str) or len(v.strip()) > 80
                                                      or ILLEGAL_CHARACTERS_RE.search(v) for v in extra_labels):
            raise ValueError("Название дополнительного поля должно содержать до 80 символов")
        fields = {k: WAYBILL_SHIFT_FIELDS[k] for k in auto_fields}
        if metric_fields is not None:
            if not isinstance(metric_fields, list) or any(not isinstance(k, str) or k not in WAYBILL_METRIC_FIELDS for k in metric_fields):
                raise ValueError("Выберите показатели из списка")
            fields.update({k: WAYBILL_METRIC_FIELDS[k] for k in metric_fields})
        for index, label in enumerate(extra_labels, 1):
            if label.strip():
                fields[f"field_{index}"] = label.strip()
        if not 1 <= len(fields) <= 100:
            raise ValueError("Добавьте от 1 до 100 полей")
        return fields

    def create_template(self, token, organization_id, name, auto_fields, extra_labels, *,
                        metric_fields=None, replace=None, copy_from=None):
        name = self._new_name(token, organization_id, name, replace)
        fields = self._builder_fields(auto_fields, extra_labels, metric_fields)
        return self._store_template(token, organization_id, name, templates.new_template(name, fields), fields,
                                    "builder", replace, copy_from)

    def preview_builder(self, token, organization_id, name, auto_fields, extra_labels, *, metric_fields=None, replace=None):
        name = self._new_name(token, organization_id, name, replace)
        fields = self._builder_fields(auto_fields, extra_labels, metric_fields)
        book = load_workbook(BytesIO(templates.new_template(name, fields)))
        templates.fill_sheet(book.active, {})
        return templates.workbook_bytes(book)

    def add_template(self, token, organization_id, name, content, *, replace=None, copy_from=None):
        name = self._new_name(token, organization_id, name, replace)
        content, fields = templates.import_template(content, name)
        fields.update({key: WAYBILL_METRIC_FIELDS[key] for key in fields.keys() & WAYBILL_METRIC_FIELDS.keys()})
        if replace or copy_from:
            previous = self.fields(token, organization_id, replace or copy_from)
            fields = {key: previous.get(key, label) for key, label in fields.items()}
        return self._store_template(token, organization_id, name, content, fields, "excel", replace, copy_from)

    def _store_template(self, token, organization_id, name, content, fields, layout_type, replace, copy_from):
        _scope(self.journal, token, organization_id, manage=True)
        try:
            with self.journal._connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if copy_from:
                    if replace:
                        raise ValueError("Выберите обновление существующего макета или создание нового вида")
                    _template_name(conn, organization_id, copy_from)
                if replace:
                    _template_name(conn, organization_id, replace)
                    if _retired(conn, organization_id, replace):
                        raise ValueError("Макет уже изменился. Откройте его заново.")
                    if replace not in forms.form_catalog():
                        conn.execute("UPDATE waybill_templates SET name_key=? WHERE organization_id=? AND id=?",
                                     ("\x00" + replace, organization_id, _custom_id(replace)))
                result = conn.execute("""INSERT INTO waybill_templates
                    (organization_id,name,name_key,content,fields,layout_type,source_id) VALUES(?,?,?,?,?,?,?)""",
                    (organization_id, name, name.casefold(), content, json.dumps(fields), layout_type, replace or copy_from or ""))
                if replace:
                    conn.execute("INSERT INTO waybill_template_replacements VALUES(?,?,?)",
                                 (organization_id, replace, result.lastrowid))
                return f"custom:{result.lastrowid}"
        except sqlite3.IntegrityError as exc:
            raise ValueError("Такой бланк уже есть") from exc

    def book(self, token, organization_id):
        _scope(self.journal, token, organization_id)
        base = forms.form_book()
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            retired = {r[0] for r in conn.execute("SELECT previous_id FROM waybill_template_replacements WHERE organization_id=?",
                                                   (organization_id,))}
            rows = conn.execute("""SELECT name,content FROM waybill_templates t WHERE organization_id=?
                AND NOT EXISTS (SELECT 1 FROM waybill_template_replacements r
                  WHERE r.organization_id=t.organization_id AND r.previous_id='custom:' || t.id) ORDER BY name""",
                                (organization_id,)).fetchall()
        if not rows:
            return base
        book = load_workbook(BytesIO(base))
        for key, name in forms.form_catalog().items():
            if key in retired:
                book.remove(book[name])
        for row in rows:
            custom = load_workbook(BytesIO(row["content"]))
            target = templates.append_sheet(book, custom.active, row["name"])
            templates.fill_sheet(target, {})
        return templates.workbook_bytes(book)

    def fields(self, token, organization_id, template_id):
        _scope(self.journal, token, organization_id)
        with self.journal._connection() as conn:
            return _fields(conn, organization_id, template_id)

    def editable_template(self, token, organization_id, template_id):
        _scope(self.journal, token, organization_id, manage=True)
        if template_id in forms.form_catalog():
            return forms.editable_form(template_id)
        with self.journal._connection() as conn:
            row = conn.execute("SELECT content FROM waybill_templates WHERE organization_id=? AND id=?",
                               (organization_id, _custom_id(template_id))).fetchone()
            if row is None:
                raise ValueError("Выберите доступный бланк")
            return row[0]

    def template_editor(self, token, organization_id, template_id):
        _scope(self.journal, token, organization_id, manage=True)
        with self.journal._connection() as conn:
            if _retired(conn, organization_id, template_id):
                raise ValueError("Макет уже изменился. Откройте его заново.")
            name = _template_name(conn, organization_id, template_id)
            fields = _fields(conn, organization_id, template_id)
            kind = "excel"
            if template_id not in forms.form_catalog():
                kind = conn.execute("SELECT layout_type FROM waybill_templates WHERE organization_id=? AND id=?",
                                    (organization_id, _custom_id(template_id))).fetchone()[0]
        if kind == "builder":
            return dict(id=template_id, name=name, builder=True, fields=fields)
        sheet = load_workbook(BytesIO(self.editable_template(token, organization_id, template_id))).active
        builder = kind == "legacy" and templates.matches_builder(sheet, fields)
        cells = {c.coordinate: str(c.value) for c in sheet._cells.values() if c.value is not None}
        return dict(id=template_id, name=name, builder=builder, fields=fields, cells=cells)

    def revise_cells(self, token, organization_id, template_id, changes, *, copy_name=None):
        _scope(self.journal, token, organization_id, manage=True)
        with self.journal._connection() as conn:
            name = _template_name(conn, organization_id, template_id)
        content = self.editable_template(token, organization_id, template_id)
        if copy_name is not None:
            edited = templates.edit_cells(content, changes) if changes else content
            return self.add_template(token, organization_id, copy_name, edited, copy_from=template_id)
        edited = templates.edit_cells(content, changes)
        return self.add_template(token, organization_id, name, edited, replace=template_id)

    def preview_cells(self, token, organization_id, template_id, changes):
        content = self.editable_template(token, organization_id, template_id)
        if changes:
            content = templates.edit_cells(content, changes)
        book = load_workbook(BytesIO(content))
        templates.fill_sheet(book.active, {})
        return templates.workbook_bytes(book)

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

    def usage(self, token, organization_id, template_id):
        _scope(self.journal, token, organization_id)
        if template_id in forms.form_catalog():
            return forms.form_usage(template_id)
        with self.journal._connection() as conn:
            fields = _fields(conn, organization_id, template_id)
            original = _original_template(conn, organization_id, template_id)
        measures = forms.fields_usage(fields)
        if original in ("truck_4p", "truck_4s"):
            base = "повременная 4-П" if original == "truck_4p" else "сдельная 4-С"
            about = f"Основа макета — {base}."
            return about + (" Показатели — " + measures + "." if measures else "")
        return "Показатели формы — " + measures + "." if measures else ""

    def preference(self, token, shift_id):
        user = self.journal._user(token)
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            shift = self.journal._row(conn, user, shift_id)
            if shift["entry_type"] != "vehicle":
                return None
            record = conn.execute("""SELECT template_id FROM shift_waybill_preferences
                WHERE organization_id=? AND equipment_key=? AND partner_key=?""",
                                  (shift["organization_id"], shift["equipment_key"], shift["partner"].casefold())).fetchone()
            return _current_template(conn, shift["organization_id"], record[0]) if record else None

    def set_preference(self, token, shift_id, template_id, *, expected_pair=None):
        user = self.journal._user(token)
        with self.journal._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            shift = self.journal._row(conn, user, shift_id)
            org = shift["organization_id"]
            if not _manager(user, org):
                raise PermissionError("Бланк по умолчанию выбирает диспетчер организации")
            if shift["entry_type"] != "vehicle":
                raise ValueError("Для явки без техники путевой лист не оформляется")
            if expected_pair is not None and expected_pair != (shift["equipment"], shift["partner"]):
                raise ValueError("Техника или контрагент изменились. Откройте путевой лист заново.")
            key = (org, shift["equipment_key"], shift["partner"].casefold())
            if template_id is None:
                conn.execute("""DELETE FROM shift_waybill_preferences
                    WHERE organization_id=? AND equipment_key=? AND partner_key=?""", key)
                return
            current = _current_template(conn, org, template_id)
            if current != template_id:
                raise ValueError("Макет обновлён. Выберите актуальную версию.")
            conn.execute("""INSERT INTO shift_waybill_preferences(organization_id,equipment_key,partner_key,template_id)
                VALUES(?,?,?,?) ON CONFLICT(organization_id,equipment_key,partner_key) DO UPDATE SET
                template_id=excluded.template_id""", (*key, current))

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
            manual = _manual_fields(conn, shift["organization_id"], template_id, state, values)
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

    def preview(self, token, shift_id, template_id, values, revision, shift_revision):
        user = self.journal._user(token)
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            shift = self.journal._row(conn, user, shift_id)
            state = _document_state(conn, shift, template_id)
            _check_revision(shift_revision, state["shift_revision"])
            _check_revision(revision, state["revision"])
            manual = _manual_fields(conn, shift["organization_id"], template_id, state, values)
            return _render(conn, shift["organization_id"], template_id, {**state["values"], **manual})

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
