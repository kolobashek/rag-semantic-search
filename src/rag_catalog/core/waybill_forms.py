"""Printable Excel waybills based on the dispatcher's forms."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from importlib.resources import files
from io import BytesIO
from xml.etree import ElementTree
from zipfile import ZIP_DEFLATED, ZipFile

from openpyxl import load_workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.utils import coordinate_to_tuple, get_column_letter


@dataclass(frozen=True)
class _Form:
    sheet: str
    front: dict[str, str]
    reverse: dict[str, str]
    reverse_offset: tuple[int, int] = (0, 0)


_LABELS = {
    "waybill_number": "Номер путевого листа",
    "work_date": "Дата путевого листа",
    "valid_until": "Действителен по",
    "organization": "Организация",
    "partner": "Заказчик",
    "equipment": "Марка и модель",
    "registration_number": "Госномер",
    "shift_number": "Смена №",
    "fleet_number": "Гаражный / бортовой номер",
    "employee_name": "ФИО водителя или машиниста",
    "insurance_number": "СНИЛС",
    "licence_number": "Номер удостоверения",
    "licence_date": "Дата выдачи удостоверения",
    "workplace": "Участок, объект или маршрут",
    "destination": "Место назначения",
    "work_description": "Выполненные работы",
    "time_from": "Время выезда",
    "time_to": "Время возвращения",
    "hours": "Отработано часов",
    "idle_hours": "Время простоя",
    "meter_start": "Показание счётчика при выезде",
    "meter_end": "Показание счётчика при возвращении",
    "odometer_start": "Показание спидометра при выезде, км",
    "odometer_end": "Показание спидометра при возвращении, км",
    "meter_hours": "Наработка моточасов",
    "fuel_grade": "Марка топлива",
    "fuel_issued": "Топливо выдано, л",
    "fuel_start": "Остаток при выезде, л",
    "fuel_end": "Остаток при возвращении, л",
    "fuel_used": "Расход топлива, л",
    "distance": "Пробег, км",
    "cargo": "Наименование груза",
    "trips": "Количество ездок",
    "tonnes": "Масса груза, т",
    "planned_trips": "Ездок по заданию",
    "actual_trips": "Выполнено ездок",
    "actual_tonnes": "Перевезено по факту, т",
    "comment": "Особые отметки",
}


def _machine(sheet: str, header_rows: int = 1, *, meter: str = "meter", total: str = "meter_hours") -> _Form:
    # The dozer source has an extra date row above the validity period.
    def row(n: int) -> int:
        return n + header_rows

    bindings = {
        "A1": "Место для штампа предприятия                      ПУТЕВОЙ ЛИСТ № {waybill_number}",
        f"A{row(3)}": "С {work_date}                         Действителен по {valid_until}",
        f"A{row(4)}": "Организация: {organization}",
        f"A{row(6)}": "Марка/модель: {equipment}   Госномер: {registration_number}   Смена № {shift_number}   Бортовой № {fleet_number}",
        f"A{row(7)}": "Водитель: {employee_name}                      СНИЛС: {insurance_number}",
        f"{'F' if header_rows else 'D'}{row(8)}": "Удостоверение № {licence_number}    Дата выдачи {licence_date}",
        f"D{row(14)}": "{time_from}",
        f"D{row(15)}": "{time_to}",
        f"E{row(14)}": "{" + meter + "_start}",
        f"E{row(15)}": "{" + meter + "_end}",
        f"F{row(14)}": "{" + total + "}",
        f"G{row(14)}": "{fuel_grade}",
        f"H{row(14)}": "{fuel_issued}",
        f"J{row(14)}": "{fuel_start}",
        f"K{row(14)}": "{fuel_end}",
        f"L{row(14)}": "{fuel_used}",
        f"A{row(24)}": "{workplace}",
        f"D{row(24)}": "{work_description}",
        f"I{row(24)}": "{idle_hours}",
        f"J{row(24)}": "{hours}",
        f"J{row(33)}": "{comment}",
    }
    if header_rows:
        bindings.update({"F3": "{work_date}", "B6": "{partner}"})
    else:
        bindings["A5"] = "Заказчик: {partner}"
    return _Form(sheet, bindings, {})


_FORMS = {
    "bulldozer": _machine("Бульдозер", 1),
    "grader": _machine("Грейдер", 0),
    "loader": _machine("Погрузчик"),
    "excavator": _machine("Экскаватор"),
    "backhoe": _machine("Экскаватор-погрузчик"),
    "tractor": _machine("Трактор"),
    "crane": _machine("Автокран"),
    "dump_truck": _machine("Самосвал (пробег)", total="distance"),
    "dump_truck_hours": _machine("Самосвал (моточасы)"),
    "truck_4p": _Form(
        "Грузовой автомобиль (4-П)",
        {
            "DA3 X47 DP47": "{waybill_number}", "BJ6 AP47 EH47": "{work_date.day}",
            "BT6 AU47 EM47": "{work_date.month}", "CP6 BQ47 FI47": "{work_date.year}",
            "DH6": "{valid_until.day}", "DQ6": "{valid_until.month}", "EL6": "{valid_until.year}",
            "Q7": "{organization}", "P13": "{equipment}", "Z14": "{registration_number}",
            "CF14": "{fleet_number}", "I15": "{employee_name}", "AB17": "{licence_number}",
            "BH17": "{licence_date}", "Q18": "{insurance_number}", "A33": "{partner}",
            "FJ14": "{time_from}", "FJ15": "{time_to}", "BW33": "{hours}", "CF33": "{planned_trips}", "EX14": "{meter_start}",
            "EX15": "{meter_end}", "CS25": "{fuel_grade}", "DM25": "{fuel_issued}",
            "DW25": "{fuel_start}", "EF25": "{fuel_end}", "EU33": "{comment}",
        },
        {
            "S4 V40": "{partner}", "AB11": "{workplace}", "P36": "{organization}",
            "V38": "{equipment}", "BO38": "{registration_number}",
        },
        (58, 190),
    ),
    "passenger_3": _Form(
        "Легковой автомобиль (3)",
        {
            "BX4": "{waybill_number}", "AD5": "{work_date.day}", "AI5": "{work_date.month}",
            "AU5": "{work_date.year}", "R8": "{organization}", "V10": "{equipment}",
            "AI11": "{registration_number}", "BP11": "{fleet_number}", "M12": "{employee_name}",
            "S14": "{licence_number}", "BQ14": "{licence_date}", "Q15": "{insurance_number}",
            "Q20": "{partner}", "R26": "{workplace}", "BF29": "{fuel_grade}",
            "AE30": "{time_from}", "AG35": "{time_to}", "BT37": "{fuel_start}",
            "BT38": "{fuel_end}", "BT40": "{fuel_used}", "A41": "{comment}",
        },
        {"E5": "{workplace}", "H5": "{destination}", "O5": "{distance}", "E33": "{hours}"},
        (48, 92),
    ),
    "truck_4s": _Form(
        "Грузовой автомобиль (4-С)",
        {
            "CW3": "{waybill_number}", "BG5": "{work_date.day}", "BP5": "{work_date.month}",
            "CL5": "{work_date.year}", "CV5": "по {valid_until}", "Q6": "{organization}",
            "Q7 A35": "{partner}", "Q13": "{equipment}", "AB14": "{registration_number}",
            "CI14": "{fleet_number}", "I15": "{employee_name}", "AA20": "{licence_number}",
            "BT20": "{licence_date}", "Q21": "{insurance_number}", "FH14": "{time_from}",
            "FH15": "{time_to}", "EV14": "{meter_start}", "EV15": "{meter_end}",
            "CX24": "{fuel_grade}", "DO24": "{fuel_issued}", "DX24": "{fuel_start}",
            "EG24": "{fuel_end}", "AT35": "{workplace}", "CE35": "{destination}",
            "DP35": "{cargo}", "FB35": "{trips}", "FV35": "{tonnes}",
        },
        {"A7": "{workplace}", "A22": "{comment}", "I36": "{fuel_used}", "R36": "{hours}", "DI36": "{distance}",
         "CQ36": "{actual_trips}", "EN36": "{actual_tonnes}"},
        (54, 188),
    ),
    "special_3": _Form(
        "Специальный автомобиль (3 спец)",
        {
            "AC4 BB3 BB33": "{waybill_number}", "K5 AV4 AV35": "{work_date.day}", "M5 AX4 AX35": "{work_date.month}",
            "Q5 BC4 BB35": "{work_date.year}", "G8 AX7 AX36": "{organization}", "I13 AZ9 AZ38": "{equipment}",
            "N14 BD10 BB39": "{registration_number}", "AL14": "{fleet_number}", "E15": "{employee_name}",
            "H17": "{licence_number}", "P17": "{licence_date}", "AD17": "{insurance_number}",
            "O31": "{time_from}", "O32": "{time_to}", "J31": "{meter_start}", "J32": "{meter_end}",
            "L31": "{meter_hours}", "R31": "{fuel_grade}", "W31": "{fuel_issued}",
            "Z31": "{fuel_start}", "AB31": "{fuel_end}", "AT11": "Заказчик: {partner}",
            "A41": "{partner}", "AA41": "{work_description}", "AI41": "{comment}",
        },
        {"Q11": "{workplace}", "AC11": "{work_description}", "Y37": "{hours}", "AI37": "{distance}"},
        (56, 61),
    ),
    "bus_6": _Form(
        "Автобус (6 спец)",
        {
            "BZ4": "{waybill_number}", "ER2": "листу № {waybill_number}", "O6": "{work_date.day}", "X6": "{work_date.month}",
            "AR6": "{work_date.year}", "O9": "{organization}", "R15": "{equipment}",
            "AH17": "{registration_number}", "DT17": "{fleet_number}", "L18": "{employee_name}",
            "T21": "{licence_number}", "BS31": "{partner}", "AZ31": "{time_from}",
            "AZ32": "{time_to}", "DQ31": "{hours}", "AM31": "{meter_start}", "AM32": "{meter_end}",
            "A39": "{fuel_grade}", "O39": "{fuel_issued}", "Z39": "{fuel_start}",
            "AI39": "{fuel_end}", "CJ39": "{comment}",
            "ES4": "Организация: {organization}", "ER5": "Автобус: {equipment}",
            "ES6": "Государственный номерной знак: {registration_number}", "ER7": "Заказчик: {partner}",
        },
        {
            "AF4": "{waybill_number}", "P7": "{organization}", "L12": "{equipment}",
            "AF15": "{registration_number}", "M17": "{partner}", "BL8": "{workplace}",
            "CU8": "{destination}", "DG36": "{distance}",
        },
        (59, 186),
    ),
    "manipulator_distance": _machine("Манипулятор (пробег)", meter="odometer", total="distance"),
    "manipulator_hours": _machine("Манипулятор (моточасы)"),
    "loader_distance": _machine("Погрузчик (пробег)", meter="odometer", total="distance"),
    "bulldozer_distance": _machine("Бульдозер (пробег)", meter="odometer", total="distance"),
    "excavator_mixed": _machine("Экскаватор (спидометр и мч)", meter="odometer"),
    "bulldozer_mixed": _machine("Бульдозер (спидометр и мч)", meter="odometer"),
    "dump_truck_mixed": _machine("Самосвал (спидометр и мч)", meter="odometer"),
}

_FIELD = re.compile(r"\{([a-z_]+)(?:\.(day|month|year))?\}")


def form_catalog() -> dict[str, str]:
    return {key: form.sheet for key, form in _FORMS.items()}


def fields_usage(fields) -> str:
    measures = {
        "hours": "отработанные часы", "idle_hours": "простой", "meter_hours": "моточасы",
        "odometer_start": "показания спидометра в км",
        "distance": "пробег в км", "trips": "количество ездок", "tonnes": "масса груза в тоннах",
        "planned_trips": "ездки по заданию", "actual_trips": "выполненные ездки",
        "actual_tonnes": "фактически перевезённые тонны",
    }
    return ", ".join(label for key, label in measures.items() if key in fields)


def form_usage(form_id: str) -> str:
    if form_id == "truck_4p":
        return "Повременный путевой 4-П. Часы работы у заказчика и отрывные талоны."
    if form_id == "truck_4s":
        return "Сдельный путевой 4-С. Груз, адреса погрузки и разгрузки, ездки и тонны."
    return "Показатели формы — " + fields_usage(form_fields(form_id)) + "."


def form_fields(form_id: str) -> dict[str, str]:
    form = _FORMS[form_id]
    used = {m[1] for pattern in (*form.front.values(), *form.reverse.values()) for m in _FIELD.finditer(pattern)}
    labels = {key: label for key, label in _LABELS.items() if key in used}
    if form_id == "truck_4s":
        # Existing printed documents used these keys on the assignment side.
        labels.update(trips="Ездок по заданию", tonnes="Перевезти по заданию, т")
    return labels


@lru_cache(maxsize=1)
def form_book() -> bytes:
    return files(__package__).joinpath("waybill_forms.xlsx").read_bytes()


@lru_cache(maxsize=len(_FORMS))
def _single_sheet(form_id: str) -> bytes:
    # Preserve native page layout and avoid parsing every detailed form for each download.
    # The bundled book contains only standalone worksheets, shared styles and labels.
    sheet_title = _FORMS[form_id].sheet
    output = BytesIO()
    with ZipFile(BytesIO(form_book())) as source, ZipFile(output, "w", ZIP_DEFLATED) as dest:
        manifest = ElementTree.fromstring(source.read("xl/workbook.xml"))
        sheets = manifest.find("{*}sheets")
        selected = next(node for node in sheets if node.get("name") == sheet_title)
        sheet_index = list(sheets).index(selected)
        sheets[:] = [selected]
        names = manifest.find("{*}definedNames")
        for name in list(names):
            if name.get("localSheetId") == str(sheet_index):
                name.set("localSheetId", "0")
            else:
                names.remove(name)
        rels = ElementTree.fromstring(source.read("xl/_rels/workbook.xml.rels"))
        selected_id = selected.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        removed = set()
        for rel in list(rels):
            if rel.get("Type").endswith("/worksheet") and rel.get("Id") != selected_id:
                path = rel.get("Target")
                removed.add(path.lstrip("/") if path.startswith("/") else f"xl/{path}")
                rels.remove(rel)
        types = ElementTree.fromstring(source.read("[Content_Types].xml"))
        types[:] = [entry for entry in types if entry.get("PartName", "").lstrip("/") not in removed]
        replacements = {
            "xl/workbook.xml": manifest,
            "xl/_rels/workbook.xml.rels": rels,
            "[Content_Types].xml": types,
        }
        for part in source.namelist():
            if part not in removed:
                data = ElementTree.tostring(replacements[part], encoding="utf-8") if part in replacements else source.read(part)
                dest.writestr(part, data)
    return output.getvalue()


def _printed_value(value: object, key: str, part: str | None) -> str:
    if value is None or value == "":
        return ""
    if key in {"work_date", "valid_until", "licence_date"}:
        if isinstance(value, datetime):
            value = value.date()
        if not isinstance(value, date):
            for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
                try:
                    value = datetime.strptime(str(value), fmt).date()
                    break
                except ValueError:
                    continue
            else:
                raise ValueError(f"Проверьте дату в поле «{_LABELS[key]}»")
        return value.strftime({"day": "%d", "month": "%m", "year": "%Y", None: "%d.%m.%Y"}[part])
    return str(value)


def _filled_cells(form_id: str, values: dict):
    form = _FORMS[form_id]
    for bindings, (rows, cols) in ((form.front, (0, 0)), (form.reverse, form.reverse_offset)):
        for addresses, pattern in bindings.items():
            if not any(m[1] in values for m in _FIELD.finditer(pattern)):
                continue
            text = _FIELD.sub(lambda m: _printed_value(values.get(m[1]), m[1], m[2]), pattern)
            if len(text) > 32767 or ILLEGAL_CHARACTERS_RE.search(text) or re.search(r"[\ud800-\udfff\ufffe\uffff]", text):
                raise ValueError("Текст поля содержит недопустимые для Excel символы или превышает 32767 знаков")
            for address in addresses.split():
                row, col = coordinate_to_tuple(address)
                yield f"{get_column_letter(col + cols)}{row + rows}", text


def validate_values(form_id: str, values: dict) -> None:
    """Check printable fields without opening Excel files. Extra journal fields are ignored."""
    for _ in _filled_cells(form_id, values):
        pass


def editable_form(form_id: str) -> bytes:
    """Retain the printed layout and expose the same bindings as editable XLSX markers."""
    form = _FORMS[form_id]
    book = load_workbook(BytesIO(_single_sheet(form_id)))
    for bindings, (rows, cols) in ((form.front, (0, 0)), (form.reverse, form.reverse_offset)):
        for addresses, pattern in bindings.items():
            marker_text = _FIELD.sub(lambda m: "{" + m[0] + "}", pattern)
            for address in addresses.split():
                row, col = coordinate_to_tuple(address)
                book.active.cell(row + rows, col + cols).value = marker_text
    output = BytesIO()
    book.save(output)
    return output.getvalue()


def render_form(form_id: str, values: dict) -> bytes:
    filled = list(_filled_cells(form_id, values))
    template = _single_sheet(form_id)
    if not filled:
        return template
    book = load_workbook(BytesIO(template))
    for address, text in filled:
        cell = book.active[address]
        cell.value = text or None
        if text:
            cell.data_type = "s"
    output = BytesIO()
    book.save(output)
    return output.getvalue()
