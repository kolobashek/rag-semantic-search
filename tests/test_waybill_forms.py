from io import BytesIO
from zipfile import ZipFile

import pytest
from openpyxl import load_workbook

from rag_catalog.core.waybill_forms import form_book, form_catalog, form_fields, render_form, validate_values


def test_blank_book_contains_printable_forms_without_external_attachments():
    payload = form_book()
    catalog = form_catalog()
    book = load_workbook(BytesIO(payload))
    assert book.sheetnames == list(catalog.values())
    for code, title in catalog.items():
        sheet = book[title]
        expected_pages = 2 if code in {"passenger_3", "special_3", "truck_4p", "truck_4s", "bus_6"} else 1
        assert len(sheet.print_area.split(",")) == expected_pages
        assert str(sheet.page_setup.paperSize) == sheet.PAPERSIZE_A4
        assert sheet.sheet_state == "visible"
    with ZipFile(BytesIO(payload)) as archive:
        assert not any(any(marker in path for marker in ("vbaProject", "externalLinks", "/media/", "comments")) for path in archive.namelist())


@pytest.mark.parametrize("form_id", list(form_catalog()))
def test_population_keeps_faces_and_print_settings_and_accepts_literal_text(form_id):
    fields = form_fields(form_id)
    filled = dict.fromkeys(fields, "Пример")
    filled.update(waybill_number="00027", employee_name="=A1+100", work_date="2026-11-27", hours=0)
    for key in ("licence_date", "valid_until"):
        if key in fields:
            filled[key] = "2026-11-28"
    validate_values(form_id, filled)
    sheet = load_workbook(BytesIO(render_form(form_id, filled))).active
    cells = [cell for row in sheet.iter_rows() for cell in row if cell.value is not None]
    assert sheet.title == form_catalog()[form_id]
    assert any("00027" in str(c.value) for c in cells)
    assert any("=A1+100" in str(c.value) and c.data_type == "s" for c in cells)
    assert any(c.value == "0" for c in cells)
    assert any("27.11.2026" in str(c.value) or c.value == "2026" for c in cells)
    assert not any(c.data_type == "f" for c in cells)
    assert sheet.print_area == load_workbook(BytesIO(render_form(form_id, {}))).active.print_area
    assert len(sheet.parent.sheetnames) == 1


def test_invalid_values_rejected_before_excel_rendering():
    with pytest.raises(ValueError, match="дату"):
        validate_values("truck_4p", {"work_date": "2026-02-30"})
    with pytest.raises(ValueError, match="Excel"):
        validate_values("grader", {"employee_name": "Имя\x03"})
    with pytest.raises(KeyError):
        validate_values("unknown", {})


def test_printed_forms_remain_independent_between_requests():
    assert "Второй" in str(load_workbook(BytesIO(render_form("grader", {"employee_name": "Второй"}))).active["A7"].value)
    assert "Второй" not in str(load_workbook(BytesIO(render_form("grader", {}))).active["A7"].value)
