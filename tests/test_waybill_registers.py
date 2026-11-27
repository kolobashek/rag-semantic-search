from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from types import SimpleNamespace

import pytest
from openpyxl import load_workbook

from rag_catalog.core.waybill_registers import WaybillRegisters
from rag_catalog.core.work_shifts import WorkShiftJournal


@pytest.fixture
def app(tmp_path):
    directory = [{"id": "north", "name": "Север"}, {"id": "south", "name": "Юг"}]
    users = {}
    for name, role, org in (("dispatcher", "dispatcher", "north"), ("driver", "driver", "north"),
                            ("south-dispatcher", "dispatcher", "south"), ("south-driver", "driver", "south")):
        users[name] = dict(username=name, display_name=name.capitalize(), role=role, organization_roles={org: [role]},
                           status="active", organizations=directory)
    auth = SimpleNamespace(get_user_by_session=users.get, get_user=lambda username: users.get(username), list_users=lambda: list(users.values()),
                           list_employee_organizations=lambda: directory)
    journal = WorkShiftJournal(tmp_path / "journal.db", auth)
    return journal, WaybillRegisters(journal)


def shift(journal, *, day="2026-11-03", approved=True, partner="Заказчик", org="north", **extra):
    driver, manager = ("driver", "dispatcher") if org == "north" else ("south-driver", "south-dispatcher")
    data = dict(work_date=day, organization_id=org, organization="Север" if org == "north" else "Юг",
                workplace="Площадка", equipment="Трактор", shift_number=1, waybill_number="0010",
                hours=8.5, partner=partner, comment="НЕ ПЕЧАТАТЬ")
    data.update(extra)
    row = journal.save(driver, data)
    if approved:
        journal.transition(manager, row["id"], row["revision"], "approved")
    return row["id"]


ARGS = ("dispatcher", "north", "2026-11", "Заказчик")


def test_generation_includes_only_scoped_approved_vehicle_rows_and_excludes_private_pay(app):
    journal, registers = app
    source_id = shift(journal)
    shift(journal, org="south")
    shift(journal, day="2026-10-31")
    shift(journal, day="2026-12-01")
    shift(journal, day="2026-11-06", partner="Другой заказчик")
    shift(journal, day="2026-11-07", approved=False)
    shift(journal, day="2026-11-09", entry_type="attendance", waybill_number="", equipment="")
    # Existing journal stores payroll prices together with the public description and volume.
    with journal._connection() as conn:
        conn.execute("""UPDATE shifts SET work_lines='[{"work":"Вывоз","quantity":"3.0","unit":"trip",
            "rate_id":1,"kopecks":9912345,"amount":188888}]' WHERE id=?""", (source_id,))
    assert registers.partners(*ARGS[:3]) == ["Другой заказчик", "Заказчик"]
    current = registers.open(*ARGS)
    assert current["revision"] == 0
    assert current["organization"] == "Север"
    assert [r["shift_id"] for r in current["rows"]] == [source_id]
    assert current["rows"][0]["trips"] == 3
    assert current["rows"][0]["work"] == "Вывоз (3.0 ход.)"
    for private in ("НЕ ПЕЧАТАТЬ", "9912345", "188888"):
        assert private not in str(current)
    for token in ("driver", "south-dispatcher"):
        for call in (lambda: registers.partners(token, *ARGS[1:3]), lambda: registers.open(token, *ARGS[1:]),
                     lambda: registers.xlsx(token, *ARGS[1:]),
                     lambda: registers.save(token, *ARGS[1:], 0, current["rows"]),
                     lambda: registers.refresh(token, *ARGS[1:], 0)):
            with pytest.raises(PermissionError):
                call()


def test_snapshot_preserves_corrections_concurrent_save_and_refresh(app):
    journal, regs = app
    first = shift(journal)
    draft = regs.open(*ARGS)
    rows = draft["rows"]
    rows[0].update(waybill_number="0042", hours=7.5, rate_with_vat=0)
    def save():
        try:
            return regs.save(*ARGS, 0, rows)["revision"]
        except ValueError:
            return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [f.result() for f in [pool.submit(save), pool.submit(save)]]
    assert results.count(1) == results.count(None) == 1
    assert WaybillRegisters(WorkShiftJournal(journal.path, journal.auth)).open(*ARGS)["rows"][0]["hours"] == 7.5
    new = shift(journal, day="2026-11-29")
    assert len(regs.open(*ARGS)["rows"]) == 1
    out = regs.refresh(*ARGS, 1)
    assert [r["shift_id"] for r in out["rows"]] == [first, new]
    assert out["rows"][0]["waybill_number"] == "0042"
    assert out["rows"][0]["rate_with_vat"] == 0
    assert regs.refresh(*ARGS, 2)["revision"] == 2
    with pytest.raises(ValueError, match="изменился"):
        regs.xlsx(*ARGS, revision=1)
    for changed in ([], [rows[0], rows[0]], [dict(rows[0], shift_id=12345), out["rows"][1]]):
        with pytest.raises(ValueError):
            regs.save(*ARGS, 2, changed)
    with pytest.raises(ValueError):
        regs.save(*ARGS, 2, [dict(out["rows"][0], work_date="2026-12-01"), out["rows"][1]])


def test_export_has_numeric_dates_formulas_zero_blank_and_both_signature_lines(app):
    journal, regs = app
    shift(journal, waybill_number="=1+2")
    shift(journal, day="2026-11-10")
    rows = regs.open(*ARGS)["rows"]
    rows[0].update(rate_with_vat=4321.75, duty_hours=0, trips=2)
    regs.save(*ARGS, 0, rows)
    sheet = load_workbook(BytesIO(regs.xlsx(*ARGS, revision=1))).active
    cols = {cell.value: cell.column_letter for cell in sheet[5]}
    assert sheet[cols["Дата п/л"]+"6"].is_date
    code = sheet[cols["№ п/л"]+"6"]
    assert (code.value, code.data_type) == ("=1+2", "s")
    assert sheet[cols["Тариф за час с НДС, руб."]+"6"].value == 4321.75
    assert sheet[cols["Тариф за час с НДС, руб."]+"7"].value is None
    assert sheet[cols["Техдежурство, ч"]+"6"].value == 0
    amount = cols["Стоимость с НДС, руб."]
    assert sheet[amount+"6"].data_type == "f"
    assert "COUNTBLANK(" in sheet[amount+"8"].value
    assert sheet[cols["Отработано, ч"]+"8"].data_type == "f"
    assert str(sheet.print_area).endswith("$10")
    assert any("Исполнитель  _" in str(c.value) for c in sheet[10])
    assert any("Заказчик  _" in str(c.value) for c in sheet[10])
    assert sheet.page_setup.fitToWidth == 1


def test_changed_printed_form_invalidates_initial_snapshot_and_free_text_stays_optional(app):
    journal, regs = app
    shift_id = shift(journal)
    old = regs.open(*ARGS)
    with journal._connection() as conn:
        conn.execute("""INSERT INTO shift_waybill_documents VALUES(?, 'form:example', 1,
            '{"work_description":"Расчистка площадки","fuel_used":"11,5","trips":"два"}','dispatcher','')""", (shift_id,))
    with pytest.raises(ValueError, match="Исходные путевые"):
        regs.save(*ARGS, 0, old["rows"])
    current = regs.open(*ARGS)
    assert current["rows"][0]["work"] == "Расчистка площадки"
    assert current["rows"][0]["fuel_used"] == 11.5
    assert current["rows"][0]["trips"] is None
    regs.save(*ARGS, 0, current["rows"])


def test_subtotals_group_vehicles_and_bill_duty_hours_separately(app):
    journal, regs = app
    shift(journal, equipment="Экскаватор")
    shift(journal, day="2026-11-05", equipment="Самосвал")
    rows = regs.open(*ARGS)["rows"]
    rows[0].update(rate_with_vat=1200, duty_hours=2, duty_rate_with_vat=500)
    rows[1].update(rate_with_vat=1000)
    regs.save(*ARGS, 0, rows)
    sheet = load_workbook(BytesIO(regs.xlsx(*ARGS))).active
    cols = {cell.value: cell.column_letter for cell in sheet[5]}
    assert sheet["A7"].value.startswith("Итого по технике")
    assert sheet["A9"].value.startswith("Итого по технике")
    assert sheet["A10"].value == "Всего"
    duty = cols["Техдежурство с НДС, руб."]
    work = cols["Работа с НДС, руб."]
    total = cols["Стоимость с НДС, руб."]
    assert (cols["Тариф техдежурства за час с НДС, руб."]+"6") in sheet[duty+"6"].value
    assert work+"6" in sheet[total+"6"].value and duty+"6" in sheet[total+"6"].value
    assert 'SUMIF(A6:A9,">0"' in sheet[total+"10"].value


def test_month_book_preserves_snapshots_uses_live_unsaved_rows_and_isolates_organizations(app):
    journal, regs = app
    prefix = "Строительная транспортная компания"
    first, second = f"{prefix}/Север", f"{prefix}:Юг"
    shift(journal, partner=first)
    shift(journal, partner=second, day="2026-11-25")
    shift(journal, partner="History", day="2026-11-24")
    shift(journal, partner="'АО [А/Б]*?'", day="2026-11-23")
    shift(journal, org="south", partner="Чужой контрагент")
    shift(journal, day="2026-12-01", partner="Другой месяц")
    shift(journal, day="2026-11-27", partner="Черновик", approved=False)
    scope = ARGS[:3]
    edited = regs.open(*scope, first)["rows"]
    edited[0].update(waybill_number="Согласовано")
    regs.save(*scope, first, 0, edited)
    shift(journal, partner=first, day="2026-11-21", waybill_number="Новое")
    book = load_workbook(BytesIO(regs.month_book(*scope)))
    assert len(book.sheetnames) == 4
    assert len({s.casefold() for s in book.sheetnames}) == 4
    assert all(len(s.encode("utf-16-le")) <= 62 and not set(s) & set(r"\/:*?[]") for s in book.sheetnames)
    assert all(s.casefold() != "history" and not s.startswith("'") and not s.endswith("'") for s in book.sheetnames)
    by_partner = {sheet["A3"].value.removeprefix("Заказчик: "): sheet for sheet in book}
    assert set(by_partner) == {first, second, "History", "'АО [А/Б]*?'"}
    assert by_partner[first]["B6"].value == "Согласовано"
    assert by_partner[first]["A7"].value == "Всего"
    assert by_partner[second]["B6"].value == "0010"
    assert all(sheet["A2"].value == "Исполнитель: Север" for sheet in book)
    assert regs.open(*scope, second)["revision"] == 0
    with pytest.raises(PermissionError):
        regs.month_book("south-dispatcher", *scope[1:])
    with pytest.raises(ValueError):
        regs.month_book("dispatcher", "north", "2025-01")
