from io import BytesIO
from types import SimpleNamespace

import pytest
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font

from rag_catalog.core.waybill_documents import WaybillDocuments, register_details
from rag_catalog.core.waybill_templates import workbook_bytes
from rag_catalog.core.work_shifts import WorkShiftJournal


@pytest.fixture
def service(tmp_path):
    people = {}
    for username, org, role in (("driver", "one", "driver"), ("other", "one", "driver"),
                                ("dispatcher", "one", "dispatcher"), ("second", "two", "dispatcher")):
        people[username] = dict(username=username, role=role, status="active", organization_roles={org: [role]})
    orgs = [dict(id="one", name="Первая"), dict(id="two", name="Вторая")]
    auth = SimpleNamespace(get_user_by_session=people.get, get_user=lambda username: people.get(username),
                           list_employee_organizations=lambda: orgs)
    journal = WorkShiftJournal(tmp_path / "shifts.db", auth)
    row = journal.save("driver", dict(organization="Первая", organization_id="one", work_date="2026-11-14",
                                       equipment="Экскаватор", workplace="Карьер", partner="Заказчик",
                                       waybill_number="001", hours=8.5, shift_number=2))
    return WaybillDocuments(journal), row["id"]


def source_book():
    wb = Workbook()
    sheet = wb.active
    sheet["A1"] = "Путевой № {{waybill_number}}"
    sheet["A2"] = "{{маршрут}}"
    sheet["A3"] = "{{hours}} ч, {{work_date}}"
    sheet.merge_cells("A1:B1")
    sheet["A1"].font = Font(bold=True)
    sheet.row_dimensions[1].height = 32
    sheet.column_dimensions["A"].width = 45
    sheet.page_setup.orientation = "landscape"
    sheet.print_area = "A1:B3"
    return wb


def test_saved_forms_and_org_workbook_retain_layout_and_literal_text(service):
    store, sid = service
    key = store.add_template("dispatcher", "one", "Сменная работа", workbook_bytes(source_book()))
    assert store.fields("driver", "one", key)["маршрут"] == "Маршрут"
    state = store.draft("driver", sid, key)
    assert state["values"] == {"waybill_number": "001", "маршрут": "", "hours": "8.5", "work_date": "2026-11-14"}
    assert state["revision"] == 0
    state = store.save("driver", sid, key, {"маршрут": "=1+2"}, 0, state["shift_revision"])
    assert state == WaybillDocuments(store.journal).draft("dispatcher", sid, key)
    sheet = load_workbook(BytesIO(store.export("driver", sid, key, state["revision"], state["shift_revision"]))).active
    assert sheet["A1"].value == "Путевой № 001"
    assert sheet["A2"].value == "=1+2" and sheet["A2"].data_type == "s"
    assert sheet["A3"].value == "8.5 ч, 14.11.2026"
    book = load_workbook(BytesIO(store.book("dispatcher", "one")))
    sheet = book["Сменная работа"]
    assert len(book.sheetnames) > 1
    assert sheet["A1"].font.bold and "A1:B1" in sheet.merged_cells
    assert sheet.row_dimensions[1].height == 32 and sheet.column_dimensions["A"].width == 45
    assert sheet.page_setup.orientation == "landscape" and "$A$1:$B$3" in sheet.print_area
    assert sheet["A2"].value is None and "{{" not in sheet["A1"].value
    assert load_workbook(BytesIO(store.blank("driver", "one", key))).active["A2"].value is None
    assert key not in store.templates("second", "two")


def test_revisions_roles_and_approval(service):
    store, sid = service
    key = store.create_template("dispatcher", "one", "Заказ", ["work_date"], ["Сдано, т"])
    other = store.create_template("second", "two", "Заказ", ["work_date"], [])
    with pytest.raises(ValueError, match="уже есть"):
        store.create_template("dispatcher", "one", "ЗАКАЗ", ["work_date"], [])
    for operation in (lambda: store.draft("other", sid, key),
                      lambda: store.fields("driver", "two", other),
                      lambda: store.book("second", "one"),
                      lambda: store.create_template("driver", "one", "Мой", ["work_date"], [])):
        with pytest.raises(PermissionError):
            operation()
    with pytest.raises(ValueError):
        store.draft("driver", sid, other)
    store.journal.transition("dispatcher", sid, 1, "approved")
    with pytest.raises(ValueError):
        store.save("driver", sid, key, {"work_date": "2026-01-01"}, 0, 2)
    with pytest.raises(ValueError):
        store.save("driver", sid, key, {"field_1": "4"}, 0, 1)
    state = store.save("driver", sid, key, {"field_1": "4"}, 0, 2)
    assert state["revision"] == 1 and state["shift_revision"] == 3
    assert store.journal.get("driver", sid)["status"] == "submitted"
    assert store.save("driver", sid, key, {"field_1": "4"}, 1, 3) == state
    with pytest.raises(ValueError):
        store.save("dispatcher", sid, key, {"field_1": "6"}, 0, 3)
    store.journal.transition("dispatcher", sid, 3, "approved")
    with pytest.raises(ValueError):
        store.export("driver", sid, key, 1, 3)
    assert store.export("driver", sid, key, 1, 4).startswith(b"PK")
    assert store.journal.get("driver", sid)["status"] == "approved"


def test_upload_and_attendance_checks(service):
    store, _ = service
    for content in (b"broken", b""):
        with pytest.raises(ValueError):
            store.add_template("dispatcher", "one", "Бланк", content)
    wb = source_book()
    wb.active["A4"] = "=1+1"
    with pytest.raises(ValueError, match="формул"):
        store.add_template("dispatcher", "one", "Бланк", workbook_bytes(wb))
    key = store.create_template("dispatcher", "one", "Бланк", ["equipment"], [])
    row = store.journal.save("driver", dict(organization="Первая", work_date="2026-11-15", hours=2,
                                            shift_number=1, entry_type="attendance"))
    with pytest.raises(ValueError, match="без техники"):
        store.draft("driver", row["id"], key)


def test_register_reads_latest_agreed_waybill_fields(service):
    store, sid = service
    source = Workbook()
    source.active["A1"] = "{{fuel_used}}"
    first, second = (store.add_template("dispatcher", "one", name, workbook_bytes(source))
                     for name in ("Лицевой", "Дополнительный"))
    store.save("driver", sid, first, {"fuel_used": "24"}, 0, 1)
    store.save("driver", sid, second, {"fuel_used": "32"}, 0, 2)
    assert store.issued_templates("driver", sid) == [second, first]
    with store.journal._connection() as conn:
        assert register_details(conn, sid) == {}
    store.save("driver", sid, first, {"fuel_used": "32"}, 1, 3)
    with store.journal._connection() as conn:
        assert register_details(conn, sid) == {"fuel_used": "32"}
    with pytest.raises(PermissionError):
        store.issued_templates("other", sid)


def test_suggestions_follow_shift_edits_until_overridden(service):
    store, sid = service
    with store.journal._connection() as conn:
        rate = conn.execute("""INSERT INTO shift_rates
            (organization_id,work,work_key,unit,equipment,workplace,since,kopecks,created_by)
            VALUES ('one','Перевозка','перевозка','trip','','','2026-01-01',100,'dispatcher')""").lastrowid
    values = dict(organization="Первая", work_date="2026-11-14", equipment="Тягач · борт 17",
                  workplace="Карьер", hours=8, shift_number=2, work_lines=[dict(rate_id=rate, quantity=2)])
    store.journal.save("driver", values, shift_id=sid, revision=1)
    state = store.draft("driver", sid, "bulldozer")
    assert state["values"]["fleet_number"] == "17"
    assert "2 ход." in state["values"]["work_description"]
    store.save("driver", sid, "bulldozer", {"work_description": state["values"]["work_description"]}, 0, 2)
    with store.journal._connection() as conn:
        assert register_details(conn, sid) == {}
    values.update(equipment="Тягач · борт 21", work_lines=[dict(rate_id=rate, quantity=5)])
    store.journal.save("driver", values, shift_id=sid, revision=2)
    state = store.draft("driver", sid, "bulldozer")
    assert state["values"]["fleet_number"] == "21"
    assert "5 ход." in state["values"]["work_description"]


def test_preview_approved_waybill_is_read_only_and_rechecks_versions(service):
    store, sid = service
    key = store.add_template("dispatcher", "one", "Проверка", workbook_bytes(source_book()))
    store.journal.transition("dispatcher", sid, 1, "approved")
    content = store.preview("driver", sid, key, {"маршрут": "На погрузку"}, 0, 2)
    assert load_workbook(BytesIO(content)).active["A2"].value == "На погрузку"
    assert store.issued_templates("driver", sid) == []
    assert store.journal.get("driver", sid)["status"] == "approved"
    with pytest.raises(ValueError):
        store.preview("driver", sid, key, {"hours": "5"}, 0, 2)
    with pytest.raises(PermissionError):
        store.preview("other", sid, key, {}, 0, 2)
    store.save("driver", sid, key, {"маршрут": "Карьер"}, 0, 2)
    with pytest.raises(ValueError):
        store.preview("driver", sid, key, {}, 0, 2)
    assert load_workbook(BytesIO(store.preview("driver", sid, key, {"маршрут": "База"}, 1, 3))).active["A2"].value == "База"
    assert store.draft("driver", sid, key)["values"]["маршрут"] == "Карьер"


def test_preferred_template_is_scoped_to_equipment_counterparty_and_organisation(service):
    store, sid = service
    def shift(token, org, partner, equipment):
        return store.journal.save(token, dict(organization=org, partner=partner, equipment=equipment,
                                  work_date="2026-11-16", workplace="Карьер", shift_number=1, hours=8))["id"]
    other_day = shift("driver", "Первая", "заказчик", "экскаватор")
    other_vehicle = shift("driver", "Первая", "Заказчик", "Самосвал")
    other_client = shift("other", "Первая", "Иной заказчик", "Экскаватор")
    other_org = shift("second", "Вторая", "Заказчик", "Экскаватор")
    their_template = store.create_template("second", "two", "Их бланк", ["work_date"], [])
    with pytest.raises(PermissionError):
        store.set_preference("driver", sid, "truck_4s")
    with pytest.raises(ValueError):
        store.set_preference("dispatcher", sid, their_template)
    with pytest.raises(ValueError, match="изменились"):
        store.set_preference("dispatcher", sid, "truck_4s", expected_pair=("Другая машина", "Заказчик"))
    store.set_preference("dispatcher", sid, "truck_4s")
    assert store.preference("driver", other_day) == "truck_4s"
    for token, rec in (("driver", other_vehicle), ("other", other_client), ("second", other_org)):
        assert store.preference(token, rec) is None
    store.set_preference("dispatcher", other_day, None)
    assert store.preference("driver", sid) is None
    assert "Повременный" in store.usage("driver", "one", "truck_4p")
    assert "Сдельный" in store.usage("driver", "one", "truck_4s")


def test_4s_keeps_dispatch_assignment_apart_from_completed_trips_on_both_versions(service):
    store, sid = service
    loan = store.revise_cells("dispatcher", "one", "truck_4s", {}, copy_name="Форма карьера")
    store.set_preference("dispatcher", sid, loan)
    key = store.revise_cells("dispatcher", "one", loan, {"DP35": "Груз {{cargo}}"})
    assert store.preference("driver", sid) == key
    assert "сдельная 4-С" in store.usage("driver", "one", key)
    fields = store.fields("driver", "one", key)
    assert "заданию" in fields["trips"] and "Выполнено" in fields["actual_trips"]
    for template_id in ("truck_4s", key):
        shift_revision = store.journal.get("driver", sid)["revision"]
        with pytest.raises(ValueError, match="дат"):
            store.save("driver", sid, template_id, {"valid_until": "завтра"}, 0, shift_revision)
        state = store.save("driver", sid, template_id, {"trips": "8", "tonnes": "100"}, 0, shift_revision)
        with store.journal._connection() as conn:
            assert "trips" not in register_details(conn, sid)
        preview = store.preview("driver", sid, template_id, {"actual_trips": "5", "actual_tonnes": "50"},
                                state["revision"], state["shift_revision"])
        sheet = load_workbook(BytesIO(preview)).active
        assert [sheet[k].value for k in ("FB35", "FV35", "JW90", "LT90")] == ["8", "100", "5", "50"]
    store.save("driver", sid, key, {"actual_trips": "5"}, 1, state["shift_revision"])
    with store.journal._connection() as conn:
        assert register_details(conn, sid)["trips"] == "5"
