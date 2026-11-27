"""Exercise the dispatcher workflow through the journal and the actual Excel downloads."""

from datetime import date
from io import BytesIO
from pathlib import Path

from openpyxl import Workbook, load_workbook
from test_employee_access_browser import ADMIN, ORG, sign_in
from test_employee_access_browser import office as office
from test_employee_access_browser import pages as pages

from rag_catalog.core.extractors.files import _resolve_soffice
from rag_catalog.core.waybill_documents import WaybillDocuments
from rag_catalog.core.work_shifts import WorkShiftJournal


def downloaded_book(download):
    return load_workbook(BytesIO(download.value.path().read_bytes()))


def choose_waybill(page, shift, title=None):
    page.get_by_role("row").filter(has_text=shift["waybill_number"]).get_by_role("checkbox").click()
    page.get_by_role("button", name="Оформить путевой лист").click()
    choice = page.get_by_role("dialog").filter(has=page.get_by_text(f"Путевой лист · смена №{shift['id']}", exact=True))
    if title:
        choice.get_by_label("Бланк", exact=True).click()
        page.get_by_role("option", name=title, exact=True).click()
    return choice


def test_imported_form_is_available_without_selecting_a_shift(office, pages):
    from playwright.sync_api import expect

    _, url, auth, password = office
    manager, driver = pages
    sign_in(manager, url, ADMIN, password)
    manager.get_by_role("button", name="Бланки путевых листов", exact=True).click()
    library = manager.get_by_role("dialog")
    library.get_by_role("button", name="Импорт из Excel").click()
    form = manager.get_by_role("dialog").filter(has=manager.get_by_text("Импорт бланка Excel", exact=True))
    source = Workbook()
    source.active["A1"] = "Водитель"
    source.active["B1"] = "{{employee_name}}"
    source.active.print_area = "A1:B1"
    data = BytesIO()
    source.save(data)
    form.locator('input[type="file"]').set_input_files(
        {"name": "Маршрут.xlsx", "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
         "buffer": data.getvalue()}
    )
    expect(form.get_by_label("Название бланка")).to_have_value("Маршрут")
    form.get_by_role("button", name="Добавить бланк").click()
    expect(library.get_by_text("Маршрут", exact=True)).to_be_visible()
    assert auth.admin_create_user(username="forms-driver", password=password, roles=["driver"],
                                  organization_id="north", must_change_password=False)
    sign_in(driver, url, "forms-driver", password)
    driver.get_by_role("button", name="Бланки путевых листов", exact=True).click()
    library = driver.get_by_role("dialog")
    expect(library.get_by_role("button", name="Создать бланк")).to_be_hidden()
    expect(driver.get_by_role("button", name="Реестры путевых листов")).to_be_hidden()
    library.get_by_label("Бланк", exact=True).click()
    driver.get_by_role("option", name="Маршрут", exact=True).click()
    with driver.expect_download() as download:
        library.get_by_role("button", name="Скачать чистый бланк").click()
    sheet = downloaded_book(download).active
    assert sheet["A1"].value == "Водитель" and sheet["B1"].value in (None, "")


def test_dispatcher_issues_waybill_and_edits_register(office, pages):
    from playwright.sync_api import expect

    _, url, auth, password = office
    page, _ = pages
    token = auth.create_session(username=ADMIN)
    journal = WorkShiftJournal(Path(auth.db_path).parent / "work_shifts.db", auth)
    shift = journal.save(token, {
        "organization_id": "north", "organization": ORG, "employee": ADMIN, "work_date": date.today().isoformat(),
        "shift_number": 1, "equipment": "Автокран 15", "workplace": "Площадка 7", "hours": 8, "breaks": 0,
        "partner": "Заказчик 75", "waybill_number": "ПЛ-75",
    })
    sign_in(page, url, ADMIN, password)
    page.get_by_role("button", name="Бланки путевых листов", exact=True).click()
    library = page.get_by_role("dialog").filter(has=page.get_by_text("Бланки путевых листов", exact=True))
    library.get_by_role("button", name="Создать бланк", exact=True).click()
    create = page.get_by_role("dialog").filter(has=page.get_by_text("Создание бланка", exact=True))
    create.get_by_label("Название бланка").fill("Крановщик")
    create.get_by_label("Поля для ручного ввода").fill("Маршрут")
    create.get_by_role("button", name="Создать бланк", exact=True).click()
    expect(library.get_by_text("Крановщик", exact=True)).to_be_visible()
    with page.expect_download() as book:
        library.get_by_role("button", name="Книга всех бланков").click()
    workbook = downloaded_book(book)
    assert "Крановщик" in workbook.sheetnames
    library.get_by_role("button", name="Закрыть").click()

    page.get_by_role("row").filter(has_text="ПЛ-75").get_by_role("checkbox").click()
    page.get_by_role("button", name="Оформить путевой лист").click()
    choice = page.get_by_role("dialog").filter(has=page.get_by_text(f"Путевой лист · смена №{shift['id']}", exact=True))
    choice.get_by_label("Бланк", exact=True).click()
    page.get_by_role("option", name="Крановщик", exact=True).click()
    choice.get_by_role("button", name="Открыть бланк").click()
    editor = page.get_by_role("dialog").filter(has=page.get_by_text(f"Крановщик · смена №{shift['id']}", exact=True))
    expect(editor.get_by_label("Номер путевого листа", exact=True)).to_have_value("ПЛ-75")
    route = ", ".join(f"склад {i} — ремонтная площадка {i} — выезд через северные ворота"
                      for i in range(1, 9)) + ", конечная Лесная"
    editor.get_by_label("Маршрут", exact=True).fill(route)
    if _resolve_soffice():
        import pdfplumber

        editor.get_by_role("button", name="Предпросмотр", exact=True).click()
        preview = page.get_by_role("dialog").filter(has=page.locator("iframe"))
        expect(preview.locator("iframe")).to_be_visible(timeout=60000)
        assert preview.locator("iframe").get_attribute("src").startswith("blob:" + url)
        preview.locator("iframe").evaluate(
            "frame => { frame.contentWindow.print = () => { frame.dataset.printRequested = 'yes' } }"
        )
        preview.get_by_role("button", name="Печать", exact=True).click()
        expect(preview.locator("iframe")).to_have_attribute("data-print-requested", "yes")
        with page.expect_download() as pdf:
            preview.get_by_role("button", name="Скачать PDF").click()
        with pdfplumber.open(pdf.value.path()) as printed:
            text = " ".join(page.extract_text() for page in printed.pages)
        assert "склад 1" in text and "Лесная" in text
        preview.get_by_role("button", name="Закрыть").click()
        assert WaybillDocuments(journal).issued_templates(token, shift["id"]) == []
        assert journal.get(token, shift["id"])["revision"] == shift["revision"]
    with page.expect_download() as document:
        editor.get_by_role("button", name="Скачать Excel").click()
    values = [cell.value for row in downloaded_book(document).active for cell in row]
    assert route in values and "ПЛ-75" in values
    editor.get_by_role("button", name="Закрыть").click()
    choice.get_by_role("button", name="Закрыть").click()

    page.get_by_role("row").filter(has_text="ПЛ-75").get_by_role("checkbox").click()
    page.get_by_role("button", name="Оформить путевой лист").click()
    expect(choice.get_by_text("Крановщик (сохранён)", exact=True)).to_be_visible()
    choice.get_by_role("button", name="Открыть бланк").click()
    expect(editor.get_by_label("Маршрут", exact=True)).to_have_value(route)
    editor.get_by_role("button", name="Закрыть").click()
    choice.get_by_role("button", name="Закрыть").click()

    shift = journal.get(token, shift["id"])
    journal.transition(token, shift["id"], shift["revision"], "approved")
    page.get_by_role("button", name="Реестры путевых листов", exact=True).click()
    select = page.get_by_role("dialog").filter(has=page.get_by_text("Реестры путевых листов", exact=True))
    select.get_by_role("button", name="Открыть реестр").click()
    register = page.get_by_role("dialog").filter(has=page.get_by_role("button", name="Изменить строку"))
    register.get_by_role("row").filter(has_text="ПЛ-75").get_by_role("checkbox").click()
    register.get_by_role("button", name="Изменить строку").click()
    line = page.get_by_role("dialog").filter(has=page.get_by_text("Строка реестра", exact=True))
    line.get_by_label("Путевой лист", exact=True).fill("ПЛ-75/1")
    line.get_by_role("button", name="Применить").click()
    expect(register.get_by_role("cell", name="ПЛ-75/1", exact=True)).to_be_visible()
    with page.expect_download() as exported:
        register.get_by_role("button", name="Скачать Excel").click()
    sheet = downloaded_book(exported).active
    assert any(cell.value == "ПЛ-75/1" for row in sheet for cell in row)
    register.get_by_role("button", name="Закрыть").click()
    with page.expect_download() as monthly:
        select.get_by_role("button", name="Книга реестров за месяц").click()
    book = downloaded_book(monthly)
    assert "Заказчик 75" in book.sheetnames
    assert any(cell.value == "ПЛ-75/1" for row in book["Заказчик 75"] for cell in row)


def test_edit_shift_from_waybill_keeps_manual_input_and_uses_employee_and_partner_lists(office, pages):
    from playwright.sync_api import expect

    _, url, auth, password = office
    page, _ = pages
    token = auth.create_session(username=ADMIN)
    journal = WorkShiftJournal(Path(auth.db_path).parent / "work_shifts.db", auth)
    store = WaybillDocuments(journal)
    template = "Заявка на перевозку"
    key = store.create_template(token, "north", template, ["organization", "partner", "employee_name"], ["Маршрут"])
    equipment = journal.save_reference(token, "equipment", "Самосвал 224", organization_id="north",
                                       equipment_category="truck", licence_kind="road", licence_mark="C")
    employees = [
        journal.save_employee(token, organization_id="north", create=True, offline=True, display_name=name,
                              categories=["truck"], licences={"road": {"marks": ["C"], "since": "2020-01-01",
                                                                        "until": "2090-12-31"}})
        for name in ("Первый водитель", "Второй водитель")
    ]
    journal.save_reference(token, "partner", "Получатель 224", organization_id="north")
    shift = journal.save(token, dict(organization_id="north", organization=ORG, employee=employees[0],
                                     work_date=date.today().isoformat(), shift_number=2, equipment=equipment,
                                     workplace="Карьер", partner="Первый заказчик", waybill_number="ПЛ-224", hours=8))
    sign_in(page, url, ADMIN, password)
    choose_waybill(page, shift, template).get_by_role("button", name="Открыть бланк").click()
    editor = page.get_by_role("dialog").filter(has=page.get_by_role("button", name="Изменить данные смены"))
    expect(editor.get_by_label("Организация", exact=True)).to_have_value(ORG)
    expect(editor.get_by_label("Контрагент", exact=True)).to_have_value("Первый заказчик")
    expect(editor.get_by_label("Фактически работал", exact=True)).to_have_value("Первый водитель")
    route = "Северный склад — участок 224"
    editor.get_by_label("Маршрут", exact=True).fill(route)
    editor.get_by_role("button", name="Изменить данные смены").click()
    shift_form = page.get_by_role("dialog").filter(has=page.get_by_text(f"Смена №{shift['id']}", exact=True))
    for field, choice in (("Сотрудник", "Второй водитель (без входа)"), ("Контрагент", "Получатель 224")):
        shift_form.get_by_label(field, exact=True).click()
        page.get_by_role("option", name=choice, exact=True).click()
    shift_form.get_by_role("button", name="Сохранить", exact=True).click()
    expect(shift_form).to_be_hidden()
    expect(editor.get_by_label("Маршрут", exact=True)).to_have_value(route)
    expect(editor.get_by_label("Фактически работал", exact=True)).to_have_value("Второй водитель")
    expect(editor.get_by_label("Контрагент", exact=True)).to_have_value("Получатель 224")
    assert store.issued_templates(token, shift["id"]) == []
    revised = journal.get(token, shift["id"])
    assert (revised["employee"], revised["partner"], revised["organization"]) == (employees[1], "Получатель 224", ORG)
    with page.expect_download() as download:
        editor.get_by_role("button", name="Скачать Excel").click()
    values = {cell.value for row in downloaded_book(download).active for cell in row}
    assert {ORG, "Получатель 224", "Второй водитель", route} <= values
    assert store.issued_templates(token, shift["id"]) == [key]


def test_edit_form_design_preserves_previously_issued_waybill(office, pages):
    from playwright.sync_api import expect

    _, url, auth, password = office
    page, _ = pages
    token = auth.create_session(username=ADMIN)
    journal = WorkShiftJournal(Path(auth.db_path).parent / "work_shifts.db", auth)
    store = WaybillDocuments(journal)
    name = "Сменное задание 225"
    key = store.create_template(token, "north", name, ["employee_name"], ["Маршрут"])
    shift = journal.save(token, dict(organization_id="north", organization=ORG, employee=ADMIN,
                                     work_date=date.today().isoformat(), shift_number=3, equipment="Погрузчик 225",
                                     workplace="База 225", partner="Получатель 225", waybill_number="ПЛ-225", hours=4))
    store.save(token, shift["id"], key, {"field_1": "Сохранённый рейс"}, 0, shift["revision"])
    sign_in(page, url, ADMIN, password)
    page.get_by_role("button", name="Бланки путевых листов").click()
    library = page.get_by_role("dialog").filter(has=page.get_by_role("button", name="Книга всех бланков"))
    library.get_by_label("Бланк", exact=True).click()
    page.get_by_role("option", name=name, exact=True).click()
    library.get_by_role("button", name="Изменить макет").click()
    edit = page.get_by_role("dialog").filter(has=page.get_by_role("button", name="Сохранить макет"))
    edit.get_by_label("Поля для ручного ввода").fill("Маршрут\nДополнительный груз")
    edit.get_by_label("Показатели работы техники").click()
    page.get_by_role("option", name="Пробег, км", exact=True).click()
    page.keyboard.press("Escape")
    edit.get_by_role("button", name="Сохранить макет").click()
    expect(edit).to_be_hidden()
    with page.expect_download() as download:
        library.get_by_role("button", name="Скачать чистый бланк").click()
    values = {c.value for row in downloaded_book(download).active for c in row}
    assert {"Маршрут", "Дополнительный груз", "Пробег, км"} <= values
    library.get_by_role("button", name="Закрыть").click()
    choice = choose_waybill(page, shift)
    choice.get_by_role("button", name="Открыть бланк").click()
    editor = page.get_by_role("dialog").filter(has=page.get_by_role("button", name="Скачать Excel"))
    expect(editor.get_by_label("Маршрут", exact=True)).to_have_value("Сохранённый рейс")
    expect(editor.get_by_label("Дополнительный груз", exact=True)).to_have_count(0)
    editor.get_by_role("button", name="Закрыть").click()
    choice.get_by_label("Бланк", exact=True).click()
    page.get_by_role("option", name=name, exact=True).click()
    choice.get_by_role("button", name="Открыть бланк").click()
    expect(editor.get_by_label("Дополнительный груз", exact=True)).to_be_visible()
    expect(editor.get_by_label("Пробег, км", exact=True)).to_be_visible()


def test_dispatcher_previews_and_edits_cell_in_standard_form(office, pages):
    from playwright.sync_api import expect

    _, url, auth, password = office
    page, _ = pages
    token = auth.create_session(username=ADMIN)
    store = WaybillDocuments(WorkShiftJournal(Path(auth.db_path).parent / "work_shifts.db", auth))
    sign_in(page, url, ADMIN, password)
    page.get_by_role("button", name="Бланки путевых листов").click()
    library = page.get_by_role("dialog").filter(has=page.get_by_role("button", name="Книга всех бланков"))
    library.get_by_label("Бланк", exact=True).click()
    page.get_by_role("option", name="Грейдер", exact=True).click()
    library.get_by_role("button", name="Изменить макет").click()
    edit = page.get_by_role("dialog").filter(has=page.get_by_label("Ячейка макета"))
    edit.get_by_label("Ячейка макета").click()
    page.get_by_role("option", name="A2 — Грейдер", exact=True).click()
    edit.get_by_label("Содержимое ячейки").fill("Ночной грейдер")
    if _resolve_soffice():
        import pdfplumber

        edit.get_by_role("button", name="Предпросмотр макета", exact=True).click()
        preview = page.get_by_role("dialog").filter(has=page.locator("iframe"))
        expect(preview.locator("iframe")).to_be_visible(timeout=60000)
        with page.expect_download() as pdf:
            preview.get_by_role("button", name="Скачать PDF").click()
        with pdfplumber.open(pdf.value.path()) as printed:
            assert "Ночной грейдер" in printed.pages[0].extract_text()
        assert "grader" in store.templates(token, "north")
        preview.get_by_role("button", name="Закрыть").click()
    edit.get_by_role("button", name="Сохранить макет").click()
    expect(edit).to_be_hidden()
    with page.expect_download() as download:
        library.get_by_role("button", name="Скачать чистый бланк").click()
    assert downloaded_book(download).active["A2"].value == "Ночной грейдер"
    assert "grader" not in store.templates(token, "north")
    library.get_by_role("button", name="Изменить макет").click()
    edit.get_by_role("button", name="Сохранить как новый вид").click()
    copy = page.get_by_role("dialog").filter(has=page.get_by_label("Название нового вида"))
    copy.get_by_label("Название нового вида").fill("Грейдер заказчика")
    copy.get_by_role("button", name="Сохранить новый вид").click()
    expect(library.get_by_text("Грейдер заказчика", exact=True)).to_be_visible()
    assert {"Грейдер", "Грейдер заказчика"} <= set(store.templates(token, "north").values())


def test_dispatcher_remembers_form_for_next_shift_of_same_vehicle_and_customer(office, pages):
    from playwright.sync_api import expect

    _, url, auth, password = office
    page, _ = pages
    token = auth.create_session(username=ADMIN)
    journal = WorkShiftJournal(Path(auth.db_path).parent / "work_shifts.db", auth)
    common = dict(organization_id="north", organization=ORG, employee=ADMIN, work_date=date.today().isoformat(),
                  equipment="Автокран 226", partner="Заказчик 226", workplace="Участок 226", hours=5)
    first, second = [journal.save(token, dict(common, shift_number=number, waybill_number=f"ПЛ-226/{number}"))
                     for number in (1, 2)]
    sign_in(page, url, ADMIN, password)
    choose = choose_waybill(page, first, "Автокран")
    choose.get_by_role("button", name="Запомнить для техники и контрагента").click()
    expect(choose.get_by_role("button", name="Сбросить выбор по умолчанию")).to_be_visible()
    choose.get_by_role("button", name="Закрыть").click()
    choose = choose_waybill(page, second)
    expect(choose.get_by_text("Автокран", exact=True)).to_be_visible()
    assert WaybillDocuments(journal).issued_templates(token, second["id"]) == []
