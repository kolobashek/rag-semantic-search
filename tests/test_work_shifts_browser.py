"""Check the shift editor against the organization permissions used by the journal."""

from datetime import date
from pathlib import Path

from test_employee_access_browser import ADMIN, ORG, sign_in
from test_employee_access_browser import office as office
from test_employee_access_browser import pages as pages

from rag_catalog.core.work_shifts import WorkShiftJournal


def test_org_shift_editor_uses_local_references_and_employee(office, pages):
    _, url, auth, password = office
    page, _ = pages
    token = auth.create_session(username=ADMIN)
    journal = WorkShiftJournal(Path(auth.db_path).parent / "work_shifts.db", auth)
    for kind, name in [("equipment", "Машина Север"), ("workplace", "Склад Север")]:
        journal.save_reference(token, kind, name, organization_id="north")
    assert auth.admin_create_user(username="local-driver", display_name="Водитель Севера", password=password,
                                  roles=["driver"], organization_id="north", must_change_password=False)
    sign_in(page, url, ADMIN, password)
    page.get_by_role("button", name="Новая смена", exact=True).click()
    dialog = page.get_by_role("dialog")
    org = dialog.locator(".q-select").filter(has_text="Организация журнала")
    assert not org.is_visible()
    for label, option in [("Сотрудник", "Водитель Севера"), ("Техника / госномер", "Машина Север"), ("Объект", "Склад Север")]:
        dialog.get_by_label(label,exact=True).click()
        page.get_by_role("option", name=option, exact=True).click()
    dialog.get_by_role("button", name="Сохранить", exact=True).click()
    dialog.wait_for(state="hidden")
    today = date.today().isoformat()
    rows = journal.list(token, date_from=today, date_to=today)["rows"]
    assert [(r["employee"], r["organization_id"], r["equipment"], r["workplace"]) for r in rows] == [
        ("local-driver", "north", "Машина Север", "Склад Север")
    ]


def test_changing_shift_company_rechecks_mixed_role_and_reference_options(office, pages):
    _, url, auth, password = office
    page, _ = pages
    root = next(u["username"] for u in auth.list_users() if u["is_platform_admin"])
    assert auth.create_employee_organization(admin_username=root, organization_id="south", name="Южный филиал")["ok"]
    assert auth.admin_create_user(username="two-offices", password=password, roles=["dispatcher"],
                                  organization_id="north", must_change_password=False)
    assert auth.set_employee_org_roles(admin_username=root, username="two-offices", organization_id="south",
                                       roles=["driver"])["ok"]
    journal = WorkShiftJournal(Path(auth.db_path).parent / "work_shifts.db", auth)
    root_token = auth.create_session(username=root)
    for org, item in [("north", "Север-101"), ("south", "Юг-202")]:
        journal.save_reference(root_token, "equipment", item, organization_id=org)

    sign_in(page, url, "two-offices", password)
    page.get_by_role("button", name="Новая смена", exact=True).click()
    dialog = page.get_by_role("dialog")
    org = dialog.locator(".q-select").filter(has_text="Организация журнала")
    employee = dialog.locator(".q-select").filter(has=page.get_by_text("Сотрудник",exact=True))
    equipment = dialog.locator(".q-select").filter(has_text="Техника / госномер")
    assert not dialog.get_by_role("button", name="Сохранить и отправить").is_enabled()
    for name, reference, forbidden, save_label, manages in [
        (ORG, "Север-101", "Юг-202", "Сохранить", True),
        ("Южный филиал", "Юг-202", "Север-101", "Сохранить и отправить", False),
    ]:
        org.click()
        page.get_by_role("option", name=name).click()
        dialog.get_by_role("button", name=save_label, exact=True).wait_for()
        assert ("q-field--disabled" not in employee.get_attribute("class")) == manages
        equipment.click()
        page.get_by_role("option", name=reference, exact=True).wait_for()
        assert not page.get_by_role("option", name=forbidden, exact=True).is_visible()
        page.get_by_role("option", name=reference, exact=True).click()


def test_inline_offline_employee_equipment_work_and_timesheet(office, pages):
    from playwright.sync_api import expect
    _, url, auth, password = office
    page, _ = pages
    sign_in(page, url, ADMIN, password)
    page.get_by_role("button", name="Новая смена", exact=True).click()
    form = page.get_by_role("dialog").first
    form.get_by_label("Номер путевого листа",exact=True).fill("INLINE-19")
    form.get_by_role("button", name="Добавить сотрудника",exact=True).click()
    child = page.get_by_role("dialog").last
    child.get_by_label("ФИО",exact=True).fill("Тестовый машинист")
    assert not child.get_by_label("Логин",exact=True).is_visible()
    child.get_by_role("button",name="Сохранить",exact=True).click()
    expect(form.get_by_label("Сотрудник",exact=True)).to_have_value("Тестовый машинист (без входа)")
    for action, name, field in [("Добавить технику","Погрузчик","Техника / госномер"),("Добавить объект","Карьер 19","Объект"),
                                ("Добавить контрагента","Партнёр 19","Контрагент")]:
        form.get_by_role("button",name=action,exact=True).click()
        child.get_by_label("Название",exact=True).fill(name)
        if action == "Добавить технику":
            child.get_by_label("Госномер",exact=True).fill("А123ВС")
            child.get_by_label("Бортовой номер",exact=True).fill("Б-019")
        child.get_by_role("button",name="Сохранить",exact=True).click()
        expect(form.get_by_label(field,exact=True)).to_have_value(name if action != "Добавить технику" else "Погрузчик · гос. А123ВС · борт Б-019")
    equipment = form.get_by_label("Техника / госномер",exact=True)
    equipment.fill("Б-019")
    page.get_by_role("option",name="Погрузчик · гос. А123ВС · борт Б-019",exact=True).click()
    assert form.get_by_label("Номер путевого листа",exact=True).input_value() == "INLINE-19"
    form.get_by_text("Работы и начисление",exact=True).click()
    form.get_by_role("button",name="Новый тариф",exact=True).click()
    child.get_by_label("Вид работы",exact=True).fill("Ремонт")
    child.get_by_label("Ставка, ₽ за единицу",exact=True).fill("250")
    child.get_by_role("button",name="Сохранить тариф",exact=True).click()
    form.get_by_label("Объём",exact=True).fill("2")
    form.get_by_label("Отработано без перерывов, ч",exact=True).fill("2")
    form.get_by_role("button",name="Сохранить",exact=True).click()
    form.wait_for(state="hidden")
    page.get_by_role("button",name="Табель",exact=True).click()
    page.get_by_role("dialog").get_by_text("Тестовый машинист",exact=True).wait_for()
