"""Check the shift editor against the organization permissions used by the journal."""

from datetime import date
from pathlib import Path

from test_employee_access_browser import ADMIN, ORG, sign_in
from test_employee_access_browser import office as office
from test_employee_access_browser import pages as pages

from rag_catalog.core.work_shifts import WorkShiftJournal

TRUCK = dict(equipment_category="truck",licence_kind="road",licence_mark="C")
TRUCK_DOC = {"road":dict(marks=["C"],since="2020-01-01",until="2090-01-01")}


def test_org_shift_editor_uses_local_references_and_employee(office, pages):
    _, url, auth, password = office
    page, _ = pages
    token = auth.create_session(username=ADMIN)
    journal = WorkShiftJournal(Path(auth.db_path).parent / "work_shifts.db", auth)
    for kind, name in [("equipment", "Машина Север"), ("workplace", "Склад Север")]:
        journal.save_reference(token, kind, name, organization_id="north",**(TRUCK if kind=="equipment" else {}))
    assert auth.admin_create_user(username="local-driver", display_name="Водитель Севера", password=password,
                                  roles=["driver"], organization_id="north", must_change_password=False)
    journal.set_categories(token,"north","local-driver",["truck"],licences=TRUCK_DOC)
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
        journal.save_reference(root_token, "equipment", item, organization_id=org,**TRUCK)
        journal.set_categories(root_token,org,"two-offices",["truck"],licences=TRUCK_DOC)

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
    child.get_by_label("Категории сотрудника",exact=True).click()
    page.get_by_role("option",name="Погрузчики",exact=True).click()
    page.keyboard.press("Escape")
    child.get_by_text("Удостоверение тракториста-машиниста",exact=True).click()
    child.get_by_label("Открытые категории: Удостоверение тракториста-машиниста",exact=True).click()
    page.get_by_role("option",name="C",exact=True).click()
    page.keyboard.press("Escape")
    child.get_by_label("Дата выдачи",exact=True).locator("visible=true").fill("2020-01-01")
    child.get_by_label("Действительно до",exact=True).locator("visible=true").fill("2090-01-01")
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
            for label,option in [("Категория техники","Погрузчики"),("Требуемое удостоверение","Удостоверение тракториста-машиниста"),
                                 ("Требуемая категория прав","C")]:
                child.get_by_label(label,exact=True).click()
                page.get_by_role("option",name=option,exact=True).click()
        child.get_by_role("button",name="Сохранить",exact=True).click()
        expect(form.get_by_label(field,exact=True)).to_have_value(name if action != "Добавить технику" else "Погрузчик · гос. А123ВС · борт Б-019")
    equipment = form.get_by_label("Техника / госномер",exact=True)
    equipment.fill("Б-019")
    page.get_by_role("option",name="Погрузчик · гос. А123ВС · борт Б-019",exact=True).click()
    assert form.get_by_label("Номер путевого листа",exact=True).input_value() == "INLINE-19"
    form.get_by_text("Выполненные работы",exact=True).click()
    assert not form.get_by_role("button",name="Новый тариф",exact=True).count()
    assert not page.get_by_role("button",name="Расчёт за месяц",exact=True).count()
    form.get_by_label("Отработано без перерывов, ч",exact=True).fill("2")
    form.get_by_role("button",name="Сохранить",exact=True).click()
    form.wait_for(state="hidden")
    page.get_by_role("button",name="Табель",exact=True).click()
    page.get_by_role("dialog").get_by_text("Тестовый машинист",exact=True).wait_for()


def test_monthly_payroll_ui_bonus_and_cancellation(office,pages):
    _,url,auth,password = office
    page,_ = pages
    assert auth.admin_create_user(username="office-payroll-test",display_name="Проверка ведомости",
                                  password=password,roles=["user"],organization_id="north",must_change_password=False)
    auth.set_payroll_owner("office-payroll-test")
    sign_in(page,url,"office-payroll-test",password)
    page.get_by_role("button",name="Расчёт за месяц",exact=True).click()
    form = page.get_by_role("dialog").first
    child = page.get_by_role("dialog").last
    assert not page.get_by_role("button",name="Новая смена",exact=True).count()
    form.get_by_label("Организация журнала",exact=True).click()
    page.get_by_role("option",name=ORG,exact=True).click()
    form.get_by_role("button",name="Новый тариф",exact=True).click()
    child.get_by_label("Вид работы",exact=True).fill("Ремонт")
    child.get_by_label("Ставка, ₽ за единицу",exact=True).fill("250")
    child.get_by_role("button",name="Сохранить тариф",exact=True).click()
    form.get_by_role("tab",name="Тарифы",exact=True).click()
    form.get_by_role("cell",name="250.00",exact=True).wait_for()
    form.get_by_role("tab",name="Предварительный расчёт",exact=True).click()
    form.get_by_role("button",name="Норма месяца").click()
    child.get_by_role("button",name="Подтвердить норму").click()
    form.get_by_text("Норма:",exact=False).wait_for()
    form.get_by_role("button",name="Оклад",exact=True).click()
    child.get_by_label("Сотрудник",exact=True).click()
    page.get_by_role("option",name="Проверка ведомости",exact=True).click()
    child.get_by_label("Оклад, ₽",exact=True).fill("48000")
    child.get_by_role("button",name="Сохранить",exact=True).click()
    form.get_by_role("cell",name="48000.00",exact=True).wait_for()
    form.get_by_role("button",name="Добавить операцию",exact=True).click()
    child.get_by_label("Сотрудник",exact=True).click()
    page.get_by_role("option",name="Проверка ведомости",exact=True).click()
    child.get_by_label("Вид операции",exact=True).click()
    page.get_by_role("option",name="Премия · Начисление",exact=True).click()
    child.get_by_label("Сумма, ₽",exact=True).fill("3000")
    child.get_by_label("Основание / документ",exact=True).fill("Приказ 15")
    child.get_by_role("button",name="Сохранить",exact=True).click()
    form.get_by_role("tab",name="Начисления и удержания").click()
    form.get_by_role("button",name="Добавить операцию",exact=True).click()
    child.get_by_label("Сотрудник",exact=True).click()
    page.get_by_role("option",name="Проверка ведомости",exact=True).click()
    child.get_by_label("Вид операции",exact=True).click()
    page.get_by_role("option",name="Профсоюзный взнос · Добровольный взнос",exact=True).click()
    assert child.get_by_label("Ставка, %",exact=True).input_value()=="1"
    child.get_by_role("checkbox",name="Есть заявление сотрудника на взнос").click()
    child.get_by_label("Основание / документ",exact=True).fill("Заявление 3")
    child.get_by_role("button",name="Сохранить",exact=True).click()
    form.get_by_role("row").filter(has_text="Заявление 3").get_by_role("cell",name="30.00",exact=True).wait_for()
    item = form.get_by_role("row").filter(has_text="Приказ 15")
    item.get_by_role("checkbox").click()
    form.get_by_role("button",name="Отменить с месяца").click()
    page.get_by_role("dialog").filter(has_text="Отменить «Премия»").get_by_role("button",name="Отменить с месяца").click()
    item.get_by_text("Отменена",exact=True).wait_for()


def test_payroll_button_for_designated_admin_but_not_other_admin(office, pages):
    _, url, auth, password = office
    allowed_page, denied_page = pages
    admin = next(u["username"] for u in auth.list_users() if u["is_platform_admin"])
    auth.set_payroll_admin(admin)
    sign_in(allowed_page, url, admin, password)
    allowed_page.get_by_role("button", name="Расчёт за месяц", exact=True).click()
    allowed_page.get_by_role("dialog").get_by_role("tab", name="Тарифы", exact=True).wait_for()
    sign_in(denied_page, url, ADMIN, password)
    denied_page.get_by_role("button", name="Табель", exact=True).wait_for()
    assert not denied_page.get_by_role("button", name="Расчёт за месяц", exact=True).count()


def test_driver_and_machine_filter_each_other(office,pages):
    _,url,auth,password=office
    page,_=pages
    j=WorkShiftJournal(Path(auth.db_path).parent/"work_shifts.db",auth)
    t=auth.create_session(username=ADMIN)
    for name,category,kind,mark in [("Грейдер Г-78","grader","tractor","C"),("Легковой Б-78","car","road","B")]:
        j.save_reference(t,"equipment",name,organization_id="north",equipment_category=category,licence_kind=kind,licence_mark=mark)
        j.save_employee(t,organization_id="north",create=True,offline=True,display_name="Оператор "+name,
                        categories=[category],licences={kind:dict(marks=[mark],since="2020-01-01",until="2090-01-01")})
    sign_in(page,url,ADMIN,password)
    def open_form():
        page.get_by_role("button",name="Новая смена",exact=True).click()
        return page.get_by_role("dialog")
    form=open_form()
    form.get_by_label("Техника / госномер",exact=True).click()
    page.get_by_role("option",name="Грейдер Г-78",exact=True).click()
    form.get_by_label("Сотрудник",exact=True).click()
    page.get_by_role("option",name="Оператор Грейдер Г-78 (без входа)",exact=True).wait_for()
    assert not page.get_by_role("option",name="Оператор Легковой Б-78 (без входа)",exact=True).is_visible()
    page.keyboard.press("Escape")
    form.get_by_role("button",name="Отмена",exact=True).click()
    form=open_form()
    form.get_by_label("Сотрудник",exact=True).click()
    page.get_by_role("option",name="Оператор Легковой Б-78 (без входа)",exact=True).click()
    form.get_by_label("Техника / госномер",exact=True).click()
    page.get_by_role("option",name="Легковой Б-78",exact=True).wait_for()
    assert not page.get_by_role("option",name="Грейдер Г-78",exact=True).is_visible()
