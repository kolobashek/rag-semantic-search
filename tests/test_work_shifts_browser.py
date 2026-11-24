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
    org = dialog.locator(".q-select").filter(has_text="Организация доступа")
    assert ORG in org.inner_text() and "q-field--disabled" in org.get_attribute("class")
    for label, option in [("Сотрудник", "Водитель Севера"), ("Техника / госномер", "Машина Север"), ("Объект", "Склад Север")]:
        dialog.locator(".q-select:visible").filter(has_text=label).click()
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
    org = dialog.locator(".q-select").filter(has_text="Организация доступа")
    employee = dialog.locator(".q-select").filter(has_text="Сотрудник")
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
