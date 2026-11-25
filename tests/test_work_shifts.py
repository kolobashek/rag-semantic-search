from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from types import SimpleNamespace

import pytest

from rag_catalog.core.work_shifts import WorkShiftJournal


@pytest.fixture
def journal(tmp_path):
    users = {name: dict(username=name, display_name=name.title(), role=role, status="active")
             for name, role in (("admin", "admin"), ("alice", "driver"), ("bob", "driver"), ("dispatcher", "dispatcher"))}
    auth = SimpleNamespace(get_user_by_session=lambda token: users.get(token),
                           get_user=lambda username: users.get(username), list_users=lambda: list(users.values()))
    return WorkShiftJournal(tmp_path / "shifts.db", auth)


@pytest.fixture
def payload():
    return dict(work_date="2026-10-06", shift_number=1, organization="Specmash", equipment="Truck A",
                workplace="Site A", partner="Customer", waybill_number="001", hours=8.5, breaks=0.5, comment="")


def listing(journal, token="alice", **kwargs):
    return journal.list(token, date_from="2026-10-01", date_to="2026-10-31", **kwargs)


def test_create_edit_persist_and_history(journal, payload):
    row = journal.save("alice", payload)
    assert row["work_minutes"] == 510
    assert row["break_minutes"] == 30
    result = journal.save("alice", dict(payload, hours=9), shift_id=row["id"], revision=row["revision"])
    assert result["revision"] == 2
    assert listing(WorkShiftJournal(journal.path, journal.auth))["work_minutes"] == 540
    history = journal.history("alice", row["id"])
    assert [e["action"] for e in history] == ["create", "edit"]
    assert '"work_minutes": 510' in history[0]["snapshot"]


def test_acl_covers_listing_history_export_and_writes(journal, payload):
    row = journal.save("alice", payload)
    assert listing(journal, "bob", employee="alice")["count"] == 0
    assert "Truck A" not in journal.export_csv("bob", date_from="2026-10-01", date_to="2026-10-31").decode("utf-8-sig")
    assert journal.users("bob") == {"bob": "Bob"}
    assert listing(journal, "admin")["count"] == 1
    with pytest.raises(PermissionError):
        journal.save("bob", dict(payload, employee="alice"))
    for operation in (
        lambda: journal.history("bob", row["id"]),
        lambda: journal.save("bob", payload, shift_id=row["id"], revision=1),
        lambda: journal.delete("bob", row["id"], 1),
        lambda: journal.transition("bob", row["id"], 1, "submitted"),
    ):
        with pytest.raises(PermissionError):
            operation()


def test_admin_can_record_for_employee(journal, payload):
    row = journal.save("admin", dict(payload, employee="alice"))
    assert row["employee"] == "alice"
    assert journal.history("alice", row["id"])[0]["actor"] == "admin"


@pytest.mark.parametrize("token", ["", "expired"])
def test_auth_required(journal, payload, token):
    with pytest.raises(PermissionError):
        journal.save(token, payload)
    with pytest.raises(PermissionError):
        listing(journal, token)


@pytest.mark.parametrize("change", [dict(status="disabled"), dict(must_change_password=1)])
def test_account_changes_apply_immediately(journal, payload, change):
    journal.auth.get_user_by_session("alice").update(change)
    with pytest.raises(PermissionError):
        journal.save("alice", payload)


def test_role_revocation_is_checked_on_each_operation(journal, payload):
    row = journal.save("alice", payload)
    journal.auth.get_user_by_session("admin")["role"] = "user"
    with pytest.raises(PermissionError):
        journal.transition("admin", row["id"], 1, "approved")


def test_driver_changes_invalidate_approval_and_manager_reopen_requires_reason(journal, payload):
    row = journal.save("alice", payload)
    assert row["status"] == "submitted"
    with pytest.raises(PermissionError):
        journal.transition("alice", row["id"], 1, "approved")
    journal.transition("dispatcher", row["id"], 1, "approved")
    result = journal.save("alice", dict(payload, hours=9), shift_id=row["id"], revision=2)
    assert result["status"] == "submitted"
    with pytest.raises(PermissionError):
        journal.transition("alice", row["id"], 3, "draft", "Correction")
    journal.transition("admin", row["id"], 3, "approved")
    with pytest.raises(ValueError):
        journal.transition("admin", row["id"], 4, "draft")
    journal.transition("admin", row["id"], 4, "draft", "Correction")
    assert journal.history("alice", row["id"])[-1]["reason"] == "Correction"


def test_conflicting_edit_and_delete_never_overwrite(journal, payload):
    row = journal.save("alice", payload)
    journal.save("alice", dict(payload, hours=10), shift_id=row["id"], revision=1)
    for operation in (
        lambda: journal.save("alice", payload, shift_id=row["id"], revision=1),
        lambda: journal.delete("alice", row["id"], 1),
        lambda: journal.transition("alice", row["id"], 1, "submitted"),
        lambda: journal.save("alice", payload, shift_id=row["id"]),
        lambda: journal.delete("alice", row["id"], None),
        lambda: journal.transition("alice", row["id"], None, "submitted"),
    ):
        with pytest.raises(ValueError):
            operation()
    assert listing(journal)["work_minutes"] == 600
    assert len(journal.history("alice", row["id"])) == 2


def test_duplicate_rejected_without_overwriting_and_soft_delete(journal, payload):
    row = journal.save("alice", payload)
    with pytest.raises(ValueError):
        journal.save("alice", dict(payload, equipment="TRUCK A", hours=1))
    assert listing(journal)["work_minutes"] == 510
    journal.save("bob", payload)
    journal.delete("dispatcher", row["id"], 1)
    assert listing(journal)["count"] == 0
    journal.save("alice", payload)
    with journal._connection() as conn:
        assert conn.execute("SELECT deleted FROM shifts WHERE id=?", (row["id"],)).fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM shift_events WHERE shift_id=?", (row["id"],)).fetchone()[0] == 2


def test_concurrent_duplicate_creates_one_row(journal, payload):
    def create():
        try:
            return journal.save("alice", payload)["id"]
        except ValueError:
            return None
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: create(), range(2)))
    assert len([result for result in results if result is not None]) == 1
    assert listing(journal)["count"] == 1


@pytest.mark.parametrize("changes", [
    {"hours": -1}, {"hours": "nan"}, {"hours": "inf"}, {"hours": 25}, {"breaks": -1},
    {"hours": 24, "breaks": 1}, {"shift_number": 0}, {"shift_number": True},
    {"work_date": "2026-02-30"}, {"organization": ""}, {"equipment": " "}, {"workplace": ""},
    {"comment": "a" * 2001}, {"employee": "missing"},
])
def test_validation(journal, payload, changes):
    with pytest.raises((ValueError, PermissionError)):
        journal.save("alice", dict(payload, **changes))
    assert listing(journal)["count"] == 0


def test_zero_hours_draft_cannot_be_submitted(journal, payload):
    row = journal.save("admin", dict(payload, hours=0, employee="alice"))
    with pytest.raises(ValueError):
        journal.transition("admin", row["id"], 1, "submitted")


def test_filters_pagination_and_totals(journal, payload):
    for number in range(1, 4):
        journal.save("alice", dict(payload, shift_number=number))
    result = listing(journal, limit=1, offset=1)
    assert result["count"] == 3
    assert result["work_minutes"] == 1530
    assert len(result["rows"]) == 1
    assert result["rows"][0]["shift_number"] == 2
    assert listing(journal, status="approved")["count"] == 0


def test_csv_unicode_and_formula_injection(journal, payload):
    journal.save("alice", dict(payload, organization="ООО Спецмаш", comment="  =HYPERLINK(1)"))
    csv = journal.export_csv("alice", date_from="2026-10-01", date_to="2026-10-31")
    assert csv.startswith(b"\xef\xbb\xbf")
    assert "ООО Спецмаш" in csv.decode("utf-8-sig")
    assert "'=HYPERLINK(1)" in csv.decode("utf-8-sig")


def test_failed_validation_does_not_change_input(journal, payload):
    original = deepcopy(payload)
    journal.save("alice", payload)
    assert payload == original


def test_real_session_revocation(tmp_path, monkeypatch, payload):
    from rag_catalog.core.user_auth_db import UserAuthDB

    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    auth = UserAuthDB(str(tmp_path / "users.db"))
    auth.admin_create_user(username="alice", password="only-for-test", must_change_password=False, roles=["driver"])
    token = auth.create_session(username="alice")
    journal = WorkShiftJournal(tmp_path / "shifts.db", auth)
    journal.save(token, payload)
    assert listing(journal, token)["count"] == 1
    auth.revoke_session(token)
    with pytest.raises(PermissionError):
        listing(journal, token)


@pytest.fixture
def company_journal(tmp_path, monkeypatch):
    from rag_catalog.core.user_auth_db import UserAuthDB

    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    auth = UserAuthDB(str(tmp_path / "staff.db"))

    def staff(name, role, org="default", platform=False):
        assert auth.admin_create_user(username=name, password="test-only-password", role=role,
                                      organization_id=org, platform_admin=platform, must_change_password=False)
        return auth.create_session(username=name)

    tokens = {"root": staff("root", "admin", platform=True)}
    assert auth.create_employee_organization(admin_username="root", name="Север", organization_id="north")["ok"]
    assert auth.create_employee_organization(admin_username="root", name="Юг", organization_id="south")["ok"]
    for name, role, org in [("mixed", "dispatcher", "north"), ("northie", "driver", "north"),
                            ("southie", "driver", "south"), ("southadmin", "admin", "south")]:
        tokens[name] = staff(name, role, org)
    assert auth.set_employee_org_roles(admin_username="root", username="mixed", organization_id="south", roles=["driver"])["ok"]
    return WorkShiftJournal(tmp_path / "shifts.db", auth), tokens


def test_mixed_org_shift_acl_for_all_operations(company_journal, payload):
    from rag_catalog.core.shift_attachments import ShiftAttachments

    journal, tokens = company_journal
    mixed, south = tokens["mixed"], tokens["southadmin"]
    north_shift = journal.save(mixed, dict(payload, organization="Север", employee="northie", equipment="North truck"))
    own_shift = journal.save(mixed, dict(payload, organization="Юг", equipment="Own truck"))
    other = journal.save(south, dict(payload, organization="Юг", employee="southie", equipment="South truck"))
    other_id = other["id"]
    assert north_shift["status"] == "draft" and own_shift["status"] == "submitted"
    assert journal.get(mixed, own_shift["id"])["organization_id"] == "south"
    assert journal.organizations(mixed) == {"north": "Север", "south": "Юг"}
    assert journal.organizations(mixed, manage=True) == {"north": "Север"}
    assert set(journal.users(mixed, organization_id="south")) == {"mixed"}
    assert "southie" not in journal.users(mixed)
    assert "northie" in journal.users(mixed, organization_id="north")
    assert {r["id"] for r in listing(journal, mixed)["rows"]} == {north_shift["id"], own_shift["id"]}
    assert not listing(journal, mixed, employee="southie")["count"]
    exported = journal.export_csv(mixed, date_from="2026-10-01", date_to="2026-10-31").decode("utf-8-sig")
    assert "North truck" in exported and "Own truck" in exported and "South truck" not in exported
    assert {r["shift_id"] for r in journal.audit(mixed)["rows"]} == {north_shift["id"]}
    store = ShiftAttachments(journal)
    attachment = store.add(south, other_id, "delivery", "private.pdf", b"private")
    for action in (
        lambda: journal.get(mixed, other_id), lambda: journal.history(mixed, other_id),
        lambda: journal.save(mixed, dict(payload, organization="Юг"), shift_id=other_id, revision=1),
        lambda: journal.save(mixed, dict(payload, organization="Юг", employee="southie")),
        lambda: journal.transition(mixed, own_shift["id"], 1, "approved"),
        lambda: journal.transition(mixed, other_id, 1, "approved"), lambda: journal.delete(mixed, other_id, 1),
        lambda: store.list(mixed, other_id), lambda: store.read(mixed, other_id, attachment),
        lambda: store.read(mixed, own_shift["id"], attachment),
        lambda: store.add(mixed, other_id, "wrong", "wrong.pdf", b"wrong"),
        lambda: journal.save(mixed, dict(payload, organization="Юг", organization_id="north")),
        lambda: journal.save(mixed, dict(payload, organization="unknown")),
    ):
        with pytest.raises(PermissionError):
            action()
    with pytest.raises(ValueError):
        journal.save(mixed, dict(payload, organization="Север", employee="southie"))
    journal.delete(south, other_id, 1)
    with pytest.raises(PermissionError):
        journal.history(mixed, other_id)
    assert journal.history(south, other_id)[-1]["action"] == "delete"


def test_company_references_directory_and_driver_management(company_journal):
    journal, tokens = company_journal
    mixed = tokens["mixed"]
    for token, org in [(mixed, "north"), (tokens["southadmin"], "south")]:
        journal.save_reference(token, "equipment", "Машина", organization_id=org)
    south_ref = next(r for r in journal.references(mixed) if r["organization_id"] == "south")
    journal.save_reference(tokens["southadmin"], "equipment", "Архив юга", record_id=south_ref["id"],
                           organization_id="south", revision=1, archived=True)
    assert {r["name"] for r in journal.references(mixed, archived=True)} == {"Машина"}
    for org, error in [("north", ValueError), ("south", PermissionError)]:
        with pytest.raises(error):
            journal.save_reference(mixed, "equipment", "rename", organization_id=org, record_id=south_ref["id"], revision=2)
    with pytest.raises(PermissionError):
        journal.save_reference(tokens["root"], "equipment", "Ghost", organization_id="not_a_company")
    journal.save_employee(mixed, username="hired", display_name="Новый", organization_id="north",
                          create=True, password="only-for-test-12345")
    assert journal.auth.get_user(username="hired")["organization_ids"] == ["north"]
    staff = {p["username"]: p for p in journal.employees(mixed)}
    assert "southie" not in staff and staff["hired"]["editable"] and not staff["mixed"]["editable"]
    journal.save_employee(mixed, username="hired", display_name="Изменён", organization_id=staff["hired"]["organization_ids"][0])
    with pytest.raises(PermissionError):
        journal.save_employee(mixed, username="southie", display_name="Forbidden", organization_id="south")


def test_legacy_shift_name_colliding_with_new_org_keeps_original_scope(company_journal, payload):
    journal, tokens = company_journal
    original = journal.save(tokens["root"], dict(payload, organization="Старая компания"))
    assert original["organization_id"] == "default"
    assert journal.auth.create_employee_organization(admin_username="root", name="Старая компания", organization_id="later")["ok"]
    edited = journal.save(tokens["root"], dict(payload, organization="Старая компания", comment="Исправление"),
                          shift_id=original["id"], revision=1)
    assert edited["organization_id"] == "default"
    with pytest.raises(PermissionError):
        journal.save(tokens["root"], dict(payload, organization="Старая компания", organization_id="later"),
                     shift_id=original["id"], revision=2)
    newer = journal.save(tokens["root"], dict(payload, organization="Старая компания", shift_number=2))
    assert newer["organization_id"] == "later"


def test_upgrade_assigns_existing_shifts_and_archived_reference_to_default(company_journal, payload):
    journal, tokens = company_journal
    old = journal.save(tokens["root"], payload)
    with journal._connection() as conn:
        conn.execute("ALTER TABLE shifts DROP COLUMN organization_id")
        conn.execute("DROP TABLE shift_references")
        conn.execute("""CREATE TABLE shift_references(id INTEGER PRIMARY KEY, kind TEXT NOT NULL,
                     name TEXT NOT NULL, name_key TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0,
                     revision INTEGER NOT NULL DEFAULT 1, UNIQUE(kind,name_key))""")
        conn.execute("INSERT INTO shift_references VALUES(31,'equipment','История','история',1,4)")
    reopened = WorkShiftJournal(journal.path, journal.auth)
    assert reopened.get(tokens["root"], old["id"])["organization_id"] == "default"
    assert reopened.references(tokens["root"], archived=True) == [{
        "id": 31, "kind": "equipment", "name": "История", "name_key": "история",
        "organization_id": "default", "archived": 1, "revision": 4,
        "equipment_name": "", "registration_number": "", "fleet_number": "", "equipment_category":"",
        "licence_kind":"", "licence_mark":"",
    }]
    assert not reopened.references(tokens["mixed"], archived=True)
    with pytest.raises(PermissionError):
        reopened.get(tokens["mixed"], old["id"])
