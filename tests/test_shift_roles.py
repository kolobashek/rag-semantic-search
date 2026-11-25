import json
import sqlite3

import pytest

from rag_catalog.core.roles import can_manage_shifts, can_open_screen, can_use_catalog, user_roles
from rag_catalog.core.user_auth_db import SCHEMA_VERSION, UserAuthDB
from rag_catalog.core.work_shifts import WorkShiftJournal


@pytest.fixture
def accounts(tmp_path, monkeypatch):
    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    auth = UserAuthDB(str(tmp_path / "users.db"))
    roles = {"driver": ["driver"], "dispatch": ["dispatcher"], "both": ["driver", "user"],
             "admin": ["admin"], "cloud": ["user"]}
    for name, values in roles.items():
        auth.admin_create_user(username=name, password="test-only-password", roles=values,
                               platform_admin=name == "admin", must_change_password=False)
    return auth, {name: auth.create_session(username=name) for name in roles}


def data(**changes):
    return dict(work_date="2026-10-07", shift_number=1, organization="Org", equipment="Truck", workplace="Site",
                hours=8, **changes)


def test_multiple_roles_and_live_revocation(accounts):
    auth, tokens = accounts
    both = auth.get_user_by_session(tokens["both"])
    assert user_roles(both) == {"driver", "user"}
    assert both["role"] == "user"
    assert can_use_catalog(both) and can_open_screen(both, "shifts")
    for name in ("driver", "dispatch"):
        u = auth.get_user_by_session(tokens[name])
        assert can_open_screen(u, "shifts")
        for screen in ("search", "explorer", "jobs", "index", "stats", "settings"):
            assert not can_open_screen(u, screen)
    assert can_manage_shifts(auth.get_user(username="dispatch"))
    auth.admin_update_user(username="both", display_name="Both", telegram_chat_id="", role="driver",
                           roles=["driver"], status="active", must_change_password=False)
    assert not can_use_catalog(auth.get_user_by_session(tokens["both"]))
    assert auth.login_with_reason(username="both", password="test-only-password")["user"]["roles"] == ["driver"]


def test_legacy_role_migration(accounts):
    auth, _ = accounts
    with sqlite3.connect(auth.db_path) as conn:
        conn.execute("UPDATE users SET roles_json='' WHERE username='admin'")
    assert user_roles(auth.get_user(username="admin")) == {"admin"}
    assert user_roles({"role": "admin", "roles_json": "broken"}) == set()
    with pytest.raises(ValueError):
        auth.admin_create_user(username="bad", password="test-password", roles=["superadmin"])


def test_dispatcher_employee_management_cannot_escalate(accounts):
    auth, tokens = accounts
    auth.save_driver(tokens["dispatch"], username="new-driver", display_name="Driver Name",
                     password="temporary-password", create=True)
    assert auth.get_user(username="new-driver")["roles"] == ["driver"]
    assert auth.get_user(username="new-driver")["must_change_password"] == 1
    auth.save_driver(tokens["dispatch"], username="new-driver", display_name="Renamed", archived=True)
    assert auth.get_user(username="new-driver")["status"] == "blocked"
    for name in ("admin", "dispatch", "both", "cloud"):
        with pytest.raises(PermissionError):
            auth.save_driver(tokens["dispatch"], username=name, display_name="Unauthorized", password="new-password-123")
    with pytest.raises(PermissionError):
        auth.save_driver(tokens["driver"], username="new-driver", display_name="Unauthorized")
    auth.revoke_session(tokens["dispatch"])
    with pytest.raises(PermissionError):
        auth.save_driver(tokens["dispatch"], username="new-driver", display_name="Unauthorized")


def test_journal_workflow_audit_and_archives(accounts, tmp_path):
    auth, tokens = accounts
    journal = WorkShiftJournal(tmp_path / "shifts.db", auth)
    driver, dispatcher = tokens["driver"], tokens["dispatch"]
    row = journal.save(driver, data())
    assert row["status"] == "submitted"
    journal.transition(dispatcher, row["id"], 1, "approved")
    edited = journal.save(driver, data(comment="Correction"), shift_id=row["id"], revision=2)
    assert edited["status"] == "submitted"
    with pytest.raises(ValueError):
        journal.transition(dispatcher, row["id"], 2, "approved")
    with pytest.raises(PermissionError):
        journal.delete(driver, row["id"], 3)
    journal.delete(dispatcher, row["id"], 3)
    audit = journal.audit(dispatcher, limit=2)
    assert audit["count"] == 4
    assert audit["rows"][0]["changes"]["deleted"] == [0, 1]
    assert audit["rows"][1]["changes"]["status"] == ["approved", "submitted"]
    assert audit["rows"][1]["actor"] == "driver"
    assert len(journal.history(dispatcher, row["id"])) == 4
    for token in (driver, tokens["cloud"]):
        with pytest.raises(PermissionError):
            journal.audit(token)
    with pytest.raises(PermissionError):
        journal.save(tokens["cloud"], data())


def test_dispatcher_directories_persist_archive_and_conflict(accounts, tmp_path):
    auth, tokens = accounts
    journal = WorkShiftJournal(tmp_path / "shifts.db", auth)
    token = tokens["dispatch"]
    for kind in ("organization", "equipment", "workplace", "partner"):
        journal.save_reference(token, kind, "Sample")
    row = journal.references(token)[0]
    journal.save_reference(token, row["kind"], "Renamed", record_id=row["id"], revision=1)
    with pytest.raises(ValueError):
        journal.save_reference(token, row["kind"], "Stale", record_id=row["id"], revision=1)
    journal.save_reference(token, row["kind"], "Renamed", record_id=row["id"], revision=2, archived=True)
    assert len(WorkShiftJournal(journal.path, auth).references(tokens["driver"])) == 3
    assert len(journal.references(token, archived=True)) == 4
    with pytest.raises(PermissionError):
        journal.save_reference(tokens["driver"], "equipment", "Illegal")


def test_cloud_api_rejects_driver_and_dispatcher(accounts, monkeypatch):
    from fastapi import HTTPException

    from rag_catalog.ui import api
    auth, tokens = accounts
    monkeypatch.setattr(api, "_get_api_auth_db", lambda cfg: auth)
    for name in ("driver", "dispatch"):
        with pytest.raises(HTTPException) as exc:
            api._require_cloud_drive_api_user({}, authorization=f"Bearer {tokens[name]}")
        assert exc.value.status_code == 403
    assert api._require_cloud_drive_api_user({}, authorization=tokens["both"])["username"] == "both"


def test_driver_has_telegram_identity_without_catalog_access(accounts):
    from rag_catalog.integrations.telegram_bot import get_authorized_telegram_user
    from rag_catalog.ui.helpers import _cd_acl_allows, _cd_acl_allows_local_file

    auth, _ = accounts
    driver = auth.get_user(username="driver")
    assert not _cd_acl_allows({}, driver, "anything")
    assert not _cd_acl_allows_local_file({}, driver, "C:/example.pdf")
    class TelegramAuth:
        def get_user_by_telegram_chat_id(self, chat):
            return driver
    assert get_authorized_telegram_user(TelegramAuth(), "123") == driver


def test_v3_database_migrates_without_losing_existing_users(accounts):
    auth, tokens = accounts
    with sqlite3.connect(auth.db_path) as conn:
        conn.execute("DROP TRIGGER employee_membership_after_insert")
        conn.execute("DROP TABLE employee_memberships")
        conn.execute("DROP TABLE employee_platform_admins")
        conn.execute("ALTER TABLE users DROP COLUMN roles_json")
        conn.execute("UPDATE schema_meta SET schema_version=3 WHERE db_kind='user_auth'")
    reopened = UserAuthDB(str(auth.db_path))
    assert reopened.get_user_by_session(tokens["admin"])["roles"] == ["admin"]
    assert reopened.get_user_by_session(tokens["cloud"])["roles"] == ["user"]
    with sqlite3.connect(auth.db_path) as conn:
        assert conn.execute("SELECT schema_version FROM schema_meta WHERE db_kind='user_auth'").fetchone()[0] == SCHEMA_VERSION


def test_manager_can_correct_archived_employee_without_resurrecting_renamed_reference(accounts, tmp_path):
    auth, tokens = accounts
    journal = WorkShiftJournal(tmp_path / "shifts.db", auth)
    token = tokens["dispatch"]
    row = journal.save(token, data(employee="driver"))
    reference = next(r for r in journal.references(token) if r["kind"] == "equipment")
    journal.save_reference(token, "equipment", "New name", record_id=reference["id"], revision=1)
    auth.save_driver(token, username="driver", display_name="Former Driver", archived=True)
    journal.save(token, data(employee="driver", comment="Correction"), shift_id=row["id"], revision=1)
    assert [r["name"] for r in journal.references(token) if r["kind"] == "equipment"] == ["New name"]
    with pytest.raises(ValueError):
        journal.save(token, data(employee="driver"))


def test_offline_builder_attendance_and_approved_month_sheet(accounts,tmp_path):
    auth,tokens=accounts
    j=WorkShiftJournal(tmp_path/"shifts.db",auth)
    t=tokens["dispatch"]
    args=dict(create=True,offline=True,display_name="Builder",categories=["builder","car"])
    with pytest.raises(PermissionError):
        j.save_employee(tokens["driver"],**args)
    key=j.save_employee(t,**args)
    assert auth.get_user(username=key) is None
    assert key in j.users(t,organization_id="default",category="builder")
    assert key not in j.users(tokens["driver"],organization_id="default")
    work={**data(),"equipment":"","entry_type":"attendance","employee":key}
    r=j.save(t,work)
    assert j.month_sheet(t,"2026-10",attendance_only=True)[key]["days"]=={7:480}
    assert j.month_sheet(t,"2026-10")[key]["approved"]==0
    j.transition(t,r["id"],1,"approved")
    assert j.month_sheet(t,"2026-10")[key]["approved"]==480
    with pytest.raises(PermissionError):
        j.month_sheet(tokens["driver"],"2026-10")
    j.save_employee(t,offline=True,username=key,display_name="Builder",archived=True)
    assert key not in j.users(t) and key in j.month_sheet(t,"2026-10")


def test_machine_requires_specialty_correct_document_category_and_shift_date(accounts,tmp_path):
    auth,tokens=accounts
    j=WorkShiftJournal(tmp_path/"shifts.db",auth)
    manager=tokens["dispatch"]
    eq=j.save_reference(manager,"equipment","Grader",equipment_category="grader",licence_kind="tractor",licence_mark="C")
    doc=dict(marks=["C"],since="2026-10-01",until="2026-10-07")
    j.set_categories(manager,"default","driver",["grader"],licences={"road":doc})
    with pytest.raises(ValueError,match="удостоверения"):
        j.save(tokens["driver"],{**data(),"equipment":eq})
    j.set_categories(manager,"default","driver",["truck"],licences={"tractor":doc})
    with pytest.raises(ValueError,match="допуска"):
        j.save(tokens["driver"],{**data(),"equipment":eq})
    j.set_categories(manager,"default","driver",["grader"],licences={"tractor":doc})
    r=j.save(tokens["driver"],{**data(),"equipment":eq})
    for day in ("2026-09-30","2026-10-08"):
        with pytest.raises(ValueError,match="удостоверения"):
            j.save(manager,{**data(),"equipment":eq,"employee":"driver","work_date":day},shift_id=r["id"],revision=1)
    assert "tractor:C" in j.qualifications(tokens["driver"],"default",work_date="2026-10-07")["driver"]
    assert "tractor:C" not in j.qualifications(tokens["driver"],"default",work_date="2026-10-08")["driver"]
    with pytest.raises(PermissionError):
        j.set_categories(tokens["driver"],"default","driver",["grader"],licences={"tractor":doc})


def test_rate_snapshots_units_substitution_and_overlap(accounts,tmp_path):
    auth,tokens=accounts
    j=WorkShiftJournal(tmp_path/"shifts.db",auth)
    t=tokens["dispatch"]
    loader=j.save_reference(t,"equipment","Loader",registration_number="X 10",fleet_number="7")
    truck=j.save_reference(t,"equipment","Truck",registration_number="X10",fleet_number="8")
    j.save_reference(t,"workplace","Site")
    rate_args=dict(organization_id="default",since="2026-01-01",work="Repair",unit="hour",price="100.25")
    auth.set_payroll_owner("cloud")
    pay = tokens["cloud"]
    generic=j.add_rate(pay,**rate_args)
    exact=j.add_rate(pay,**rate_args,equipment=loader,workplace="Site")
    trip=j.add_rate(pay,**{**rate_args,"work":"Transport","unit":"trip","price":"500"})
    km=j.add_rate(pay,**{**rate_args,"work":"Distance","unit":"km","price":"2.30"})
    ton=j.add_rate(pay,**{**rate_args,"work":"Load","unit":"ton","price":"10"})
    values={**data(),"equipment":loader,"employee":"driver","assigned_employee":"both","assigned_equipment":truck,
            "time_from":"22:00","time_to":"06:00","work_lines":[{"rate_id":exact,"quantity":"1.5","amount":1},
            {"rate_id":trip,"quantity":2},{"rate_id":km,"quantity":10},{"rate_id":ton,"quantity":"1.225"}]}
    with pytest.raises(ValueError):
        j.save(t,{**values,"work_lines":[dict(rate_id=trip,quantity=1.25)]})
    with pytest.raises(ValueError):
        j.save(t,{**values,"work_lines":[dict(rate_id=generic,quantity=1)]})
    with pytest.raises(PermissionError):
        j.save(tokens["driver"],values)
    r=j.save(t,values)
    assert r["registration_number"]=="X 10" and r["assigned_registration_number"]=="X10"
    assert all(set(v)=={"rate_id","work","unit","quantity"} for v in json.loads(r["work_lines"]))
    j.add_rate(pay,**{**rate_args,"since":"2026-10-01","price":"300"},equipment=loader,workplace="Site")
    edited=j.save(t,{**values,"comment":"check"},shift_id=r["id"],revision=1)
    assert edited["work_lines"]==r["work_lines"]
    next_day={**data(),"work_date":"2026-10-08","employee":"both","equipment":truck,"time_from":"05:00","time_to":"07:00"}
    with pytest.raises(PermissionError):
        j.add_rate(tokens["driver"],**rate_args)
    warning=j.conflict_preview(t,organization_id="default",**{k:next_day[k] for k in ("work_date","shift_number","employee","equipment","time_from","time_to")})
    assert set(warning.split(", "))=={"госномер","сотрудник","техника"}
    j.save(t,next_day)
    assert all(v["conflict"] for v in j.list(t,date_from="2026-10-07",date_to="2026-10-08")["rows"])
    assert not j.list(tokens["driver"],date_from="2026-10-07",date_to="2026-10-08")["rows"][0]["conflict"]
    assert not j.conflict_preview(t,organization_id="default",work_date="2026-10-08",shift_number=1,
                                  employee="driver",time_from="07:00",time_to="08:00")
    j.transition(t,r["id"],2,"approved")
    people=j.month_sheet(t,"2026-10")
    assert "kopecks" not in people["driver"] and "kopecks" not in people["both"]
    from rag_catalog.core.shift_payroll import MonthlyPayroll
    assert MonthlyPayroll(j).sheet(pay,"default","2026-10")["people"]["driver"]["piece"]==118563
