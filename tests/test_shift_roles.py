import sqlite3

import pytest

from rag_catalog.core.roles import can_manage_shifts, can_open_screen, can_use_catalog, user_roles
from rag_catalog.core.user_auth_db import UserAuthDB
from rag_catalog.core.work_shifts import WorkShiftJournal


@pytest.fixture
def accounts(tmp_path, monkeypatch):
    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    auth = UserAuthDB(str(tmp_path / "users.db"))
    roles = {"driver": ["driver"], "dispatch": ["dispatcher"], "both": ["driver", "user"],
             "admin": ["admin"], "cloud": ["user"]}
    for name, values in roles.items():
        auth.admin_create_user(username=name, password="test-only-password", roles=values, must_change_password=False)
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


def test_driver_cannot_use_catalog_helpers_or_telegram(accounts):
    from rag_catalog.integrations.telegram_bot import get_authorized_telegram_user
    from rag_catalog.ui.helpers import _cd_acl_allows, _cd_acl_allows_local_file

    auth, _ = accounts
    driver = auth.get_user(username="driver")
    assert not _cd_acl_allows({}, driver, "anything")
    assert not _cd_acl_allows_local_file({}, driver, "C:/example.pdf")
    class TelegramAuth:
        def get_user_by_telegram_chat_id(self, chat):
            return driver
    assert get_authorized_telegram_user(TelegramAuth(), "123") is None


def test_v3_database_migrates_without_losing_existing_users(accounts):
    auth, tokens = accounts
    with sqlite3.connect(auth.db_path) as conn:
        conn.execute("ALTER TABLE users DROP COLUMN roles_json")
        conn.execute("UPDATE schema_meta SET schema_version=3 WHERE db_kind='user_auth'")
    reopened = UserAuthDB(str(auth.db_path))
    assert reopened.get_user_by_session(tokens["admin"])["roles"] == ["admin"]
    assert reopened.get_user_by_session(tokens["cloud"])["roles"] == ["user"]
    with sqlite3.connect(auth.db_path) as conn:
        assert conn.execute("SELECT schema_version FROM schema_meta WHERE db_kind='user_auth'").fetchone()[0] == 4


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
