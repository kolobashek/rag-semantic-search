from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from types import SimpleNamespace

import pytest

from rag_catalog.core.work_shifts import WorkShiftJournal


@pytest.fixture
def journal(tmp_path):
    users = {name: dict(username=name, display_name=name.title(), role=role, status="active")
             for name, role in (("admin", "admin"), ("alice", "user"), ("bob", "user"))}
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
    journal.transition("alice", row["id"], 1, "submitted")
    journal.auth.get_user_by_session("admin")["role"] = "user"
    with pytest.raises(PermissionError):
        journal.transition("admin", row["id"], 2, "approved")


def test_approval_locks_and_reopen_requires_admin_reason(journal, payload):
    row = journal.save("alice", payload)
    journal.transition("alice", row["id"], 1, "submitted")
    with pytest.raises(PermissionError):
        journal.transition("alice", row["id"], 2, "approved")
    journal.transition("admin", row["id"], 2, "approved")
    with pytest.raises(ValueError):
        journal.save("admin", dict(payload, employee="alice"), shift_id=row["id"], revision=3)
    with pytest.raises(ValueError):
        journal.delete("admin", row["id"], 3)
    with pytest.raises(PermissionError):
        journal.transition("alice", row["id"], 3, "draft", "Correction")
    with pytest.raises(ValueError):
        journal.transition("admin", row["id"], 3, "draft")
    journal.transition("admin", row["id"], 3, "draft", "Correction")
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
    journal.delete("alice", row["id"], 1)
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
    row = journal.save("alice", dict(payload, hours=0))
    with pytest.raises(ValueError):
        journal.transition("alice", row["id"], 1, "submitted")


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
    auth.admin_create_user(username="alice", password="only-for-test", must_change_password=False)
    token = auth.create_session(username="alice")
    journal = WorkShiftJournal(tmp_path / "shifts.db", auth)
    journal.save(token, payload)
    assert listing(journal, token)["count"] == 1
    auth.revoke_session(token)
    with pytest.raises(PermissionError):
        listing(journal, token)
