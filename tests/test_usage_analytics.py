import json
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rag_catalog.core import usage_analytics as usage
from rag_catalog.core.user_auth_db import UserAuthDB
from rag_catalog.ui import usage_api


def event(**values):
    return usage.clean_event(dict(kind="click", action="files.open_file", screen="explorer", **values), browser=True)


def test_private_fields_and_identity_never_enter_storage(tmp_path):
    db = usage.UsageAnalyticsDB(str(tmp_path / "usage.db"))
    clean = event(username="admin", query="secret query", path="/private/document", password="secret",
                  details={"token": "secret"}, ts=1, session_id="session-123")
    db.append("veronika", [clean, clean])
    rows = db.report()["recent"]
    assert len(rows) == 1 and rows[0]["username"] == "veronika"
    assert rows[0]["ts"] > time.time() - 10
    assert "secret" not in json.dumps(clean)
    with db.connect() as conn:
        assert "secret" not in str([tuple(r) for r in conn.execute("SELECT * FROM usage_events")])
    db.append("other", [clean])
    assert db.report()["overview"]["events"] == 2


@pytest.mark.parametrize("patch", [
    {"action": "arbitrary.secret"}, {"screen": "/share/token"}, {"kind": "operation"},
    {"duration_ms": float("nan")}, {"duration_ms": float("inf")}, {"duration_ms": -1},
    {"duration_ms": "100"}, {"duration_ms": True}, {"event_id": "?token"}, {"session_id": []},
    {"action": []}, {"kind": "timing", "action": "page.ready"},
])
def test_browser_schema_rejects_unbounded_or_invalid_values(patch):
    with pytest.raises(ValueError):
        usage.clean_event(dict(dict(kind="click", action="files.open_file", screen="explorer"), **patch), browser=True)


def test_retention_and_row_limit_reclaim_old_history(tmp_path, monkeypatch):
    db = usage.UsageAnalyticsDB(str(tmp_path / "usage.db"))
    db.append("old", [event()])
    now = time.time()
    monkeypatch.setattr(usage.time, "time", lambda: now + 91 * 86400)
    assert db.report(days=90)["overview"]["events"] == 0
    monkeypatch.setattr(usage, "MAX_ROWS", 5)
    db.append("new", [event() for _ in range(10)])
    assert db.report()["overview"]["events"] == 5


def test_report_percentiles_filters_and_counts_do_not_mix_clicks_with_operations(tmp_path):
    db = usage.UsageAnalyticsDB(str(tmp_path / "usage.db"))
    db.append("veronika", [event() for _ in range(3)])
    db.append("veronika", [usage.clean_event(dict(kind="timing", action="page.ready", screen="explorer",
                                                 duration_ms=n * 10), browser=True) for n in range(1, 21)])
    db.append("other", [usage.clean_event(dict(kind="operation", action="shifts.save", screen="shifts", ok=False))])
    report = db.report(username="veronika")
    assert report["overview"]["clicks"] == 3
    assert report["overview"]["users"] == 1
    assert report["overview"]["errors"] == 0
    timing = report["timings"][0]
    assert timing["count"] == 20 and timing["avg_ms"] == 105
    assert timing["p50_ms"] == 105
    assert timing["p95_ms"] == 190 and timing["max_ms"] == 200
    assert db.report(screen="shifts")["overview"]["errors"] == 1
    assert not db.report(username="' OR 1=1 --")["recent"]


def test_browser_timestamp_uses_bounded_age_not_client_wall_clock():
    before = time.time()
    clean = event(age_ms=4000, ts=1)
    assert before - 4.1 <= clean["ts"] <= time.time() - 4
    with pytest.raises(ValueError):
        event(age_ms=300001)


def test_background_folder_sizes_are_not_counted_as_usage(monkeypatch):
    captured = []
    class Sink:
        def submit(self, username, events):
            captured.extend(events)
    monkeypatch.setattr(usage, "collector", lambda path: Sink())
    usage.record_app_event({}, "u", "explorer", "cd_explorer", "folder_sizes_loaded", True, {"duration_ms": 3})
    assert captured[0]["kind"] == "timing"


def test_server_event_selection_records_completions_without_document_details(monkeypatch):
    captured = []
    class Sink:
        def submit(self, username, events):
            captured.extend(events)
    monkeypatch.setattr(usage, "collector", lambda path: Sink())
    usage.record_app_event({}, "u", "search", "search", "run_render_final", True,
                           {"duration_ms": 700, "query": "secret", "has_error": True})
    usage.record_app_event({}, "u", "search", "search", "schedule", True, {"query": "secret"})
    usage.record_app_event({"usage_analytics_enabled": False}, "u", "search", "search", "run_render_final", True, {})
    assert len(captured) == 1
    assert captured[0]["action"] == "search.complete"
    assert captured[0]["ok"] is False and captured[0]["duration_ms"] == 700
    assert "secret" not in json.dumps(captured)


def test_background_collector_flushes_without_ui_sqlite_work(tmp_path):
    sink = usage.UsageCollector(str(tmp_path / "usage.db"))
    assert sink.submit("user", [event()]) == 1
    sink.close()
    assert not sink.thread.is_alive()
    assert usage.UsageAnalyticsDB(str(tmp_path / "usage.db")).report()["overview"]["clicks"] == 1


@pytest.fixture
def api(tmp_path, monkeypatch):
    cfg = {"users_db_path": str(tmp_path / "users.db"), "usage_analytics_db_path": str(tmp_path / "usage.db")}
    db = UserAuthDB(cfg["users_db_path"])
    headers = {}
    for username, role in (("admin", "admin"), ("driver", "driver"), ("veronika", "user")):
        db.admin_create_user(username=username, password="Testing-12345678", role=role, status="active")
        headers[username] = {"Authorization": "Bearer " + db.create_session(username=username)}
    monkeypatch.setattr(usage_api, "load_config", lambda: cfg)
    usage_api._buckets.clear()
    app = FastAPI()
    app.post("/events")(usage_api.usage_events)
    app.get("/report")(usage_api.usage_report)
    with TestClient(app) as client:
        yield client, headers, cfg, db
    usage.collector(usage.analytics_path(cfg)).close()


def test_api_requires_auth_and_reports_are_admin_only(api):
    client, headers, cfg, _ = api
    assert client.get("/report").status_code == 401
    assert client.post("/events", json={"events": [event()]}).status_code == 401
    for user in ("veronika", "driver"):
        assert client.get("/report", headers=headers[user]).status_code == 403
        assert client.post("/events", json={"username": "admin", "events": [event()]}, headers=headers[user]).status_code == 200
    usage.collector(usage.analytics_path(cfg)).pending.join()
    result = client.get("/report", headers=headers["admin"])
    assert result.status_code == 200
    assert {r["username"] for r in result.json()["users"]} == {"driver", "veronika"}
    assert client.get("/report?days=900", headers=headers["admin"]).status_code == 400


def test_api_limits_origin_payload_and_revoked_sessions(api):
    client, headers, cfg, db = api
    assert client.post("/events", headers={**headers["veronika"], "Origin": "https://evil.example"},
                       json={"events": [event()]}).status_code == 403
    assert client.post("/events", headers=headers["veronika"], content=b"x" * (usage.MAX_BODY + 1)).status_code == 413
    for payload in ([], {}, {"events": []}, {"events": [event()] * 51}, {"events": [dict(event(), action="evil")]}):
        assert client.post("/events", headers=headers["veronika"], json=payload).status_code == 400
    cfg["usage_analytics_enabled"] = False
    assert client.post("/events", headers=headers["veronika"], json={"events": [event()]}).json()["accepted"] == 0
    db.revoke_session(headers["veronika"]["Authorization"][7:])
    assert client.post("/events", headers=headers["veronika"], json={"events": [event()]}).status_code == 401


def test_rate_limit_is_per_user_with_bounded_buckets(monkeypatch):
    usage_api._buckets.clear()
    usage_api._rate_limit("one", 240)
    with pytest.raises(Exception) as exc:
        usage_api._rate_limit("one", 1)
    assert exc.value.status_code == 429
    usage_api._rate_limit("two", 1)
    now = time.monotonic()
    monkeypatch.setattr(usage_api.time, "monotonic", lambda: now + 61)
    usage_api._rate_limit("one", 1)
    for n in range(600):
        usage_api._rate_limit(str(n), 1)
    assert len(usage_api._buckets) == 512
