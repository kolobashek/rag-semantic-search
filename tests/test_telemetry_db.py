from __future__ import annotations

from rag_catalog.core.telemetry_db import TelemetryDB


def test_search_logs_include_username_and_migrate(tmp_path) -> None:
    db = TelemetryDB(str(tmp_path / "telemetry.db"))

    db.log_search(
        source="nicegui",
        query="passport",
        limit_value=10,
        file_type=None,
        content_only=False,
        results_count=3,
        duration_ms=42,
        ok=True,
        username="admin",
    )

    rows = db.fetch_dicts("SELECT username, query, results_count FROM search_logs")
    assert rows == [{"username": "admin", "query": "passport", "results_count": 3}]


def test_app_events_are_logged(tmp_path) -> None:
    db = TelemetryDB(str(tmp_path / "telemetry.db"))

    db.log_app_event(
        username="admin",
        screen="explorer",
        feature="favorites",
        action="add",
        details={"path": "O:\\Docs"},
    )

    rows = db.fetch_dicts("SELECT username, screen, feature, action, details_json FROM app_events")
    assert rows[0]["username"] == "admin"
    assert rows[0]["screen"] == "explorer"
    assert rows[0]["feature"] == "favorites"
    assert rows[0]["action"] == "add"
    assert "O:\\\\Docs" in rows[0]["details_json"]
