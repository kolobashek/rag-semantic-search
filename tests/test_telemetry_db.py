from __future__ import annotations

import sqlite3

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


def test_existing_search_logs_without_username_are_migrated_before_indexes(tmp_path) -> None:
    db_path = tmp_path / "telemetry.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE search_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                source TEXT NOT NULL,
                query TEXT NOT NULL,
                limit_value INTEGER,
                file_type TEXT,
                content_only INTEGER NOT NULL DEFAULT 0,
                results_count INTEGER NOT NULL DEFAULT 0,
                duration_ms INTEGER NOT NULL DEFAULT 0,
                ok INTEGER NOT NULL DEFAULT 1,
                error TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO search_logs (
                ts, source, query, limit_value, file_type, content_only,
                results_count, duration_ms, ok, error
            )
            VALUES ('2026-01-01T00:00:00+00:00', 'nicegui', 'old', 10, '', 0, 0, 1, 1, '')
            """
        )

    db = TelemetryDB(str(db_path))
    db.log_search(
        source="nicegui",
        query="new",
        limit_value=10,
        file_type=None,
        content_only=False,
        results_count=1,
        duration_ms=2,
        ok=True,
        username="admin",
    )

    rows = db.fetch_dicts("SELECT query, username FROM search_logs ORDER BY id")
    assert rows == [{"query": "old", "username": ""}, {"query": "new", "username": "admin"}]


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
