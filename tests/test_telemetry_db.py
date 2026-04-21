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


def test_search_feedback_scores_are_aggregated(tmp_path) -> None:
    db = TelemetryDB(str(tmp_path / "telemetry.db"))
    db.log_search_feedback(
        username="ivan",
        source="telegram",
        query="паспорт",
        result_path=r"O:\a.pdf",
        result_title="a.pdf",
        feedback=3,
    )
    db.log_search_feedback(
        username="ivan",
        source="telegram",
        query="паспорт",
        result_path=r"O:\a.pdf",
        result_title="a.pdf",
        feedback=-1,
    )
    db.log_search_feedback(
        username="ivan",
        source="telegram",
        query="договор",
        result_path=r"O:\a.pdf",
        result_title="a.pdf",
        feedback=-3,
    )

    assert db.get_search_feedback_scores(query="паспорт", paths=[r"O:\a.pdf"]) == {r"O:\a.pdf": 2}


def test_default_search_aliases_expand_query(tmp_path) -> None:
    db = TelemetryDB(str(tmp_path / "telemetry.db"))

    expanded = db.expand_search_query("реквизиты спецмаш")

    assert "карточка предприятия" in expanded["expanded_query"].lower()
    assert any(group["key"] == "company_card" for group in expanded["groups"])


def test_search_alias_group_can_be_saved_and_deleted(tmp_path) -> None:
    db = TelemetryDB(str(tmp_path / "telemetry.db"))

    saved = db.save_search_alias_group(
        key="custom_docs",
        label="Внутренний документ",
        aliases=["служебная записка", "заявление"],
        negative_aliases=["личное заявление"],
    )

    assert saved["key"] == "custom_docs"
    expanded = db.expand_search_query("служебная записка")
    assert any(group["key"] == "custom_docs" for group in expanded["groups"])

    assert db.delete_search_alias_group(key="custom_docs") is True
    expanded = db.expand_search_query("служебная записка")
    assert not any(group["key"] == "custom_docs" for group in expanded["groups"])


def test_search_alias_candidates_use_positive_feedback(tmp_path) -> None:
    db = TelemetryDB(str(tmp_path / "telemetry.db"))
    db.log_search_feedback(
        username="ivan",
        source="nicegui",
        query="возврат аванса",
        result_path=r"O:\Письма\Запрос на возврат аванса Альфа.docx",
        result_title="Запрос на возврат аванса Альфа.docx",
        feedback=3,
    )

    candidates = db.suggest_search_alias_candidates(limit=10)

    assert any("альфа" in item["candidate"] for item in candidates)


def test_index_settings_are_persisted_and_normalized(tmp_path) -> None:
    db_path = tmp_path / "telemetry.db"
    db = TelemetryDB(str(db_path))

    saved = db.save_index_settings(
        {
            "schedule_enabled": True,
            "cadence": "weekly",
            "time": "02:30",
            "stage": "small",
            "workers": 99,
            "max_chunks": 1500,
            "recreate": True,
            "ocr_enabled": True,
            "ocr_min_text_len": 0,
        }
    )

    assert saved["schedule_enabled"] is True
    assert saved["cadence"] == "weekly"
    assert saved["stage"] == "small"
    assert saved["workers"] == 32
    assert saved["ocr_min_text_len"] == 1

    reopened = TelemetryDB(str(db_path))
    loaded = reopened.get_index_settings()
    assert loaded["schedule_enabled"] is True
    assert loaded["time"] == "02:30"
    assert loaded["recreate"] is True
