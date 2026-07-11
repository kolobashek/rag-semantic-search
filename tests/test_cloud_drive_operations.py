from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from rag_catalog.core.cloud_drive.operations import _index_state_snapshot, _queue_health


def test_queue_health_reports_oldest_pending_lag(tmp_path) -> None:
    registry_path = tmp_path / "cloud_drive.db"
    created_at = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    with sqlite3.connect(registry_path) as conn:
        conn.execute("CREATE TABLE cloud_jobs (status TEXT NOT NULL, created_at TEXT NOT NULL)")
        conn.execute("INSERT INTO cloud_jobs(status, created_at) VALUES ('pending', ?)", (created_at,))

    health = _queue_health(registry_path, {"cloud_drive_queue_lag_warn_sec": 60})

    assert health["ok"] is False
    assert health["status"] == "lagging"
    assert health["pending"] == 1
    assert health["oldest_pending_age_sec"] >= 590


def test_index_state_snapshot_is_constant_query_health(tmp_path) -> None:
    state_path = tmp_path / "index_state.db"
    with sqlite3.connect(state_path) as conn:
        conn.execute("CREATE TABLE state_entries (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE failed_paths (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE index_queue (status TEXT NOT NULL)")
        conn.executemany("INSERT INTO state_entries DEFAULT VALUES", [(), (), ()])
        conn.execute("INSERT INTO failed_paths DEFAULT VALUES")
        conn.executemany("INSERT INTO index_queue(status) VALUES (?)", [("pending",), ("running",)])

    health = _index_state_snapshot(state_path)

    assert health["ok"] is True
    assert health["entries"] == 3
    assert health["failed_paths"] == 1
    assert health["queue"] == {"pending": 1, "running": 1}


def test_index_state_snapshot_reads_active_wal_writer(tmp_path) -> None:
    state_path = tmp_path / "index_state.db"
    with sqlite3.connect(state_path) as setup:
        setup.execute("PRAGMA journal_mode=WAL")
        setup.execute("CREATE TABLE state_entries (id INTEGER PRIMARY KEY)")
        setup.execute("CREATE TABLE failed_paths (id INTEGER PRIMARY KEY)")
        setup.execute("CREATE TABLE index_queue (status TEXT NOT NULL)")
        setup.execute("INSERT INTO state_entries DEFAULT VALUES")

    with sqlite3.connect(state_path) as writer:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO state_entries DEFAULT VALUES")

        health = _index_state_snapshot(state_path)

    assert health["ok"] is True
    assert health["entries"] == 1
