"""
telemetry_db.py — SQLite-телеметрия для индексации и поисковых запросов.
"""

import sqlite3
import threading
import uuid
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TelemetryDB:
    """Простой потокобезопасный слой записи/чтения телеметрии."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS search_logs (
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
                        error TEXT,
                        username TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS fact_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts TEXT NOT NULL,
                        source TEXT NOT NULL,
                        question TEXT NOT NULL,
                        ok INTEGER NOT NULL DEFAULT 0,
                        answer TEXT,
                        source_type TEXT,
                        value_kg INTEGER,
                        duration_ms INTEGER NOT NULL DEFAULT 0,
                        error TEXT
                    );

                    CREATE TABLE IF NOT EXISTS index_runs (
                        run_id TEXT PRIMARY KEY,
                        ts_started TEXT NOT NULL,
                        ts_finished TEXT,
                        status TEXT NOT NULL,
                        catalog_path TEXT,
                        collection_name TEXT,
                        recreate INTEGER NOT NULL DEFAULT 0,
                        total_files INTEGER NOT NULL DEFAULT 0,
                        added_files INTEGER NOT NULL DEFAULT 0,
                        updated_files INTEGER NOT NULL DEFAULT 0,
                        skipped_files INTEGER NOT NULL DEFAULT 0,
                        deleted_files INTEGER NOT NULL DEFAULT 0,
                        error_files INTEGER NOT NULL DEFAULT 0,
                        points_added INTEGER NOT NULL DEFAULT 0,
                        note TEXT
                    );

                    CREATE TABLE IF NOT EXISTS index_stage_progress (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        ts_started TEXT NOT NULL,
                        ts_updated TEXT NOT NULL,
                        ts_finished TEXT,
                        status TEXT NOT NULL,
                        total_files INTEGER NOT NULL DEFAULT 0,
                        processed_files INTEGER NOT NULL DEFAULT 0,
                        added_files INTEGER NOT NULL DEFAULT 0,
                        updated_files INTEGER NOT NULL DEFAULT 0,
                        skipped_files INTEGER NOT NULL DEFAULT 0,
                        error_files INTEGER NOT NULL DEFAULT 0,
                        points_added INTEGER NOT NULL DEFAULT 0,
                        UNIQUE(run_id, stage),
                        FOREIGN KEY(run_id) REFERENCES index_runs(run_id)
                    );

                    CREATE TABLE IF NOT EXISTS app_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts TEXT NOT NULL,
                        username TEXT NOT NULL DEFAULT '',
                        screen TEXT NOT NULL DEFAULT '',
                        feature TEXT NOT NULL DEFAULT '',
                        action TEXT NOT NULL DEFAULT '',
                        ok INTEGER NOT NULL DEFAULT 1,
                        details_json TEXT NOT NULL DEFAULT '{}'
                    );

                    CREATE INDEX IF NOT EXISTS idx_search_logs_ts
                      ON search_logs(ts);
                    CREATE INDEX IF NOT EXISTS idx_search_logs_username
                      ON search_logs(username, ts);
                    CREATE INDEX IF NOT EXISTS idx_fact_logs_ts
                      ON fact_logs(ts);
                    CREATE INDEX IF NOT EXISTS idx_index_runs_started
                      ON index_runs(ts_started);
                    CREATE INDEX IF NOT EXISTS idx_stage_run
                      ON index_stage_progress(run_id, stage);
                    CREATE INDEX IF NOT EXISTS idx_app_events_ts
                      ON app_events(ts);
                    CREATE INDEX IF NOT EXISTS idx_app_events_feature
                      ON app_events(feature, action, ts);
                    """
                )
                self._migrate_schema(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        search_cols = {row["name"] for row in conn.execute("PRAGMA table_info(search_logs)").fetchall()}
        if "username" not in search_cols:
            conn.execute("ALTER TABLE search_logs ADD COLUMN username TEXT NOT NULL DEFAULT ''")

    def log_search(
        self,
        *,
        source: str,
        query: str,
        limit_value: int,
        file_type: Optional[str],
        content_only: bool,
        results_count: int,
        duration_ms: int,
        ok: bool,
        error: str = "",
        username: str = "",
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO search_logs (
                        ts, source, query, limit_value, file_type, content_only,
                        results_count, duration_ms, ok, error, username
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now(),
                        source or "unknown",
                        query or "",
                        int(limit_value),
                        file_type or "",
                        1 if content_only else 0,
                        int(results_count),
                        int(duration_ms),
                        1 if ok else 0,
                        error or "",
                        (username or "").strip().lower(),
                    ),
                )

    def log_app_event(
        self,
        *,
        username: str,
        screen: str,
        feature: str,
        action: str,
        ok: bool = True,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO app_events (ts, username, screen, feature, action, ok, details_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now(),
                        (username or "").strip().lower(),
                        screen or "",
                        feature or "",
                        action or "",
                        1 if ok else 0,
                        json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
                    ),
                )

    def log_fact(
        self,
        *,
        source: str,
        question: str,
        ok: bool,
        answer: str,
        source_type: str,
        value_kg: Optional[int],
        duration_ms: int,
        error: str = "",
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO fact_logs (
                        ts, source, question, ok, answer, source_type, value_kg, duration_ms, error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now(),
                        source or "unknown",
                        question or "",
                        1 if ok else 0,
                        answer or "",
                        source_type or "",
                        int(value_kg) if value_kg is not None else None,
                        int(duration_ms),
                        error or "",
                    ),
                )

    def start_index_run(
        self,
        *,
        catalog_path: str,
        collection_name: str,
        recreate: bool,
        note: str = "",
    ) -> str:
        run_id = str(uuid.uuid4())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO index_runs (
                        run_id, ts_started, status, catalog_path, collection_name, recreate, note
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        _utc_now(),
                        "running",
                        catalog_path,
                        collection_name,
                        1 if recreate else 0,
                        note or "",
                    ),
                )
        return run_id

    def start_stage(self, *, run_id: str, stage: str, total_files: int) -> None:
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO index_stage_progress (
                        run_id, stage, ts_started, ts_updated, status, total_files
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, stage) DO UPDATE SET
                        ts_updated=excluded.ts_updated,
                        status=excluded.status,
                        total_files=excluded.total_files
                    """,
                    (run_id, stage, now, now, "running", int(total_files)),
                )

    def update_stage(
        self,
        *,
        run_id: str,
        stage: str,
        processed_files: int,
        added_files: int,
        updated_files: int,
        skipped_files: int,
        error_files: int,
        points_added: int,
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE index_stage_progress
                    SET
                        ts_updated=?,
                        processed_files=?,
                        added_files=?,
                        updated_files=?,
                        skipped_files=?,
                        error_files=?,
                        points_added=?
                    WHERE run_id=? AND stage=?
                    """,
                    (
                        _utc_now(),
                        int(processed_files),
                        int(added_files),
                        int(updated_files),
                        int(skipped_files),
                        int(error_files),
                        int(points_added),
                        run_id,
                        stage,
                    ),
                )

    def finish_stage(
        self,
        *,
        run_id: str,
        stage: str,
        status: str,
        processed_files: int,
        added_files: int,
        updated_files: int,
        skipped_files: int,
        error_files: int,
        points_added: int,
    ) -> None:
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE index_stage_progress
                    SET
                        ts_updated=?,
                        ts_finished=?,
                        status=?,
                        processed_files=?,
                        added_files=?,
                        updated_files=?,
                        skipped_files=?,
                        error_files=?,
                        points_added=?
                    WHERE run_id=? AND stage=?
                    """,
                    (
                        now,
                        now,
                        status,
                        int(processed_files),
                        int(added_files),
                        int(updated_files),
                        int(skipped_files),
                        int(error_files),
                        int(points_added),
                        run_id,
                        stage,
                    ),
                )

    def finish_index_run(
        self,
        *,
        run_id: str,
        status: str,
        total_files: int,
        added_files: int,
        updated_files: int,
        skipped_files: int,
        deleted_files: int,
        error_files: int,
        points_added: int,
        note: str = "",
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE index_runs
                    SET
                        ts_finished=?,
                        status=?,
                        total_files=?,
                        added_files=?,
                        updated_files=?,
                        skipped_files=?,
                        deleted_files=?,
                        error_files=?,
                        points_added=?,
                        note=CASE WHEN ? = '' THEN note ELSE ? END
                    WHERE run_id=?
                    """,
                    (
                        _utc_now(),
                        status,
                        int(total_files),
                        int(added_files),
                        int(updated_files),
                        int(skipped_files),
                        int(deleted_files),
                        int(error_files),
                        int(points_added),
                        note or "",
                        note or "",
                        run_id,
                    ),
                )

    def fetch_dicts(self, query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(query, params or [])
                return [dict(r) for r in cur.fetchall()]
