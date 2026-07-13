"""
index_state_db.py — SQLite-хранилище состояния индексатора.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .db_contract import ensure_schema_version

SCHEMA_VERSION = 9


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class IndexStateDB:
    """Потокобезопасное хранилище state_entries для индексатора."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _prepare_connection(self, conn: sqlite3.Connection) -> None:
        last_error: Optional[Exception] = None
        for _ in range(3):
            try:
                conn.execute("PRAGMA busy_timeout=30000;")
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                return
            except sqlite3.OperationalError as exc:
                last_error = exc
                time.sleep(0.25)
        if last_error is not None:
            raise last_error

    def _init_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                self._prepare_connection(conn)
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS state_entries (
                        full_path TEXT PRIMARY KEY,
                        fingerprint TEXT NOT NULL DEFAULT '',
                        mtime REAL NOT NULL DEFAULT 0,
                        stage TEXT NOT NULL DEFAULT 'metadata',
                        size_bytes INTEGER NOT NULL DEFAULT 0,
                        extension TEXT NOT NULL DEFAULT '',
                        updated_at TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_state_entries_stage
                      ON state_entries(stage);
                    CREATE INDEX IF NOT EXISTS idx_state_entries_updated
                      ON state_entries(updated_at);
                    CREATE INDEX IF NOT EXISTS idx_state_entries_extension
                      ON state_entries(extension);

                    CREATE TABLE IF NOT EXISTS index_config (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL DEFAULT '',
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS failed_paths (
                        full_path TEXT PRIMARY KEY,
                        fingerprint TEXT NOT NULL DEFAULT '',
                        retry_count INTEGER NOT NULL DEFAULT 0,
                        next_retry_at REAL NOT NULL DEFAULT 0,
                        last_error TEXT NOT NULL DEFAULT '',
                        updated_at TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_failed_paths_retry
                      ON failed_paths(next_retry_at);

                    CREATE TABLE IF NOT EXISTS index_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        full_path TEXT NOT NULL,
                        stage TEXT NOT NULL DEFAULT 'content',
                        reason TEXT NOT NULL DEFAULT 'changed',
                        priority INTEGER NOT NULL DEFAULT 100,
                        status TEXT NOT NULL DEFAULT 'pending',
                        attempts INTEGER NOT NULL DEFAULT 0,
                        available_at REAL NOT NULL DEFAULT 0,
                        locked_at REAL NOT NULL DEFAULT 0,
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(full_path, stage)
                    );

                    CREATE INDEX IF NOT EXISTS idx_index_queue_status_priority
                      ON index_queue(status, available_at, priority, updated_at);
                    """
                )
                existing_cols = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(state_entries)").fetchall()
                }
                optional_columns = {
                    "cloud_file_id": "TEXT NOT NULL DEFAULT ''",
                    "cloud_version_id": "TEXT NOT NULL DEFAULT ''",
                    "cloud_path": "TEXT NOT NULL DEFAULT ''",
                    "storage_key": "TEXT NOT NULL DEFAULT ''",
                    "content_hash": "TEXT NOT NULL DEFAULT ''",
                    "indexed_stage": "TEXT NOT NULL DEFAULT ''",
                    "status": "TEXT NOT NULL DEFAULT 'ok'",
                    "last_error": "TEXT NOT NULL DEFAULT ''",
                    "last_attempt_at": "TEXT NOT NULL DEFAULT ''",
                    "next_retry_at": "REAL NOT NULL DEFAULT 0",
                    "indexed_chunks": "INTEGER NOT NULL DEFAULT 0",
                    "total_chunks": "INTEGER NOT NULL DEFAULT 0",
                }
                for name, ddl in optional_columns.items():
                    if name not in existing_cols:
                        conn.execute(f"ALTER TABLE state_entries ADD COLUMN {name} {ddl}")
                conn.execute(
                    """
                    UPDATE state_entries
                    SET status=CASE
                            WHEN stage='error' THEN 'error'
                            WHEN stage='empty' THEN 'empty'
                            WHEN status='' THEN 'ok'
                            ELSE status
                        END,
                        indexed_stage=CASE
                            WHEN indexed_stage != '' THEN indexed_stage
                            WHEN stage IN ('metadata', 'content', 'small', 'large') THEN stage
                            ELSE indexed_stage
                        END
                    WHERE status='' OR stage IN ('error', 'empty') OR indexed_stage=''
                    """
                )
                conn.executescript(
                    """
                    CREATE INDEX IF NOT EXISTS idx_state_entries_cloud_file
                      ON state_entries(cloud_file_id);
                    CREATE INDEX IF NOT EXISTS idx_state_entries_cloud_version
                      ON state_entries(cloud_version_id);
                    CREATE INDEX IF NOT EXISTS idx_state_entries_content_hash
                      ON state_entries(content_hash);
                    CREATE INDEX IF NOT EXISTS idx_state_entries_status
                      ON state_entries(status);
                    CREATE INDEX IF NOT EXISTS idx_state_entries_indexed_stage
                      ON state_entries(indexed_stage);
                    """
                )
                ensure_schema_version(
                    conn,
                    db_kind="index_state",
                    db_path=self.db_path,
                    expected_version=SCHEMA_VERSION,
                    code_root=Path(__file__).resolve().parents[3],
                )

    def count(self) -> int:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute("SELECT COUNT(*) AS c FROM state_entries").fetchone()
                return int(row["c"] if row else 0)

    def exists(self) -> bool:
        return self.db_path.exists()

    def clear(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM state_entries")
                conn.execute("DELETE FROM failed_paths")
                conn.execute("DELETE FROM index_queue")

    def get_config(self) -> Dict[str, str]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute("SELECT key, value FROM index_config").fetchall()
                return {str(row["key"]): str(row["value"]) for row in rows}

    def set_config_many(self, values: Dict[str, Any]) -> None:
        now = _utc_now()
        rows = [(str(key), str(value), now) for key, value in values.items()]
        if not rows:
            return
        with self._lock:
            with self._connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO index_config (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value=excluded.value,
                        updated_at=excluded.updated_at
                    """,
                    rows,
                )

    def validate_embedding_config(
        self,
        *,
        embedding_model: str,
        vector_size: int,
        collection_name: str,
        recreate: bool = False,
    ) -> None:
        desired = {
            "embedding_model": str(embedding_model or ""),
            "vector_size": str(int(vector_size or 0)),
            "collection_name": str(collection_name or ""),
        }
        current = self.get_config()
        keys = ("embedding_model", "vector_size", "collection_name")
        if recreate or not any(current.get(key) for key in keys):
            self.set_config_many(desired)
            return
        mismatches = {
            key: (current.get(key, ""), desired[key])
            for key in keys
            if current.get(key, "") != desired[key]
        }
        if mismatches:
            details = ", ".join(f"{key}: stored={old!r}, current={new!r}" for key, (old, new) in mismatches.items())
            raise RuntimeError(
                "Индекс создан с другой embedding-конфигурацией. "
                f"{details}. Запустите индексатор с --recreate или используйте отдельную collection."
            )
        self.set_config_many(desired)

    def bootstrap_from_json(self, json_path: Path) -> int:
        """
        Импортировать state из JSON в пустую таблицу.
        Возвращает количество импортированных записей.
        """
        if self.count() > 0:
            return 0
        if not json_path.exists():
            return 0
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"Не удалось прочитать legacy state JSON: {exc}") from exc
        files = payload.get("files", {})
        if not isinstance(files, dict):
            raise RuntimeError("Legacy state JSON имеет некорректный формат: ожидается объект files")
        rows: List[Dict[str, Any]] = []
        for full_path, meta in files.items():
            full_path_s = str(full_path or "").strip()
            if not full_path_s:
                continue
            m = meta if isinstance(meta, dict) else {}
            fingerprint = str(m.get("fingerprint") or "")
            stage = str(m.get("stage") or "content")
            try:
                mtime = float(m.get("mtime") or 0.0)
            except (TypeError, ValueError):
                mtime = 0.0
            size_bytes = 0
            if "_" in fingerprint:
                try:
                    size_bytes = int(float(fingerprint.split("_", 1)[0]))
                except (TypeError, ValueError):
                    size_bytes = 0
            ext = Path(full_path_s).suffix.lower()
            rows.append(
                {
                    "full_path": full_path_s,
                    "fingerprint": fingerprint,
                    "mtime": mtime,
                    "stage": stage,
                    "size_bytes": int(size_bytes),
                    "extension": ext,
                }
            )
        self.upsert_many(rows)
        return len(rows)

    def get_entry(self, full_path: str) -> Optional[Dict[str, Any]]:
        key = str(full_path or "").strip()
        if not key:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM state_entries WHERE full_path=?",
                    (key,),
                ).fetchone()
                return dict(row) if row else None

    def entries_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Load current entries once for a full catalog stage."""
        with self._lock:
            with self._connect() as conn:
                self._prepare_connection(conn)
                rows = conn.execute("SELECT * FROM state_entries").fetchall()
                return {str(row["full_path"]): dict(row) for row in rows}

    def get_failed_path(self, full_path: str) -> Optional[Dict[str, Any]]:
        key = str(full_path or "").strip()
        if not key:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute("SELECT * FROM failed_paths WHERE full_path=?", (key,)).fetchone()
                return dict(row) if row else None

    def record_failed_path(
        self,
        full_path: str,
        *,
        fingerprint: str = "",
        error: str = "",
        base_delay_seconds: int = 300,
        max_delay_seconds: int = 86_400,
    ) -> Dict[str, Any]:
        key = str(full_path or "").strip()
        if not key:
            return {}
        now_iso = _utc_now()
        now_ts = time.time()
        with self._lock:
            with self._connect() as conn:
                existing = conn.execute(
                    "SELECT retry_count FROM failed_paths WHERE full_path=?",
                    (key,),
                ).fetchone()
                retry_count = int(existing["retry_count"] if existing else 0) + 1
                delay = min(max_delay_seconds, max(1, int(base_delay_seconds)) * (2 ** min(retry_count - 1, 8)))
                next_retry_at = now_ts + delay
                conn.execute(
                    """
                    INSERT INTO failed_paths (
                        full_path, fingerprint, retry_count, next_retry_at, last_error, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(full_path) DO UPDATE SET
                        fingerprint=excluded.fingerprint,
                        retry_count=excluded.retry_count,
                        next_retry_at=excluded.next_retry_at,
                        last_error=excluded.last_error,
                        updated_at=excluded.updated_at
                    """,
                    (key, str(fingerprint or ""), retry_count, next_retry_at, str(error or "")[:1000], now_iso),
                )
        return {
            "full_path": key,
            "fingerprint": str(fingerprint or ""),
            "retry_count": retry_count,
            "next_retry_at": next_retry_at,
            "last_error": str(error or "")[:1000],
            "updated_at": now_iso,
        }

    def clear_failed_path(self, full_path: str) -> int:
        key = str(full_path or "").strip()
        if not key:
            return 0
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM failed_paths WHERE full_path=?", (key,))
                return int(cur.rowcount or 0)

    def is_failed_retry_due(self, full_path: str, *, now: Optional[float] = None) -> bool:
        row = self.get_failed_path(full_path)
        if not row:
            return True
        try:
            next_retry_at = float(row.get("next_retry_at") or 0.0)
        except (TypeError, ValueError):
            next_retry_at = 0.0
        return next_retry_at <= float(time.time() if now is None else now)

    def list_due_failed_paths(self, *, now: Optional[float] = None) -> List[Dict[str, Any]]:
        ts = float(time.time() if now is None else now)
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM failed_paths WHERE next_retry_at <= ? ORDER BY next_retry_at, updated_at",
                    (ts,),
                ).fetchall()
                return [dict(row) for row in rows]

    def enqueue_index_task(
        self,
        full_path: str,
        *,
        stage: str = "content",
        reason: str = "changed",
        priority: int = 100,
        available_at: Optional[float] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        key = str(full_path or "").strip()
        if not key:
            return {}
        now_iso = _utc_now()
        available = float(time.time() if available_at is None else available_at)
        payload_json = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO index_queue (
                        full_path, stage, reason, priority, status, attempts,
                        available_at, locked_at, payload_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'pending', 0, ?, 0, ?, ?, ?)
                    ON CONFLICT(full_path, stage) DO UPDATE SET
                        reason=excluded.reason,
                        priority=MIN(index_queue.priority, excluded.priority),
                        status='pending',
                        available_at=MIN(index_queue.available_at, excluded.available_at),
                        locked_at=0,
                        payload_json=excluded.payload_json,
                        updated_at=excluded.updated_at
                    """,
                    (key, str(stage or "content"), str(reason or "changed"), int(priority), available, payload_json, now_iso, now_iso),
                )
                row = conn.execute(
                    "SELECT * FROM index_queue WHERE full_path=? AND stage=?",
                    (key, str(stage or "content")),
                ).fetchone()
                return dict(row) if row else {}

    def lease_index_tasks(
        self,
        *,
        limit: int = 1,
        lease_seconds: int = 300,
        now: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        ts = float(time.time() if now is None else now)
        lock_until = ts + max(1, int(lease_seconds))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT * FROM index_queue
                    WHERE status='pending' AND available_at <= ?
                    ORDER BY priority ASC, updated_at ASC, id ASC
                    LIMIT ?
                    """,
                    (ts, max(1, int(limit))),
                ).fetchall()
                ids = [int(row["id"]) for row in rows]
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    now_iso = _utc_now()
                    conn.execute(
                        f"""
                        UPDATE index_queue
                        SET status='running',
                            attempts=attempts + 1,
                            locked_at=?,
                            updated_at=?
                        WHERE id IN ({placeholders}) AND status='pending'
                        """,
                        (lock_until, now_iso, *ids),
                    )
                    refreshed = conn.execute(
                        f"SELECT * FROM index_queue WHERE id IN ({placeholders}) ORDER BY priority ASC, updated_at ASC, id ASC",
                        ids,
                    ).fetchall()
                    return [dict(row) for row in refreshed]
                return []

    def requeue_expired_index_tasks(self, *, now: Optional[float] = None) -> int:
        ts = float(time.time() if now is None else now)
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    UPDATE index_queue
                    SET status='pending', locked_at=0, updated_at=?
                    WHERE status='running' AND locked_at <= ?
                    """,
                    (_utc_now(), ts),
                )
                return int(cur.rowcount or 0)

    def complete_index_task(self, task_id: int) -> int:
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM index_queue WHERE id=?", (int(task_id),))
                return int(cur.rowcount or 0)

    def fail_index_task(
        self,
        task_id: int,
        *,
        error: str = "",
        retry_delay_seconds: int = 300,
    ) -> int:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute("SELECT attempts FROM index_queue WHERE id=?", (int(task_id),)).fetchone()
                if not row:
                    return 0
                attempts = int(row["attempts"] or 0)
                delay = min(86_400, max(1, int(retry_delay_seconds)) * (2 ** min(max(0, attempts - 1), 8)))
                cur = conn.execute(
                    """
                    UPDATE index_queue
                    SET status='pending',
                        reason=?,
                        available_at=?,
                        locked_at=0,
                        updated_at=?
                    WHERE id=?
                    """,
                    (f"retry:{str(error or '')[:200]}", time.time() + delay, _utc_now(), int(task_id)),
                )
                return int(cur.rowcount or 0)

    def queue_stats(self) -> Dict[str, int]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT status, COUNT(*) AS cnt FROM index_queue GROUP BY status"
                ).fetchall()
                return {str(row["status"]): int(row["cnt"]) for row in rows}

    def iter_entries(self) -> List[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    SELECT full_path, fingerprint, mtime, stage, size_bytes, extension, updated_at,
                           cloud_file_id, cloud_version_id, cloud_path, storage_key, content_hash,
                           indexed_stage, status, last_error, last_attempt_at, next_retry_at,
                           indexed_chunks, total_chunks
                    FROM state_entries
                    ORDER BY full_path
                    """
                )
                return [dict(row) for row in cur.fetchall()]

    def find_by_content_hash(self, content_hash: str, *, exclude_path: str = "") -> Optional[Dict[str, Any]]:
        value = str(content_hash or "").strip()
        if not value:
            return None
        with self._lock:
            with self._connect() as conn:
                if exclude_path:
                    row = conn.execute(
                        """
                        SELECT * FROM state_entries
                        WHERE content_hash=? AND full_path != ?
                        ORDER BY updated_at ASC
                        LIMIT 1
                        """,
                        (value, str(exclude_path)),
                    ).fetchone()
                else:
                    row = conn.execute(
                        "SELECT * FROM state_entries WHERE content_hash=? ORDER BY updated_at ASC LIMIT 1",
                        (value,),
                    ).fetchone()
                return dict(row) if row else None

    def upsert_many(self, entries: Iterable[Dict[str, Any]]) -> None:
        now = _utc_now()
        rows = []
        for entry in entries:
            full_path = str(entry.get("full_path") or "").strip()
            if not full_path:
                continue
            fingerprint = str(entry.get("fingerprint") or "")
            try:
                mtime = float(entry.get("mtime") or 0.0)
            except (TypeError, ValueError):
                mtime = 0.0
            stage = str(entry.get("stage") or "metadata")
            indexed_stage = str(entry.get("indexed_stage") or (stage if stage in {"metadata", "content", "small", "large"} else ""))
            status = str(entry.get("status") or ("error" if stage == "error" else "empty" if stage == "empty" else "ok"))
            last_error = str(entry.get("last_error") or "")
            last_attempt_at = str(entry.get("last_attempt_at") or now)
            try:
                next_retry_at = float(entry.get("next_retry_at") or 0.0)
            except (TypeError, ValueError):
                next_retry_at = 0.0
            try:
                indexed_chunks = int(entry.get("indexed_chunks") or 0)
            except (TypeError, ValueError):
                indexed_chunks = 0
            try:
                total_chunks = int(entry.get("total_chunks") or 0)
            except (TypeError, ValueError):
                total_chunks = 0
            try:
                size_bytes = int(entry.get("size_bytes") or 0)
            except (TypeError, ValueError):
                size_bytes = 0
            extension = str(entry.get("extension") or Path(full_path).suffix.lower() or "")
            cloud_file_id = str(entry.get("cloud_file_id") or "")
            cloud_version_id = str(entry.get("cloud_version_id") or "")
            cloud_path = str(entry.get("cloud_path") or "")
            storage_key = str(entry.get("storage_key") or "")
            content_hash = str(entry.get("content_hash") or "")
            rows.append(
                (
                    full_path,
                    fingerprint,
                    mtime,
                    stage,
                    size_bytes,
                    extension,
                    now,
                    cloud_file_id,
                    cloud_version_id,
                    cloud_path,
                    storage_key,
                    content_hash,
                    indexed_stage,
                    status,
                    last_error,
                    last_attempt_at,
                    next_retry_at,
                    indexed_chunks,
                    total_chunks,
                )
            )
        if not rows:
            return
        with self._lock:
            with self._connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO state_entries (
                        full_path, fingerprint, mtime, stage, size_bytes, extension, updated_at,
                        cloud_file_id, cloud_version_id, cloud_path, storage_key, content_hash,
                        indexed_stage, status, last_error, last_attempt_at, next_retry_at,
                        indexed_chunks, total_chunks
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(full_path) DO UPDATE SET
                        fingerprint=excluded.fingerprint,
                        mtime=excluded.mtime,
                        stage=excluded.stage,
                        size_bytes=excluded.size_bytes,
                        extension=excluded.extension,
                        updated_at=excluded.updated_at,
                        cloud_file_id=excluded.cloud_file_id,
                        cloud_version_id=excluded.cloud_version_id,
                        cloud_path=excluded.cloud_path,
                        storage_key=excluded.storage_key,
                        content_hash=excluded.content_hash,
                        indexed_stage=excluded.indexed_stage,
                        status=excluded.status,
                        last_error=excluded.last_error,
                        last_attempt_at=excluded.last_attempt_at,
                        next_retry_at=excluded.next_retry_at,
                        indexed_chunks=excluded.indexed_chunks,
                        total_chunks=excluded.total_chunks
                    """,
                    rows,
                )

    def delete_entries(self, paths: Iterable[str]) -> int:
        cleaned = [str(path or "").strip() for path in paths]
        values = [path for path in cleaned if path]
        if not values:
            return 0
        placeholders = ",".join("?" for _ in values)
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    f"DELETE FROM state_entries WHERE full_path IN ({placeholders})",
                    values,
                )
                return int(cur.rowcount or 0)

    def update_stage_for_extensions(self, extensions: Iterable[str], stage: str = "metadata") -> int:
        exts = []
        for ext in extensions:
            value = str(ext or "").strip().lower()
            if not value:
                continue
            if not value.startswith("."):
                value = "." + value
            exts.append(value)
        if not exts:
            return 0
        placeholders = ",".join("?" for _ in exts)
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    f"""
                    UPDATE state_entries
                    SET stage=?,
                        indexed_stage=?,
                        indexed_chunks=0,
                        total_chunks=0,
                        status='ok',
                        last_error='',
                        next_retry_at=0,
                        updated_at=?
                    WHERE extension IN ({placeholders}) AND stage != ?
                    """,
                    (stage, stage, now, *exts, stage),
                )
                return int(cur.rowcount or 0)

    def mark_all_for_reindex(self, *, stage: str = "metadata") -> int:
        now = _utc_now()
        target_stage = str(stage or "metadata")
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    UPDATE state_entries
                    SET stage=?,
                        indexed_stage=?,
                        indexed_chunks=0,
                        total_chunks=0,
                        status='ok',
                        last_error='',
                        next_retry_at=0,
                        updated_at=?
                    """,
                    (target_stage, target_stage, now),
                )
                return int(cur.rowcount or 0)

    def list_deleted_candidates(self, existing_paths: Iterable[str]) -> List[str]:
        existing = {str(path or "").strip() for path in existing_paths if str(path or "").strip()}
        if not existing:
            return [row["full_path"] for row in self.iter_entries()]
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute("SELECT full_path FROM state_entries")
                candidates: List[str] = []
                for row in cur.fetchall():
                    path = str(row["full_path"] or "")
                    if path and path not in existing:
                        candidates.append(path)
                return candidates

    def list_entries_by_prefix(self, prefix: str) -> List[str]:
        value = str(prefix or "")
        if not value:
            return []
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT full_path
                    FROM state_entries
                    WHERE substr(full_path, 1, ?) = ?
                    ORDER BY full_path
                    """,
                    (len(value), value),
                ).fetchall()
                return [str(row["full_path"]) for row in rows]

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            with self._connect() as conn:
                total_row = conn.execute(
                    "SELECT COUNT(*) AS total, COALESCE(SUM(size_bytes), 0) AS total_size FROM state_entries"
                ).fetchone()
                failed_row = conn.execute("SELECT COUNT(*) AS total FROM failed_paths").fetchone()
                queue_rows = conn.execute(
                    "SELECT status, COUNT(*) AS cnt FROM index_queue GROUP BY status"
                ).fetchall()
                duplicate_rows = conn.execute(
                    """
                    SELECT content_hash, COUNT(*) AS cnt
                    FROM state_entries
                    WHERE content_hash != ''
                    GROUP BY content_hash
                    HAVING COUNT(*) > 1
                    """
                ).fetchall()
                ext_rows = conn.execute(
                    """
                    SELECT
                        CASE WHEN extension = '' THEN '(без расширения)' ELSE extension END AS ext,
                        COUNT(*) AS cnt,
                        COALESCE(SUM(size_bytes), 0) AS size_sum
                    FROM state_entries
                    GROUP BY ext
                    ORDER BY cnt DESC
                    """
                ).fetchall()
                stage_rows = conn.execute(
                    """
                    SELECT
                        CASE WHEN stage = '' THEN 'metadata' ELSE stage END AS stage,
                        COUNT(*) AS cnt
                    FROM state_entries
                    GROUP BY stage
                    ORDER BY cnt DESC
                    """
                ).fetchall()
                status_rows = conn.execute(
                    """
                    SELECT
                        CASE WHEN status = '' THEN 'ok' ELSE status END AS status,
                        COUNT(*) AS cnt
                    FROM state_entries
                    GROUP BY status
                    ORDER BY cnt DESC
                    """
                ).fetchall()
                indexed_stage_rows = conn.execute(
                    """
                    SELECT
                        CASE WHEN indexed_stage = '' THEN '(unknown)' ELSE indexed_stage END AS indexed_stage,
                        COUNT(*) AS cnt
                    FROM state_entries
                    GROUP BY indexed_stage
                    ORDER BY cnt DESC
                    """
                ).fetchall()
        by_ext = {str(row["ext"]): int(row["cnt"]) for row in ext_rows}
        by_ext_size = {str(row["ext"]): int(row["size_sum"]) for row in ext_rows}
        by_stage = {str(row["stage"]): int(row["cnt"]) for row in stage_rows}
        by_status = {str(row["status"]): int(row["cnt"]) for row in status_rows}
        by_indexed_stage = {str(row["indexed_stage"]): int(row["cnt"]) for row in indexed_stage_rows}
        duplicate_groups = len(duplicate_rows)
        duplicate_files = sum(int(row["cnt"]) for row in duplicate_rows)
        queue_by_status = {str(row["status"]): int(row["cnt"]) for row in queue_rows}
        return {
            "total": int(total_row["total"] if total_row else 0),
            "total_size_bytes": int(total_row["total_size"] if total_row else 0),
            "by_ext": by_ext,
            "by_ext_size": by_ext_size,
            "by_stage": by_stage,
            "by_status": by_status,
            "by_indexed_stage": by_indexed_stage,
            "failed_paths": int(failed_row["total"] if failed_row else 0),
            "duplicate_groups": duplicate_groups,
            "duplicate_files": duplicate_files,
            "queue_by_status": queue_by_status,
        }
