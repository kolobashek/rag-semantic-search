"""
index_state_db.py — SQLite-хранилище состояния индексатора.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .db_contract import ensure_schema_version


SCHEMA_VERSION = 2


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

    def _init_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA busy_timeout=30000;")
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

    def iter_entries(self) -> List[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    SELECT full_path, fingerprint, mtime, stage, size_bytes, extension, updated_at
                    FROM state_entries
                    ORDER BY full_path
                    """
                )
                return [dict(row) for row in cur.fetchall()]

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
            try:
                size_bytes = int(entry.get("size_bytes") or 0)
            except (TypeError, ValueError):
                size_bytes = 0
            extension = str(entry.get("extension") or Path(full_path).suffix.lower() or "")
            rows.append((full_path, fingerprint, mtime, stage, size_bytes, extension, now))
        if not rows:
            return
        with self._lock:
            with self._connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO state_entries (
                        full_path, fingerprint, mtime, stage, size_bytes, extension, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(full_path) DO UPDATE SET
                        fingerprint=excluded.fingerprint,
                        mtime=excluded.mtime,
                        stage=excluded.stage,
                        size_bytes=excluded.size_bytes,
                        extension=excluded.extension,
                        updated_at=excluded.updated_at
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
                    SET stage=?, updated_at=?
                    WHERE extension IN ({placeholders}) AND stage != ?
                    """,
                    (stage, now, *exts, stage),
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

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            with self._connect() as conn:
                total_row = conn.execute(
                    "SELECT COUNT(*) AS total, COALESCE(SUM(size_bytes), 0) AS total_size FROM state_entries"
                ).fetchone()
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
        by_ext = {str(row["ext"]): int(row["cnt"]) for row in ext_rows}
        by_ext_size = {str(row["ext"]): int(row["size_sum"]) for row in ext_rows}
        by_stage = {str(row["stage"]): int(row["cnt"]) for row in stage_rows}
        return {
            "total": int(total_row["total"] if total_row else 0),
            "total_size_bytes": int(total_row["total_size"] if total_row else 0),
            "by_ext": by_ext,
            "by_ext_size": by_ext_size,
            "by_stage": by_stage,
        }
