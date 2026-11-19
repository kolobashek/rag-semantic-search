"""SQLite runtime pragmas shared by app databases."""

from __future__ import annotations

import sqlite3
import time
import threading
from collections import OrderedDict
from pathlib import Path


_DATABASES = OrderedDict()
_DATABASES_LOCK = threading.RLock()


class ClosingConnection(sqlite3.Connection):
    """Commit/rollback and release file handles when leaving a transaction."""

    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


def cached_database(factory, path):
    """Reuse initialized stores, not connections or query results.

    A replaced/deleted database must run initialization again. Never cache a
    failed initialization. The lock also prevents concurrent first-use DDL.
    """
    resolved = Path(path).resolve()
    key = (factory, str(resolved))
    with _DATABASES_LOCK:
        stat = resolved.stat() if resolved.exists() else None
        identity = (stat.st_dev, stat.st_ino) if stat else None
        cached = _DATABASES.get(key)
        if cached and identity and identity == cached[0]:
            _DATABASES.move_to_end(key)
            return cached[1]
        store = factory(str(resolved))
        if not resolved.exists():
            return store
        stat = resolved.stat()
        _DATABASES[key] = ((stat.st_dev, stat.st_ino), store)
        _DATABASES.move_to_end(key)
        while len(_DATABASES) > 32:
            _DATABASES.popitem(last=False)
        return store


def _try_normal_sync(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA synchronous=NORMAL;")
    except sqlite3.OperationalError:
        pass


def prepare_sqlite_connection(
    conn: sqlite3.Connection,
    *,
    retries: int = 8,
    journal_mode: str = "wal",
    require_journal_mode: bool = False,
) -> None:
    """Prepare a SQLite connection without failing on redundant WAL setup races.

    Several app processes may open the same SQLite database at startup. Re-running
    ``PRAGMA journal_mode=WAL`` on every connection can temporarily raise
    ``OperationalError: disk I/O error`` on Windows or on network/external drives
    even when the database remains readable. In that case we fall back to the
    current/default journal mode instead of failing service startup.
    """
    target_mode = str(journal_mode or "wal").strip().lower()
    if target_mode not in {"wal", "delete", "truncate"}:
        raise ValueError(f"Unsupported SQLite journal mode: {journal_mode}")
    last_error: sqlite3.OperationalError | None = None
    for attempt in range(max(1, int(retries))):
        try:
            conn.execute("PRAGMA busy_timeout=30000;")
            current_mode = ""
            try:
                row = conn.execute("PRAGMA journal_mode;").fetchone()
                current_mode = str(row[0] or "").strip().lower() if row else ""
            except sqlite3.OperationalError as exc:
                last_error = exc

            if current_mode == target_mode:
                _try_normal_sync(conn)
                return

            try:
                row = conn.execute(f"PRAGMA journal_mode={target_mode.upper()};").fetchone()
                applied_mode = str(row[0] or "").strip().lower() if row else ""
                if require_journal_mode and applied_mode != target_mode:
                    raise sqlite3.OperationalError(
                        f"SQLite journal mode remains {applied_mode or 'unknown'}, expected {target_mode}"
                    )
            except sqlite3.OperationalError as exc:
                last_error = exc
                if require_journal_mode:
                    raise
                if current_mode and not require_journal_mode:
                    _try_normal_sync(conn)
                    return
                try:
                    row = conn.execute("PRAGMA journal_mode;").fetchone()
                    if row and str(row[0] or "").strip():
                        _try_normal_sync(conn)
                        return
                except sqlite3.OperationalError as journal_exc:
                    last_error = journal_exc
                try:
                    conn.execute("SELECT 1;")
                    _try_normal_sync(conn)
                    return
                except sqlite3.OperationalError as readable_exc:
                    last_error = readable_exc
                    raise readable_exc from exc
            _try_normal_sync(conn)
            return
        except sqlite3.OperationalError as exc:
            last_error = exc
            time.sleep(min(1.0, 0.2 * (attempt + 1)))
    if last_error is not None:
        raise last_error
