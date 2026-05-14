"""SQLite runtime pragmas shared by app databases."""

from __future__ import annotations

import sqlite3
import time


def prepare_sqlite_connection(conn: sqlite3.Connection, *, retries: int = 3) -> None:
    """Prepare a SQLite connection without failing on redundant WAL setup races.

    Several app processes may open the same SQLite database at startup. Re-running
    ``PRAGMA journal_mode=WAL`` on every connection can temporarily raise
    ``OperationalError: disk I/O error`` on Windows or on network/external drives
    even when the database remains readable. In that case we fall back to the
    current/default journal mode instead of failing service startup.
    """
    last_error: sqlite3.OperationalError | None = None
    for _ in range(max(1, int(retries))):
        try:
            conn.execute("PRAGMA busy_timeout=30000;")
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
            except sqlite3.OperationalError as exc:
                last_error = exc
                try:
                    row = conn.execute("PRAGMA journal_mode;").fetchone()
                    if row and str(row[0] or "").strip():
                        conn.execute("PRAGMA synchronous=NORMAL;")
                        return
                except sqlite3.OperationalError:
                    try:
                        conn.execute("PRAGMA journal_mode=DELETE;")
                        conn.execute("PRAGMA synchronous=NORMAL;")
                        return
                    except sqlite3.OperationalError:
                        raise exc
            conn.execute("PRAGMA synchronous=NORMAL;")
            return
        except sqlite3.OperationalError as exc:
            last_error = exc
            time.sleep(0.25)
    if last_error is not None:
        raise last_error
