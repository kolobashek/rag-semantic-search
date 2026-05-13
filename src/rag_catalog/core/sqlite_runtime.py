"""SQLite runtime pragmas shared by app databases."""

from __future__ import annotations

import sqlite3
import time


def prepare_sqlite_connection(conn: sqlite3.Connection, *, retries: int = 3) -> None:
    """Prepare a SQLite connection without failing on redundant WAL setup races.

    Several app processes may open the same SQLite database at startup. Re-running
    ``PRAGMA journal_mode=WAL`` on every connection can temporarily raise
    ``OperationalError: disk I/O error`` on Windows even when the database is
    healthy and already in WAL mode. In that case we keep using the connection.
    """
    last_error: sqlite3.OperationalError | None = None
    for _ in range(max(1, int(retries))):
        try:
            conn.execute("PRAGMA busy_timeout=30000;")
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
            except sqlite3.OperationalError as exc:
                mode = ""
                try:
                    row = conn.execute("PRAGMA journal_mode;").fetchone()
                    mode = str(row[0] if row else "").lower()
                except sqlite3.OperationalError:
                    mode = ""
                if mode != "wal":
                    raise exc
            conn.execute("PRAGMA synchronous=NORMAL;")
            return
        except sqlite3.OperationalError as exc:
            last_error = exc
            time.sleep(0.25)
    if last_error is not None:
        raise last_error
