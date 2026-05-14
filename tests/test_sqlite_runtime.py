from __future__ import annotations

import sqlite3

from rag_catalog.core.sqlite_runtime import prepare_sqlite_connection


class _Cursor:
    def __init__(self, value: str) -> None:
        self.value = value

    def fetchone(self) -> tuple[str]:
        return (self.value,)


class _WalFailsButReadableConnection:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, sql: str):
        self.calls.append(sql)
        if sql == "PRAGMA journal_mode=WAL;":
            raise sqlite3.OperationalError("disk I/O error")
        if sql == "PRAGMA journal_mode;":
            return _Cursor("delete")
        return _Cursor("")


def test_prepare_sqlite_connection_falls_back_when_wal_fails() -> None:
    conn = _WalFailsButReadableConnection()

    prepare_sqlite_connection(conn)  # type: ignore[arg-type]

    assert "PRAGMA busy_timeout=30000;" in conn.calls
    assert "PRAGMA journal_mode=WAL;" in conn.calls
    assert "PRAGMA journal_mode;" in conn.calls
    assert "PRAGMA synchronous=NORMAL;" in conn.calls
