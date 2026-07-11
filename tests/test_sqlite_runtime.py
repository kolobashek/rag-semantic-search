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


class _JournalPragmasFailButReadableConnection:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, sql: str):
        self.calls.append(sql)
        if sql in {"PRAGMA journal_mode=WAL;", "PRAGMA journal_mode;"}:
            raise sqlite3.OperationalError("disk I/O error")
        return _Cursor("")


class _AlreadyWalConnection:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, sql: str):
        self.calls.append(sql)
        if sql == "PRAGMA journal_mode;":
            return _Cursor("wal")
        if sql == "PRAGMA journal_mode=WAL;":
            raise AssertionError("journal mode must not be rewritten for every connection")
        return _Cursor("")


def test_prepare_sqlite_connection_falls_back_when_wal_fails() -> None:
    conn = _WalFailsButReadableConnection()

    prepare_sqlite_connection(conn)  # type: ignore[arg-type]

    assert "PRAGMA busy_timeout=30000;" in conn.calls
    assert "PRAGMA journal_mode=WAL;" in conn.calls
    assert "PRAGMA journal_mode;" in conn.calls
    assert "PRAGMA synchronous=NORMAL;" in conn.calls


def test_prepare_sqlite_connection_keeps_readable_connection_when_journal_pragmas_fail() -> None:
    conn = _JournalPragmasFailButReadableConnection()

    prepare_sqlite_connection(conn)  # type: ignore[arg-type]

    assert "PRAGMA journal_mode=WAL;" in conn.calls
    assert "PRAGMA journal_mode;" in conn.calls
    assert "SELECT 1;" in conn.calls


def test_prepare_sqlite_connection_does_not_rewrite_existing_wal_mode() -> None:
    conn = _AlreadyWalConnection()

    prepare_sqlite_connection(conn)  # type: ignore[arg-type]

    assert conn.calls == [
        "PRAGMA busy_timeout=30000;",
        "PRAGMA journal_mode;",
        "PRAGMA synchronous=NORMAL;",
    ]
