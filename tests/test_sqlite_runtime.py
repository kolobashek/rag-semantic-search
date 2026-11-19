from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from rag_catalog.core.sqlite_runtime import ClosingConnection, cached_database, prepare_sqlite_connection


def test_cached_store_initializes_once_across_threads_and_detects_replacement(tmp_path):
    path = tmp_path / 'cache.db'
    calls = []

    def factory(name):
        calls.append(name)
        with sqlite3.connect(name, factory=ClosingConnection) as db:
            db.execute('CREATE TABLE IF NOT EXISTS example (id INTEGER)')
        return object()

    with ThreadPoolExecutor(max_workers=8) as pool:
        stores = list(pool.map(lambda _: cached_database(factory, path), range(16)))
    assert len(calls) == 1
    assert all(s is stores[0] for s in stores)
    path.rename(tmp_path / 'old.db')
    assert cached_database(factory, path) is not stores[0]
    assert len(calls) == 2


def test_cached_store_retries_failed_initialization(tmp_path):
    calls = []

    def factory(path):
        calls.append(path)
        if len(calls) == 1:
            raise sqlite3.OperationalError('locked')
        with sqlite3.connect(path, factory=ClosingConnection) as db:
            db.execute('CREATE TABLE example (id INTEGER)')
        return object()

    with pytest.raises(sqlite3.OperationalError):
        cached_database(factory, tmp_path / 'db')
    assert cached_database(factory, tmp_path / 'db')
    assert len(calls) == 2


def test_connection_closes_and_rolls_back_on_error(tmp_path):
    path = tmp_path / 'transactions.db'
    with sqlite3.connect(path, factory=ClosingConnection) as db:
        db.execute('CREATE TABLE example (id INTEGER)')
        db.execute('INSERT INTO example VALUES (1)')
    with pytest.raises(sqlite3.ProgrammingError):
        db.execute('SELECT 1')
    with pytest.raises(ValueError), sqlite3.connect(path, factory=ClosingConnection) as db:
        db.execute('INSERT INTO example VALUES (2)')
        raise ValueError('rollback')
    with sqlite3.connect(path, factory=ClosingConnection) as db:
        assert db.execute('SELECT id FROM example').fetchall() == [(1,)]


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


class _JournalModeMigrationConnection:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, sql: str):
        self.calls.append(sql)
        if sql == "PRAGMA journal_mode;":
            return _Cursor("wal")
        if sql == "PRAGMA journal_mode=TRUNCATE;":
            return _Cursor("truncate")
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


def test_prepare_sqlite_connection_can_require_non_wal_mode() -> None:
    conn = _JournalModeMigrationConnection()

    prepare_sqlite_connection(conn, journal_mode="truncate", require_journal_mode=True)  # type: ignore[arg-type]

    assert "PRAGMA journal_mode=TRUNCATE;" in conn.calls
