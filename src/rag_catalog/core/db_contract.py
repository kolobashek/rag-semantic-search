"""
db_contract.py — единый контракт совместимости SQLite-схем между worktree.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SchemaMismatchError(RuntimeError):
    """Код worktree не совместим с уже обновленной схемой SQLite-базы."""


def ensure_schema_version(
    conn: sqlite3.Connection,
    *,
    db_kind: str,
    db_path: Path,
    expected_version: int,
    code_root: Path,
) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            db_kind TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            code_root TEXT NOT NULL DEFAULT ''
        )
        """
    )
    row = conn.execute(
        "SELECT schema_version, code_root FROM schema_meta WHERE db_kind=?",
        (db_kind,),
    ).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO schema_meta (db_kind, schema_version, updated_at, code_root)
            VALUES (?, ?, ?, ?)
            """,
            (db_kind, int(expected_version), _utc_now(), str(code_root)),
        )
        return

    actual_version = int(row["schema_version"])
    if actual_version > int(expected_version):
        source_root = str(row["code_root"] or "").strip()
        source_hint = f" Последний апгрейд схемы выполнен кодом из {source_root}." if source_root else ""
        raise SchemaMismatchError(
            f"Схема БД '{db_kind}' в {db_path} имеет версию {actual_version}, "
            f"а этот worktree ожидает только {expected_version}. "
            f"Обновите код текущего worktree или запустите приложение из актуального каталога.{source_hint}"
        )

    if actual_version < int(expected_version):
        conn.execute(
            """
            UPDATE schema_meta
            SET schema_version=?, updated_at=?, code_root=?
            WHERE db_kind=?
            """,
            (int(expected_version), _utc_now(), str(code_root), db_kind),
        )
