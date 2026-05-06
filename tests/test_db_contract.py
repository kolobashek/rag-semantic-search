from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from rag_catalog.core import rag_core
from rag_catalog.core.db_contract import SchemaMismatchError
from rag_catalog.core.telemetry_db import SCHEMA_VERSION as TELEMETRY_SCHEMA_VERSION, TelemetryDB


def test_load_config_uses_nearest_ancestor_config(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    nested_root = repo_root / ".codex" / "worktrees" / "task"
    nested_root.mkdir(parents=True)
    (repo_root / "config.json").write_text('{"telemetry_db_path":"X:/shared/telemetry.db"}', encoding="utf-8")
    monkeypatch.setattr(rag_core, "PROJECT_ROOT", nested_root)

    cfg = rag_core.load_config()

    assert cfg["telemetry_db_path"] == "X:/shared/telemetry.db"


def test_telemetry_db_rejects_newer_schema_version(tmp_path: Path) -> None:
    db_path = tmp_path / "telemetry.db"
    TelemetryDB(str(db_path))
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE schema_meta SET schema_version=? WHERE db_kind='telemetry'",
            (TELEMETRY_SCHEMA_VERSION + 1,),
        )

    with pytest.raises(SchemaMismatchError):
        TelemetryDB(str(db_path))
