"""Регрессии протокола standalone-агента синхронизации (rag_sync_client)."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import rag_sync_client
from rag_sync_client import _find_pair_for_cloud, classify_change, save_config


def _server_change(**overrides):
    """Запись в том виде, в каком её реально отдаёт /api/cloud-drive/changes."""
    row = {
        "node_type": "file",
        "id": "file-1",
        "path": "Docs/plan.docx",
        "name": "plan.docx",
        "created_at": "2026-08-01T10:00:00+00:00",
        "updated_at": "2026-08-01T10:00:00+00:00",
        "deleted_at": "",
        "current_version_id": "v1",
        "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "size_bytes": 10,
        "checksum": "abc",
    }
    row.update(overrides)
    return row


def test_server_change_rows_are_classified_and_not_dropped() -> None:
    """Сервер шлёт node_type/deleted_at; чтение type/change_type роняло весь фид."""
    assert classify_change(_server_change()) == "upsert"
    assert classify_change(_server_change(deleted_at="2026-08-02T10:00:00+00:00")) == "delete"
    assert classify_change(_server_change(node_type="folder")) == "folder"
    assert classify_change({"path": "x"}) == ""


def test_legacy_explicit_change_type_is_still_supported() -> None:
    assert classify_change({"type": "delete", "node_type": "file"}) == "delete"
    assert classify_change({"change_type": "upsert"}) == "upsert"


def test_root_pair_matches_every_cloud_path() -> None:
    """Пара с пустым cloud_path — корень диска, её создаёт сам клиент."""
    pairs = [{"id": "p1", "cloud_path": "", "local_path": "/tmp/sync"}]

    assert _find_pair_for_cloud(pairs, "Docs/plan.docx") is pairs[0]
    assert _find_pair_for_cloud(pairs, "plan.docx") is pairs[0]


def test_more_specific_pair_wins_over_root_pair() -> None:
    root_pair = {"id": "root", "cloud_path": "", "local_path": "/tmp/all"}
    docs_pair = {"id": "docs", "cloud_path": "Docs", "local_path": "/tmp/docs"}

    assert _find_pair_for_cloud([root_pair, docs_pair], "Docs/plan.docx") is docs_pair
    assert _find_pair_for_cloud([root_pair, docs_pair], "Other/x.txt") is root_pair


def test_upload_creates_missing_cloud_folders(monkeypatch) -> None:
    api = rag_sync_client.SyncAPIClient.__new__(rag_sync_client.SyncAPIClient)
    api._ensured_folders = set()
    created: list[tuple[str, str]] = []
    api.create_folder = lambda parent_path, name: created.append((parent_path, name)) or {"ok": True}

    api.ensure_remote_folders("Docs/2026/Q3")

    assert created == [("", "Docs"), ("Docs", "2026"), ("Docs/2026", "Q3")]

    created.clear()
    api.ensure_remote_folders("Docs/2026/Q3")
    assert created == [], "повторный вызов не должен дёргать сервер"


def test_saved_config_with_token_is_not_world_readable(tmp_path: Path) -> None:
    target = tmp_path / "cfg" / "config.json"

    save_config(target, {"server": "http://localhost:8080", "token": "secret-token"})

    assert json.loads(target.read_text(encoding="utf-8"))["token"] == "secret-token"
    if os.name != "nt":
        mode = stat.S_IMODE(target.stat().st_mode)
        assert mode & (stat.S_IRGRP | stat.S_IROTH) == 0


def test_module_imports_without_watchdog() -> None:
    """Агент должен импортироваться и без watchdog: иначе падают CI и тесты."""
    assert hasattr(rag_sync_client, "_PairEventHandler")
