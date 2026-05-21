from __future__ import annotations

import json
from pathlib import Path

import pytest

from rag_catalog.core.index_state_db import IndexStateDB


def test_state_db_upsert_get_delete_and_count(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {
                "full_path": r"O:\docs\a.pdf",
                "fingerprint": "100_1",
                "mtime": 1.0,
                "stage": "metadata",
                "size_bytes": 100,
                "extension": ".pdf",
            },
            {
                "full_path": r"O:\docs\b.docx",
                "fingerprint": "200_2",
                "mtime": 2.0,
                "stage": "content",
                "size_bytes": 200,
                "extension": ".docx",
            },
        ]
    )
    assert db.count() == 2
    row = db.get_entry(r"O:\docs\a.pdf")
    assert row is not None
    assert row["fingerprint"] == "100_1"
    deleted = db.delete_entries([r"O:\docs\a.pdf"])
    assert deleted == 1
    assert db.count() == 1


def test_state_db_persists_cloud_drive_identity(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {
                "full_path": "cloud:file-1",
                "fingerprint": "sha256:abc",
                "mtime": 1.0,
                "stage": "content",
                "size_bytes": 5,
                "extension": ".txt",
                "cloud_file_id": "file-1",
                "cloud_version_id": "version-1",
                "cloud_path": "Folder A/hello.txt",
                "storage_key": "Folder A/hello.txt",
            }
        ]
    )

    row = db.get_entry("cloud:file-1")

    assert row is not None
    assert row["cloud_file_id"] == "file-1"
    assert row["cloud_version_id"] == "version-1"
    assert row["cloud_path"] == "Folder A/hello.txt"
    assert row["storage_key"] == "Folder A/hello.txt"


def test_state_db_stats_aggregates_by_extension(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {"full_path": "a.pdf", "fingerprint": "5_1", "mtime": 1.0, "stage": "content", "size_bytes": 5, "extension": ".pdf"},
            {"full_path": "b.pdf", "fingerprint": "7_1", "mtime": 1.0, "stage": "content", "size_bytes": 7, "extension": ".pdf"},
            {"full_path": "c.docx", "fingerprint": "3_1", "mtime": 1.0, "stage": "metadata", "size_bytes": 3, "extension": ".docx"},
        ]
    )
    stats = db.stats()
    assert stats["total"] == 3
    assert stats["total_size_bytes"] == 15
    assert stats["by_ext"][".pdf"] == 2
    assert stats["by_ext_size"][".pdf"] == 12


def test_state_db_validates_embedding_config(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.validate_embedding_config(
        embedding_model="model-a",
        vector_size=384,
        collection_name="catalog",
    )
    assert db.get_config()["embedding_model"] == "model-a"

    db.validate_embedding_config(
        embedding_model="model-a",
        vector_size=384,
        collection_name="catalog",
    )

    with pytest.raises(RuntimeError, match="--recreate"):
        db.validate_embedding_config(
            embedding_model="model-b",
            vector_size=384,
            collection_name="catalog",
        )

    db.validate_embedding_config(
        embedding_model="model-b",
        vector_size=768,
        collection_name="catalog_v2",
        recreate=True,
    )
    cfg = db.get_config()
    assert cfg["embedding_model"] == "model-b"
    assert cfg["vector_size"] == "768"
    assert cfg["collection_name"] == "catalog_v2"


def test_state_db_tracks_failed_paths_with_retry_backoff(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))

    first = db.record_failed_path("a.docx", fingerprint="fp1", error="locked", base_delay_seconds=10)
    second = db.record_failed_path("a.docx", fingerprint="fp1", error="locked again", base_delay_seconds=10)

    assert first["retry_count"] == 1
    assert second["retry_count"] == 2
    assert second["next_retry_at"] > first["next_retry_at"]
    assert db.get_failed_path("a.docx")["last_error"] == "locked again"
    assert not db.is_failed_retry_due("a.docx", now=second["next_retry_at"] - 1)
    assert db.is_failed_retry_due("a.docx", now=second["next_retry_at"] + 1)
    assert db.list_due_failed_paths(now=second["next_retry_at"] + 1)[0]["full_path"] == "a.docx"
    assert db.clear_failed_path("a.docx") == 1
    assert db.get_failed_path("a.docx") is None


def test_bootstrap_from_json_imports_only_once(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    legacy = tmp_path / "index_state.json"
    payload = {
        "files": {
            "x.pdf": {"fingerprint": "10_11", "mtime": 11.0, "stage": "metadata"},
            "y.docx": {"fingerprint": "20_22", "mtime": 22.0, "stage": "content"},
        }
    }
    legacy.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    imported = db.bootstrap_from_json(legacy)
    assert imported == 2
    imported_again = db.bootstrap_from_json(legacy)
    assert imported_again == 0
    assert db.count() == 2


def test_bootstrap_from_json_fail_fast_on_invalid_payload(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    legacy = tmp_path / "index_state.json"
    legacy.write_text("{bad json", encoding="utf-8")
    with pytest.raises(RuntimeError):
        db.bootstrap_from_json(legacy)
