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
    assert row["status"] == "ok"
    assert row["indexed_stage"] == "metadata"
    assert row["indexed_chunks"] == 0
    assert row["total_chunks"] == 0
    snapshot = db.entries_snapshot()
    assert set(snapshot) == {r"O:\docs\a.pdf", r"O:\docs\b.docx"}
    assert snapshot[r"O:\docs\b.docx"]["stage"] == "content"
    search_entries = db.iter_search_entries()
    assert search_entries == [
        {
            "full_path": r"O:\docs\a.pdf",
            "mtime": 1.0,
            "size_bytes": 100,
            "extension": ".pdf",
        },
        {
            "full_path": r"O:\docs\b.docx",
            "mtime": 2.0,
            "size_bytes": 200,
            "extension": ".docx",
        },
    ]
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


def test_update_stage_for_paths_preserves_identity_and_resets_progress(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {
                "full_path": "cloud:file-1",
                "fingerprint": "sha256:abc",
                "mtime": 1.0,
                "stage": "content",
                "indexed_stage": "large",
                "indexed_chunks": 7,
                "total_chunks": 7,
                "size_bytes": 5,
                "extension": ".pdf",
                "cloud_file_id": "file-1",
                "cloud_version_id": "version-1",
                "cloud_path": "Folder/report.pdf",
                "storage_key": "objects/report.pdf",
            }
        ]
    )

    assert db.update_stage_for_paths(["cloud:file-1", "missing"], stage="metadata") == 1

    row = db.get_entry("cloud:file-1")
    assert row is not None
    assert row["stage"] == "metadata"
    assert row["indexed_chunks"] == 0
    assert row["cloud_file_id"] == "file-1"
    assert row["cloud_version_id"] == "version-1"
    assert row["storage_key"] == "objects/report.pdf"


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
    assert stats["by_status"]["ok"] == 3
    assert stats["by_indexed_stage"]["content"] == 2
    assert stats["by_indexed_stage"]["metadata"] == 1


def test_state_db_separates_legacy_stage_from_result_status(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {
                "full_path": "broken.pdf",
                "fingerprint": "1",
                "mtime": 1.0,
                "stage": "error",
                "indexed_stage": "large",
                "status": "error",
                "last_error": "locked",
                "next_retry_at": 123.0,
                "size_bytes": 10,
                "extension": ".pdf",
            },
            {
                "full_path": "empty.docx",
                "fingerprint": "2",
                "mtime": 2.0,
                "stage": "empty",
                "indexed_stage": "small",
                "status": "empty",
                "size_bytes": 20,
                "extension": ".docx",
            },
        ]
    )

    broken = db.get_entry("broken.pdf")
    stats = db.stats()

    assert broken is not None
    assert broken["stage"] == "error"
    assert broken["indexed_stage"] == "large"
    assert broken["status"] == "error"
    assert broken["last_error"] == "locked"
    assert broken["next_retry_at"] == 123.0
    assert stats["by_stage"] == {"empty": 1, "error": 1}
    assert stats["by_status"] == {"empty": 1, "error": 1}
    assert stats["by_indexed_stage"] == {"large": 1, "small": 1}


def test_state_db_tracks_content_hash_duplicates(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {"full_path": "a.txt", "fingerprint": "1", "mtime": 1.0, "stage": "content", "size_bytes": 1, "extension": ".txt", "content_hash": "same"},
            {"full_path": "b.txt", "fingerprint": "2", "mtime": 2.0, "stage": "content", "size_bytes": 1, "extension": ".txt", "content_hash": "same"},
            {"full_path": "c.txt", "fingerprint": "3", "mtime": 3.0, "stage": "content", "size_bytes": 1, "extension": ".txt", "content_hash": "other"},
        ]
    )

    assert db.find_by_content_hash("same", exclude_path="b.txt")["full_path"] == "a.txt"
    stats = db.stats()
    assert stats["duplicate_groups"] == 1
    assert stats["duplicate_files"] == 2


def test_state_db_lists_entries_by_prefix(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {"full_path": "docs.zip::a.txt", "fingerprint": "1", "mtime": 1.0, "stage": "content", "size_bytes": 1, "extension": ".txt"},
            {"full_path": "docs.zip::b.txt", "fingerprint": "2", "mtime": 1.0, "stage": "content", "size_bytes": 1, "extension": ".txt"},
            {"full_path": "other.zip::a.txt", "fingerprint": "3", "mtime": 1.0, "stage": "content", "size_bytes": 1, "extension": ".txt"},
        ]
    )

    assert db.list_entries_by_prefix("docs.zip::") == ["docs.zip::a.txt", "docs.zip::b.txt"]


def test_state_db_marks_all_entries_for_reindex(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {
                "full_path": "a.txt",
                "fingerprint": "1",
                "mtime": 1.0,
                "stage": "content",
                "indexed_stage": "small",
                "status": "ok",
                "size_bytes": 1,
                "extension": ".txt",
            }
        ]
    )

    assert db.mark_all_for_reindex(stage="metadata") == 1
    row = db.get_entry("a.txt")
    assert row["stage"] == "metadata"
    assert row["indexed_stage"] == "metadata"
    assert row["status"] == "ok"


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


def test_index_queue_coalesces_and_leases_tasks(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))

    first = db.enqueue_index_task("a.txt", stage="small", reason="watch", priority=50, available_at=10.0)
    second = db.enqueue_index_task("a.txt", stage="small", reason="changed", priority=10, available_at=5.0)
    db.enqueue_index_task("b.txt", stage="small", reason="watch", priority=20, available_at=5.0)

    assert first["id"] == second["id"]
    assert second["reason"] == "changed"
    assert second["priority"] == 10
    assert second["available_at"] == 5.0
    assert db.queue_stats() == {"pending": 2}

    leased = db.lease_index_tasks(limit=2, now=5.0, lease_seconds=60)

    assert [row["full_path"] for row in leased] == ["a.txt", "b.txt"]
    assert all(row["status"] == "running" for row in leased)
    assert all(int(row["attempts"]) == 1 for row in leased)
    assert db.queue_stats() == {"running": 2}

    assert db.complete_index_task(int(leased[0]["id"])) == 1
    assert db.fail_index_task(int(leased[1]["id"]), error="locked", retry_delay_seconds=10) == 1
    assert db.queue_stats() == {"pending": 1}
    assert db.lease_index_tasks(limit=1, now=6.0) == []


def test_index_queue_requeues_expired_running_tasks(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.enqueue_index_task("a.txt", stage="small", available_at=1.0)
    leased = db.lease_index_tasks(limit=1, now=2.0, lease_seconds=5)

    assert db.requeue_expired_index_tasks(now=6.0) == 0
    assert db.requeue_expired_index_tasks(now=8.0) == 1
    assert db.queue_stats() == {"pending": 1}
    assert db.lease_index_tasks(limit=1, now=8.0)[0]["id"] == leased[0]["id"]


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
