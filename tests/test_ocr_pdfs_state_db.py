from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pytest

from rag_catalog.core import ocr_pdfs
from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.ocr_pdfs import (
    ensure_ocr_payload_indexes,
    find_pending_ocr_candidates_from_runtime,
    find_state_db_ocr_candidates,
    remove_from_state_db,
)
from rag_catalog.core.telemetry_db import TelemetryDB


def test_remove_from_state_db_deletes_matching_paths(tmp_path: Path) -> None:
    db_path = tmp_path / "index_state.db"
    db = IndexStateDB(str(db_path))
    db.upsert_many(
        [
            {"full_path": r"O:\a.pdf", "fingerprint": "1_1", "mtime": 1.0, "stage": "metadata", "size_bytes": 1, "extension": ".pdf"},
            {"full_path": r"O:\b.pdf", "fingerprint": "2_2", "mtime": 2.0, "stage": "metadata", "size_bytes": 2, "extension": ".pdf"},
        ]
    )
    removed = remove_from_state_db(db_path, [r"O:\a.pdf"])
    assert removed == 1
    assert db.get_entry(r"O:\a.pdf") is None
    assert db.get_entry(r"O:\b.pdf") is not None


def test_remove_from_state_db_raises_when_db_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        remove_from_state_db(tmp_path / "missing_index_state.db", [r"O:\a.pdf"])


def test_find_state_db_ocr_candidates_returns_large_unprocessed_pdfs(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {
                "full_path": r"O:\large-metadata.pdf",
                "fingerprint": "1",
                "mtime": 1.0,
                "stage": "metadata",
                "indexed_stage": "metadata",
                "status": "ok",
                "size_bytes": 3 * 1_048_576,
                "extension": ".pdf",
            },
            {
                "full_path": r"O:\large-empty.pdf",
                "fingerprint": "2",
                "mtime": 1.0,
                "stage": "empty",
                "indexed_stage": "small",
                "status": "empty",
                "size_bytes": 4 * 1_048_576,
                "extension": ".pdf",
            },
            {
                "full_path": r"O:\small-metadata.pdf",
                "fingerprint": "3",
                "mtime": 1.0,
                "stage": "metadata",
                "indexed_stage": "metadata",
                "status": "ok",
                "size_bytes": 1_048_576,
                "extension": ".pdf",
            },
            {
                "full_path": r"O:\large-content.pdf",
                "fingerprint": "4",
                "mtime": 1.0,
                "stage": "content",
                "indexed_stage": "large",
                "status": "ok",
                "size_bytes": 5 * 1_048_576,
                "extension": ".pdf",
            },
        ]
    )

    assert find_state_db_ocr_candidates(tmp_path, small_pdf_mb=2.0) == [r"O:\large-empty.pdf", r"O:\large-metadata.pdf"]


def test_find_pending_ocr_candidates_from_runtime_skips_completed_state_entries(tmp_path: Path) -> None:
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {
                "full_path": r"O:\done.pdf",
                "fingerprint": "1",
                "mtime": 1.0,
                "stage": "content",
                "indexed_stage": "large",
                "status": "ok",
                "size_bytes": 3 * 1_048_576,
                "extension": ".pdf",
            },
            {
                "full_path": r"O:\empty.pdf",
                "fingerprint": "2",
                "mtime": 1.0,
                "stage": "empty",
                "indexed_stage": "small",
                "status": "empty",
                "size_bytes": 3 * 1_048_576,
                "extension": ".pdf",
            },
        ]
    )
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "ocr_candidates_old.txt").write_text(
        "\n".join([r"O:\done.pdf", r"O:\empty.pdf", r"O:\missing.pdf"]) + "\n",
        encoding="utf-8",
    )

    assert find_pending_ocr_candidates_from_runtime(tmp_path, runtime_dir) == [r"O:\empty.pdf", r"O:\missing.pdf"]


def test_ensure_ocr_payload_indexes_creates_missing_qdrant_indexes(monkeypatch) -> None:
    class Info:
        payload_schema = {"type": object()}

    class Client:
        def __init__(self) -> None:
            self.created: list[tuple[str, str, object, bool, int]] = []
            self.polls = 0

        def create_payload_index(self, *, collection_name: str, field_name: str, field_schema: object, wait: bool, timeout: int) -> None:
            self.created.append((collection_name, field_name, field_schema, wait, timeout))

        def get_collection(self, _collection: str) -> object:
            self.polls += 1
            info = Info()
            info.payload_schema = {"type": object(), "extension": object()}
            return info

    client = Client()

    monkeypatch.setattr(ocr_pdfs.time, "sleep", lambda _seconds: None)

    ensure_ocr_payload_indexes(client, "catalog", collection_info=Info(), timeout_sec=123)

    assert [(row[0], row[1], row[3], row[4]) for row in client.created] == [("catalog", "extension", False, 60)]
    assert client.polls == 1


def test_ensure_ocr_payload_indexes_warns_when_queued_index_is_not_ready(monkeypatch) -> None:
    class Info:
        payload_schema: dict[str, object] = {}

    class Client:
        def __init__(self) -> None:
            self.created: list[str] = []

        def create_payload_index(self, *, field_name: str, **_kwargs: object) -> None:
            self.created.append(field_name)

        def get_collection(self, _collection: str) -> object:
            return Info()

    now = {"value": 0.0}
    monkeypatch.setattr(ocr_pdfs.time, "monotonic", lambda: now["value"])
    monkeypatch.setattr(ocr_pdfs.time, "sleep", lambda seconds: now.__setitem__("value", now["value"] + float(seconds)))

    client = Client()
    ensure_ocr_payload_indexes(client, "catalog", collection_info=Info(), timeout_sec=1)

    assert client.created == ["type", "extension"]


def test_ensure_ocr_payload_indexes_skips_existing_qdrant_indexes() -> None:
    class Info:
        payload_schema = {"type": object(), "extension": object()}

    class Client:
        def create_payload_index(self, **_kwargs: object) -> None:
            raise AssertionError("index already exists")

    ensure_ocr_payload_indexes(Client(), "catalog", collection_info=Info(), timeout_sec=123)


def test_ocr_main_creates_progress_row_before_qdrant_scan(monkeypatch, tmp_path: Path) -> None:
    telemetry_path = tmp_path / "rag_telemetry.db"
    cfg = {
        "qdrant_db_path": str(tmp_path),
        "qdrant_url": "http://localhost:6333",
        "collection_name": "catalog",
        "embedding_model": "",
        "embedding_collection_versioning": False,
        "embedding_collection_suffix": "",
        "index_read_workers": 1,
        "telemetry_db_path": str(telemetry_path),
    }
    observed: dict[str, object] = {}

    def fake_find_scanned_pdfs(*_args: object, **_kwargs: object) -> list[str]:
        active = TelemetryDB(str(telemetry_path)).get_active_ocr_run()
        observed["active_note"] = active["note"] if active else ""
        observed["active_found"] = active["found_scanned"] if active else None
        return []

    monkeypatch.setattr(ocr_pdfs, "load_config", lambda: cfg)
    monkeypatch.setattr(ocr_pdfs, "find_scanned_pdfs", fake_find_scanned_pdfs)
    monkeypatch.setattr(ocr_pdfs.sys, "argv", ["ocr_pdfs.py"])

    assert ocr_pdfs.main() == 0

    assert str(observed["active_note"]).startswith("searching_scanned_pdfs")
    assert observed["active_found"] == 0
    rows = TelemetryDB(str(telemetry_path)).fetch_dicts("SELECT status, note FROM ocr_runs")
    assert rows == [{"status": "completed", "note": "no_scanned_pdfs"}]


def test_ocr_main_passes_only_candidate_paths_to_indexer(monkeypatch, tmp_path: Path) -> None:
    telemetry_path = tmp_path / "rag_telemetry.db"
    candidate = r"O:\large-metadata.pdf"
    db = IndexStateDB(str(tmp_path / "index_state.db"))
    db.upsert_many(
        [
            {
                "full_path": candidate,
                "fingerprint": "1",
                "mtime": 1.0,
                "stage": "metadata",
                "indexed_stage": "metadata",
                "status": "ok",
                "size_bytes": 3 * 1_048_576,
                "extension": ".pdf",
            }
        ]
    )
    cfg = {
        "catalog_path": str(tmp_path),
        "qdrant_db_path": str(tmp_path),
        "qdrant_url": "http://localhost:6333",
        "collection_name": "catalog",
        "embedding_model": "",
        "embedding_collection_versioning": False,
        "embedding_collection_suffix": "",
        "index_read_workers": 1,
        "small_pdf_mb": 2.0,
        "telemetry_db_path": str(telemetry_path),
    }
    captured: dict[str, object] = {}

    def fake_run(cmd: list[str], **_kwargs: object) -> SimpleNamespace:
        captured["cmd"] = list(cmd)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(ocr_pdfs, "load_config", lambda: cfg)
    monkeypatch.setattr(ocr_pdfs.subprocess, "run", fake_run)
    monkeypatch.setattr(ocr_pdfs.sys, "argv", ["ocr_pdfs.py"])

    assert ocr_pdfs.main() == 0

    cmd = captured["cmd"]
    assert "--only-paths-file" in cmd
    candidates_path = Path(cmd[cmd.index("--only-paths-file") + 1])
    assert candidates_path.read_text(encoding="utf-8").splitlines() == [candidate]
    assert "--force-ocr" in cmd
