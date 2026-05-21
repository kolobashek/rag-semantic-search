from __future__ import annotations

from pathlib import Path

import pytest

from rag_catalog.core import ocr_pdfs
from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.ocr_pdfs import remove_from_state_db
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
