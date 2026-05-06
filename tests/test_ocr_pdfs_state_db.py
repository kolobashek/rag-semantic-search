from __future__ import annotations

from pathlib import Path

import pytest

from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.ocr_pdfs import remove_from_state_db


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
