from __future__ import annotations

import os
import time
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from index_rag import RAGIndexer


class _FakeVec:
    def __init__(self, vals):
        self.vals = vals

    def tolist(self):
        return list(self.vals)


class _FakeEmbedder:
    def encode(self, chunks, normalize_embeddings=True, batch_size=256, show_progress_bar=False):
        return [_FakeVec([0.1, 0.2, 0.3]) for _ in chunks]


class _FakeQdrant:
    def __init__(self) -> None:
        self.points_count = 0
        self.points = []

    def upsert(self, collection_name, points):
        self.points_count += len(points)
        self.points.extend(points)

    def get_collection(self, collection_name):
        return SimpleNamespace(points_count=self.points_count)


class _FakeStateDB:
    def __init__(self) -> None:
        self.entries: dict[str, dict] = {}
        self.failures: dict[str, dict] = {}

    def get_entry(self, full_path: str):
        row = self.entries.get(full_path)
        return dict(row) if row else None

    def upsert_many(self, entries):
        for entry in entries:
            key = str(entry.get("full_path") or "")
            if key:
                self.entries[key] = dict(entry)

    def record_failed_path(self, full_path: str, *, fingerprint: str = "", error: str = "", **_kwargs):
        row = self.failures.get(full_path, {"retry_count": 0})
        row = {
            "full_path": full_path,
            "fingerprint": fingerprint,
            "last_error": error,
            "retry_count": int(row.get("retry_count") or 0) + 1,
            "next_retry_at": time.time() + 300,
        }
        self.failures[full_path] = row
        return dict(row)

    def clear_failed_path(self, full_path: str):
        return 1 if self.failures.pop(full_path, None) else 0

    def is_failed_retry_due(self, full_path: str):
        row = self.failures.get(full_path)
        return not row or float(row.get("next_retry_at") or 0) <= time.time()


def _make_indexer(tmp_path: Path, extracted_text: str) -> RAGIndexer:
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.current_stage = "small"
    idx.catalog_path = tmp_path
    idx.collection_name = "catalog"
    idx.chunk_size = 500
    idx.chunk_overlap = 100
    idx.batch_size = 1000
    idx.max_chunks_per_file = 0
    idx.read_workers = 1
    idx.metadata_only_extensions = set()
    idx.state_db = _FakeStateDB()
    idx._points_buffer = []
    idx.point_count = 0
    idx.run_id = ""
    idx._run_deleted_files = 0
    idx.small_office_mb = 20.0
    idx.small_pdf_mb = 2.0
    idx.embedder = _FakeEmbedder()
    idx.qdrant = _FakeQdrant()
    idx._delete_file_vectors = lambda _p: None
    idx._cleanup_deleted_files = lambda _files: 0
    idx._extract_doc = lambda _p: extracted_text
    idx._extract_docx = lambda _p: extracted_text
    idx._extract_spreadsheet = lambda _p: extracted_text
    idx._extract_rtf = lambda _p: extracted_text
    idx._extract_pptx = lambda _p: extracted_text
    idx._extract_pdf = lambda _p: extracted_text
    idx._extract_text = lambda p: p.read_text(encoding="utf-8")
    idx._extract_csv = lambda p: p.read_text(encoding="utf-8")
    return idx


def test_small_stage_file_without_content_is_marked_empty_for_retry(tmp_path: Path) -> None:
    p = tmp_path / "a.docx"
    p.write_text("dummy", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="")
    stats = idx.index_directory(stage="small")
    key = str(p)
    assert idx.state_db.get_entry(key)["stage"] == "empty"
    assert stats["processed_files"] >= 1


def test_read_error_is_marked_error_and_backed_off(tmp_path: Path) -> None:
    p = tmp_path / "broken.docx"
    p.write_text("dummy", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="")

    def _raise(_path: Path) -> str:
        raise OSError("locked")

    idx._extract_docx = _raise

    stats = idx.index_directory(stage="small")

    key = str(p)
    assert idx.state_db.get_entry(key)["stage"] == "error"
    assert idx.state_db.failures[key]["last_error"] == "locked"
    assert stats["error_files"] == 1

    stats_retry_wait = idx.index_directory(stage="small")
    assert stats_retry_wait["skipped_files"] == 1


def test_small_stage_file_with_content_becomes_content(tmp_path: Path) -> None:
    p = tmp_path / "b.docx"
    p.write_text("dummy", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="hello world")
    idx.index_directory(stage="small")
    key = str(p)
    assert idx.state_db.get_entry(key)["stage"] == "content"


def test_text_and_csv_files_are_indexed_as_content(tmp_path: Path) -> None:
    txt = tmp_path / "note.txt"
    csv = tmp_path / "table.csv"
    txt.write_text("plain text document", encoding="utf-8")
    csv.write_text("name;value\nalpha;42\n", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="")

    idx.index_directory(stage="small")

    assert idx.state_db.get_entry(str(txt))["stage"] == "content"
    assert idx.state_db.get_entry(str(csv))["stage"] == "content"


def test_rtf_pptx_and_doc_files_are_supported(tmp_path: Path) -> None:
    rtf = tmp_path / "note.rtf"
    pptx = tmp_path / "slides.pptx"
    doc = tmp_path / "legacy.doc"
    rtf.write_text("dummy", encoding="utf-8")
    pptx.write_bytes(b"dummy")
    doc.write_bytes(b"dummy")
    idx = _make_indexer(tmp_path, extracted_text="extracted content")

    idx.index_directory(stage="small")

    assert idx.state_db.get_entry(str(rtf))["stage"] == "content"
    assert idx.state_db.get_entry(str(pptx))["stage"] == "content"
    assert idx.state_db.get_entry(str(doc)) is None

    idx.index_directory(stage="large")
    assert idx.state_db.get_entry(str(doc))["stage"] == "content"


def test_zip_members_are_indexed_with_logical_archive_paths(tmp_path: Path) -> None:
    archive = tmp_path / "docs.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as zf:
        zf.writestr("folder/readme.txt", "zip text")
    idx = _make_indexer(tmp_path, extracted_text="")

    idx.index_directory(stage="small")

    state_key = f"{archive}::folder/readme.txt"
    assert idx.state_db.get_entry(state_key)["stage"] == "content"
    payloads = [point.payload for point in idx.qdrant.points]
    assert any(payload.get("path") == "docs.zip/folder/readme.txt" for payload in payloads)
    assert any(payload.get("archive_member") == "folder/readme.txt" for payload in payloads)


def test_dry_run_reports_work_without_writing_points_or_state(tmp_path: Path) -> None:
    doc = tmp_path / "new.txt"
    doc.write_text("new content", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="")
    idx.dry_run = True

    stats = idx.index_directory(stage="small")

    assert stats["dry_run_files"] == 1
    assert stats["processed_files"] == 1
    assert idx.qdrant.points == []
    assert idx.state_db.get_entry(str(doc)) is None


def test_exclude_patterns_skip_matching_files(tmp_path: Path) -> None:
    keep = tmp_path / "keep.txt"
    ignored_dir = tmp_path / "node_modules"
    ignored_dir.mkdir()
    ignored = ignored_dir / "skip.txt"
    keep.write_text("keep me", encoding="utf-8")
    ignored.write_text("skip me", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="")
    idx.exclude_patterns = ["**/node_modules/**"]

    idx.index_directory(stage="small")

    assert idx.state_db.get_entry(str(keep)) is not None
    assert idx.state_db.get_entry(str(ignored)) is None


def test_stage_runner_prioritizes_newer_files(tmp_path: Path) -> None:
    older = tmp_path / "older.txt"
    newer = tmp_path / "newer.txt"
    older.write_text("older", encoding="utf-8")
    newer.write_text("newer", encoding="utf-8")
    old_ts = time.time() - 3600
    new_ts = time.time()
    os.utime(older, (old_ts, old_ts))
    os.utime(newer, (new_ts, new_ts))
    idx = _make_indexer(tmp_path, extracted_text="")
    seen: list[str] = []

    def _track_text(path: Path) -> str:
        seen.append(path.name)
        return path.read_text(encoding="utf-8")

    idx._extract_text = _track_text

    idx.index_directory(stage="small")

    assert seen[:2] == ["newer.txt", "older.txt"]


def test_indexer_init_fails_after_catalog_wait_attempts(tmp_path: Path) -> None:
    missing = tmp_path / "missing"

    with pytest.raises(RuntimeError, match="Папка каталога недоступна"):
        RAGIndexer(
            catalog_path=str(missing),
            qdrant_db_path=str(tmp_path / "qdrant"),
            embedding_model="unused",
            collection_name="catalog",
            vector_size=3,
            chunk_size=500,
            chunk_overlap=100,
            batch_size=100,
            catalog_wait_attempts=0,
            catalog_wait_seconds=1,
        )


def test_chunk_text_has_overlap() -> None:
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.chunk_size = 5
    idx.chunk_overlap = 2
    out = idx._chunk_text("abcdefghij")
    assert out == ["abcde", "defgh", "ghij"]

