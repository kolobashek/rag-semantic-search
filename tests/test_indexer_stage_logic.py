from __future__ import annotations

import os
import tarfile
import time
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from index_rag import PAYLOAD_SCHEMA_VERSION, RAGIndexer
from rag_catalog.core.extractors import ExtractedDocument, TextBlock, document_from_legacy_text
from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.indexing import stage_runner
from rag_catalog.core.indexing.stage_runner import _normalize_only_path_key, _task_matches_only_paths
from rag_catalog.core.telemetry_db import TelemetryDB


class _FakeVec:
    def __init__(self, vals):
        self.vals = vals

    def tolist(self):
        return list(self.vals)


class _FakeEmbedder:
    def encode(self, chunks, normalize_embeddings=True, batch_size=256, show_progress_bar=False):
        return [_FakeVec([0.1, 0.2, 0.3]) for _ in chunks]


def test_only_path_filter_matches_windows_and_posix_forms() -> None:
    allowed = {_normalize_only_path_key(r"O:\Обмен\docs\a.pdf")}
    assert _task_matches_only_paths({"state_key": "O:/Обмен/docs/a.pdf"}, allowed)
    assert not _task_matches_only_paths({"state_key": r"O:\Обмен\docs\b.pdf"}, allowed)


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

    def find_by_content_hash(self, content_hash: str, *, exclude_path: str = ""):
        for key, row in self.entries.items():
            if key != exclude_path and row.get("content_hash") == content_hash:
                return dict(row)
        return None

    def list_entries_by_prefix(self, prefix: str):
        return sorted(key for key in self.entries if key.startswith(prefix))

    def delete_entries(self, paths):
        count = 0
        for path in paths:
            if self.entries.pop(str(path), None) is not None:
                count += 1
        return count


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
    idx.payload_schema_version = PAYLOAD_SCHEMA_VERSION
    idx.run_id = ""
    idx._run_deleted_files = 0
    idx.small_office_mb = 20.0
    idx.small_pdf_mb = 2.0
    idx.skip_ocr = False
    idx.synonym_map = {}
    idx.embedder = _FakeEmbedder()
    idx.qdrant = _FakeQdrant()
    idx._delete_file_vectors = lambda _p: None
    idx._cleanup_deleted_files = lambda _files: 0
    idx._extract_doc = lambda _p: extracted_text
    idx._extract_docx = lambda _p: extracted_text
    idx._extract_spreadsheet = lambda _p: extracted_text
    idx._extract_spreadsheet_document = lambda _p: document_from_legacy_text(extracted_text)
    idx._extract_rtf = lambda _p: extracted_text
    idx._extract_pptx = lambda _p: extracted_text
    idx._extract_pptx_document = lambda _p: document_from_legacy_text(extracted_text)
    idx._extract_pdf = lambda _p: extracted_text
    idx._extract_pdf_document = lambda _p: document_from_legacy_text(extracted_text)
    idx._extract_text = lambda p: p.read_text(encoding="utf-8")
    idx._extract_csv = lambda p: p.read_text(encoding="utf-8")
    idx._extract_html = lambda p: p.read_text(encoding="utf-8")
    return idx


def test_structured_chunking_coalesces_rows_and_drops_isolated_fragments(tmp_path: Path) -> None:
    idx = _make_indexer(tmp_path, extracted_text="")
    idx.min_chunk_chars = 120
    rows = tuple(
        TextBlock(text=f"Строка {number}: Спецмайнинг реквизиты и данные документа", sheet="Лист1", row_start=number, row_end=number)
        for number in range(1, 5)
    )
    document = ExtractedDocument(
        blocks=(
            TextBlock(text="смотренных.", page=1),
            TextBlock(text="Полный содержательный абзац " * 8, page=2),
            *rows,
        )
    )

    chunks = idx._chunk_text_with_provenance(document)

    assert all(len(item["text"]) >= 120 for item in chunks)
    assert all("смотренных" not in item["text"] for item in chunks)
    sheet_chunk = next(item for item in chunks if item["block"].sheet == "Лист1")
    assert sheet_chunk["block"].row_start == 1
    assert sheet_chunk["block"].row_end == 4


def test_short_whole_document_is_not_discarded(tmp_path: Path) -> None:
    idx = _make_indexer(tmp_path, extracted_text="")
    idx.min_chunk_chars = 120

    chunks = idx._chunk_text_with_provenance(ExtractedDocument(blocks=(TextBlock(text="Краткая записка"),)))

    assert [item["text"] for item in chunks] == ["Краткая записка"]


def test_small_stage_file_without_content_is_marked_empty_for_retry(tmp_path: Path) -> None:
    p = tmp_path / "a.docx"
    p.write_text("dummy", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="")
    stats = idx.index_directory(stage="small")
    key = str(p)
    row = idx.state_db.get_entry(key)
    assert row["stage"] == "empty"
    assert row["indexed_stage"] == "small"
    assert row["status"] == "empty"
    assert stats["processed_files"] >= 1


def test_metadata_stage_does_not_open_files_for_document_properties(tmp_path: Path, monkeypatch) -> None:
    p = tmp_path / "metadata-only.docx"
    p.write_text("not a real office document", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="must not be read")

    def _unexpected_extract(_path: Path):
        raise AssertionError("metadata stage must not read document properties")

    monkeypatch.setattr(stage_runner, "extract_doc_meta", _unexpected_extract)

    stats = idx.index_directory(stage="metadata")

    assert stats["error_files"] == 0
    assert idx.state_db.get_entry(str(p))["stage"] == "metadata"


def test_no_ocr_pdf_without_cached_text_is_deferred_and_not_retried_by_quick_pass(tmp_path: Path) -> None:
    p = tmp_path / "scan.pdf"
    p.write_bytes(b"%PDF-1.4\n")
    idx = _make_indexer(tmp_path, extracted_text="")
    idx.skip_ocr = True

    first = idx.index_directory(stage="small")
    key = str(p)
    row = idx.state_db.get_entry(key)
    assert row["stage"] == "metadata"
    assert row["indexed_stage"] == "small"
    assert row["status"] == "deferred_ocr"
    assert row["last_error"] == "deferred_ocr"
    assert first["processed_files"] == 1
    assert first["error_files"] == 0

    second = idx.index_directory(stage="small")
    assert second["processed_files"] == 1
    assert second["skipped_files"] == 1
    assert second["error_files"] == 0


def test_read_error_is_marked_error_and_backed_off(tmp_path: Path) -> None:
    p = tmp_path / "broken.docx"
    p.write_text("dummy", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="")

    def _raise(_path: Path) -> str:
        raise OSError("locked")

    idx._extract_docx = _raise

    stats = idx.index_directory(stage="small")

    key = str(p)
    row = idx.state_db.get_entry(key)
    assert row["stage"] == "error"
    assert row["indexed_stage"] == "small"
    assert row["status"] == "error"
    assert row["last_error"] == "locked"
    assert row["next_retry_at"] > time.time()
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
    row = idx.state_db.get_entry(key)
    assert row["stage"] == "content"
    assert row["indexed_stage"] == "small"
    assert row["status"] == "ok"


def test_small_stage_truncates_all_files_and_large_appends_remainder(tmp_path: Path) -> None:
    p = tmp_path / "long.docx"
    p.write_text("dummy", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text=" ".join(["word"] * 500))
    idx.max_chunks_per_file = 1
    deleted: list[Path] = []
    idx._delete_file_vectors = lambda path: deleted.append(path)

    idx.index_directory(stage="small")

    key = str(p)
    quick = idx.state_db.get_entry(key)
    assert quick["stage"] == "partial"
    assert quick["indexed_stage"] == "small"
    assert quick["indexed_chunks"] == 1
    assert quick["total_chunks"] > quick["indexed_chunks"]
    quick_points = len(idx.qdrant.points)

    idx.index_directory(stage="large")

    full = idx.state_db.get_entry(key)
    assert full["stage"] == "content"
    assert full["indexed_stage"] == "large"
    assert full["indexed_chunks"] == full["total_chunks"]
    assert deleted == []
    assert len(idx.qdrant.points) > quick_points


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


def test_zip_members_with_leading_slash_are_read_by_raw_archive_name(tmp_path: Path) -> None:
    archive = tmp_path / "backup.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as zf:
        zf.writestr("/folder/readme.txt", "zip text")
    idx = _make_indexer(tmp_path, extracted_text="")

    stats = idx.index_directory(stage="small")

    state_key = f"{archive}::folder/readme.txt"
    row = idx.state_db.get_entry(state_key)
    assert row["stage"] == "content"
    assert stats["error_files"] == 0
    payloads = [point.payload for point in idx.qdrant.points]
    assert any(payload.get("path") == "backup.zip/folder/readme.txt" for payload in payloads)
    assert any(payload.get("archive_member") == "/folder/readme.txt" for payload in payloads)
    assert any(payload.get("archive_member_display") == "folder/readme.txt" for payload in payloads)


def test_tar_gz_members_are_indexed_with_logical_archive_paths(tmp_path: Path) -> None:
    archive = tmp_path / "docs.tar.gz"
    source = tmp_path / "source.txt"
    source.write_text("tar text", encoding="utf-8")
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(source, arcname="folder/readme.txt")
    source.unlink()
    idx = _make_indexer(tmp_path, extracted_text="")

    stats = idx.index_directory(stage="small")

    state_key = f"{archive}::folder/readme.txt"
    row = idx.state_db.get_entry(state_key)
    assert row["stage"] == "content"
    assert stats["error_files"] == 0
    payloads = [point.payload for point in idx.qdrant.points]
    assert any(payload.get("path") == "docs.tar.gz/folder/readme.txt" for payload in payloads)
    assert any(payload.get("archive_member") == "folder/readme.txt" for payload in payloads)


def test_7z_members_are_indexed_with_logical_archive_paths(tmp_path: Path) -> None:
    py7zr = pytest.importorskip("py7zr")
    archive = tmp_path / "docs.7z"
    source = tmp_path / "source.txt"
    source.write_text("seven zip text", encoding="utf-8")
    with py7zr.SevenZipFile(archive, "w") as zf:
        zf.write(source, "folder/readme.txt")
    source.unlink()
    idx = _make_indexer(tmp_path, extracted_text="")

    stats = idx.index_directory(stage="small")

    state_key = f"{archive}::folder/readme.txt"
    row = idx.state_db.get_entry(state_key)
    assert row["stage"] == "content"
    assert stats["error_files"] == 0
    payloads = [point.payload for point in idx.qdrant.points]
    assert any(payload.get("path") == "docs.7z/folder/readme.txt" for payload in payloads)
    assert any(payload.get("archive_member") == "folder/readme.txt" for payload in payloads)


def test_rar_members_are_indexed_through_7z_fallback(monkeypatch, tmp_path: Path) -> None:
    archive = tmp_path / "docs.rar"
    archive.write_bytes(b"fake rar payload")
    idx = _make_indexer(tmp_path, extracted_text="")

    def fake_which(name: str) -> str | None:
        return "C:/Tools/7z.exe" if name == "7z" else None

    def fake_run(cmd, **kwargs):  # noqa: ANN001
        if cmd[1:3] == ["l", "-slt"]:
            stdout = "\n".join(
                [
                    "Listing archive: docs.rar",
                    "----------",
                    "Path = folder/readme.txt",
                    "Size = 8",
                    "Attributes = A",
                    "CRC = 1234ABCD",
                    "",
                ]
            )
            return SimpleNamespace(returncode=0, stdout=stdout, stderr="")
        if cmd[1:4] == ["x", "-so", "-y"]:
            return SimpleNamespace(returncode=0, stdout=b"rar text", stderr=b"")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(stage_runner.shutil, "which", fake_which)
    monkeypatch.setattr(stage_runner.subprocess, "run", fake_run)

    stats = idx.index_directory(stage="small")

    state_key = f"{archive}::folder/readme.txt"
    row = idx.state_db.get_entry(state_key)
    assert row["stage"] == "content"
    assert stats["error_files"] == 0
    payloads = [point.payload for point in idx.qdrant.points]
    assert any(payload.get("path") == "docs.rar/folder/readme.txt" for payload in payloads)
    assert any(payload.get("archive_member") == "folder/readme.txt" for payload in payloads)


def test_zip_member_cleanup_removes_stale_archive_entries(tmp_path: Path) -> None:
    archive = tmp_path / "docs.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as zf:
        zf.writestr("folder/readme.txt", "zip text")
    idx = _make_indexer(tmp_path, extracted_text="")
    stale_key = f"{archive}::old.txt"
    idx.state_db.upsert_many(
        [
            {
                "full_path": stale_key,
                "fingerprint": "old",
                "mtime": 1.0,
                "stage": "content",
                "size_bytes": 1,
                "extension": ".txt",
            }
        ]
    )
    deleted: list[Path] = []
    idx._delete_file_vectors = lambda path: deleted.append(path)

    idx.index_directory(stage="small")

    assert idx.state_db.get_entry(stale_key) is None
    assert deleted == [Path(stale_key)]


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


def test_duplicate_content_is_marked_in_payload(tmp_path: Path) -> None:
    first = tmp_path / "a.txt"
    second = tmp_path / "b.txt"
    first.write_text("same text", encoding="utf-8")
    second.write_text("same text", encoding="utf-8")
    now = time.time()
    os.utime(first, (now, now))
    os.utime(second, (now - 10, now - 10))
    idx = _make_indexer(tmp_path, extracted_text="")

    idx.index_directory(stage="small")

    payloads = [point.payload for point in idx.qdrant.points if point.payload.get("filename") == "b.txt"]
    assert any(payload.get("is_duplicate") is True for payload in payloads)
    assert idx.state_db.get_entry(str(second))["content_hash"] == idx.state_db.get_entry(str(first))["content_hash"]


def test_payload_schema_version_is_written_to_payloads(tmp_path: Path) -> None:
    doc = tmp_path / "versioned.txt"
    doc.write_text("versioned text", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="")

    idx.index_directory(stage="small")

    assert idx.qdrant.points
    assert {point.payload.get("payload_schema_version") for point in idx.qdrant.points} == {PAYLOAD_SCHEMA_VERSION}


def test_payload_schema_change_marks_existing_state_for_reindex(tmp_path: Path) -> None:
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.recreate = False
    idx.payload_schema_version = PAYLOAD_SCHEMA_VERSION
    idx.state_db = IndexStateDB(str(tmp_path / "index_state.db"))
    idx.state_db.upsert_many(
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
    idx.state_db.set_config_many({"payload_schema_version": str(PAYLOAD_SCHEMA_VERSION - 1)})

    changed = idx._ensure_payload_schema_version()

    row = idx.state_db.get_entry("a.txt")
    assert changed == 1
    assert row["stage"] == "metadata"
    assert idx.state_db.get_config()["payload_schema_version"] == str(PAYLOAD_SCHEMA_VERSION)


def test_quality_report_summarizes_state_and_duplicates(tmp_path: Path) -> None:
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.state_db = IndexStateDB(str(tmp_path / "index_state.db"))
    idx.telemetry = TelemetryDB(str(tmp_path / "telemetry.db"))
    idx.state_db.upsert_many(
        [
            {"full_path": "a.txt", "fingerprint": "1", "mtime": 1.0, "stage": "content", "size_bytes": 1, "extension": ".txt", "content_hash": "same"},
            {"full_path": "b.txt", "fingerprint": "2", "mtime": 2.0, "stage": "content", "size_bytes": 1, "extension": ".txt", "content_hash": "same"},
            {"full_path": "c.pdf", "fingerprint": "3", "mtime": 3.0, "stage": "error", "size_bytes": 1, "extension": ".pdf"},
        ]
    )
    idx.state_db.record_failed_path("c.pdf", error="locked")

    report = idx.quality_report()

    assert report["total_files"] == 3
    assert report["content_coverage_pct"] == 66.67
    assert report["error_files"] == 1
    assert report["status_distribution"]["error"] == 1
    assert report["indexed_stage_distribution"]["content"] == 2
    assert report["failed_paths"] == 1
    assert report["duplicate_groups"] == 1


def test_process_index_queue_once_completes_existing_file(tmp_path: Path) -> None:
    doc = tmp_path / "queued.txt"
    doc.write_text("queued", encoding="utf-8")
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.state_db = IndexStateDB(str(tmp_path / "index_state.db"))
    idx.read_workers = 1
    idx._is_excluded_path = lambda _p: False
    seen: list[Path] = []
    idx.process_file = lambda path: seen.append(path)

    task = idx.state_db.enqueue_index_task(str(doc), stage="small", reason="watch")
    stats = idx.process_index_queue_once(limit=1)

    assert stats["leased"] == 1
    assert stats["completed"] == 1
    assert seen == [doc]
    assert idx.state_db.queue_stats() == {}
    assert idx.state_db.complete_index_task(int(task["id"])) == 0


def test_process_index_queue_once_deletes_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.txt"
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.state_db = IndexStateDB(str(tmp_path / "index_state.db"))
    idx.read_workers = 1
    idx._delete_file_vectors = lambda _p: None
    idx._is_excluded_path = lambda _p: False

    idx.state_db.upsert_many(
        [{"full_path": str(missing), "fingerprint": "1", "mtime": 1.0, "stage": "content", "size_bytes": 1, "extension": ".txt"}]
    )
    idx.state_db.enqueue_index_task(str(missing), stage="small", reason="deleted")

    stats = idx.process_index_queue_once(limit=1)

    assert stats["missing"] == 1
    assert idx.state_db.get_entry(str(missing)) is None
    assert idx.state_db.queue_stats() == {}


def test_drain_index_queue_processes_bounded_batches(tmp_path: Path) -> None:
    docs = []
    for name in ("a.txt", "b.txt", "c.txt"):
        doc = tmp_path / name
        doc.write_text(name, encoding="utf-8")
        docs.append(doc)
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.state_db = IndexStateDB(str(tmp_path / "index_state.db"))
    idx.read_workers = 1
    idx._is_excluded_path = lambda _p: False
    seen: list[Path] = []
    idx.process_file = lambda path: seen.append(path)
    for doc in docs:
        idx.state_db.enqueue_index_task(str(doc), stage="small", reason="watch")

    stats = idx.drain_index_queue(limit=1, max_batches=2)

    assert stats["leased"] == 2
    assert len(seen) == 2
    assert idx.state_db.queue_stats() == {"pending": 1}


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

