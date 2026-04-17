from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

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

    def upsert(self, collection_name, points):
        self.points_count += len(points)

    def get_collection(self, collection_name):
        return SimpleNamespace(points_count=self.points_count)


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
    idx.state = {"files": {}}
    idx._points_buffer = []
    idx.point_count = 0
    idx.run_id = ""
    idx._run_deleted_files = 0
    idx.small_office_mb = 20.0
    idx.small_pdf_mb = 2.0
    idx.embedder = _FakeEmbedder()
    idx.qdrant = _FakeQdrant()
    idx._save_state = lambda: None
    idx._delete_file_vectors = lambda _p: None
    idx._cleanup_deleted_files = lambda _files: 0
    idx._extract_docx = lambda _p: extracted_text
    idx._extract_spreadsheet = lambda _p: extracted_text
    idx._extract_pdf = lambda _p: extracted_text
    return idx


def test_small_stage_file_without_content_stays_metadata(tmp_path: Path) -> None:
    p = tmp_path / "a.docx"
    p.write_text("dummy", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="")
    stats = idx.index_directory(stage="small")
    key = str(p)
    assert idx.state["files"][key]["stage"] == "metadata"
    assert stats["processed_files"] >= 1


def test_small_stage_file_with_content_becomes_content(tmp_path: Path) -> None:
    p = tmp_path / "b.docx"
    p.write_text("dummy", encoding="utf-8")
    idx = _make_indexer(tmp_path, extracted_text="hello world")
    idx.index_directory(stage="small")
    key = str(p)
    assert idx.state["files"][key]["stage"] == "content"


def test_chunk_text_has_overlap() -> None:
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.chunk_size = 5
    idx.chunk_overlap = 2
    out = idx._chunk_text("abcdefghij")
    assert out == ["abcde", "defgh", "ghij"]

