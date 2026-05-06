from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from qdrant_client.models import Filter

from rag_core import MAX_QUERY_LEN, RAGSearcher
from rag_catalog.core.index_state_db import IndexStateDB


class _FakeTelemetry:
    def __init__(self) -> None:
        self.search_calls = []
        self.fact_calls = []

    def log_search(self, **kwargs) -> None:
        self.search_calls.append(kwargs)

    def log_fact(self, **kwargs) -> None:
        self.fact_calls.append(kwargs)

    def get_search_feedback_scores(self, *, query: str, paths: list[str]) -> dict[str, int]:
        return {}


class _FakeEmbedder:
    def __init__(self, mode: str = "ok") -> None:
        self.mode = mode

    def encode(self, query, normalize_embeddings=True):
        if self.mode == "raise":
            raise RuntimeError("embed failed")
        return SimpleNamespace(tolist=lambda: [0.1, 0.2, 0.3])


class _FakeQdrant:
    def __init__(self, mode: str = "ok") -> None:
        self.mode = mode
        self.last_kwargs = {}

    def query_points(self, **kwargs):
        self.last_kwargs = kwargs
        if self.mode == "raise":
            raise RuntimeError("qdrant failed")
        return SimpleNamespace(points=[])


def _make_searcher(*, connected: bool, embed_mode: str = "ok", qdrant_mode: str = "ok") -> RAGSearcher:
    s = RAGSearcher.__new__(RAGSearcher)
    s.connected = connected
    s.collection_name = "catalog"
    s.config = {}
    s.telemetry = _FakeTelemetry()
    s._embedder = _FakeEmbedder(mode=embed_mode)
    s.qdrant = _FakeQdrant(mode=qdrant_mode)
    return s


def test_search_not_connected_raises_connection_error() -> None:
    s = _make_searcher(connected=False)
    with pytest.raises(ConnectionError):
        s.search("abc", source="test")
    assert s.telemetry.search_calls[-1]["error"] == "not_connected"


def test_search_embed_error_raises_runtime_error() -> None:
    s = _make_searcher(connected=True, embed_mode="raise")
    with pytest.raises(RuntimeError):
        s.search("abc", source="test")
    assert "embed_error" in s.telemetry.search_calls[-1]["error"]


def test_search_qdrant_error_raises_runtime_error() -> None:
    s = _make_searcher(connected=True, qdrant_mode="raise")
    with pytest.raises(RuntimeError):
        s.search("abc", source="test")
    assert "qdrant_error" in s.telemetry.search_calls[-1]["error"]


def test_search_no_results_is_valid_empty_list_and_truncates_query() -> None:
    s = _make_searcher(connected=True)
    long_query = "x" * (MAX_QUERY_LEN + 50)
    out = s.search(long_query, source="test")
    assert out == []
    call = s.telemetry.search_calls[-1]
    assert call["query"] == long_query
    assert len(call["query_used"]) == MAX_QUERY_LEN
    assert "truncated_from=" in call["error"]


def test_search_sets_content_only_must_not_filter() -> None:
    s = _make_searcher(connected=True)
    s.search("abc", content_only=True, source="test")
    qf = s.qdrant.last_kwargs["query_filter"]
    assert isinstance(qf, Filter)
    assert qf.must_not and len(qf.must_not) == 1
    assert qf.must_not[0].key == "type"
    assert qf.must_not[0].match.value == "file_metadata"


def test_search_title_only_disables_content_only_filter_and_logs_original_query() -> None:
    s = _make_searcher(connected=True)
    s.search("expanded query", title_only=True, content_only=True, query_original="typed query", source="test")
    qf = s.qdrant.last_kwargs["query_filter"]
    assert isinstance(qf, Filter)
    assert not qf.must_not
    assert qf.should
    assert qf.should[0].key == "type"
    assert set(qf.should[0].match.any) == {"file_metadata", "folder_metadata"}
    call = s.telemetry.search_calls[-1]
    assert call["query"] == "typed query"
    assert call["query_original"] == "typed query"
    assert call["query_used"] == "expanded query"


def test_search_title_only_returns_only_metadata_points() -> None:
    s = _make_searcher(connected=True)

    s.qdrant.query_points = lambda **kwargs: SimpleNamespace(points=[  # type: ignore[method-assign]
        SimpleNamespace(
            score=0.91,
            payload={
                "type": "docx_content",
                "filename": "chunk.docx",
                "path": "p/chunk.docx",
                "full_path": r"O:\p\chunk.docx",
                "extension": ".docx",
            },
        ),
        SimpleNamespace(
            score=0.88,
            payload={
                "type": "file_metadata",
                "filename": "meta.docx",
                "path": "p/meta.docx",
                "full_path": r"O:\p\meta.docx",
                "extension": ".docx",
            },
        ),
    ])
    s._lexical_catalog_search = lambda **kwargs: []  # type: ignore[method-assign]

    out = s.search("abc", title_only=True, source="test")

    assert [item["type"] for item in out] == ["file_metadata"]


def test_refresh_fs_cache_uses_state_db_with_ancestor_and_empty_folders(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog"
    qdrant = tmp_path / "qdrant"
    file_path = catalog / "a" / "b" / "doc.pdf"
    empty_dir = catalog / "empty"
    file_path.parent.mkdir(parents=True)
    empty_dir.mkdir()
    file_path.write_text("x", encoding="utf-8")
    state_db = IndexStateDB(str(qdrant / "index_state.db"))
    state_db.upsert_many(
        [
            {
                "full_path": str(file_path),
                "fingerprint": "1_1",
                "mtime": 1.0,
                "stage": "content",
                "size_bytes": 1,
                "extension": ".pdf",
            }
        ]
    )
    s = _make_searcher(connected=True)
    s.config = {"catalog_path": str(catalog), "qdrant_db_path": str(qdrant)}

    rows = s._refresh_fs_cache()
    folders = {item["path"] for item in rows if item["kind"] == "folder"}

    assert "a" in folders
    assert str(Path("a") / "b") in {Path(path).as_posix().replace("/", "\\") for path in folders}
    assert "empty" in folders


def test_merge_ranked_results_applies_feedback_signal() -> None:
    s = _make_searcher(connected=True)

    def scores(*, query: str, paths: list[str]) -> dict[str, int]:
        return {r"O:\low.pdf": 3, r"O:\high.pdf": -3}

    s.telemetry.get_search_feedback_scores = scores  # type: ignore[method-assign]
    out = s._merge_ranked_results(
        [
            {"filename": "high.pdf", "full_path": r"O:\high.pdf", "score": 0.90, "type": "file_metadata"},
            {"filename": "low.pdf", "full_path": r"O:\low.pdf", "score": 0.86, "type": "file_metadata"},
        ],
        [],
        limit=2,
        query="паспорт",
    )
    assert out[0]["filename"] == "low.pdf"
    assert out[0]["feedback_score"] == 3


def test_merge_ranked_results_applies_recency_boost() -> None:
    s = _make_searcher(connected=True)
    s.config = {
        "rank_recency_enabled": True,
        "rank_recency_half_life_days": 180.0,
        "rank_recency_max_boost": 0.03,
        "rank_feedback_step": 0.02,
        "rank_feedback_cap": 0.18,
    }
    now = datetime.now(timezone.utc)
    old_ts = (now - timedelta(days=365 * 2)).isoformat()
    fresh_ts = (now - timedelta(days=2)).isoformat()
    out = s._merge_ranked_results(
        [
            {
                "filename": "old.pdf",
                "full_path": r"O:\old.pdf",
                "score": 0.90,
                "type": "file_metadata",
                "modified": old_ts,
            },
            {
                "filename": "fresh.pdf",
                "full_path": r"O:\fresh.pdf",
                "score": 0.90,
                "type": "file_metadata",
                "modified": fresh_ts,
            },
        ],
        [],
        limit=2,
        query="договор",
    )
    assert out[0]["filename"] == "fresh.pdf"
    assert float(out[0]["rank_score"]) > float(out[1]["rank_score"])


def test_answer_fact_question_handles_search_error() -> None:
    s = _make_searcher(connected=True)

    def _boom(*args, **kwargs):
        raise RuntimeError("search boom")

    s.search = _boom  # type: ignore[method-assign]
    out = s.answer_fact_question("сколько весит")
    assert out["ok"] is False
    assert "Ошибка поиска" in out["error"]
    assert s.telemetry.fact_calls[-1]["error"].startswith("fact_search_error")
