from __future__ import annotations

from types import SimpleNamespace

import pytest
from qdrant_client.models import Filter

from rag_core import MAX_QUERY_LEN, RAGSearcher


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
    assert len(call["query"]) == MAX_QUERY_LEN
    assert "truncated_from=" in call["error"]


def test_search_sets_content_only_must_not_filter() -> None:
    s = _make_searcher(connected=True)
    s.search("abc", content_only=True, source="test")
    qf = s.qdrant.last_kwargs["query_filter"]
    assert isinstance(qf, Filter)
    assert qf.must_not and len(qf.must_not) == 1
    assert qf.must_not[0].key == "type"
    assert qf.must_not[0].match.value == "file_metadata"


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


def test_answer_fact_question_handles_search_error() -> None:
    s = _make_searcher(connected=True)

    def _boom(*args, **kwargs):
        raise RuntimeError("search boom")

    s.search = _boom  # type: ignore[method-assign]
    out = s.answer_fact_question("сколько весит")
    assert out["ok"] is False
    assert "Ошибка поиска" in out["error"]
    assert s.telemetry.fact_calls[-1]["error"].startswith("fact_search_error")
