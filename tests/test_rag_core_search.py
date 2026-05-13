from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from qdrant_client.models import Filter

from rag_catalog.core.index_state_db import IndexStateDB
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
        self.last_query = ""

    def encode(self, query, normalize_embeddings=True):
        self.last_query = query
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


class _FakeReranker:
    def predict(self, pairs):
        return [10.0 if "target" in text else 1.0 for _query, text in pairs]


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


def test_search_can_expand_query_in_core_pipeline(monkeypatch) -> None:
    from rag_catalog.core import llm

    s = _make_searcher(connected=True)
    s.config = {
        "llm_enabled": True,
        "llm_search_expand_enabled": True,
        "llm_expand_model": "fake-model",
        "ollama_url": "http://ollama.test",
    }
    monkeypatch.setattr(llm, "expand_query", lambda query, **kwargs: f"{query} расширенный")
    s.qdrant.query_points = lambda **kwargs: SimpleNamespace(points=[])  # type: ignore[method-assign]
    s._lexical_catalog_search = lambda **kwargs: []  # type: ignore[method-assign]

    s.search("паспорт", source="test")

    assert s._embedder.last_query == "паспорт расширенный"
    assert s.telemetry.search_calls[-1]["query_original"] == "паспорт"
    assert s.telemetry.search_calls[-1]["query_used"] == "паспорт расширенный"


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


def test_search_retrieval_v2_uses_rrf_fusion() -> None:
    s = _make_searcher(connected=True)
    s.config = {"retrieval_pipeline": "v2"}
    s.qdrant.query_points = lambda **kwargs: SimpleNamespace(points=[  # type: ignore[method-assign]
        SimpleNamespace(
            score=0.99,
            payload={
                "type": "file_metadata",
                "filename": "semantic.docx",
                "path": "semantic.docx",
                "full_path": r"O:\semantic.docx",
            },
        ),
        SimpleNamespace(
            score=0.70,
            payload={
                "type": "file_metadata",
                "filename": "both.docx",
                "path": "both.docx",
                "full_path": r"O:\both.docx",
            },
        ),
    ])
    s._lexical_catalog_search = lambda **kwargs: [  # type: ignore[method-assign]
        {"type": "file_metadata", "filename": "both.docx", "path": "both.docx", "full_path": r"O:\both.docx", "score": 0.80},
        {"type": "file_metadata", "filename": "lexical.docx", "path": "lexical.docx", "full_path": r"O:\lexical.docx", "score": 0.75},
    ]

    out = s.search("both", limit=3, source="test")

    assert out[0]["filename"] == "both.docx"
    assert out[0]["fusion"] == "rrf"


def test_bm25_catalog_search_returns_metadata_channel(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    exact = catalog / "Карточка предприятия ООО ТСК.docx"
    other = catalog / "Акт сверки.docx"
    exact.write_text("x", encoding="utf-8")
    other.write_text("x", encoding="utf-8")

    s = _make_searcher(connected=True)
    s.config = {"catalog_path": str(catalog), "retrieval_bm25_enabled": True}

    out = s._bm25_catalog_search(
        query="карточка предприятия тск",
        limit=5,
        file_type=None,
        content_only=False,
    )

    assert out[0]["filename"] == exact.name
    assert out[0]["type"] == "file_metadata"
    assert out[0]["retrieval_source"] == "bm25"


def test_rerank_results_reorders_top_candidates_with_cross_encoder_scores() -> None:
    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_reranker_enabled": True,
        "retrieval_reranker_model": "fake",
        "retrieval_reranker_weight": 1.0,
        "retrieval_reranker_top_n": 3,
    }
    s._reranker = _FakeReranker()
    results = [
        {"filename": "first.docx", "text": "generic", "score": 0.99, "rank_score": 0.99},
        {"filename": "target.docx", "text": "target answer", "score": 0.50, "rank_score": 0.50},
    ]

    out = s._rerank_results("target", results, limit=2)

    assert out[0]["filename"] == "target.docx"
    assert out[0]["retrieval_reranked"] is True
    assert out[0]["reranker_score"] == 10.0


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


def test_merge_ranked_results_limits_chunks_per_document() -> None:
    s = _make_searcher(connected=True)
    s.config = {"rank_max_chunks_per_document": 2}
    same_doc = [
        {
            "type": "content",
            "filename": "same.docx",
            "full_path": r"O:\same.docx",
            "path": "same.docx",
            "chunk_index": idx,
            "score": 0.99 - idx * 0.01,
        }
        for idx in range(4)
    ]
    other_doc = {
        "type": "content",
        "filename": "other.docx",
        "full_path": r"O:\other.docx",
        "path": "other.docx",
        "chunk_index": 0,
        "score": 0.50,
    }

    out = s._merge_ranked_results([], [*same_doc, other_doc], limit=3, query="same")

    assert [item["filename"] for item in out].count("same.docx") == 2
    assert any(item["filename"] == "other.docx" for item in out)


def test_answer_documents_returns_no_answer_without_text_sources() -> None:
    s = _make_searcher(connected=True)
    s.search = lambda *args, **kwargs: [  # type: ignore[method-assign]
        {"filename": "meta.docx", "text": "", "full_path": r"O:\meta.docx"}
    ]

    out = s.answer_documents("что написано?")

    assert out["ok"] is False
    assert out["error"] == "no_text_sources"
    assert "не нашёл" in out["answer"]


def test_answer_documents_generates_answer_with_sources(monkeypatch) -> None:
    from rag_catalog.core import llm

    s = _make_searcher(connected=True)
    s.config = {"llm_rag_model": "fake", "ollama_url": "http://ollama.test", "llm_answer_top_k": 2}
    s.search = lambda *args, **kwargs: [  # type: ignore[method-assign]
        {
            "filename": "source.docx",
            "path": "source.docx",
            "full_path": r"O:\source.docx",
            "text": "Подтвержденный фрагмент документа",
            "score": 0.91,
            "chunk_index": 0,
            "doc_id": "file:source",
            "parent_id": "file:source:chunk-group:0",
            "page": 3,
            "section": "1. Условия",
        }
    ]
    monkeypatch.setattr(llm, "rag_answer", lambda query, results, **kwargs: "Ответ на основе источника")

    out = s.answer_documents("что известно?")

    assert out["ok"] is True
    assert out["answer"] == "Ответ на основе источника"
    assert out["sources"][0]["filename"] == "source.docx"
    assert out["sources"][0]["excerpt"].startswith("Подтвержденный")
    assert out["sources"][0]["doc_id"] == "file:source"
    assert out["sources"][0]["page"] == 3
    assert out["sources"][0]["section"] == "1. Условия"


def test_answer_documents_rejects_unsupported_numeric_facts(monkeypatch) -> None:
    from rag_catalog.core import llm

    s = _make_searcher(connected=True)
    s.config = {"llm_rag_model": "fake", "ollama_url": "http://ollama.test", "llm_answer_top_k": 1}
    s.search = lambda *args, **kwargs: [  # type: ignore[method-assign]
        {
            "filename": "source.docx",
            "full_path": r"O:\source.docx",
            "text": "В источнике указана сумма 100 рублей.",
            "score": 0.91,
        }
    ]
    monkeypatch.setattr(llm, "rag_answer", lambda query, results, **kwargs: "Сумма составляет 999 рублей.")

    out = s.answer_documents("какая сумма?")

    assert out["ok"] is False
    assert out["error"] == "unsupported_facts"
    assert "999" in out["verification"]["missing_facts"]


def test_answer_fact_question_handles_search_error() -> None:
    s = _make_searcher(connected=True)

    def _boom(*args, **kwargs):
        raise RuntimeError("search boom")

    s.search = _boom  # type: ignore[method-assign]
    out = s.answer_fact_question("сколько весит")
    assert out["ok"] is False
    assert "Ошибка поиска" in out["error"]
    assert s.telemetry.fact_calls[-1]["error"].startswith("fact_search_error")
