from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from qdrant_client.models import Filter

from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.rag_core import _payload_index_type, apply_retrieval_preset
from rag_catalog.core.retrieval import prepare_passage_text, prepare_query_text
from rag_core import MAX_QUERY_LEN, RAGSearcher


def test_apply_retrieval_release_preset_preserves_explicit_overrides() -> None:
    cfg = apply_retrieval_preset(
        {
            "retrieval_preset": "release_v2",
            "retrieval_final_top_k": 25,
            "retrieval_reranker_enabled": True,
        },
        {"retrieval_preset", "retrieval_final_top_k", "retrieval_reranker_enabled"},
    )

    assert cfg["retrieval_pipeline"] == "v2"
    assert cfg["retrieval_bm25_enabled"] is True
    assert cfg["retrieval_final_top_k"] == 25
    assert cfg["retrieval_reranker_enabled"] is True


def test_multilingual_e5_inputs_use_asymmetric_prefixes() -> None:
    model = "intfloat/multilingual-e5-small"

    assert prepare_query_text(model, "спецмайнинг") == "query: спецмайнинг"
    assert prepare_passage_text(model, "устав компании") == "passage: устав компании"
    assert prepare_query_text("sentence-transformers/all-MiniLM-L6-v2", "query") == "query"


def test_fulltext_runtime_requires_text_payload_index_type() -> None:
    assert _payload_index_type({"text": {"data_type": "text"}}, "text") == "text"
    assert _payload_index_type({"text": {"data_type": "keyword"}}, "text") == "keyword"
    assert _payload_index_type({"type": {"data_type": "keyword"}}, "text") == ""


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


class _FakeQdrantScroll:
    def __init__(self, payloads):
        self.payloads = payloads
        self.last_kwargs = {}

    def scroll(self, **kwargs):
        self.last_kwargs = kwargs
        return [SimpleNamespace(payload=payload) for payload in self.payloads], None


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
    assert call["details"] == {
        "channels": {
            "dense": 0,
            "numeric_exact": 0,
            "lexical": 0,
            "fulltext": 0,
            "merged": 0,
        },
        "relevance_gate": {
            "enabled": False,
            "input_count": 0,
            "output_count": 0,
            "rejected_count": 0,
            "rejected_by_reason": {},
        },
    }


def test_search_does_not_run_spreadsheet_numeric_scan_for_alphanumeric_model(monkeypatch) -> None:
    s = _make_searcher(connected=True)
    called = False

    def fake_scan(**_kwargs):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(s, "_spreadsheet_numeric_exact_scan", fake_scan)

    s.search("паспорт PC300", source="test")

    assert called is False


def test_search_does_not_scan_source_spreadsheets_for_numeric_query(monkeypatch) -> None:
    s = _make_searcher(connected=True)

    def fail_scan(**_kwargs):
        raise AssertionError("source spreadsheet scan must not run in the request path")

    monkeypatch.setattr(s, "_spreadsheet_numeric_exact_scan", fail_scan)

    assert s.search("СТС 9941 210904", source="test") == []


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


def test_search_uses_original_query_for_lexical_channel() -> None:
    s = _make_searcher(connected=True)
    s._search_alias_expansion = lambda query: {"aliases": ["alias-added"], "groups": []}  # type: ignore[method-assign]
    s.qdrant.query_points = lambda **kwargs: SimpleNamespace(points=[])  # type: ignore[method-assign]
    captured = {}

    def lexical(**kwargs):
        captured.update(kwargs)
        return []

    s._lexical_catalog_search = lexical  # type: ignore[method-assign]

    s.search("Налоговая Дзержинка", source="test")

    assert s._embedder.last_query == "Налоговая Дзержинка alias-added"
    assert captured["query"] == "Налоговая Дзержинка"


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


def test_fulltext_content_channel_returns_exact_russian_match() -> None:
    s = _make_searcher(connected=True)
    s.config = {"retrieval_fulltext_enabled": True}
    s._fulltext_available = True
    s.qdrant = _FakeQdrantScroll(
        [
            {
                "type": "pdf_content",
                "filename": "Устав.pdf",
                "path": "Компании/Устав.pdf",
                "full_path": r"O:\Компании\Устав.pdf",
                "extension": ".pdf",
                "text": "Устав общества Спецмайнинг утвержден решением участников",
            },
            {
                "type": "pdf_content",
                "filename": "Случайный.pdf",
                "path": "Архив/Случайный.pdf",
                "full_path": r"O:\Архив\Случайный.pdf",
                "extension": ".pdf",
                "text": "Устав другого общества без искомой организации",
            },
        ]
    )

    out = s._fulltext_content_search(
        query="спецмайнинг устав",
        limit=10,
        file_type=None,
        content_only=False,
    )

    assert [item["filename"] for item in out] == ["Устав.pdf"]
    assert out[0]["retrieval_source"] == "fulltext"
    assert out[0]["fulltext_matched_terms"] == 2


def test_relevance_gate_rejects_partial_fulltext_evidence() -> None:
    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_relevance_gate_enabled": True,
        "retrieval_min_dense_score": 0.84,
        "retrieval_min_content_chars": 120,
    }
    partial = {
        "type": "pdf_content",
        "filename": "partial.pdf",
        "text": "Устав общества без искомой организации. " * 5,
        "score": 0.96,
        "retrieval_source": "fulltext",
        "fulltext_matched_terms": 1,
        "fulltext_query_terms": 2,
    }

    assert s._apply_relevance_gate("спецмайнинг устав", [partial]) == []


def test_relevance_gate_rejects_weak_dense_noise_and_microchunks() -> None:
    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_relevance_gate_enabled": True,
        "retrieval_min_dense_score": 0.78,
        "retrieval_single_term_min_dense_score": 0.80,
        "retrieval_min_content_chars": 120,
    }
    noise = {
        "type": "pdf_content",
        "filename": "random.pdf",
        "text": "смотренных.",
        "score": 0.749,
        "dense_score": 0.749,
        "retrieval_source": "dense",
    }
    exact = {
        "type": "pdf_content",
        "filename": "Устав.pdf",
        "text": "Спецмайнинг " * 20,
        "score": 0.99,
        "retrieval_source": "fulltext",
        "fulltext_matched_terms": 1,
        "fulltext_query_terms": 1,
    }

    diagnostics = {}
    assert s._apply_relevance_gate("спецмайнинг", [noise, exact], diagnostics=diagnostics) == [
        {**exact, "relevance_evidence": "lexical", "relevance_floor": 0.8}
    ]
    assert diagnostics["relevance_gate"] == {
        "enabled": True,
        "input_count": 2,
        "output_count": 1,
        "rejected_count": 1,
        "rejected_by_reason": {"short_content": 1},
        "dense_floor": 0.8,
        "min_content_chars": 120,
        "reranker_floor": -4.0,
        "machine_document_intent": False,
    }


def test_relevance_gate_accepts_weak_dense_candidate_confirmed_by_reranker() -> None:
    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_relevance_gate_enabled": True,
        "retrieval_min_dense_score": 0.84,
        "retrieval_single_term_min_dense_score": 0.86,
        "retrieval_min_content_chars": 120,
        "retrieval_reranker_min_score": -2.0,
    }
    candidate = {
        "type": "pdf_content",
        "filename": "relevant.pdf",
        "text": "Содержательно релевантный фрагмент документа. " * 5,
        "score": 0.81,
        "dense_score": 0.81,
        "retrieval_source": "dense",
        "reranker_score": 1.25,
    }

    assert s._apply_relevance_gate("запрос", [candidate]) == [
        {**candidate, "relevance_evidence": "reranker", "relevance_floor": 0.86}
    ]


def test_relevance_gate_uses_original_terms_instead_of_alias_expansion() -> None:
    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_relevance_gate_enabled": True,
        "retrieval_min_dense_score": 0.78,
        "retrieval_single_term_min_dense_score": 0.80,
        "retrieval_min_content_chars": 120,
    }
    exact = {
        "type": "file_metadata",
        "filename": "Карточка предприятия ТСК.doc",
        "text": "Файл: Карточка предприятия ТСК.doc",
        "score": 1.0,
        "retrieval_source": "lexical",
        "lexical_matched_terms": 3,
        "lexical_query_terms": 10,
        "lexical_raw_matched_terms": 3,
        "lexical_raw_query_terms": 3,
    }

    assert s._apply_relevance_gate("карточка предприятия тск", [exact]) == [
        {**exact, "relevance_evidence": "lexical", "relevance_floor": 0.78}
    ]


def test_relevance_gate_requires_text_evidence_for_mixed_numeric_query() -> None:
    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_relevance_gate_enabled": True,
        "retrieval_min_dense_score": 0.84,
        "retrieval_single_term_min_dense_score": 0.86,
        "retrieval_min_content_chars": 120,
    }
    numeric_only = {
        "type": "file_metadata",
        "filename": "unrelated.jpg",
        "text": "Файл с совпавшим номером",
        "score": 0.999,
        "retrieval_source": "numeric_exact",
    }

    assert s._apply_relevance_gate("несуществующая организация 847291", [numeric_only]) == []
    assert s._apply_relevance_gate("qzxv-несуществующий-документ-999999", [numeric_only]) == []
    assert s._apply_relevance_gate("СТС 847291", [numeric_only]) == []
    assert s._apply_relevance_gate("договор 847291", [numeric_only])[0]["relevance_evidence"] == "lexical"


def test_relevance_gate_requires_machine_document_evidence_after_fusion() -> None:
    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_relevance_gate_enabled": True,
        "retrieval_min_dense_score": 0.84,
        "retrieval_single_term_min_dense_score": 0.86,
        "retrieval_min_content_chars": 120,
    }
    generic_pdf = {
        "type": "pdf_content",
        "filename": "PC300.pdf",
        "path": r"Почта\PC300.pdf",
        "text": "Акт сверки по экскаватору Komatsu PC300. " * 5,
        "dense_score": 0.99,
        "retrieval_source": "dense",
    }
    passport = {
        **generic_pdf,
        "filename": "Выписка из электронного паспорта PC300.pdf",
        "text": "Выписка из электронного паспорта PC300. " * 5,
    }

    assert s._apply_relevance_gate("паспорт PC300", [generic_pdf, passport]) == [
        {**passport, "relevance_evidence": "dense", "relevance_floor": 0.84}
    ]


def test_rrf_recency_boost_is_relative_and_does_not_displace_exact_match() -> None:
    s = _make_searcher(connected=True)
    s.config = {"rank_recency_enabled": True, "rank_recency_max_boost": 0.03}
    exact = {
        "type": "folder_metadata",
        "filename": "1 Фактуры ТСК",
        "path": "1 Фактуры ТСК",
        "full_path": r"O:\1 Фактуры ТСК",
        "score": 0.97,
        "rank_score": 0.029,
        "fusion": "rrf",
    }
    recent_noise = {
        "type": "file_metadata",
        "filename": "fresh.xlsx",
        "path": "fresh.xlsx",
        "full_path": r"O:\fresh.xlsx",
        "modified": datetime.now(timezone.utc).isoformat(),
        "score": 0.99,
        "rank_score": 0.016,
        "fusion": "rrf",
    }

    out = s._merge_ranked_results([], [recent_noise, exact], limit=2, query="1 Фактуры ТСК")

    assert out[0]["filename"] == "1 Фактуры ТСК"
    assert float(recent_noise["rank_score"]) < float(exact["rank_score"])


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


def test_warm_retrieval_cache_builds_metadata_candidate_index(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    (catalog / "Паспорт PC300.pdf").write_text("x", encoding="utf-8")
    (catalog / "Акт сверки.docx").write_text("x", encoding="utf-8")
    s = _make_searcher(connected=True)
    s.config = {
        "catalog_path": str(catalog),
        "retrieval_bm25_enabled": True,
        "metadata_needle_cache_size": 16,
    }
    s._fs_cache = {"ts": 0.0, "items": []}

    assert s.warm_retrieval_cache() == 2
    items = s._refresh_fs_cache()
    candidates = s._metadata_candidates(items, ["pc300"])

    assert [item["filename"] for item in candidates] == ["Паспорт PC300.pdf"]
    s._metadata_candidates(items, [f"missing{index}" for index in range(20)])
    assert len(s._metadata_needle_docs) == 16


def test_numeric_exact_search_uses_payload_tokens() -> None:
    s = _make_searcher(connected=True)
    s.qdrant = _FakeQdrantScroll([
        {
            "type": "xlsx_content",
            "filename": "тс полный список.xlsx",
            "path": "тс полный список.xlsx",
            "full_path": r"O:\Обмен\тс полный список.xlsx",
            "extension": ".xlsx",
            "text": "HONDA | СТС 9941�210904",
            "numeric_tokens": ["9941", "210904", "9941210904"],
        }
    ])

    out = s._numeric_exact_search(
        query="стс 9941 210904",
        limit=5,
        file_type=None,
        content_only=False,
    )

    assert out
    assert out[0]["filename"] == "тс полный список.xlsx"
    assert out[0]["score"] > 0.99
    assert out[0]["retrieval_source"] == "numeric_exact"


def test_numeric_exact_search_uses_source_fallback_only_when_enabled(monkeypatch) -> None:
    s = _make_searcher(connected=True)
    s.qdrant = _FakeQdrantScroll([])
    fallback = [
        {
            "type": "xlsx_content",
            "filename": "legacy.xlsx",
            "full_path": r"O:\Обмен\legacy.xlsx",
            "chunk_index": None,
        }
    ]
    monkeypatch.setattr(s, "_spreadsheet_numeric_exact_scan", lambda **_kwargs: fallback)

    disabled = s._numeric_exact_search(
        query="СТС 9941 210904",
        limit=5,
        file_type=None,
        content_only=False,
    )
    s.config = {"numeric_exact_fs_fallback_enabled": True}
    enabled = s._numeric_exact_search(
        query="СТС 9941 210904",
        limit=5,
        file_type=None,
        content_only=False,
    )

    assert disabled == []
    assert enabled == fallback


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


def test_reranker_eval_mode_raises_instead_of_silent_fallback() -> None:
    class FailingReranker:
        def predict(self, _pairs):
            raise RuntimeError("onnx failed")

    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_reranker_enabled": True,
        "retrieval_reranker_model": "fake",
        "retrieval_reranker_fail_open": False,
    }
    s._reranker = FailingReranker()

    with pytest.raises(RuntimeError, match="Reranker prediction failed"):
        s._rerank_results("target", [{"text": "candidate", "score": 0.5}], limit=1)


def test_reranker_rejects_incomplete_score_vector_in_eval_mode() -> None:
    class IncompleteReranker:
        def predict(self, _pairs):
            return [1.0]

    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_reranker_enabled": True,
        "retrieval_reranker_model": "fake",
        "retrieval_reranker_fail_open": False,
    }
    s._reranker = IncompleteReranker()
    results = [{"text": "one", "score": 0.5}, {"text": "two", "score": 0.4}]

    with pytest.raises(RuntimeError, match="1 scores for 2 candidates"):
        s._rerank_results("target", results, limit=2)


def test_reranker_interactive_mode_keeps_fused_fallback() -> None:
    class FailingReranker:
        def predict(self, _pairs):
            raise RuntimeError("onnx failed")

    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_reranker_enabled": True,
        "retrieval_reranker_model": "fake",
        "retrieval_reranker_fail_open": True,
    }
    s._reranker = FailingReranker()
    results = [{"text": "candidate", "score": 0.5}]

    assert s._rerank_results("target", results, limit=1) == results


def test_onnx_reranker_uses_cached_local_snapshot(monkeypatch) -> None:
    import sentence_transformers

    captured = {}

    class FakeCrossEncoder:
        def __init__(self, model_name, **kwargs):
            captured["model_name"] = model_name
            captured["kwargs"] = kwargs

    s = _make_searcher(connected=True)
    s.config = {
        "retrieval_reranker_model": "cross-encoder/example",
        "retrieval_reranker_backend": "onnx",
        "retrieval_reranker_onnx_provider": "CPUExecutionProvider",
        "retrieval_reranker_onnx_file_name": "onnx/model_qint8.onnx",
    }
    s._reranker = None
    monkeypatch.setattr(sentence_transformers, "CrossEncoder", FakeCrossEncoder, raising=False)
    monkeypatch.setattr(
        "rag_catalog.core.rag_core._local_model_reference",
        lambda _model_name: r"C:\cache\cross-encoder-example",
    )

    assert isinstance(s.reranker, FakeCrossEncoder)
    assert captured["model_name"] == r"C:\cache\cross-encoder-example"
    assert captured["kwargs"] == {
        "backend": "onnx",
        "model_kwargs": {
            "provider": "CPUExecutionProvider",
            "file_name": "onnx/model_qint8.onnx",
        },
        "local_files_only": True,
    }


def test_refresh_fs_cache_uses_state_db_without_network_tree_walk(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog"
    qdrant = tmp_path / "qdrant"
    file_path = catalog / "a" / "b" / "doc.pdf"
    file_path.parent.mkdir(parents=True)
    (catalog / "empty").mkdir()
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
    assert "empty" not in folders


def test_refresh_fs_cache_ignores_empty_catalog_path() -> None:
    s = _make_searcher(connected=True)
    s.config = {}

    assert s._refresh_fs_cache() == []


def test_term_matches_latin_o_and_zero_vehicle_codes() -> None:
    s = _make_searcher(connected=True)

    assert s._term_matches("volkswagen touareg 050", "o50")
    assert s._term_matches("volkswagen touareg o50", "050")
    assert s._term_matches("фольксваген y 050 by", "touareg")
    assert s._term_matches("volkswagen y 050 by", "туарег")


def test_verify_rag_answer_rejects_unsupported_numbers() -> None:
    s = _make_searcher(connected=True)
    sources = [{"excerpt": "В документе указана масса 3200 кг."}]

    verification = s._verify_rag_answer("Масса составляет 3400 кг.", sources)

    assert verification["ok"] is False
    assert verification["error"] == "unsupported_facts"
    assert "weight_kg:3400" in verification["missing_facts"]


def test_verify_rag_answer_rejects_conflicting_weight_sources() -> None:
    s = _make_searcher(connected=True)
    sources = [{"excerpt": "ПСМ: масса 3400 кг."}, {"excerpt": "СТС: масса 3600 кг."}]

    verification = s._verify_rag_answer("Масса составляет 3400 кг.", sources)

    assert verification["ok"] is False
    assert verification["error"] == "conflicting_facts"
    assert verification["conflicting_facts"]["weight_kg"] == ["3400", "3600"]


def test_answer_documents_replaces_unsupported_model_answer(monkeypatch) -> None:
    from rag_catalog.core import llm

    s = _make_searcher(connected=True)
    s.config = {"llm_answer_top_k": 2, "llm_rag_model": "fake", "ollama_url": "http://ollama.test"}
    s.search = lambda *args, **kwargs: [  # type: ignore[method-assign]
        {
            "filename": "psm.pdf",
            "path": "Документы/psm.pdf",
            "full_path": r"O:\Документы\psm.pdf",
            "text": "В документе указана масса 3200 кг.",
            "score": 0.9,
            "page": 4,
            "chunk_index": 2,
        }
    ]
    monkeypatch.setattr(llm, "rag_answer", lambda *args, **kwargs: "Масса составляет 3400 кг.")

    result = s.answer_documents("Сколько весит техника?", source="test")

    assert result["ok"] is False
    assert result["answer"] == "Не нашёл подтверждения этому ответу в найденных фрагментах документов."
    assert result["verification"]["model_answer"] == "Масса составляет 3400 кг."
    assert result["sources"][0]["source_id"] == "S1"
    assert "стр. 4" in result["sources"][0]["citation"]


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
    assert "money_rub:999" in out["verification"]["missing_facts"]
    assert out["answer"] == "Не нашёл подтверждения этому ответу в найденных фрагментах документов."


def test_answer_fact_question_handles_search_error() -> None:
    s = _make_searcher(connected=True)

    def _boom(*args, **kwargs):
        raise RuntimeError("search boom")

    s.search = _boom  # type: ignore[method-assign]
    out = s.answer_fact_question("сколько весит")
    assert out["ok"] is False
    assert "Ошибка поиска" in out["error"]
    assert s.telemetry.fact_calls[-1]["error"].startswith("fact_search_error")
