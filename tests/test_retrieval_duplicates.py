from __future__ import annotations

from types import SimpleNamespace

from rag_catalog.core.retrieval.duplicates import collapse_duplicate_results
from rag_core import RAGSearcher


def _res(path: str, score: float, *, content_hash: str = "", duplicate_of: str = "", chunk_index=None) -> dict:
    return {
        "filename": path.rsplit("\\", 1)[-1],
        "path": path,
        "full_path": path,
        "score": score,
        "content_hash": content_hash,
        "is_duplicate": bool(duplicate_of),
        "duplicate_of": duplicate_of,
        "chunk_index": chunk_index,
    }


def test_collapse_keeps_best_copy_and_lists_others() -> None:
    results = [
        _res(r"O:\a\договор.pdf", 0.99, content_hash="h1"),
        _res(r"O:\other.pdf", 0.95, content_hash="h2"),
        _res(r"O:\b\договор (копия).pdf", 0.90, content_hash="h1"),
        _res(r"O:\c\договор.pdf", 0.80, content_hash="h1"),
    ]

    out = collapse_duplicate_results(results)

    assert [item["full_path"] for item in out] == [r"O:\a\договор.pdf", r"O:\other.pdf"]
    assert out[0]["duplicates"] == [r"O:\b\договор (копия).pdf", r"O:\c\договор.pdf"]
    assert out[0]["duplicate_count"] == 2
    assert "duplicates" not in out[1]
    # Input dicts are not mutated.
    assert "duplicates" not in results[0]


def test_collapse_uses_duplicate_of_when_hash_missing() -> None:
    results = [
        _res(r"O:\copy.pdf", 0.99, duplicate_of=r"O:\ORIGINAL.pdf"),
        _res(r"O:\original.pdf", 0.90),
    ]

    out = collapse_duplicate_results(results)

    assert [item["full_path"] for item in out] == [r"O:\copy.pdf"]
    assert out[0]["duplicates"] == [r"O:\original.pdf"]


def test_collapse_links_duplicate_of_to_hashed_original() -> None:
    results = [
        _res(r"O:\original.pdf", 0.99, content_hash="h1"),
        _res(r"O:\copy.pdf", 0.95, duplicate_of=r"O:\original.pdf"),
    ]
    out = collapse_duplicate_results(results)
    assert [item["full_path"] for item in out] == [r"O:\original.pdf"]
    assert out[0]["duplicates"] == [r"O:\copy.pdf"]


def test_collapse_keeps_multiple_chunks_of_surviving_file() -> None:
    results = [
        _res(r"O:\a.pdf", 0.99, content_hash="h1", chunk_index=0),
        _res(r"O:\b.pdf", 0.95, content_hash="h1", chunk_index=0),
        _res(r"O:\a.pdf", 0.90, content_hash="h1", chunk_index=3),
    ]
    out = collapse_duplicate_results(results)
    assert [(item["full_path"], item["chunk_index"]) for item in out] == [(r"O:\a.pdf", 0), (r"O:\a.pdf", 3)]
    assert out[0]["duplicate_count"] == 1


def test_collapse_without_markers_is_noop() -> None:
    results = [_res(r"O:\a.pdf", 0.9), _res(r"O:\b.pdf", 0.8)]
    assert collapse_duplicate_results(results) == results


def _make_searcher(points: list, config: dict) -> RAGSearcher:
    s = RAGSearcher.__new__(RAGSearcher)
    s.connected = True
    s.collection_name = "catalog"
    s.config = config
    s._embedder = SimpleNamespace(encode=lambda q, normalize_embeddings=True: SimpleNamespace(tolist=lambda: [0.1]))
    s.telemetry = SimpleNamespace(
        search_calls=[],
        log_search=lambda **kw: s.telemetry.search_calls.append(kw),
        get_search_feedback_scores=lambda **kw: {},
    )
    s.qdrant = SimpleNamespace(query_points=lambda **_kw: SimpleNamespace(points=points))
    s._lexical_catalog_search = lambda **_kw: []  # type: ignore[method-assign]
    s._numeric_exact_search = lambda **_kw: []  # type: ignore[method-assign]
    s._fulltext_content_search = lambda **_kw: []  # type: ignore[method-assign]
    return s


def _points() -> list:
    def point(score: float, path: str, is_duplicate: bool, duplicate_of: str = "") -> SimpleNamespace:
        return SimpleNamespace(
            score=score,
            payload={
                "type": "pdf_content",
                "filename": path.rsplit("\\", 1)[-1],
                "path": path,
                "full_path": path,
                "text": "договор аренды " * 10,
                "content_hash": "abc",
                "is_duplicate": is_duplicate,
                "duplicate_of": duplicate_of,
                "chunk_index": 0,
            },
        )

    return [
        point(0.9, r"O:\копия\договор.pdf", True, r"O:\договор.pdf"),
        point(0.85, r"O:\договор.pdf", False),
        point(0.5, r"O:\ещё\договор.pdf", True, r"O:\договор.pdf"),
    ]


def test_search_collapses_duplicates_by_default() -> None:
    s = _make_searcher(_points(), {})

    out = s.search("договор", limit=10, source="test")

    assert [item["full_path"] for item in out] == [r"O:\копия\договор.pdf"]
    assert out[0]["duplicates"] == [r"O:\договор.pdf", r"O:\ещё\договор.pdf"]
    assert out[0]["duplicate_count"] == 2
    assert s.telemetry.search_calls[-1]["details"]["duplicates_collapsed"] == 2


def test_search_collapse_can_be_disabled() -> None:
    s = _make_searcher(_points(), {"retrieval_collapse_duplicates": False})

    out = s.search("договор", limit=10, source="test")

    assert len(out) == 3
    assert all("duplicates" not in item for item in out)
