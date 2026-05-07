from __future__ import annotations

from rag_catalog.core.search_eval import GoldenQuery, evaluate_search, mrr_at_k, ndcg_at_k, recall_at_k


def test_relevance_metrics() -> None:
    results = [
        {"filename": "wrong.txt", "path": "x/wrong.txt"},
        {"filename": "Карточка предприятия ООО ТСК.docx", "path": "Катя/Карточка предприятия ООО ТСК.docx"},
    ]

    assert recall_at_k(results, ["карточка предприятия"], k=1) == 0
    assert recall_at_k(results, ["карточка предприятия"], k=2) == 1
    assert mrr_at_k(results, ["карточка предприятия"], k=2) == 0.5
    assert 0 < ndcg_at_k(results, ["карточка предприятия"], k=2) < 1


def test_evaluate_search_summary() -> None:
    golden = [GoldenQuery(query="карточка тск", expected=["тск"])]

    def search_fn(_query: str, _limit: int) -> list[dict]:
        return [{"filename": "Карточка ТСК.docx", "path": "Катя/Карточка ТСК.docx", "score": 0.9}]

    report = evaluate_search(golden, search_fn, limit=10)

    assert report["queries"] == 1
    assert report["recall_at_k"] == 1
    assert report["mrr_at_k"] == 1
    assert report["ndcg_at_k"] == 1
    assert report["rows"][0]["top"][0]["filename"] == "Карточка ТСК.docx"
