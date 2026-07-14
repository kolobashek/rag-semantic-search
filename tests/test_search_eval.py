from __future__ import annotations

from rag_catalog.core.search_eval import (
    GoldenQuery,
    evaluate_retrieval_decision,
    evaluate_search,
    mrr_at_k,
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
)


def test_relevance_metrics() -> None:
    results = [
        {"filename": "wrong.txt", "path": "x/wrong.txt"},
        {"filename": "Карточка предприятия ООО ТСК.docx", "path": "Катя/Карточка предприятия ООО ТСК.docx"},
    ]

    assert recall_at_k(results, ["карточка предприятия"], k=1) == 0
    assert recall_at_k(results, ["карточка предприятия"], k=2) == 1
    assert mrr_at_k(results, ["карточка предприятия"], k=2) == 0.5
    assert precision_at_k(results, ["карточка предприятия"], k=2) == 0.5
    assert 0 < ndcg_at_k(results, ["карточка предприятия"], k=2) < 1


def test_precision_requires_complete_query_intent() -> None:
    results = [
        {"filename": "Договор аренды.pdf", "path": "Договоры/Договор аренды.pdf"},
        {"filename": "Договор поставки.pdf", "path": "Договоры/Договор поставки.pdf"},
        {"filename": "Счет на оплату.pdf", "path": "Счета/Счет на оплату.pdf"},
    ]

    assert precision_at_k(results, ["договор", "поставки"], k=3) == 1 / 3
    assert mrr_at_k(results, ["договор", "поставки"], k=3) == 0.5


def test_relevance_metrics_use_morphology_and_domain_aliases() -> None:
    results = [
        {
            "filename": "Счет на оплату.pdf",
            "path": r"Магазин\Счета на оплату\Счет на оплату.pdf",
        },
        {
            "filename": "Выписка из электронного паспорта 6357.pdf",
            "path": r"Техника\Экскаватор 6357\Выписка из электронного паспорта 6357.pdf",
        },
        {
            "filename": "свидетельство о регистрации.jpg",
            "path": r"Док-ты техника\Старые\Фольксваген Y 050 BY\свидетельство о регистрации.jpg",
        },
        {
            "filename": "Шильдик Foton Lovol FL966H.jpg",
            "path": r"Документы на Технику\фото техники\Шильдик Foton Lovol FL966H.jpg",
        },
        {
            "filename": "Карточка_предприятия_СРК.docx",
            "path": r"Катя\ООО ТСК\Услуги\ООО СРК\Карточка_предприятия_СРК.docx",
        },
    ]

    assert recall_at_k(results[:1], ["счет", "оплата"], k=1) == 1
    assert recall_at_k(results[1:2], ["6357", "псм"], k=1) == 1
    assert recall_at_k(results[2:3], ["touareg"], k=1) == 1
    assert recall_at_k(results[2:3], ["vin"], k=1) == 1
    assert recall_at_k(results[3:4], ["lovol", "vin"], k=1) == 1
    assert recall_at_k(results[4:5], ["реквизит", "технических"], k=1) == 1


def test_ndcg_is_bounded_when_many_results_match_same_expected_token() -> None:
    results = [
        {"filename": "Счет на оплату 1.pdf", "path": "Счета/Счет на оплату 1.pdf"},
        {"filename": "Счет на оплату 2.pdf", "path": "Счета/Счет на оплату 2.pdf"},
        {"filename": "Счет на оплату 3.pdf", "path": "Счета/Счет на оплату 3.pdf"},
    ]

    assert ndcg_at_k(results, ["счет", "оплата"], k=3) == 1


def test_relevance_metrics_require_actual_passport_evidence() -> None:
    results = [
        {"filename": "PC300.pdf", "path": r"Почта\PC300.pdf"},
        {"filename": "Паспорт PC300.pdf", "path": r"Документы\Паспорт PC300.pdf"},
    ]

    assert recall_at_k(results[:1], ["pc300", "паспорт"], k=1) == 0.5
    assert recall_at_k(results[1:2], ["pc300", "паспорт"], k=1) == 1


def test_evaluate_search_summary() -> None:
    golden = [GoldenQuery(query="карточка тск", expected=["тск"], category="folder_or_name")]

    def search_fn(_query: str, _limit: int) -> list[dict]:
        return [
            {
                "filename": "Карточка ТСК.docx",
                "path": "Катя/Карточка ТСК.docx",
                "score": 0.9,
                "text": "Карточка   предприятия\nООО ТСК",
            }
        ]

    report = evaluate_search(golden, search_fn, limit=10)

    assert report["queries"] == 1
    assert report["recall_at_k"] == 1
    assert report["precision_at_k"] == 1
    assert report["irrelevant_rate_at_k"] == 0
    assert report["top1_accuracy"] == 1
    assert report["mrr_at_k"] == 1
    assert report["ndcg_at_k"] == 1
    assert report["zero_result_rate"] == 0
    assert report["latency_p50_ms"] >= 0
    assert report["latency_p95_ms"] >= report["latency_p50_ms"]
    assert report["by_category"]["folder_or_name"]["queries"] == 1
    assert report["rows"][0]["category"] == "folder_or_name"
    assert report["rows"][0]["top"][0]["filename"] == "Карточка ТСК.docx"
    assert report["rows"][0]["top"][0]["excerpt"] == "Карточка предприятия ООО ТСК"


def test_retrieval_v3_metrics_cover_document_chunk_page_no_answer_and_acl() -> None:
    golden = [
        GoldenQuery(
            query="условия оплаты",
            expected=["договор"],
            expected_paths=["Договор поставки"],
            expected_chunks=["оплата в течение 10 дней"],
            expected_pages=[7],
            forbidden=["Секретный проект"],
        ),
        GoldenQuery(query="несуществующий документ", expected=[], expect_no_answer=True),
    ]

    def search_fn(query: str, _limit: int) -> list[dict]:
        if query == "несуществующий документ":
            return []
        return [
            {
                "filename": "Договор поставки.pdf",
                "path": "Договоры/Договор поставки.pdf",
                "text": "Оплата\n  в течение 10 дней после поставки.",
                "page_number": 7,
            }
        ]

    report = evaluate_search(golden, search_fn, limit=10)

    assert report["document_hit_rate"] == 1
    assert report["chunk_hit_rate"] == 1
    assert report["page_hit_rate"] == 1
    assert report["no_answer_accuracy"] == 1
    assert report["acl_leakage_rate"] == 0
    assert report["acl_results_checked"] == 1
    assert report["ground_truth_coverage"] == 1
    assert report["categories_count"] == 1
    assert report["no_answer_cases"] == 1
    assert report["document_grounded_cases"] == 1
    assert report["content_grounded_cases"] == 1
    assert report["acl_cases"] == 1
    assert report["rows"][1]["recall_at_k"] is None


def test_retrieval_decision_requires_broad_eval_and_readiness_evidence() -> None:
    candidate = {
        "queries": 36,
        "categories_count": 7,
        "no_answer_cases": 3,
        "document_grounded_cases": 20,
        "content_grounded_cases": 0,
        "recall_at_k": 1.0,
        "precision_at_k": 1.0,
        "irrelevant_rate_at_k": 0.0,
        "top1_accuracy": 1.0,
        "latency_p95_ms": 100,
        "acl_leakage_rate": 0.0,
        "acl_results_checked": 10,
        "no_answer_accuracy": 1.0,
        "ground_truth_coverage": 1.0,
    }

    decision = evaluate_retrieval_decision(candidate)

    assert decision["decision"] == "NO_GO"
    failed = {check["name"] for check in decision["checks"] if not check["ok"]}
    assert {
        "eval_query_breadth",
        "no_answer_case_breadth",
        "content_grounded_breadth",
        "index_readiness",
    } <= failed


def test_retrieval_decision_accepts_broad_ready_candidate() -> None:
    candidate = {
        "queries": 60,
        "categories_count": 8,
        "no_answer_cases": 12,
        "document_grounded_cases": 30,
        "content_grounded_cases": 15,
        "recall_at_k": 0.95,
        "precision_at_k": 0.8,
        "irrelevant_rate_at_k": 0.2,
        "top1_accuracy": 0.9,
        "latency_p95_ms": 1500,
        "acl_leakage_rate": 0.0,
        "acl_results_checked": 25,
        "no_answer_accuracy": 0.95,
        "ground_truth_coverage": 0.8,
        "index_readiness": {
            "ready": True,
            "collection_name": "catalog_v2_e5",
            "reasons": [],
        },
    }

    assert evaluate_retrieval_decision(candidate)["decision"] == "GO"


def test_retrieval_decision_rejects_regression_and_missing_safety_evidence() -> None:
    baseline = {
        "recall_at_k": 0.9,
        "precision_at_k": 0.7,
        "top1_accuracy": 0.9,
        "latency_p95_ms": 1000,
    }
    candidate = {
        "queries": 20,
        "recall_at_k": 0.88,
        "precision_at_k": 0.4,
        "irrelevant_rate_at_k": 0.6,
        "top1_accuracy": 0.75,
        "latency_p95_ms": 1700,
        "acl_leakage_rate": 0.01,
        "acl_results_checked": 10,
        "no_answer_accuracy": None,
        "ground_truth_coverage": 0.2,
        "faithfulness_evaluated": False,
    }

    decision = evaluate_retrieval_decision(candidate, baseline=baseline, require_faithfulness=True)

    assert decision["decision"] == "NO_GO"
    failed = {check["name"] for check in decision["checks"] if not check["ok"]}
    assert {
        "precision_floor",
        "top1_accuracy",
        "irrelevant_result_rate",
        "acl_leakage",
        "no_answer_accuracy",
        "ground_truth_coverage",
        "faithfulness_evaluated",
    } <= failed
    assert {"recall_regression", "precision_regression", "top1_regression", "latency_regression"} <= failed


def test_retrieval_decision_requires_comparable_precision_baseline() -> None:
    candidate = {
        "queries": 20,
        "recall_at_k": 1.0,
        "precision_at_k": 1.0,
        "irrelevant_rate_at_k": 0.0,
        "top1_accuracy": 1.0,
        "latency_p95_ms": 100,
        "acl_leakage_rate": 0.0,
        "acl_results_checked": 10,
        "no_answer_accuracy": 1.0,
        "ground_truth_coverage": 1.0,
    }

    decision = evaluate_retrieval_decision(
        candidate,
        baseline={"recall_at_k": 1.0, "latency_p95_ms": 100},
    )

    failed = {check["name"] for check in decision["checks"] if not check["ok"]}
    assert {"precision_regression", "top1_regression"} <= failed


def test_retrieval_decision_rejects_stale_baseline_fingerprint() -> None:
    candidate = {
        "queries": 60,
        "categories_count": 8,
        "no_answer_cases": 12,
        "document_grounded_cases": 30,
        "content_grounded_cases": 15,
        "recall_at_k": 0.95,
        "precision_at_k": 0.8,
        "irrelevant_rate_at_k": 0.2,
        "top1_accuracy": 0.9,
        "latency_p95_ms": 1500,
        "acl_leakage_rate": 0.0,
        "acl_results_checked": 25,
        "no_answer_accuracy": 0.95,
        "ground_truth_coverage": 0.8,
        "evaluation_fingerprint": "current",
        "index_readiness": {
            "ready": True,
            "collection_name": "catalog_v2_e5",
            "reasons": [],
        },
    }
    baseline = {
        "recall_at_k": 0.95,
        "precision_at_k": 0.8,
        "top1_accuracy": 0.9,
        "latency_p95_ms": 1500,
        "evaluation_fingerprint": "stale",
    }

    decision = evaluate_retrieval_decision(candidate, baseline=baseline)

    check = next(item for item in decision["checks"] if item["name"] == "baseline_fingerprint")
    assert check["ok"] is False
    assert decision["decision"] == "NO_GO"


def test_retrieval_decision_rejects_unfinalized_index() -> None:
    candidate = {
        "queries": 20,
        "recall_at_k": 1.0,
        "precision_at_k": 1.0,
        "irrelevant_rate_at_k": 0.0,
        "top1_accuracy": 1.0,
        "latency_p95_ms": 100,
        "acl_leakage_rate": 0.0,
        "acl_results_checked": 10,
        "no_answer_accuracy": 1.0,
        "ground_truth_coverage": 1.0,
        "index_readiness": {
            "ready": False,
            "collection_name": "catalog_v2_e5",
            "reasons": ["fulltext_index_missing", "unindexed_vectors=500000"],
        },
    }

    decision = evaluate_retrieval_decision(candidate)

    assert decision["decision"] == "NO_GO"
    check = next(item for item in decision["checks"] if item["name"] == "index_readiness")
    assert check["ok"] is False
    assert check["actual"]["collection_name"] == "catalog_v2_e5"
