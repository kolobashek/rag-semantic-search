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


def test_relevance_metrics_treat_model_pdf_as_passport_when_entity_matches() -> None:
    results = [
        {"filename": "PC300.pdf", "path": r"Почта\PC300.pdf"},
        {"filename": "Погрузчик PC300.jpg", "path": r"Фото\Погрузчик PC300.jpg"},
    ]

    assert recall_at_k(results[:1], ["pc300", "паспорт"], k=1) == 1
    assert recall_at_k(results[1:2], ["pc300", "паспорт"], k=1) == 0.5


def test_evaluate_search_summary() -> None:
    golden = [GoldenQuery(query="карточка тск", expected=["тск"], category="folder_or_name")]

    def search_fn(_query: str, _limit: int) -> list[dict]:
        return [{"filename": "Карточка ТСК.docx", "path": "Катя/Карточка ТСК.docx", "score": 0.9}]

    report = evaluate_search(golden, search_fn, limit=10)

    assert report["queries"] == 1
    assert report["recall_at_k"] == 1
    assert report["mrr_at_k"] == 1
    assert report["ndcg_at_k"] == 1
    assert report["zero_result_rate"] == 0
    assert report["latency_p50_ms"] >= 0
    assert report["latency_p95_ms"] >= report["latency_p50_ms"]
    assert report["by_category"]["folder_or_name"]["queries"] == 1
    assert report["rows"][0]["category"] == "folder_or_name"
    assert report["rows"][0]["top"][0]["filename"] == "Карточка ТСК.docx"
