from __future__ import annotations

from rag_catalog.core.retrieval import bm25_rank_items, prepare_bm25_items, tokenize


def test_tokenize_normalizes_russian_and_filters_stopwords() -> None:
    assert tokenize("Нужен паспорт техники PC300 для отчёта") == ["паспорт", "техники", "pc300", "отчета"]


def test_prepare_bm25_items_reuses_cached_tokens() -> None:
    items = [{"filename": "Паспорт PC300.pdf", "path": r"Техника\Паспорт PC300.pdf"}]

    assert prepare_bm25_items(items) == 1
    cached = items[0]["_bm25_tokens"]
    assert prepare_bm25_items(items) == 0
    assert items[0]["_bm25_tokens"] is cached


def test_bm25_rank_items_prefers_filename_match_over_parent_path_noise() -> None:
    items = [
        {
            "kind": "file",
            "filename": "Карточка предприятия ООО ТСК.docx",
            "path": r"Катя\ООО ТСК\Карточка предприятия ООО ТСК.docx",
            "full_path": r"O:\Катя\ООО ТСК\Карточка предприятия ООО ТСК.docx",
            "extension": ".docx",
        },
        {
            "kind": "file",
            "filename": "Акт сверки.docx",
            "path": r"Карточки предприятий\ООО ТСК\Акт сверки.docx",
            "full_path": r"O:\Карточки предприятий\ООО ТСК\Акт сверки.docx",
            "extension": ".docx",
        },
        {
            "kind": "folder",
            "filename": "Карточки предприятий",
            "path": "Карточки предприятий",
            "full_path": r"O:\Карточки предприятий",
            "extension": "",
        },
    ]

    out = bm25_rank_items(items, ["карточка", "предприятия", "тск"], limit=3)

    assert out[0]["filename"] == "Карточка предприятия ООО ТСК.docx"
    assert out[0]["rank_reason"] == "BM25 совпадение в имени/пути"
    assert float(out[0]["score"]) > float(out[1]["score"])


def test_bm25_matches_touareg_against_russian_vehicle_folder() -> None:
    items = [
        {
            "kind": "file",
            "filename": "свидетельство о регистрации.jpg",
            "path": r"Док-ты техника\Старые\Фольксваген Y 050 BY\свидетельство о регистрации.jpg",
            "full_path": r"O:\Док-ты техника\Старые\Фольксваген Y 050 BY\свидетельство о регистрации.jpg",
            "extension": ".jpg",
        },
        {
            "kind": "file",
            "filename": "СТС 1234.pdf",
            "path": r"Док-ты техника\СТС 1234.pdf",
            "full_path": r"O:\Док-ты техника\СТС 1234.pdf",
            "extension": ".pdf",
        },
    ]

    out = bm25_rank_items(items, ["touareg", "o50", "стс"], limit=2)

    assert out[0]["filename"] == "свидетельство о регистрации.jpg"


def test_bm25_matches_vin_against_vehicle_plate_photo() -> None:
    items = [
        {
            "kind": "file",
            "filename": "Шильдик Foton Lovol FL966H.jpg",
            "path": r"Документы на Технику\фото техники\Шильдик Foton Lovol FL966H.jpg",
            "full_path": r"O:\Документы на Технику\фото техники\Шильдик Foton Lovol FL966H.jpg",
            "extension": ".jpg",
        },
        {
            "kind": "file",
            "filename": "VIN Liugong 862H.jpg",
            "path": r"Магазин\Нужное\Картинки\VIN Liugong 862H.jpg",
            "full_path": r"O:\Магазин\Нужное\Картинки\VIN Liugong 862H.jpg",
            "extension": ".jpg",
        },
    ]

    out = bm25_rank_items(items, ["vin", "lovol"], limit=2)

    assert out[0]["filename"] == "Шильдик Foton Lovol FL966H.jpg"


def test_bm25_matches_technical_service_context() -> None:
    items = [
        {
            "kind": "file",
            "filename": "Карточка_предприятия_СРК.docx",
            "path": r"Катя\ООО ТСК\Услуги\ООО СРК\Карточка_предприятия_СРК.docx",
            "full_path": r"O:\Катя\ООО ТСК\Услуги\ООО СРК\Карточка_предприятия_СРК.docx",
            "extension": ".docx",
        },
        {
            "kind": "file",
            "filename": "Реквизиты ООО ТСК.docx",
            "path": r"Магазин\Реквизиты ООО ТСК.docx",
            "full_path": r"O:\Магазин\Реквизиты ООО ТСК.docx",
            "extension": ".docx",
        },
    ]

    out = bm25_rank_items(items, ["реквизиты", "обслуживания", "технических", "услуг"], limit=2)

    assert out[0]["filename"] == "Карточка_предприятия_СРК.docx"
