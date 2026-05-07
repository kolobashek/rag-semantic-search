from __future__ import annotations

from rag_catalog.core.retrieval import bm25_rank_items, tokenize


def test_tokenize_normalizes_russian_and_filters_stopwords() -> None:
    assert tokenize("Нужен паспорт техники PC300 для отчёта") == ["паспорт", "техники", "pc300", "отчета"]


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
