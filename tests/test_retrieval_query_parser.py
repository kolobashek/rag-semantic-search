from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from rag_catalog.core.retrieval.query_parser import apply_operator_filters, parse_query
from rag_core import RAGSearcher


def test_parse_query_extracts_all_operators() -> None:
    parsed = parse_query(
        'договор "аренда экскаватора" -черновик type:pdf path:Договоры/2024 after:2024-01-15 before:2024-12-31 PC300'
    )

    assert parsed.terms == "договор PC300"
    assert parsed.phrases == ["аренда экскаватора"]
    assert parsed.excluded == ["черновик"]
    assert parsed.file_type == ".pdf"
    assert parsed.path_contains == "Договоры/2024"
    assert parsed.after == date(2024, 1, 15)
    assert parsed.before == date(2024, 12, 31)
    assert parsed.has_operators is True
    assert parsed.search_text == "договор PC300 аренда экскаватора"


def test_parse_query_plain_text_has_no_operators() -> None:
    parsed = parse_query("СТС 9980 782471")
    assert parsed.terms == "СТС 9980 782471"
    assert parsed.has_operators is False
    assert parsed.search_text == "СТС 9980 782471"


def test_parse_query_keeps_hyphenated_tokens_and_normalizes_type() -> None:
    parsed = parse_query("9980-782471 type:.DOCX -акт")
    assert parsed.terms == "9980-782471"
    assert parsed.file_type == ".docx"
    assert parsed.excluded == ["акт"]


def test_parse_query_accepts_quoted_path_and_dotted_dates() -> None:
    parsed = parse_query('path:"Документы на технику" after:15.01.2024 акт')
    assert parsed.path_contains == "Документы на технику"
    assert parsed.after == date(2024, 1, 15)
    assert parsed.terms == "акт"


def test_parse_query_invalid_date_stays_in_terms() -> None:
    parsed = parse_query("after:вчера акт")
    assert parsed.after is None
    assert parsed.terms == "вчера акт"


def _item(name: str, *, text: str = "", path: str = "", modified=None, full_path: str = "") -> dict:
    return {
        "filename": name,
        "text": text,
        "path": path or f"docs/{name}",
        "full_path": full_path or rf"O:\docs\{name}",
        "modified": modified,
    }


def test_phrase_filter_requires_case_insensitive_occurrence_in_text_or_filename() -> None:
    parsed = parse_query('"Аренда Экскаватора"')
    items = [
        _item("a.pdf", text="Договор АРЕНДА экскаватора №5"),
        _item("Аренда экскаватора.docx", text="без фразы"),
        _item("c.pdf", text="аренда крана"),
    ]
    stats: dict = {}
    kept = apply_operator_filters(parsed, items, stats=stats)
    assert [i["filename"] for i in kept] == ["a.pdf", "Аренда экскаватора.docx"]
    assert stats == {"phrase_missing": 1}


def test_excluded_word_matches_filename_path_or_text_as_whole_word() -> None:
    parsed = parse_query("договор -акт")
    items = [
        _item("акт.pdf", text="нет"),
        _item("b.pdf", path="Акты/b.pdf", text="договор"),
        _item("c.pdf", text="подписан АКТ приёмки"),
        _item("d.pdf", text="контракт подписан"),  # "акт" внутри слова не считается
    ]
    kept = apply_operator_filters(parsed, items)
    assert [i["filename"] for i in kept] == ["b.pdf", "d.pdf"]


def test_path_filter_is_case_insensitive_and_slash_agnostic() -> None:
    parsed = parse_query("акт path:договоры/2024")
    items = [
        _item("a.pdf", path="Договоры\\2024\\a.pdf", full_path=r"O:\Обмен\Договоры\2024\a.pdf"),
        _item("b.pdf", path="Договоры/2023/b.pdf", full_path=r"O:\Обмен\Договоры\2023\b.pdf"),
    ]
    kept = apply_operator_filters(parsed, items)
    assert [i["filename"] for i in kept] == ["a.pdf"]


def test_date_filters_support_iso_and_timestamp_modified() -> None:
    parsed = parse_query("акт after:2024-03-01 before:2024-04-01")
    items = [
        _item("iso-in.pdf", modified="2024-03-15T10:00:00"),
        _item("iso-early.pdf", modified="2024-02-28T23:59:59"),
        _item("iso-late.pdf", modified="2024-04-01T00:00:00"),
        _item("ts-in.pdf", modified=1710500000),  # 2024-03-15
        _item("ts-str.pdf", modified="1710500000"),
        _item("none.pdf", modified=None),
    ]
    stats: dict = {}
    kept = apply_operator_filters(parsed, items, stats=stats)
    assert [i["filename"] for i in kept] == ["iso-in.pdf", "ts-in.pdf", "ts-str.pdf"]
    assert stats["modified_unknown"] == 1


def _make_searcher() -> RAGSearcher:
    s = RAGSearcher.__new__(RAGSearcher)
    s.connected = True
    s.collection_name = "catalog"
    s.config = {}
    s._embedder = SimpleNamespace(encode=lambda q, normalize_embeddings=True: SimpleNamespace(tolist=lambda: [0.1]))
    s.telemetry = SimpleNamespace(
        search_calls=[],
        log_search=lambda **kw: s.telemetry.search_calls.append(kw),
        get_search_feedback_scores=lambda **kw: {},
    )
    return s


def test_search_applies_operators_across_channels_and_passes_type_filter() -> None:
    s = _make_searcher()
    captured: dict = {}

    def dense(**kwargs):
        captured["dense"] = kwargs
        return SimpleNamespace(
            points=[
                SimpleNamespace(
                    score=0.9,
                    payload={
                        "type": "pdf_content",
                        "filename": "dense-old.pdf",
                        "path": "Договоры/dense-old.pdf",
                        "full_path": r"O:\Договоры\dense-old.pdf",
                        "text": "аренда экскаватора " * 10,
                        "modified": "2023-05-01T00:00:00",
                        "chunk_index": 0,
                    },
                ),
                SimpleNamespace(
                    score=0.8,
                    payload={
                        "type": "pdf_content",
                        "filename": "dense-new.pdf",
                        "path": "Договоры/dense-new.pdf",
                        "full_path": r"O:\Договоры\dense-new.pdf",
                        "text": "аренда экскаватора " * 10,
                        "modified": "2024-05-01T00:00:00",
                        "chunk_index": 0,
                    },
                ),
            ]
        )

    s.qdrant = SimpleNamespace(query_points=dense)

    def lexical(**kwargs):
        captured["lexical"] = kwargs
        return [
            {
                "type": "file_metadata",
                "filename": "аренда экскаватора черновик.pdf",
                "path": "Договоры/аренда экскаватора черновик.pdf",
                "full_path": r"O:\Договоры\аренда экскаватора черновик.pdf",
                "modified": "2024-06-01T00:00:00",
                "score": 0.99,
            },
            {
                "type": "file_metadata",
                "filename": "аренда экскаватора.pdf",
                "path": "Прочее/аренда экскаватора.pdf",
                "full_path": r"O:\Прочее\аренда экскаватора.pdf",
                "modified": "2024-06-01T00:00:00",
                "score": 0.98,
            },
        ]

    s._lexical_catalog_search = lexical  # type: ignore[method-assign]
    s._numeric_exact_search = lambda **_kw: []  # type: ignore[method-assign]
    s._fulltext_content_search = lambda **_kw: []  # type: ignore[method-assign]

    out = s.search('"аренда экскаватора" -черновик type:pdf path:Договоры after:2024-01-01', limit=10, source="test")

    assert [item["filename"] for item in out] == ["dense-new.pdf"]
    assert captured["dense"]["query_filter"].must[0].key == "extension"
    assert captured["dense"]["query_filter"].must[0].match.value == ".pdf"
    assert captured["lexical"]["file_type"] == ".pdf"
    assert captured["lexical"]["query"] == "аренда экскаватора"
    assert s._embedder is not None
    call = s.telemetry.search_calls[-1]
    assert call["query"].startswith('"аренда экскаватора"')
    assert call["details"]["operators"]["file_type"] == ".pdf"
    assert call["details"]["operators"]["rejected_by_reason"] == {
        "before_after_bound": 1,
        "excluded_word": 1,
        "path_mismatch": 1,
    }
