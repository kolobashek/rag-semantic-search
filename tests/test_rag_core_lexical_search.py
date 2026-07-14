from pathlib import Path

from rag_catalog.core.telemetry_db import TelemetryDB
from rag_core import RAGSearcher


def _searcher_with_catalog(root: Path) -> RAGSearcher:
    s = object.__new__(RAGSearcher)
    s.config = {"catalog_path": str(root)}
    s._fs_cache = {"ts": 0.0, "items": []}
    return s


def test_lexical_search_returns_parent_folder_pdf_for_passport_query(tmp_path: Path) -> None:
    folder = tmp_path / "Паспорт Габидуллина Р.Р"
    folder.mkdir()
    (folder / "2-3.pdf").write_bytes(b"%PDF")

    s = _searcher_with_catalog(tmp_path)
    out = s._lexical_catalog_search(
        query="паспорта",
        limit=10,
        file_type=None,
        content_only=False,
    )

    paths = [x["path"] for x in out]
    assert "Паспорт Габидуллина Р.Р" in paths
    assert "Паспорт Габидуллина Р.Р\\2-3.pdf" in paths or "Паспорт Габидуллина Р.Р/2-3.pdf" in paths


def test_lexical_search_requires_entity_and_document_intent_match(tmp_path: Path) -> None:
    (tmp_path / "ПСМ.pdf").write_bytes(b"%PDF")
    (tmp_path / "PC300.pdf").write_bytes(b"%PDF")

    s = _searcher_with_catalog(tmp_path)
    out = s._lexical_catalog_search(
        query="PC300 масса ПСМ",
        limit=10,
        file_type=None,
        content_only=False,
    )

    assert out == []


def test_lexical_search_rejects_generic_model_pdf_for_machine_passport_query(tmp_path: Path) -> None:
    (tmp_path / "Шильдик ДВС Komatsu PC300.jpg").write_bytes(b"jpg")
    (tmp_path / "Экскаватор KOMATSU PC300-8 калькуляция 2024.xlsx").write_bytes(b"xlsx")
    (tmp_path / "PC300.pdf").write_bytes(b"%PDF")
    (tmp_path / "Паспорт PC300.pdf").write_bytes(b"%PDF")

    s = _searcher_with_catalog(tmp_path)
    out = s._lexical_catalog_search(
        query="паспорт PC300",
        limit=10,
        file_type=None,
        content_only=False,
    )

    assert out[0]["filename"] == "Паспорт PC300.pdf"
    assert "PC300.pdf" not in [item["filename"] for item in out]


def test_lexical_search_excludes_numbered_scans_without_vehicle_doc_evidence(tmp_path: Path) -> None:
    folder = tmp_path / "VOLKSWAGEN TOUAREG 050"
    folder.mkdir()
    (folder / "doc050225.pdf").write_bytes(b"%PDF")
    (folder / "Осаго 050 туарег.pdf").write_bytes(b"%PDF")
    (folder / "ПТС 050.pdf").write_bytes(b"%PDF")

    s = _searcher_with_catalog(tmp_path)
    out = s._lexical_catalog_search(
        query="touareg O50 vin птс стс",
        limit=10,
        file_type=None,
        content_only=False,
    )

    filenames = [x["filename"] for x in out]
    assert "ПТС 050.pdf" in filenames
    assert "doc050225.pdf" not in filenames
    assert "Осаго 050 туарег.pdf" not in filenames


def test_lexical_search_treats_vehicle_plate_as_vin_candidate(tmp_path: Path) -> None:
    (tmp_path / "Погрузчик Lovol FL966H.jpg").write_bytes(b"jpg")
    (tmp_path / "Шильдик Foton Lovol FL966H.jpg").write_bytes(b"jpg")
    (tmp_path / "VIN Liugong 862H.jpg").write_bytes(b"jpg")

    s = _searcher_with_catalog(tmp_path)
    out = s._lexical_catalog_search(
        query="vin lovol",
        limit=10,
        file_type=None,
        content_only=False,
    )

    assert out[0]["filename"] == "Шильдик Foton Lovol FL966H.jpg"


def test_lexical_search_treats_vehicle_registration_docs_as_vin_candidate(tmp_path: Path) -> None:
    folder = tmp_path / "Фольксваген Y 050 BY"
    folder.mkdir()
    (folder / "свидетельство о регистрации.jpg").write_bytes(b"jpg")
    (folder / "паспорт транспортного средства.jpg").write_bytes(b"jpg")
    (tmp_path / "VIN Liugong 862H.jpg").write_bytes(b"jpg")

    s = _searcher_with_catalog(tmp_path)
    out = s._lexical_catalog_search(
        query="touareg O50 vin",
        limit=10,
        file_type=None,
        content_only=False,
    )

    assert out[0]["filename"] in {
        "паспорт транспортного средства.jpg",
        "свидетельство о регистрации.jpg",
    }


def test_lexical_search_uses_search_aliases_for_company_card(tmp_path: Path) -> None:
    (tmp_path / "Карточка предприятия Спецмаш Альфа-Банк 2026.docx").write_bytes(b"docx")
    (tmp_path / "Договор ООО Спецмаш.docx").write_bytes(b"docx")

    s = _searcher_with_catalog(tmp_path)
    s.telemetry = TelemetryDB(str(tmp_path / "telemetry.db"))
    out = s._lexical_catalog_search(
        query="реквизиты спецмаш",
        limit=10,
        file_type=None,
        content_only=False,
    )

    assert out[0]["filename"] == "Карточка предприятия Спецмаш Альфа-Банк 2026.docx"


def test_lexical_search_reports_original_and_expanded_term_coverage(tmp_path: Path) -> None:
    (tmp_path / "Карточка предприятия ТСК.doc").write_bytes(b"doc")
    s = _searcher_with_catalog(tmp_path)
    s.telemetry = TelemetryDB(str(tmp_path / "telemetry.db"))
    s.telemetry.save_search_alias_group(
        key="company_card",
        label="карточка предприятия реквизиты организации",
        aliases=["карточка предприятия"],
    )

    out = s._lexical_catalog_search(
        query="карточка предприятия тск",
        limit=10,
        file_type=None,
        content_only=False,
    )

    assert out[0]["lexical_raw_matched_terms"] == 3
    assert out[0]["lexical_raw_query_terms"] == 3
    assert out[0]["lexical_query_terms"] >= out[0]["lexical_raw_query_terms"]


def test_lexical_search_uses_service_context_for_requisites_query(tmp_path: Path) -> None:
    service_folder = tmp_path / "Услуги" / "ООО СРК"
    service_folder.mkdir(parents=True)
    (service_folder / "Карточка_предприятия_СРК.docx").write_bytes(b"docx")
    (tmp_path / "Реквизиты ООО ТСК.docx").write_bytes(b"docx")

    s = _searcher_with_catalog(tmp_path)
    out = s._lexical_catalog_search(
        query="реквизиты обслуживания технических услуг",
        limit=10,
        file_type=None,
        content_only=False,
    )

    files = [item["filename"] for item in out if item["type"] == "file_metadata"]
    assert files.index("Карточка_предприятия_СРК.docx") < files.index("Реквизиты ООО ТСК.docx")


def test_clear_filesystem_cache_forces_rescan(tmp_path: Path) -> None:
    (tmp_path / "old.pdf").write_bytes(b"%PDF")
    s = _searcher_with_catalog(tmp_path)

    first = s._lexical_catalog_search(
        query="old",
        limit=10,
        file_type=None,
        content_only=False,
    )
    assert [x["filename"] for x in first] == ["old.pdf"]

    (tmp_path / "new.pdf").write_bytes(b"%PDF")
    cached = s._lexical_catalog_search(
        query="new",
        limit=10,
        file_type=None,
        content_only=False,
    )
    assert cached == []

    s.clear_filesystem_cache()
    refreshed = s._lexical_catalog_search(
        query="new",
        limit=10,
        file_type=None,
        content_only=False,
    )
    assert [x["filename"] for x in refreshed] == ["new.pdf"]


def test_metadata_candidate_index_supports_prefix_stems(tmp_path: Path) -> None:
    from rag_catalog.core.retrieval import prepare_bm25_items

    (tmp_path / "Карточка предприятия.docx").write_bytes(b"docx")
    (tmp_path / "Договор аренды.docx").write_bytes(b"docx")
    s = _searcher_with_catalog(tmp_path)
    items = s._refresh_fs_cache()
    for item in items:
        s._prepare_metadata_search_item(item)
    prepare_bm25_items(items)
    s._build_metadata_token_index(items)

    candidates = s._metadata_candidates(items, ["предприят"])

    assert [item["filename"] for item in candidates] == ["Карточка предприятия.docx"]
    assert s._metadata_sorted_tokens == tuple(sorted(s._metadata_token_docs))
