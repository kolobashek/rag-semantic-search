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


def test_lexical_search_requires_entity_match_when_query_has_entity(tmp_path: Path) -> None:
    (tmp_path / "ПСМ.pdf").write_bytes(b"%PDF")
    (tmp_path / "PC300.pdf").write_bytes(b"%PDF")

    s = _searcher_with_catalog(tmp_path)
    out = s._lexical_catalog_search(
        query="PC300 масса ПСМ",
        limit=10,
        file_type=None,
        content_only=False,
    )

    assert [x["filename"] for x in out] == ["PC300.pdf"]


def test_lexical_search_boosts_model_pdf_for_machine_passport_query(tmp_path: Path) -> None:
    (tmp_path / "Шильдик ДВС Komatsu PC300.jpg").write_bytes(b"jpg")
    (tmp_path / "Экскаватор KOMATSU PC300-8 калькуляция 2024.xlsx").write_bytes(b"xlsx")
    (tmp_path / "PC300.pdf").write_bytes(b"%PDF")

    s = _searcher_with_catalog(tmp_path)
    out = s._lexical_catalog_search(
        query="паспорт PC300",
        limit=10,
        file_type=None,
        content_only=False,
    )

    assert out[0]["filename"] == "PC300.pdf"
    assert out[0]["score"] >= 0.9996


def test_lexical_search_keeps_numbered_scans_below_vehicle_docs(tmp_path: Path) -> None:
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
    assert filenames.index("ПТС 050.pdf") < filenames.index("doc050225.pdf")
    assert filenames.index("ПТС 050.pdf") < filenames.index("Осаго 050 туарег.pdf")


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
