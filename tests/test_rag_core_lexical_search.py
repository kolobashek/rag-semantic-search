from pathlib import Path

from rag_core import RAGSearcher
from rag_catalog.core.telemetry_db import TelemetryDB


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
