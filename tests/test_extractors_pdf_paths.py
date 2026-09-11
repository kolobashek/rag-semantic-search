"""poppler (pdf2image) на Windows не открывает PDF с кириллицей/«№» в пути.

Экстрактор должен рендерить такие файлы через временную ASCII-копию
(`_PopplerSafePdf`), а после рендера удалять её.
"""

from __future__ import annotations

import types
from pathlib import Path

import pytest

from rag_catalog.core.extractors import files as files_module
from rag_catalog.core.extractors.files import (
    _is_poppler_safe_path,
    _iter_pdf_pages,
    _pdf_page_count,
    _PopplerSafePdf,
)

UNICODE_NAME = "HOWO  Отчёт № 900 (двойной  пробел).pdf"


def _fake_pdf2image(monkeypatch, tmp_path: Path, *, pages: int = 3):
    """Подменить pdfinfo_from_path/convert_from_path, записывая переданные пути."""
    import pdf2image

    seen: dict[str, list[str]] = {"info": [], "convert": []}
    temp_dir = tmp_path / "ascii_tmp"
    temp_dir.mkdir()
    monkeypatch.setattr(files_module, "_ascii_temp_dir", lambda: str(temp_dir))

    def fake_info(path, **_kwargs):
        seen["info"].append(str(path))
        assert Path(path).is_file(), "pdfinfo должен получить существующий файл"
        return {"Pages": pages}

    def fake_convert(path, **kwargs):
        seen["convert"].append(str(path))
        assert Path(path).is_file(), "pdftoppm должен получить существующий файл"
        first_page = int(kwargs["first_page"])
        last_page = int(kwargs["last_page"])
        return [types.SimpleNamespace(number=page) for page in range(first_page, last_page + 1)]

    monkeypatch.setattr(pdf2image, "pdfinfo_from_path", fake_info)
    monkeypatch.setattr(pdf2image, "convert_from_path", fake_convert)
    return seen, temp_dir


def test_is_poppler_safe_path() -> None:
    assert _is_poppler_safe_path(Path(r"O:\Exchange\HOWO 900.pdf"))
    assert not _is_poppler_safe_path(Path(r"O:\Обмен\HOWO 900.pdf"))
    assert not _is_poppler_safe_path(Path(r"O:\Exchange\Report №900.pdf"))


def test_iter_pdf_pages_renders_unicode_path_via_ascii_copy(monkeypatch, tmp_path: Path) -> None:
    source_dir = tmp_path / "Обмен"
    source_dir.mkdir()
    source = source_dir / UNICODE_NAME
    source.write_bytes(b"%PDF-1.4 fake")
    seen, temp_dir = _fake_pdf2image(monkeypatch, tmp_path, pages=3)

    pages = list(_iter_pdf_pages(source, batch_pages=2))

    assert [number for number, _total, _img in pages] == [1, 2, 3]
    assert seen["info"] and seen["convert"]
    for passed in seen["info"] + seen["convert"]:
        assert passed.isascii(), passed
        assert passed != str(source)
        assert Path(passed).parent.parent == temp_dir
    # Одна копия на весь рендер (pdfinfo + все батчи), после — удалена.
    assert len(set(seen["info"] + seen["convert"])) == 1
    assert list(temp_dir.iterdir()) == []


def test_iter_pdf_pages_keeps_ascii_path_as_is(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "plain.pdf"
    source.write_bytes(b"%PDF-1.4 fake")
    seen, temp_dir = _fake_pdf2image(monkeypatch, tmp_path, pages=1)

    list(_iter_pdf_pages(source))

    assert seen["info"] == [str(source)]
    assert seen["convert"] == [str(source)]
    assert list(temp_dir.iterdir()) == []


def test_pdf_page_count_uses_ascii_copy(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "Счёт №12.pdf"
    source.write_bytes(b"%PDF-1.4 fake")
    seen, temp_dir = _fake_pdf2image(monkeypatch, tmp_path, pages=7)

    assert _pdf_page_count(source) == 7
    assert len(seen["info"]) == 1
    assert seen["info"][0].isascii()
    assert list(temp_dir.iterdir()) == []


def test_poppler_safe_pdf_cleans_up_after_render_error(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "Скан.pdf"
    source.write_bytes(b"%PDF-1.4 fake")
    temp_dir = tmp_path / "ascii_tmp"
    temp_dir.mkdir()
    monkeypatch.setattr(files_module, "_ascii_temp_dir", lambda: str(temp_dir))

    with pytest.raises(RuntimeError, match="poppler failed"):
        with _PopplerSafePdf(source) as safe_path:
            assert safe_path.is_file()
            assert str(safe_path).isascii()
            raise RuntimeError("poppler failed")

    assert list(temp_dir.iterdir()) == []


def test_poppler_safe_pdf_does_not_copy_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "Нет такого.pdf"

    with _PopplerSafePdf(missing) as safe_path:
        assert safe_path == missing
