from __future__ import annotations

import types
from pathlib import Path

import pytest

from rag_catalog.core import ocr_runtime
from rag_catalog.core.extractors.files import _iter_pdf_pages
from rag_catalog.core.extractors.ocr_rapid import _ocr_pdf_rapid_impl


def test_resolve_ocr_runtime_uses_config_paths(tmp_path: Path) -> None:
    tess = tmp_path / "tesseract.exe"
    poppler = tmp_path / "poppler_bin"
    tess.write_text("x", encoding="utf-8")
    poppler.mkdir()

    runtime = ocr_runtime.resolve_ocr_runtime(
        {
            "ocr_tesseract_cmd": str(tess),
            "ocr_poppler_bin": str(poppler),
        }
    )
    assert runtime["tesseract_cmd"] == str(tess)
    assert runtime["poppler_bin"] == str(poppler)


def test_resolve_ocr_runtime_uses_tools_layout(monkeypatch, tmp_path: Path) -> None:
    tools = tmp_path / "tools"
    tess = tools / "tesseract" / "tesseract.exe"
    poppler = tools / "poppler" / "Library" / "bin"
    tess.parent.mkdir(parents=True)
    poppler.mkdir(parents=True)
    tess.write_text("x", encoding="utf-8")

    monkeypatch.setattr(ocr_runtime, "TOOLS_ROOT", tools)
    runtime = ocr_runtime.resolve_ocr_runtime({})
    assert runtime["tesseract_cmd"] == str(tess)
    assert runtime["poppler_bin"] == str(poppler)


def test_apply_tesseract_runtime_sets_nested_attr() -> None:
    holder = types.SimpleNamespace(tesseract_cmd="")
    fake = types.SimpleNamespace(pytesseract=holder)
    ocr_runtime.apply_tesseract_runtime(fake, "C:/tools/tesseract.exe")
    assert holder.tesseract_cmd == "C:/tools/tesseract.exe"


def test_rapidocr_pdf_conversion_failure_is_not_reported_as_empty(monkeypatch, tmp_path: Path) -> None:
    import pdf2image

    monkeypatch.setattr(pdf2image, "pdfinfo_from_path", lambda *_args, **_kwargs: {"Pages": 1})
    monkeypatch.setattr(
        pdf2image,
        "convert_from_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("poppler failed")),
    )

    with pytest.raises(RuntimeError, match="RapidOCR PDF conversion failed"):
        _ocr_pdf_rapid_impl(tmp_path / "broken.pdf")


def test_iter_pdf_pages_renders_bounded_batches(monkeypatch, tmp_path: Path) -> None:
    import pdf2image

    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(pdf2image, "pdfinfo_from_path", lambda *_args, **_kwargs: {"Pages": 5})

    def fake_convert(*_args, **kwargs):
        first_page = int(kwargs["first_page"])
        last_page = int(kwargs["last_page"])
        calls.append((first_page, last_page))
        return [types.SimpleNamespace(number=page) for page in range(first_page, last_page + 1)]

    monkeypatch.setattr(pdf2image, "convert_from_path", fake_convert)

    pages = list(_iter_pdf_pages(tmp_path / "five-pages.pdf", batch_pages=2))

    assert calls == [(1, 2), (3, 4), (5, 5)]
    assert [(number, total, image.number) for number, total, image in pages] == [
        (1, 5, 1),
        (2, 5, 2),
        (3, 5, 3),
        (4, 5, 4),
        (5, 5, 5),
    ]
