from __future__ import annotations

import types
from io import BytesIO
from pathlib import Path

import pytest

from rag_catalog.core import ocr_runtime
from rag_catalog.core.extractors import UnreadableSourceError, is_unreadable_source_error
from rag_catalog.core.extractors.files import _iter_pdf_pages, extract_image, ocr_pdf
from rag_catalog.core.extractors.ocr_rapid import (
    _ocr_image_rapid_impl,
    _ocr_pdf_rapid_impl,
    ocr_image_rapid,
)


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


def test_rapidocr_worker_accepts_truncated_jpeg(monkeypatch, tmp_path: Path) -> None:
    from PIL import Image, ImageFile

    encoded = BytesIO()
    Image.new("RGB", (64, 64), "white").save(encoded, format="JPEG")
    source = tmp_path / "truncated.jpg"
    source.write_bytes(encoded.getvalue()[:-10])
    original_setting = bool(ImageFile.LOAD_TRUNCATED_IMAGES)
    monkeypatch.setattr("rag_catalog.core.extractors.ocr_rapid._img_to_text", lambda _image: "recovered")

    assert _ocr_image_rapid_impl(source) == "recovered"
    assert bool(ImageFile.LOAD_TRUNCATED_IMAGES) is original_setting


def test_rapidocr_image_reports_unreadable_source(tmp_path: Path) -> None:
    source = tmp_path / "invalid.jpg"
    source.write_bytes(b"not an image")

    with pytest.raises(UnreadableSourceError, match="unreadable image source"):
        ocr_image_rapid(source)


def test_unreadable_source_recognizes_wrapped_worker_error() -> None:
    source_error = UnreadableSourceError("unreadable image source")
    try:
        raise RuntimeError("worker failed") from source_error
    except RuntimeError as wrapped:
        assert is_unreadable_source_error(wrapped) is True


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


def test_iter_pdf_pages_respects_requested_range(monkeypatch, tmp_path: Path) -> None:
    import pdf2image

    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(pdf2image, "pdfinfo_from_path", lambda *_args, **_kwargs: {"Pages": 8})

    def fake_convert(*_args, **kwargs):
        first_page = int(kwargs["first_page"])
        last_page = int(kwargs["last_page"])
        calls.append((first_page, last_page))
        return [types.SimpleNamespace(number=page) for page in range(first_page, last_page + 1)]

    monkeypatch.setattr(pdf2image, "convert_from_path", fake_convert)

    pages = list(
        _iter_pdf_pages(
            tmp_path / "eight-pages.pdf",
            batch_pages=2,
            first_page=3,
            last_page=6,
        )
    )

    assert calls == [(3, 4), (5, 6)]
    assert [number for number, _total, _image in pages] == [3, 4, 5, 6]


def test_isolated_pdf_range_splits_failed_batch(monkeypatch, tmp_path: Path) -> None:
    from rag_catalog.core.extractors import ocr_rapid

    calls: list[tuple[int, int]] = []

    def fake_worker(_path, *, mode, first, last, poppler_bin=""):
        calls.append((first, last))
        assert mode == "pdf"
        if first != last:
            raise RuntimeError("range too large")
        return f"page {first}"

    monkeypatch.setattr(ocr_rapid, "_run_isolated_worker", fake_worker)

    text = ocr_rapid._run_pdf_range_isolated(
        tmp_path / "scan.pdf",
        first=1,
        last=4,
        poppler_bin="",
    )

    assert text.split() == ["page", "1", "page", "2", "page", "3", "page", "4"]
    assert calls[0] == (1, 4)
    assert (1, 1) in calls and (4, 4) in calls


def test_rapidocr_failure_does_not_fallback_when_disabled(monkeypatch, tmp_path: Path) -> None:
    def fail_rapid(*_args, **_kwargs):
        raise RuntimeError("directml failed")

    monkeypatch.setattr("rag_catalog.core.extractors.ocr_rapid.ocr_pdf_rapid", fail_rapid)
    diagnostics: dict[str, object] = {}

    with pytest.raises(RuntimeError, match="RapidOCR PDF failed"):
        ocr_pdf(
            tmp_path / "scan.pdf",
            use_rapid=True,
            rapid_fallback_enabled=False,
            diagnostics=diagnostics,
        )

    assert diagnostics == {
        "requested_engine": "rapidocr",
        "engine": "rapidocr",
        "fallback_used": False,
        "fallback_reason": "directml failed",
    }


def test_rapidocr_image_failure_does_not_fallback_when_disabled(monkeypatch, tmp_path: Path) -> None:
    def fail_rapid(*_args, **_kwargs):
        raise RuntimeError("directml image failed")

    monkeypatch.setattr("rag_catalog.core.extractors.ocr_rapid.ocr_image_rapid", fail_rapid)
    diagnostics: dict[str, object] = {}

    with pytest.raises(RuntimeError, match="RapidOCR image failed"):
        extract_image(
            tmp_path / "scan.tiff",
            use_rapid=True,
            rapid_fallback_enabled=False,
            diagnostics=diagnostics,
        )

    assert diagnostics["requested_engine"] == "rapidocr"
    assert diagnostics["engine"] == "rapidocr"
    assert diagnostics["fallback_used"] is False
    assert diagnostics["fallback_reason"] == "directml image failed"
