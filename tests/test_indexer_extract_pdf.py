from __future__ import annotations

import threading
import types
from unittest.mock import MagicMock

import pytest

from index_rag import RAGIndexer
from rag_catalog.core.extractors import extract_pdf_document


def test_extract_pdf_returns_empty_on_fitz_runtime_error(monkeypatch, tmp_path):
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.skip_ocr = True
    idx._ocr_pdf = lambda _p: "never"

    fake_fitz = types.SimpleNamespace(open=lambda _p: (_ for _ in ()).throw(RuntimeError("fitz boom")))
    monkeypatch.setitem(__import__("sys").modules, "fitz", fake_fitz)

    p = tmp_path / "x.pdf"
    p.write_bytes(b"%PDF-1.7")
    out = idx._extract_pdf(p)
    assert out == ""


def test_extract_pdf_document_returns_page_blocks(monkeypatch, tmp_path):
    class _Page:
        def __init__(self, text):
            self._text = text

        def get_text(self):
            return self._text

    class _Doc:
        def __iter__(self):
            return iter([_Page("page one"), _Page("page two")])

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    fake_fitz = types.SimpleNamespace(open=lambda _p: _Doc())
    monkeypatch.setitem(__import__("sys").modules, "fitz", fake_fitz)

    p = tmp_path / "x.pdf"
    p.write_bytes(b"%PDF-1.7")
    doc = extract_pdf_document(p, skip_ocr=True)

    assert [block.page for block in doc.blocks] == [1, 2]
    assert [block.text for block in doc.blocks] == ["page one", "page two"]


def test_ocr_pdf_records_backend_failure(monkeypatch, tmp_path):
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.telemetry = MagicMock()
    idx.telemetry.get_ocr_file_result.return_value = None
    idx.ocr_tesseract_cmd = ""
    idx.ocr_poppler_bin = ""
    idx._use_rapid_ocr = True
    path = tmp_path / "broken.pdf"
    path.write_bytes(b"%PDF-1.7")

    def fail_ocr(*_args, **_kwargs):
        raise RuntimeError("backend unavailable")

    monkeypatch.setattr("rag_catalog.core.index_rag.ocr_pdf", fail_ocr)

    with pytest.raises(RuntimeError, match="backend unavailable"):
        idx._ocr_pdf(path)

    saved = idx.telemetry.save_ocr_file_result.call_args
    assert saved.kwargs["status"] == "error"
    assert saved.kwargs["error"] == "backend unavailable"


def test_ocr_pdf_uses_logical_archive_identity(monkeypatch, tmp_path):
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.telemetry = MagicMock()
    idx.telemetry.get_ocr_file_result.return_value = None
    idx._ocr_context = threading.local()
    idx._ocr_context.logical_path = "C:/catalog/docs.zip::scans/invoice.pdf"
    idx._ocr_context.logical_mtime = 1234.5
    path = tmp_path / "invoice.pdf"
    path.write_bytes(b"%PDF-1.7")
    monkeypatch.setattr("rag_catalog.core.index_rag.ocr_pdf", lambda *_args, **_kwargs: "invoice text")

    assert idx._ocr_pdf(path) == "invoice text"

    idx.telemetry.get_ocr_file_result.assert_called_once_with(
        "C:/catalog/docs.zip::scans/invoice.pdf",
        1234.5,
    )
    saved = idx.telemetry.save_ocr_file_result.call_args
    assert saved.args[:2] == ("C:/catalog/docs.zip::scans/invoice.pdf", 1234.5)
    assert saved.kwargs["status"] == "ok"


def test_ocr_pdf_retries_cached_error(monkeypatch, tmp_path):
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.telemetry = MagicMock()
    idx.telemetry.get_ocr_file_result.return_value = {
        "status": "error",
        "extracted_text": "",
    }
    path = tmp_path / "retry.pdf"
    path.write_bytes(b"%PDF-1.7")
    calls = []

    def recover(*_args, **_kwargs):
        calls.append(True)
        return "recovered text"

    monkeypatch.setattr("rag_catalog.core.index_rag.ocr_pdf", recover)

    assert idx._ocr_pdf(path) == "recovered text"
    assert calls == [True]
    assert idx.telemetry.save_ocr_file_result.call_args.kwargs["status"] == "ok"
