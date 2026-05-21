from __future__ import annotations

import types

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
