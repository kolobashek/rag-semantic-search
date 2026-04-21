from __future__ import annotations

import types

from index_rag import RAGIndexer


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

