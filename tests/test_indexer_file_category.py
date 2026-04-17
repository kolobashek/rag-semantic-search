from __future__ import annotations

from index_rag import _file_category


def test_file_category_respects_thresholds(tmp_path):
    doc = tmp_path / "a.docx"
    pdf = tmp_path / "b.pdf"
    big_pdf = tmp_path / "c.pdf"
    doc.write_bytes(b"x" * 1024)  # tiny
    pdf.write_bytes(b"x" * 1024)  # tiny
    big_pdf.write_bytes(b"x" * (4 * 1024 * 1024))  # 4 MB

    assert _file_category(doc, small_office_mb=20.0, small_pdf_mb=2.0) == "small"
    assert _file_category(pdf, small_office_mb=20.0, small_pdf_mb=2.0) == "small"
    assert _file_category(big_pdf, small_office_mb=20.0, small_pdf_mb=2.0) == "large"

