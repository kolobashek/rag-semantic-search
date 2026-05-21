from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

from rag_catalog.core.extractors.files import extract_doc, extract_pptx, extract_rtf


def test_extract_rtf_without_optional_dependency(tmp_path: Path) -> None:
    path = tmp_path / "note.rtf"
    path.write_text(r"{\rtf1\ansi Hello \b world\b0}", encoding="utf-8")

    text = extract_rtf(path)

    assert "Hello" in text
    assert "world" in text


def test_extract_pptx_reads_slide_text(tmp_path: Path) -> None:
    path = tmp_path / "deck.pptx"
    slide_xml = """<?xml version="1.0" encoding="UTF-8"?>
<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"
       xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">
  <p:cSld><p:spTree><p:sp><p:txBody>
    <a:p><a:r><a:t>Первый слайд</a:t></a:r></a:p>
  </p:txBody></p:sp></p:spTree></p:cSld>
</p:sld>
"""
    with ZipFile(path, "w", ZIP_DEFLATED) as zf:
        zf.writestr("ppt/slides/slide1.xml", slide_xml)

    text = extract_pptx(path)

    assert "Слайд: 1" in text
    assert "Первый слайд" in text


def test_extract_doc_uses_antiword_when_available(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "legacy.doc"
    path.write_bytes(b"dummy")

    monkeypatch.setattr("rag_catalog.core.extractors.files.shutil.which", lambda name: "antiword" if name == "antiword" else None)
    monkeypatch.setattr(
        "rag_catalog.core.extractors.files.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="legacy text", stderr=""),
    )

    assert extract_doc(path) == "legacy text"
