from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

from rag_catalog.core.extractors.files import (
    _antiword_environment,
    _resolve_soffice,
    extract_doc,
    extract_pptx,
    extract_pptx_document,
    extract_rtf,
)


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


def test_extract_pptx_document_returns_slide_blocks(tmp_path: Path) -> None:
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

    doc = extract_pptx_document(path)

    assert doc.blocks[0].slide == 1
    assert doc.blocks[0].text == "Первый слайд"


def test_extract_doc_uses_antiword_when_available(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "legacy.doc"
    path.write_bytes(b"dummy")

    monkeypatch.setattr("rag_catalog.core.extractors.files.shutil.which", lambda name: "antiword" if name == "antiword" else None)
    monkeypatch.setattr(
        "rag_catalog.core.extractors.files.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="legacy text", stderr=""),
    )

    assert extract_doc(path) == "legacy text"


def test_extract_doc_uses_env_antiword_before_path(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "legacy.doc"
    path.write_bytes(b"dummy")
    antiword = tmp_path / "antiword.exe"
    antiword.write_bytes(b"fake")

    monkeypatch.setenv("RAG_ANTIWORD_CMD", str(antiword))
    monkeypatch.setattr("rag_catalog.core.extractors.files.shutil.which", lambda _name: None)
    monkeypatch.setattr(
        "rag_catalog.core.extractors.files.subprocess.run",
        lambda args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="bundled text" if str(args[0]) == str(antiword) else "",
            stderr="",
        ),
    )

    assert extract_doc(path) == "bundled text"


def test_antiword_environment_finds_bundled_share(tmp_path: Path, monkeypatch) -> None:
    binary = tmp_path / "doc2txt" / "bin" / "win-amd64" / "antiword.exe"
    binary.parent.mkdir(parents=True)
    binary.write_bytes(b"fake")
    share = tmp_path / "doc2txt" / "antiword_share"
    share.mkdir()
    monkeypatch.delenv("RAG_ANTIWORD_HOME", raising=False)

    env = _antiword_environment(str(binary))

    assert env["ANTIWORDHOME"] == str(share)


def test_resolve_soffice_finds_standard_windows_install(tmp_path: Path, monkeypatch) -> None:
    program_files = tmp_path / "Program Files"
    soffice = program_files / "LibreOffice" / "program" / "soffice.exe"
    soffice.parent.mkdir(parents=True)
    soffice.write_bytes(b"fake")

    monkeypatch.delenv("RAG_SOFFICE_CMD", raising=False)
    monkeypatch.delenv("RAG_LIBREOFFICE_CMD", raising=False)
    monkeypatch.delenv("SOFFICE", raising=False)
    monkeypatch.setenv("ProgramFiles", str(program_files))
    monkeypatch.setenv("ProgramFiles(x86)", str(tmp_path / "Program Files (x86)"))
    monkeypatch.setattr("rag_catalog.core.extractors.files.shutil.which", lambda _name: None)

    assert _resolve_soffice() == str(soffice)


def test_extract_doc_uses_isolated_libreoffice_profile(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "legacy.doc"
    path.write_bytes(b"dummy")
    soffice = tmp_path / "soffice.exe"
    soffice.write_bytes(b"fake")
    captured_args: list[str] = []
    captured_env: dict[str, str] = {}
    local_source_was_used = False

    monkeypatch.setattr("rag_catalog.core.extractors.files._resolve_antiword", lambda: "")
    monkeypatch.setattr("rag_catalog.core.extractors.files._resolve_soffice", lambda: str(soffice))

    def _run(args, **kwargs):
        nonlocal local_source_was_used
        captured_args.extend(str(item) for item in args)
        captured_env.update(kwargs["env"])
        local_source = Path(args[-1])
        local_source_was_used = local_source.parent != path.parent and local_source.read_bytes() == b"dummy"
        out_dir = Path(args[args.index("--outdir") + 1])
        (out_dir / "legacy.txt").write_text("converted text", encoding="utf-8")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("rag_catalog.core.extractors.files.subprocess.run", _run)

    assert extract_doc(path) == "converted text"
    assert any(arg.startswith("-env:UserInstallation=file:") for arg in captured_args)
    assert {"--headless", "--invisible", "--norestore", "--nolockcheck"}.issubset(captured_args)
    assert captured_env["SAL_DISABLE_PRINTERLIST"] == "1"
    assert captured_env["SAL_USE_VCLPLUGIN"] == "svp"
    assert local_source_was_used


def test_extract_doc_binary_fallback_reads_text_runs(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "legacy.doc"
    path.write_bytes(b"\xd0\xcf\x11\xe0" + "Договор аренды спецтехники 8905".encode("utf-16le") + b"\x00" * 32)

    monkeypatch.delenv("RAG_ANTIWORD_CMD", raising=False)
    monkeypatch.delenv("RAG_SOFFICE_CMD", raising=False)
    monkeypatch.setattr("rag_catalog.core.extractors.files.shutil.which", lambda _name: None)
    monkeypatch.setattr("rag_catalog.core.extractors.files._resolve_antiword", lambda: "")
    monkeypatch.setattr("rag_catalog.core.extractors.files._resolve_soffice", lambda: "")

    text = extract_doc(path)

    assert "Договор аренды спецтехники 8905" in text
