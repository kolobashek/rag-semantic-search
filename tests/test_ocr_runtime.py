from __future__ import annotations

import types
from pathlib import Path

from rag_catalog.core import ocr_runtime


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
