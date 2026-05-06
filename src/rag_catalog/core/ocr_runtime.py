"""Runtime helpers for bundled OCR binaries (tesseract + poppler)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TOOLS_ROOT = PROJECT_ROOT / "tools"


def _iter_existing_paths(paths: Iterable[Path]) -> Iterable[Path]:
    for path in paths:
        if path.exists():
            yield path


def _resolve_from_config_or_env(
    config_value: Any,
    env_name: str,
    candidates: Iterable[Path],
) -> Optional[Path]:
    explicit = str(config_value or "").strip()
    if explicit:
        path = Path(explicit)
        if path.exists():
            return path
    env_value = os.environ.get(env_name, "").strip()
    if env_value:
        path = Path(env_value)
        if path.exists():
            return path
    return next(iter(_iter_existing_paths(candidates)), None)


def resolve_ocr_runtime(config: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """Resolve absolute paths to OCR binaries bundled inside the project."""
    cfg = config or {}

    tesseract_candidates = [
        TOOLS_ROOT / "tesseract" / "tesseract.exe",
        TOOLS_ROOT / "tesseract" / "bin" / "tesseract.exe",
        TOOLS_ROOT / "Tesseract-OCR" / "tesseract.exe",
        TOOLS_ROOT / "tesseract" / "tesseract",
        TOOLS_ROOT / "tesseract" / "bin" / "tesseract",
    ]
    poppler_candidates = [
        TOOLS_ROOT / "poppler" / "Library" / "bin",
        TOOLS_ROOT / "poppler" / "bin",
    ]

    tesseract_path = _resolve_from_config_or_env(
        cfg.get("ocr_tesseract_cmd"),
        "RAG_TESSERACT_CMD",
        tesseract_candidates,
    )
    poppler_bin = _resolve_from_config_or_env(
        cfg.get("ocr_poppler_bin"),
        "RAG_POPPLER_BIN",
        poppler_candidates,
    )

    return {
        "tesseract_cmd": str(tesseract_path) if tesseract_path else "",
        "poppler_bin": str(poppler_bin) if poppler_bin else "",
        "tools_root": str(TOOLS_ROOT),
    }


def apply_tesseract_runtime(pytesseract_module: Any, tesseract_cmd: str) -> None:
    """Configure pytesseract to use an explicit executable path."""
    path = str(tesseract_cmd or "").strip()
    if not path:
        return
    target = getattr(pytesseract_module, "pytesseract", pytesseract_module)
    setattr(target, "tesseract_cmd", path)
