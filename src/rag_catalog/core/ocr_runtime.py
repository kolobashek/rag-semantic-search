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


_IMAGE_EXTS = frozenset({".png", ".jpg", ".jpeg", ".bmp", ".gif", ".tiff", ".tif", ".webp"})


def recognize_single_file(
    path: "Path",
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Extract text from a single PDF or image, caching the result in telemetry DB.

    Returns dict: {status, text, pages, chars, from_cache, error}.
    Statuses: 'ok' | 'empty' | 'error' | 'unsupported'.
    """
    # Lazy imports to avoid circular dependency (ocr_runtime ← extractors.files)
    import functools  # noqa: PLC0415

    from rag_catalog.core.extractors.files import extract_image, extract_pdf, ocr_pdf  # noqa: PLC0415
    from rag_catalog.core.telemetry_db import TelemetryDB  # noqa: PLC0415

    _cfg: Dict[str, Any] = cfg or {}
    if not _cfg:
        try:
            from rag_catalog.core.rag_core import load_config  # noqa: PLC0415
            _cfg = load_config()
        except Exception:
            pass

    db_path_str = str(_cfg.get("telemetry_db_path") or "").strip() or str(
        Path(str(_cfg.get("qdrant_db_path") or "")) / "rag_telemetry.db"
    )
    ocr_rt = resolve_ocr_runtime(_cfg)

    try:
        mtime = float(path.stat().st_mtime)
    except Exception as exc:
        return {"status": "error", "text": "", "pages": 0, "chars": 0, "from_cache": False, "error": str(exc)}

    tdb = TelemetryDB(db_path_str)

    cached = tdb.get_ocr_file_result(str(path), mtime)
    if cached is not None:
        return {
            "status": str(cached.get("status") or "ok"),
            "text": str(cached.get("extracted_text") or ""),
            "pages": int(cached.get("pages") or 0),
            "chars": int(cached.get("char_count") or 0),
            "from_cache": True,
            "error": str(cached.get("error_text") or ""),
        }

    use_rapid = str(_cfg.get("ocr_engine") or "tesseract").strip().lower() == "rapidocr"

    ext = path.suffix.lower()
    try:
        if ext == ".pdf":
            _ocr_fn = functools.partial(
                ocr_pdf,
                tesseract_cmd=ocr_rt["tesseract_cmd"],
                poppler_bin=ocr_rt["poppler_bin"],
                use_rapid=use_rapid,
            )
            text = extract_pdf(path, skip_ocr=False, ocr=_ocr_fn)
        elif ext in _IMAGE_EXTS:
            text = extract_image(
                path,
                tesseract_cmd=ocr_rt["tesseract_cmd"],
                use_rapid=use_rapid,
            )
        else:
            return {"status": "unsupported", "text": "", "pages": 0, "chars": 0, "from_cache": False, "error": f"Формат не поддерживается: {ext}"}
    except Exception as exc:
        tdb.save_ocr_file_result(str(path), mtime, status="error", error=str(exc))
        return {"status": "error", "text": "", "pages": 0, "chars": 0, "from_cache": False, "error": str(exc)}

    pages = text.count("Страница:") if text else 0
    if pages == 0 and text.strip():
        pages = 1
    chars = len(text.strip())
    status = "ok" if chars > 0 else "empty"

    tdb.save_ocr_file_result(str(path), mtime, text=text, pages=pages, chars=chars, status=status)
    return {"status": status, "text": text, "pages": pages, "chars": chars, "from_cache": False, "error": ""}
