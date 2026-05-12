"""Document and image text extraction helpers.

These functions deliberately return an empty string on recoverable extraction
errors. The indexer owns telemetry/stage accounting; extractors only read files
and log the concrete failure.
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Any, Callable

from docx import Document
from openpyxl import load_workbook

from rag_catalog.core.ocr_runtime import apply_tesseract_runtime

logger = logging.getLogger(__name__)


def extract_docx(filepath: Path) -> str:
    """Extract text from DOCX paragraphs and table cells."""
    try:
        doc = Document(filepath)
        parts = [p.text for p in doc.paragraphs]
        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    parts.append(cell.text)
        return "\n".join(parts)
    except Exception as exc:
        logger.warning("Ошибка чтения DOCX %s: %s", filepath, exc)
        return ""


def extract_xlsx(filepath: Path, *, max_chars: int = 0) -> str:
    """Extract text from XLSX with optional early stop by accumulated chars."""
    try:
        wb = load_workbook(filepath, read_only=True, data_only=True)
        parts: list[str] = []
        total_chars = 0
        done = False
        for sheet_name in wb.sheetnames:
            if done:
                break
            ws = wb[sheet_name]
            parts.append(f"Лист: {sheet_name}")
            for row in ws.iter_rows(values_only=True):
                row_text = " | ".join(str(c) if c is not None else "" for c in row)
                if row_text.strip():
                    parts.append(row_text)
                    total_chars += len(row_text)
                    if max_chars and total_chars >= max_chars:
                        done = True
                        break
        return "\n".join(parts)
    except Exception as exc:
        logger.warning("Ошибка чтения XLSX %s: %s", filepath, exc)
        return ""


def extract_xls(filepath: Path, *, max_chars: int = 0) -> str:
    """Extract text from legacy XLS files via xlrd."""
    try:
        import xlrd  # type: ignore
    except ImportError:
        logger.warning("xlrd не установлен. Установите: pip install xlrd")
        return ""
    try:
        wb = xlrd.open_workbook(str(filepath))
        parts: list[str] = []
        total_chars = 0
        done = False
        for sheet in wb.sheets():
            if done:
                break
            parts.append(f"Лист: {sheet.name}")
            for row_idx in range(sheet.nrows):
                row = sheet.row_values(row_idx)
                row_text = " | ".join(str(v) if v not in ("", None) else "" for v in row)
                if row_text.strip():
                    parts.append(row_text)
                    total_chars += len(row_text)
                    if max_chars and total_chars >= max_chars:
                        done = True
                        break
        return "\n".join(parts)
    except Exception as exc:
        logger.warning("Ошибка чтения XLS %s: %s", filepath, exc)
        return ""


def extract_spreadsheet(filepath: Path, *, max_chars: int = 0) -> str:
    """Route spreadsheet extraction by extension."""
    ext = filepath.suffix.lower()
    if ext == ".xls":
        logger.debug("Формат XLS — использую xlrd: %s", filepath.name)
        return extract_xls(filepath, max_chars=max_chars)
    if ext == ".xlsx":
        logger.debug("Формат XLSX — использую openpyxl: %s", filepath.name)
        return extract_xlsx(filepath, max_chars=max_chars)
    logger.warning("Неизвестное табличное расширение: %s", ext)
    return ""


def extract_pdf(filepath: Path, *, skip_ocr: bool = False, ocr: Callable[[Path], str] | None = None) -> str:
    """Extract PDF text via pymupdf/pdfplumber, then OCR when text layer is empty."""
    try:
        import fitz  # pymupdf

        parts: list[str] = []
        with fitz.open(str(filepath)) as doc:
            for page_idx, page in enumerate(doc, start=1):
                text = page.get_text()
                if text and text.strip():
                    parts.append(f"Страница: {page_idx}\n{text}")
        full_text = "\n".join(parts).strip()
        if full_text:
            return full_text
        if skip_ocr:
            logger.debug("Нет текстового слоя, OCR пропущен (--no-ocr): %s", filepath.name)
            return ""
        logger.info("Нет текстового слоя в %s — запуск OCR…", filepath.name)
        return ocr(filepath) if ocr else ""
    except ImportError:
        logger.debug("pymupdf не установлен, использую pdfplumber")
    except Exception as exc:
        logger.warning("pymupdf: ошибка чтения %s: %s", filepath.name, exc)
        return ""

    try:
        import pdfplumber  # type: ignore
    except ImportError:
        logger.warning("Ни pymupdf, ни pdfplumber не установлены. pip install pymupdf")
        return ""
    try:
        parts = []
        with pdfplumber.open(filepath) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                text = page.extract_text()
                if text and text.strip():
                    parts.append(f"Страница: {page_idx}\n{text}")
        full_text = "\n".join(parts).strip()
        if full_text:
            return full_text
        if skip_ocr:
            logger.debug("Нет текстового слоя, OCR пропущен (--no-ocr): %s", filepath.name)
            return ""
        logger.info("Нет текстового слоя в %s — запуск OCR…", filepath.name)
        return ocr(filepath) if ocr else ""
    except Exception as exc:
        logger.warning("pdfplumber: ошибка чтения %s: %s", filepath.name, exc)
        return ""


def _windows_hidden_popen_kwargs() -> dict[str, Any]:
    """kwargs for subprocess calls without visible console window on Windows."""
    if os.name != "nt":
        return {}
    kwargs: dict[str, Any] = {"creationflags": int(getattr(subprocess, "CREATE_NO_WINDOW", 0) or 0)}
    if hasattr(subprocess, "STARTUPINFO"):
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
        kwargs["startupinfo"] = si
    return kwargs


def _patch_pdf2image_popen_for_windows(pdf2image_module: Any) -> None:
    """Patch pdf2image internal Popen to suppress console flicker on Windows."""
    if os.name != "nt":
        return
    if getattr(pdf2image_module, "_rag_hidden_popen_patched", False):
        return
    original_popen = getattr(pdf2image_module, "Popen", None)
    if original_popen is None:
        return

    def _hidden_popen(*args: Any, **kwargs: Any) -> Any:
        hidden = _windows_hidden_popen_kwargs()
        for key, value in hidden.items():
            kwargs.setdefault(key, value)
        return original_popen(*args, **kwargs)

    pdf2image_module.Popen = _hidden_popen
    pdf2image_module._rag_hidden_popen_patched = True


def ocr_pdf(filepath: Path, *, tesseract_cmd: str = "", poppler_bin: str = "") -> str:
    """OCR scanned PDF through pytesseract + pdf2image."""
    try:
        import pdf2image.pdf2image as pdf2image_impl  # type: ignore
        import pytesseract  # type: ignore
        from pdf2image import convert_from_path  # type: ignore
    except ImportError:
        logger.warning(
            "pytesseract/pdf2image не установлены. "
            "Установите: pip install pytesseract pdf2image"
        )
        return ""

    try:
        apply_tesseract_runtime(pytesseract, tesseract_cmd)
        _patch_pdf2image_popen_for_windows(pdf2image_impl)
        convert_kwargs: dict[str, Any] = {"dpi": 200}
        if str(poppler_bin or "").strip():
            convert_kwargs["poppler_path"] = str(poppler_bin).strip()
        pages = convert_from_path(str(filepath), **convert_kwargs)
        parts: list[str] = []
        for i, page_img in enumerate(pages):
            text = pytesseract.image_to_string(page_img, lang="rus+eng")
            chars = len(text.strip())
            if text.strip():
                parts.append(f"Страница: {i + 1}\n{text}")
            logger.info("OCR страница %d/%d — %d симв. — %s", i + 1, len(pages), chars, filepath.name)
        total_chars = sum(len(p) for p in parts)
        if parts:
            logger.info("OCR завершён: %s — %d стр., %d симв.", filepath.name, len(pages), total_chars)
        else:
            logger.warning("OCR не извлёк текст ни на одной странице: %s", filepath.name)
        return "\n".join(parts)
    except Exception as exc:
        logger.warning("OCR не удался для %s: %s", filepath, exc)
        return ""


def extract_image(filepath: Path, *, tesseract_cmd: str = "", max_pages: int = 50) -> str:
    """Extract text from an image through pytesseract OCR."""
    try:
        import pytesseract  # type: ignore
    except ImportError:
        logger.debug(
            "pytesseract не установлен — OCR изображений недоступен. "
            "Установите: pip install pytesseract"
        )
        return ""
    try:
        from PIL import Image  # type: ignore
    except ImportError:
        logger.debug(
            "Pillow не установлен — OCR изображений недоступен. "
            "Установите: pip install Pillow"
        )
        return ""
    try:
        apply_tesseract_runtime(pytesseract, tesseract_cmd)
        parts: list[str] = []
        with Image.open(filepath) as img:
            n_frames: int = getattr(img, "n_frames", 1)
            if n_frames > max_pages:
                logger.warning(
                    "Изображение %s содержит %d кадров — обрабатываем только первые %d "
                    "(MAX_IMAGE_PAGES). Остальные пропущены.",
                    filepath.name,
                    n_frames,
                    max_pages,
                )
                n_frames = max_pages
            for frame_idx in range(n_frames):
                if n_frames > 1:
                    img.seek(frame_idx)
                    frame = img.copy()
                else:
                    frame = img
                if frame.mode not in ("RGB", "L", "RGBA"):
                    frame = frame.convert("RGB")
                page_text = pytesseract.image_to_string(frame, lang="rus+eng").strip()
                if page_text:
                    parts.append(page_text)
                logger.info(
                    "OCR %s стр.%d/%d: %d симв.",
                    filepath.name,
                    frame_idx + 1,
                    n_frames,
                    len(page_text),
                )
        result = "\n".join(parts).strip()
        if result:
            logger.info("OCR завершён: %s — %d симв., %d стр.", filepath.name, len(result), n_frames)
        else:
            logger.warning("OCR не извлёк текст: %s", filepath.name)
        return result
    except Exception as exc:
        logger.warning("OCR изображения не удался для %s: %s", filepath, exc)
        return ""
