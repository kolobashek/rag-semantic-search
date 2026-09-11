"""Политика «отложенного OCR» (status=deferred_ocr) при индексации с --no-ocr.

Общий код для index_rag.RAGIndexer и indexing.stage_runner: вынесен отдельно,
чтобы не плодить циклические импорты (index_rag → indexing → stage_runner).
"""

from __future__ import annotations

from typing import Any, Optional

# Расширения изображений (OCR целиком).
IMAGE_EXTENSIONS: frozenset[str] = frozenset({".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".bmp", ".webp"})
# OOXML-контейнеры, внутри которых могут лежать вставленные сканы (word/media/…).
EMBEDDED_MEDIA_EXTENSIONS: frozenset[str] = frozenset({".docx", ".docm", ".xlsx", ".xlsm", ".pptx", ".pptm"})


def is_deferred_ocr_candidate(extension: str, status: str) -> bool:
    """Запись state, которую при skip_ocr бессмысленно перечитывать до включения OCR.

    PDF/картинки: status deferred_ocr или empty (текстового слоя нет, только OCR).
    Office-контейнеры: только deferred_ocr (внутри есть картинки, ждущие OCR);
    «пустой» docx без картинок остаётся на обычном backoff-повторе.
    """
    ext = str(extension or "").lower()
    st = str(status or "").lower()
    if ext == ".pdf" or ext in IMAGE_EXTENSIONS:
        return st in {"deferred_ocr", "empty"}
    if ext in EMBEDDED_MEDIA_EXTENSIONS:
        return st == "deferred_ocr"
    return False


def document_has_deferred_embedded_ocr(doc: Optional[Any]) -> bool:
    """True если экстрактор нашёл OCR-пригодные картинки, но OCR был пропущен (skip_ocr).

    Ожидает ExtractedDocument (или любой объект с ``metadata``): экстракторы OOXML
    кладут ``embedded_images`` (число картинок) и ``ocr_skipped=True``.
    """
    if doc is None:
        return False
    metadata = getattr(doc, "metadata", None) or {}
    if not isinstance(metadata, dict):
        return False
    try:
        images = int(metadata.get("embedded_images") or 0)
    except (TypeError, ValueError):
        images = 0
    return bool(metadata.get("ocr_skipped")) and images > 0
