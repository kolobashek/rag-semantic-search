"""Embedded media inside OOXML containers (docx/xlsx/pptx and macro variants).

Office documents frequently carry the real content as a pasted scan
(``word/media/image1.jpg``) or as a nested office file
(``word/embeddings/Лист Microsoft Excel.xlsx``). Plain paragraph/cell
extraction sees nothing in that case. This module enumerates such members,
runs Tesseract on raster images and reuses the regular extractors for nested
office documents. Every function is best-effort: failures are logged and the
document simply ends up with fewer blocks.
"""

from __future__ import annotations

import logging
import tempfile
from io import BytesIO
from pathlib import Path
from typing import Any, Iterator
from zipfile import BadZipFile, ZipFile

from rag_catalog.core.ocr_runtime import apply_tesseract_runtime

from .contract import ExtractedDocument, TextBlock

logger = logging.getLogger(__name__)

OOXML_EXTENSIONS = frozenset({".docx", ".docm", ".xlsx", ".xlsm", ".pptx", ".pptm", ".ppsx"})
MEDIA_PREFIXES = ("word/media/", "xl/media/", "ppt/media/")
EMBEDDING_PREFIXES = ("word/embeddings/", "xl/embeddings/", "ppt/embeddings/")
RASTER_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".gif", ".webp"})
VECTOR_EXTENSIONS = frozenset({".emf", ".wmf"})
NESTED_OFFICE_EXTENSIONS = frozenset({".docx", ".docm", ".xlsx", ".xlsm", ".pptx", ".pptm"})

DEFAULT_MAX_IMAGES = 20
DEFAULT_MAX_IMAGE_BYTES = 15 * 1024 * 1024
DEFAULT_MIN_IMAGE_BYTES = 8 * 1024
DEFAULT_MIN_IMAGE_SIDE = 200
DEFAULT_OCR_LANG = "rus+eng"
DEFAULT_TESSERACT_CONFIG = "--psm 6"

_DIAG_KEYS = (
    "embedded_images",
    "embedded_images_ocr",
    "embedded_images_skipped_small",
    "embedded_images_skipped_large",
    "embedded_images_skipped_vector",
    "embedded_images_skipped_limit",
    "embedded_objects",
    "embedded_objects_skipped",
)


def _is_ooxml(path: Path) -> bool:
    return path.suffix.lower() in OOXML_EXTENSIONS


def _image_size(data: bytes) -> tuple[int, int] | None:
    """Read image dimensions from the header only; None when PIL cannot parse."""
    try:
        from PIL import Image  # type: ignore  # noqa: PLC0415
    except ImportError:
        return None
    try:
        with Image.open(BytesIO(data)) as img:
            width, height = img.size
            return int(width), int(height)
    except Exception:
        return None


def _bump(diagnostics: dict[str, Any] | None, key: str, value: int = 1) -> None:
    if diagnostics is not None:
        diagnostics[key] = int(diagnostics.get(key) or 0) + value


def _sorted_members(zf: ZipFile, prefixes: tuple[str, ...]) -> list[Any]:
    members = [info for info in zf.infolist() if info.filename.startswith(prefixes) and not info.is_dir()]
    members.sort(key=lambda info: info.filename)
    return members


def iter_embedded_images(
    path: Path,
    *,
    max_images: int = DEFAULT_MAX_IMAGES,
    max_bytes: int = DEFAULT_MAX_IMAGE_BYTES,
    min_bytes: int = DEFAULT_MIN_IMAGE_BYTES,
    min_side: int = DEFAULT_MIN_IMAGE_SIDE,
    diagnostics: dict[str, Any] | None = None,
) -> Iterator[tuple[str, bytes]]:
    """Yield ``(member_name, bytes)`` for raster images inside an OOXML archive.

    Tiny members (icons, rules) and vector metafiles are skipped. ``diagnostics``
    receives counters (``embedded_images`` — accepted images, plus
    ``embedded_images_skipped_*`` reasons) when provided.
    """
    path = Path(path)
    if not _is_ooxml(path):
        return
    try:
        zf = ZipFile(path, "r")
    except (BadZipFile, OSError) as exc:
        logger.warning("Вложенные картинки: не открыть архив %s: %s", path, exc)
        return

    yielded = 0
    with zf:
        for info in _sorted_members(zf, MEDIA_PREFIXES):
            name = info.filename
            ext = Path(name).suffix.lower()
            if ext in VECTOR_EXTENSIONS:
                logger.debug("Вложенная картинка %s пропущена: векторный формат %s (tesseract не читает)", name, ext)
                _bump(diagnostics, "embedded_images_skipped_vector")
                continue
            if ext not in RASTER_EXTENSIONS:
                logger.debug("Вложенный медиа-файл %s пропущен: неизвестный формат", name)
                continue
            if info.file_size < min_bytes:
                logger.debug("Вложенная картинка %s пропущена: %d байт < %d", name, info.file_size, min_bytes)
                _bump(diagnostics, "embedded_images_skipped_small")
                continue
            if max_bytes and info.file_size > max_bytes:
                logger.debug("Вложенная картинка %s пропущена: %d байт > %d", name, info.file_size, max_bytes)
                _bump(diagnostics, "embedded_images_skipped_large")
                continue
            if max_images and yielded >= max_images:
                logger.debug("Вложенная картинка %s пропущена: превышен лимит %d", name, max_images)
                _bump(diagnostics, "embedded_images_skipped_limit")
                continue
            try:
                data = zf.read(name)
            except Exception as exc:
                logger.warning("Вложенная картинка %s из %s не прочитана: %s", name, path.name, exc)
                continue
            size = _image_size(data)
            if size is not None and (size[0] < min_side or size[1] < min_side):
                logger.debug("Вложенная картинка %s пропущена: %dx%d px < %d", name, size[0], size[1], min_side)
                _bump(diagnostics, "embedded_images_skipped_small")
                continue
            yielded += 1
            _bump(diagnostics, "embedded_images")
            yield name, data


def count_embedded_images(path: Path, **kwargs: Any) -> int:
    """Number of OCR-worthy images inside an OOXML archive (same filters as iterator)."""
    return sum(1 for _ in iter_embedded_images(path, **kwargs))


def _ocr_image_bytes(data: bytes, *, lang: str, tesseract_config: str) -> str:
    import pytesseract  # type: ignore  # noqa: PLC0415
    from PIL import Image  # type: ignore  # noqa: PLC0415

    with Image.open(BytesIO(data)) as img:
        # Grayscale matters: on colour photos of certificates Tesseract loses the
        # digit rows entirely (measured on real scans: hit-rate 0/8 RGB vs 6/8 gray).
        frame = img.convert("L")
        kwargs: dict[str, Any] = {"lang": lang, "config": tesseract_config or DEFAULT_TESSERACT_CONFIG}
        return str(pytesseract.image_to_string(frame, **kwargs) or "").strip()


def ocr_embedded_images(
    path: Path,
    *,
    tesseract_cmd: str = "",
    lang: str = DEFAULT_OCR_LANG,
    tesseract_config: str = "",
    max_images: int = DEFAULT_MAX_IMAGES,
    max_bytes: int = DEFAULT_MAX_IMAGE_BYTES,
    skip_ocr: bool = False,
    diagnostics: dict[str, Any] | None = None,
) -> list[TextBlock]:
    """OCR raster images embedded in an OOXML file and return one block per image.

    With ``skip_ocr=True`` nothing is recognised, but ``diagnostics`` still gets
    ``embedded_images`` (count of OCR-worthy images) and ``ocr_skipped=True`` so
    the indexer can defer the document to the OCR stage.
    """
    path = Path(path)
    if diagnostics is not None:
        diagnostics.setdefault("embedded_images", 0)
    if skip_ocr:
        count = count_embedded_images(path, max_images=max_images, max_bytes=max_bytes, diagnostics=diagnostics)
        if diagnostics is not None:
            diagnostics["ocr_skipped"] = True
        if count:
            logger.debug("Вложенные картинки (%d) в %s: OCR пропущен (--no-ocr)", count, path.name)
        return []

    try:
        import pytesseract  # type: ignore  # noqa: PLC0415
        from PIL import Image  # type: ignore  # noqa: PLC0415,F401
    except ImportError:
        logger.debug("pytesseract/Pillow не установлены — OCR вложенных картинок недоступен")
        count_embedded_images(path, max_images=max_images, max_bytes=max_bytes, diagnostics=diagnostics)
        if diagnostics is not None:
            diagnostics["ocr_skipped"] = True
            diagnostics["ocr_error"] = "pytesseract/Pillow unavailable"
        return []

    apply_tesseract_runtime(pytesseract, tesseract_cmd)
    blocks: list[TextBlock] = []
    for member, data in iter_embedded_images(path, max_images=max_images, max_bytes=max_bytes, diagnostics=diagnostics):
        try:
            text = _ocr_image_bytes(data, lang=lang, tesseract_config=tesseract_config)
        except Exception as exc:
            logger.warning("OCR вложенной картинки %s из %s не удался: %s", member, path.name, exc)
            if diagnostics is not None:
                diagnostics["ocr_error"] = str(exc)
            continue
        logger.info("OCR вложенной картинки %s из %s: %d симв.", member, path.name, len(text))
        if not text:
            continue
        _bump(diagnostics, "embedded_images_ocr")
        blocks.append(
            TextBlock(
                text=text,
                section=f"image:{member}",
                metadata={"source": "embedded_image", "member": member, "ocr_engine": "tesseract"},
            )
        )
    return blocks


def _extract_nested_office(temp_path: Path, *, max_chars: int) -> ExtractedDocument:
    """Run the regular structured extractor for a nested office file (no deeper recursion)."""
    from . import files as _files  # noqa: PLC0415

    ext = temp_path.suffix.lower()
    if ext in {".docx", ".docm"}:
        return _files.extract_docx_document(temp_path, max_chars=max_chars, skip_ocr=True, include_embedded=False)
    if ext in {".xlsx", ".xlsm"}:
        return _files.extract_xlsx_document(temp_path, max_chars=max_chars, skip_ocr=True, include_embedded=False)
    if ext in {".pptx", ".pptm"}:
        return _files.extract_pptx_document(temp_path, max_chars=max_chars, skip_ocr=True, include_embedded=False)
    return ExtractedDocument(blocks=())


def extract_embedded_documents(
    path: Path,
    *,
    max_objects: int = DEFAULT_MAX_IMAGES,
    max_bytes: int = DEFAULT_MAX_IMAGE_BYTES,
    max_chars: int = 0,
    diagnostics: dict[str, Any] | None = None,
) -> list[TextBlock]:
    """Extract text from nested office documents under ``*/embeddings/`` (one level deep).

    ``oleObject*.bin`` and other binary payloads are skipped with a debug log.
    """
    path = Path(path)
    if not _is_ooxml(path):
        return []
    try:
        zf = ZipFile(path, "r")
    except (BadZipFile, OSError) as exc:
        logger.warning("Вложенные объекты: не открыть архив %s: %s", path, exc)
        return []

    blocks: list[TextBlock] = []
    processed = 0
    with zf:
        for info in _sorted_members(zf, EMBEDDING_PREFIXES):
            name = info.filename
            ext = Path(name).suffix.lower()
            if ext not in NESTED_OFFICE_EXTENSIONS:
                logger.debug("Вложенный объект %s пропущен: формат %s не поддержан", name, ext or "bin")
                _bump(diagnostics, "embedded_objects_skipped")
                continue
            if max_bytes and info.file_size > max_bytes:
                logger.debug("Вложенный объект %s пропущен: %d байт > %d", name, info.file_size, max_bytes)
                _bump(diagnostics, "embedded_objects_skipped")
                continue
            if max_objects and processed >= max_objects:
                _bump(diagnostics, "embedded_objects_skipped")
                continue
            processed += 1
            _bump(diagnostics, "embedded_objects")
            try:
                with tempfile.TemporaryDirectory(prefix="rag_embed_") as tmp:
                    temp_path = Path(tmp) / f"embedded{ext}"
                    temp_path.write_bytes(zf.read(name))
                    nested = _extract_nested_office(temp_path, max_chars=max_chars)
            except Exception as exc:
                logger.warning("Вложенный объект %s из %s не прочитан: %s", name, path.name, exc)
                continue
            for block in nested.blocks:
                if not block.text.strip():
                    continue
                blocks.append(
                    TextBlock(
                        text=block.text,
                        page=block.page,
                        sheet=block.sheet,
                        row_start=block.row_start,
                        row_end=block.row_end,
                        slide=block.slide,
                        section=f"embedded:{name}",
                        metadata={**block.metadata, "source": "embedded_document", "member": name},
                    )
                )
    return blocks


def extract_embedded_media_blocks(
    path: Path,
    *,
    tesseract_cmd: str = "",
    lang: str = DEFAULT_OCR_LANG,
    tesseract_config: str = "",
    max_images: int = DEFAULT_MAX_IMAGES,
    max_bytes: int = DEFAULT_MAX_IMAGE_BYTES,
    max_chars: int = 0,
    skip_ocr: bool = False,
    diagnostics: dict[str, Any] | None = None,
) -> list[TextBlock]:
    """Nested office documents first (cheap), then OCR of embedded images."""
    blocks: list[TextBlock] = []
    try:
        blocks.extend(
            extract_embedded_documents(
                path, max_objects=max_images, max_bytes=max_bytes, max_chars=max_chars, diagnostics=diagnostics
            )
        )
    except Exception as exc:
        logger.warning("Вложенные объекты %s: %s", path, exc)
    try:
        blocks.extend(
            ocr_embedded_images(
                path,
                tesseract_cmd=tesseract_cmd,
                lang=lang,
                tesseract_config=tesseract_config,
                max_images=max_images,
                max_bytes=max_bytes,
                skip_ocr=skip_ocr,
                diagnostics=diagnostics,
            )
        )
    except Exception as exc:
        logger.warning("Вложенные картинки %s: %s", path, exc)
        if diagnostics is not None:
            diagnostics["ocr_error"] = str(exc)
    return blocks


def merge_diagnostics(target: dict[str, Any], diagnostics: dict[str, Any]) -> dict[str, Any]:
    """Copy non-zero embedded-media counters (and ocr flags) into document metadata."""
    for key in _DIAG_KEYS:
        value = int(diagnostics.get(key) or 0)
        if value:
            target[key] = value
    if diagnostics.get("ocr_skipped"):
        target["ocr_skipped"] = True
    if diagnostics.get("ocr_error"):
        target["ocr_error"] = str(diagnostics["ocr_error"])
    return target
