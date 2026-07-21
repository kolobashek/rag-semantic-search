"""
ocr_rapid.py — GPU-ускоренный OCR через RapidOCR + DirectML.

Используется как альтернатива Tesseract для GPU без CUDA (AMD, Intel, NVIDIA).
DirectML работает на любой DX12-видеокарте под Windows.

Точки входа:
    ocr_pdf_rapid(filepath, poppler_bin)  → str
    ocr_image_rapid(filepath)             → str
    is_available()                         → bool
    active_device()                        → "DirectML" | "CPU"
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any, Optional

import numpy as np

from .contract import UnreadableSourceError

logger = logging.getLogger(__name__)

_rapid_engine: Optional[Any] = None
_active_device: str = "CPU"

# Limit concurrent OCR jobs to avoid overloading GPU/RAM.
# PDF rendering is batched, but recognition still has substantial per-page memory use.
_ocr_semaphore = threading.Semaphore(1)
_PDF_PAGES_PER_PROCESS = 20
_IMAGE_FRAMES_PER_PROCESS = 10
_MAX_IMAGE_PIXELS = 16_000_000
_MAX_IMAGE_DIMENSION = 4096


# ───────────────────────── DirectML monkey-patch ────────────────────────────

def _patch_ort_infer_session_for_dml() -> bool:
    """
    Заменяет выбор EP в RapidOCR: вместо CUDAExecutionProvider
    подставляет DmlExecutionProvider (DirectML) если доступен.

    Возвращает True если DirectML удалось активировать.
    """
    try:
        from onnxruntime import (  # noqa: PLC0415
            GraphOptimizationLevel,
            InferenceSession,
            SessionOptions,
            get_available_providers,
        )
        from rapidocr_onnxruntime.utils import OrtInferSession  # noqa: PLC0415
    except ImportError:
        return False

    available = get_available_providers()
    dml_available = "DmlExecutionProvider" in available

    def _patched_init(self: Any, config: Any) -> None:
        sess_opt = SessionOptions()
        sess_opt.log_severity_level = 4
        sess_opt.enable_cpu_mem_arena = False
        sess_opt.graph_optimization_level = GraphOptimizationLevel.ORT_ENABLE_ALL

        ep_list: list[Any] = []
        if dml_available:
            # DirectML не требует дополнительных опций; DX12 выберет GPU автоматически
            ep_list.append("DmlExecutionProvider")
        ep_list.append(("CPUExecutionProvider", {"arena_extend_strategy": "kSameAsRequested"}))

        self._verify_model(config["model_path"])
        self.session = InferenceSession(
            config["model_path"],
            sess_options=sess_opt,
            providers=ep_list,
        )
        active = self.session.get_providers()
        if dml_available and "DmlExecutionProvider" in active:
            logger.debug("OrtInferSession: DmlExecutionProvider активирован (%s)", config["model_path"])
        else:
            logger.debug("OrtInferSession: CPU fallback (%s)", config["model_path"])

    OrtInferSession.__init__ = _patched_init  # type: ignore[method-assign]
    return dml_available


# ───────────────────────── engine singleton ──────────────────────────────────

def _build_engine() -> Any:
    """Создать и вернуть экземпляр RapidOCR с активным DirectML (если доступен)."""
    global _active_device

    dml_patched = _patch_ort_infer_session_for_dml()
    _active_device = "DirectML" if dml_patched else "CPU"

    from rapidocr_onnxruntime import RapidOCR  # noqa: PLC0415

    # use_angle_cls=True — корректирует перевёрнутый текст (важно для сканов)
    engine = RapidOCR(use_angle_cls=True, use_cuda=False)
    logger.info(
        "RapidOCR инициализирован, устройство: %s",
        _active_device,
    )
    return engine


def _get_engine() -> Any:
    global _rapid_engine
    if _rapid_engine is None:
        _rapid_engine = _build_engine()
    return _rapid_engine


# ───────────────────────── public helpers ────────────────────────────────────

def is_available() -> bool:
    """Вернуть True если rapidocr_onnxruntime установлен."""
    try:
        import rapidocr_onnxruntime  # noqa: PLC0415, F401
        return True
    except ImportError:
        return False


def is_dml_available() -> bool:
    """Вернуть True если onnxruntime-directml установлен и GPU доступен."""
    try:
        from onnxruntime import get_available_providers  # noqa: PLC0415
        return "DmlExecutionProvider" in get_available_providers()
    except Exception:
        return False


def gpu_ocr_available() -> bool:
    """Вернуть True если GPU-ускоренный OCR готов к использованию (rapidocr + DirectML)."""
    return is_available() and is_dml_available()


def active_device() -> str:
    """Вернуть 'DirectML' или 'CPU' (определяется после первого вызова engine)."""
    return _active_device


# ───────────────────────── image OCR ────────────────────────────────────────

def _img_to_text(img_array: np.ndarray) -> str:
    """Распознать текст из numpy-массива (RGB uint8)."""
    engine = _get_engine()
    result, _ = engine(img_array)
    if not result:
        return ""
    lines = [str(item[1]) for item in result if item and len(item) >= 2]
    return "\n".join(lines)


def _bounded_image_array(image: Any) -> np.ndarray:
    """Convert a PIL image while bounding pathological scan dimensions."""
    from PIL import Image  # noqa: PLC0415

    frame = image.convert("RGB")
    width, height = frame.size
    scale = min(
        1.0,
        _MAX_IMAGE_DIMENSION / max(1, width),
        _MAX_IMAGE_DIMENSION / max(1, height),
        (_MAX_IMAGE_PIXELS / max(1, width * height)) ** 0.5,
    )
    if scale < 1.0:
        target = (max(1, int(width * scale)), max(1, int(height * scale)))
        frame = frame.resize(target, Image.Resampling.LANCZOS)
        logger.info("RapidOCR: изображение уменьшено %dx%d -> %dx%d", width, height, *target)
    return np.asarray(frame)


def _isolated_worker_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "capture_output": True,
        "text": True,
        "encoding": "utf-8",
        "errors": "replace",
        "check": False,
        "env": {**os.environ, "PYTHONUTF8": "1"},
    }
    if os.name == "nt":
        kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NO_WINDOW", 0) or 0)
    return kwargs


def _run_isolated_worker(
    filepath: Path,
    *,
    mode: str,
    first: int,
    last: int,
    poppler_bin: str = "",
) -> str:
    with tempfile.TemporaryDirectory(prefix="rag-rapidocr-") as temp_dir:
        output_path = Path(temp_dir) / "ocr.txt"
        cmd = [
            sys.executable,
            "-u",
            "-m",
            "rag_catalog.core.extractors.ocr_rapid_worker",
            "--mode",
            mode,
            "--path",
            str(filepath),
            "--first",
            str(first),
            "--last",
            str(last),
            "--output",
            str(output_path),
        ]
        if poppler_bin:
            cmd += ["--poppler-bin", str(poppler_bin)]
        result = subprocess.run(cmd, **_isolated_worker_kwargs())
        if int(result.returncode or 0) != 0:
            detail = (result.stderr or result.stdout or "isolated RapidOCR worker failed").strip()
            raise RuntimeError(detail[-2000:])
        return output_path.read_text(encoding="utf-8", errors="replace") if output_path.exists() else ""


def _run_pdf_range_isolated(filepath: Path, *, first: int, last: int, poppler_bin: str) -> str:
    try:
        return _run_isolated_worker(
            filepath,
            mode="pdf",
            first=first,
            last=last,
            poppler_bin=poppler_bin,
        )
    except RuntimeError:
        if first >= last:
            raise
        middle = (first + last) // 2
        logger.warning("RapidOCR диапазон %s стр.%d-%d не удался; делю", filepath.name, first, last)
        left = _run_pdf_range_isolated(filepath, first=first, last=middle, poppler_bin=poppler_bin)
        right = _run_pdf_range_isolated(filepath, first=middle + 1, last=last, poppler_bin=poppler_bin)
        return "\n\n".join(part for part in (left, right) if part.strip())


def ocr_image_rapid(
    filepath: Path,
    *,
    max_pages: int = 50,
    diagnostics: dict[str, Any] | None = None,
) -> str:
    """OCR одного изображения через RapidOCR."""
    with _ocr_semaphore:
        try:
            from PIL import Image, UnidentifiedImageError  # noqa: PLC0415

            try:
                with Image.open(filepath) as image:
                    frame_count = min(int(getattr(image, "n_frames", 1) or 1), max(1, int(max_pages or 50)))
            except UnidentifiedImageError as exc:
                raise UnreadableSourceError(f"unreadable image source: {filepath}: {exc}") from exc
            parts: list[str] = []
            for first in range(1, frame_count + 1, _IMAGE_FRAMES_PER_PROCESS):
                last = min(frame_count, first + _IMAGE_FRAMES_PER_PROCESS - 1)
                text = _run_isolated_worker(filepath, mode="image", first=first, last=last)
                if text.strip():
                    parts.append(text.strip())
            if diagnostics is not None:
                diagnostics["pages"] = frame_count
            return "\n\n".join(parts)
        except UnreadableSourceError:
            raise
        except Exception as exc:
            logger.warning("RapidOCR изображение %s: %s", filepath, exc)
            raise RuntimeError(f"RapidOCR image failed for {filepath}: {exc}") from exc


def _ocr_image_rapid_impl(
    filepath: Path,
    *,
    max_pages: int = 50,
    diagnostics: dict[str, Any] | None = None,
    first_frame: int = 1,
    last_frame: int = 0,
) -> str:
    previous_truncated_setting: bool | None = None
    try:
        from PIL import Image, ImageFile  # noqa: PLC0415

        # This function runs in a bounded child process. Accepting truncated
        # JPEG streams here salvages readable pixels without changing Pillow's
        # process-global decoder policy in the parent indexer.
        previous_truncated_setting = bool(ImageFile.LOAD_TRUNCATED_IMAGES)
        ImageFile.LOAD_TRUNCATED_IMAGES = True
        with Image.open(filepath) as img:
            n_frames: int = getattr(img, "n_frames", 1)
            parts: list[str] = []
            pages = min(n_frames, max(1, int(max_pages or 50)))
            range_start = max(1, int(first_frame or 1))
            range_end = min(pages, int(last_frame or pages))
            if diagnostics is not None:
                diagnostics["pages"] = max(0, range_end - range_start + 1)
            for frame_number in range(range_start, range_end + 1):
                frame_idx = frame_number - 1
                try:
                    img.seek(frame_idx)
                except EOFError:
                    break
                arr = _bounded_image_array(img.copy())
                text = _img_to_text(arr).strip()
                if text:
                    parts.append(text)
                logger.info(
                    "RapidOCR изображение %s стр.%d: %d симв.",
                    filepath.name, frame_number, len(text),
                )
        return "\n\n".join(parts)
    except Exception as exc:
        logger.warning("RapidOCR изображение %s: %s", filepath, exc)
        raise RuntimeError(f"RapidOCR image failed for {filepath}: {exc}") from exc
    finally:
        if previous_truncated_setting is not None:
            ImageFile.LOAD_TRUNCATED_IMAGES = previous_truncated_setting


# ───────────────────────── PDF OCR ──────────────────────────────────────────

def ocr_pdf_rapid(filepath: Path, *, poppler_bin: str = "", batch_pages: int = 8) -> str:
    """OCR сканированного PDF через pdf2image + RapidOCR."""
    with _ocr_semaphore:
        try:
            import pdf2image.pdf2image as pdf2image_impl  # noqa: PLC0415  # type: ignore
            from pdf2image import pdfinfo_from_path  # type: ignore  # noqa: PLC0415

            from rag_catalog.core.extractors.files import _patch_pdf2image_popen_for_windows  # noqa: PLC0415

            _patch_pdf2image_popen_for_windows(pdf2image_impl)

            info_kwargs: dict[str, Any] = {}
            if poppler_bin:
                info_kwargs["poppler_path"] = str(poppler_bin)
            page_count = int(pdfinfo_from_path(str(filepath), **info_kwargs).get("Pages") or 0)
            parts: list[str] = []
            for first in range(1, page_count + 1, _PDF_PAGES_PER_PROCESS):
                last = min(page_count, first + _PDF_PAGES_PER_PROCESS - 1)
                text = _run_pdf_range_isolated(filepath, first=first, last=last, poppler_bin=poppler_bin)
                if text.strip():
                    parts.append(text.strip())
                logger.info("RapidOCR subprocess %s стр.%d-%d/%d завершён", filepath.name, first, last, page_count)
            return "\n\n".join(parts)
        except Exception as exc:
            logger.warning("RapidOCR PDF %s: %s", filepath, exc)
            raise RuntimeError(f"RapidOCR PDF failed for {filepath}: {exc}") from exc


def _ocr_pdf_rapid_impl(
    filepath: Path,
    *,
    poppler_bin: str = "",
    batch_pages: int = 8,
    first_page: int = 1,
    last_page: int = 0,
) -> str:
    try:
        import pdf2image.pdf2image as pdf2image_impl  # noqa: PLC0415  # type: ignore
    except ImportError as exc:
        logger.warning("pdf2image не установлен — OCR PDF недоступен")
        raise RuntimeError("pdf2image is unavailable for RapidOCR PDF") from exc

    from rag_catalog.core.extractors.files import (  # noqa: PLC0415
        _iter_pdf_pages,
        _patch_pdf2image_popen_for_windows,
    )

    # Скрываем консольное окно poppler на Windows
    try:
        _patch_pdf2image_popen_for_windows(pdf2image_impl)
    except Exception:
        pass

    parts: list[str] = []
    try:
        for page_number, total_pages, page_img in _iter_pdf_pages(
            filepath,
            poppler_bin=poppler_bin,
            batch_pages=batch_pages,
            first_page=first_page,
            last_page=last_page,
        ):
            arr = _bounded_image_array(page_img)
            text = _img_to_text(arr).strip()
            if text:
                parts.append(f"Страница: {page_number}\n{text}")
            logger.info(
                "RapidOCR PDF %s стр.%d/%d — %d симв.",
                filepath.name,
                page_number,
                total_pages,
                len(text),
            )
    except Exception as exc:
        logger.warning("pdf2image не смог конвертировать %s: %s", filepath, exc)
        raise RuntimeError(f"RapidOCR PDF conversion failed for {filepath}: {exc}") from exc

    return "\n\n".join(parts)
