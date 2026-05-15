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
from pathlib import Path
from typing import Any, Optional

import numpy as np

logger = logging.getLogger(__name__)

_rapid_engine: Optional[Any] = None
_active_device: str = "CPU"


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

    original_init = OrtInferSession.__init__

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


def ocr_image_rapid(filepath: Path) -> str:
    """OCR одного изображения через RapidOCR."""
    try:
        from PIL import Image  # noqa: PLC0415
        with Image.open(filepath) as img:
            n_frames: int = getattr(img, "n_frames", 1)
            parts: list[str] = []
            for frame_idx in range(min(n_frames, 50)):
                try:
                    img.seek(frame_idx)
                except EOFError:
                    break
                frame = img.copy()
                if frame.mode not in ("RGB", "L"):
                    frame = frame.convert("RGB")
                arr = np.array(frame)
                text = _img_to_text(arr).strip()
                if text:
                    parts.append(text)
                logger.info(
                    "RapidOCR изображение %s стр.%d: %d симв.",
                    filepath.name, frame_idx + 1, len(text),
                )
        return "\n\n".join(parts)
    except Exception as exc:
        logger.warning("RapidOCR изображение %s: %s", filepath, exc)
        return ""


# ───────────────────────── PDF OCR ──────────────────────────────────────────

def ocr_pdf_rapid(filepath: Path, *, poppler_bin: str = "") -> str:
    """OCR сканированного PDF через pdf2image + RapidOCR."""
    try:
        import pdf2image.pdf2image as pdf2image_impl  # noqa: PLC0415  # type: ignore
        from pdf2image import convert_from_path  # noqa: PLC0415  # type: ignore
    except ImportError:
        logger.warning("pdf2image не установлен — OCR PDF недоступен")
        return ""

    # Скрываем консольное окно poppler на Windows
    try:
        from rag_catalog.core.extractors.files import _patch_pdf2image_popen_for_windows  # noqa: PLC0415
        _patch_pdf2image_popen_for_windows(pdf2image_impl)
    except Exception:
        pass

    try:
        convert_kwargs: dict[str, Any] = {"dpi": 200}
        if str(poppler_bin or "").strip():
            convert_kwargs["poppler_path"] = str(poppler_bin).strip()
        pages = convert_from_path(str(filepath), **convert_kwargs)
    except Exception as exc:
        logger.warning("pdf2image не смог конвертировать %s: %s", filepath, exc)
        return ""

    parts: list[str] = []
    for i, page_img in enumerate(pages):
        arr = np.array(page_img.convert("RGB"))
        text = _img_to_text(arr).strip()
        if text:
            parts.append(f"Страница: {i + 1}\n{text}")
        logger.info(
            "RapidOCR PDF %s стр.%d/%d — %d симв.",
            filepath.name, i + 1, len(pages), len(text),
        )

    return "\n\n".join(parts)
