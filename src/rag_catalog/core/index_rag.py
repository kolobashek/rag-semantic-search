"""
index_rag.py — Индексатор файлов DOCX / XLSX / XLS / PDF / изображений для RAG-системы.

Запуск:
    python index_rag.py                  # использует config.json
    python index_rag.py --recreate       # пересоздать коллекцию с нуля
    python index_rag.py --catalog "D:\\docs"  # другая папка
"""

import argparse
import fnmatch
import hashlib
import json
import logging
import os
import queue
import re
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from ._platform_compat import apply_windows_platform_workarounds

apply_windows_platform_workarounds()

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from sentence_transformers import SentenceTransformer

from .chunking import chunk_text, semantic_chunk_end
from .embedding_collections import resolve_embedding_collection_name
from .exact_tokens import add_numeric_tokens
from .extractors import (
    ExtractedDocument,
    TextBlock,
    blocks_from_legacy_text,
    document_from_legacy_text,
    extract_csv,
    extract_doc,
    extract_doc_meta,
    extract_docx,
    extract_html,
    extract_image,
    extract_pdf,
    extract_pdf_document,
    extract_pptx,
    extract_pptx_document,
    extract_rtf,
    extract_spreadsheet,
    extract_spreadsheet_document,
    extract_text,
    ocr_pdf,
)
from .index_state_db import IndexStateDB
from .indexer_control import read_indexer_control
from .indexing import delete_file_vectors, ensure_collection, upsert_points
from .log_history import build_log_handler, install_env_log_handler
from .ocr_runtime import resolve_ocr_runtime
from .rag_core import load_config
from .retrieval import prepare_passage_texts
from .telemetry_db import TelemetryDB

# ─────────────────────────── logging ───────────────────────────────────
# Явно задаём UTF-8 для FileHandler и StreamHandler, чтобы кириллица
# не превращалась в кракозябры на Windows (cmd/PowerShell cp866/cp1251).
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
try:
    _stream_handler.stream.reconfigure(encoding="utf-8")  # Python 3.7+
except AttributeError:
    pass  # не все объекты stream поддерживают reconfigure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[_stream_handler],
)
install_env_log_handler()
logger = logging.getLogger(__name__)

# Поддерживаемые расширения
SUPPORTED_EXTENSIONS = {
    ".doc",
    ".docx",
    ".xlsx",
    ".xlsm",
    ".xls",
    ".pdf",
    ".pptx",
    ".rtf",
    ".txt",
    ".csv",
    ".html",
    ".htm",
    ".zip",
    ".7z",
    ".rar",
    ".tar",
    ".tgz",
    ".tbz",
    ".tbz2",
    ".txz",
    ".gz",
    ".bz2",
    ".xz",
    # Изображения — OCR если есть текст
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".tif",
    ".tiff",
    ".bmp",
    ".webp",
}

# Расширения изображений (подмножество SUPPORTED_EXTENSIONS)
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".bmp", ".webp"}

# Максимум страниц/кадров при OCR многостраничного изображения (TIFF, GIF).
# Защита от случайных файлов с тысячами кадров, которые зависнут индексатор.
MAX_IMAGE_PAGES: int = 50
PAYLOAD_SCHEMA_VERSION: int = 3


class IndexerCancelled(RuntimeError):
    """Raised when cooperative indexer control requests cancellation."""


# ─────────────────────────── таблица синонимов ────────────────────────────────
# Сокращение → список синонимов/расшифровок.
# Встроенная база для строительной/горной техники; дополняется через config.json
# ("synonym_map": {"ключ": ["синоним1", "синоним2"]}).
DEFAULT_SYNONYM_MAP_FILE = Path(__file__).with_name("default_synonyms.json")


def _load_default_synonym_map(path: Path = DEFAULT_SYNONYM_MAP_FILE) -> Dict[str, List[str]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Не удалось загрузить базовую карту синонимов %s: %s", path, exc)
        return {}
    if not isinstance(data, dict):
        logger.warning("Базовая карта синонимов %s имеет неверный формат", path)
        return {}
    out: Dict[str, List[str]] = {}
    for key, values in data.items():
        normalized_key = str(key or "").strip().lower()
        if not normalized_key:
            continue
        if isinstance(values, list):
            out[normalized_key] = [str(item).strip() for item in values if str(item).strip()]
    return out


DEFAULT_SYNONYM_MAP: Dict[str, List[str]] = _load_default_synonym_map()

# Стоп-слова для тегов (не добавляем в список тегов)
_TAG_STOPWORDS: Set[str] = {
    "и",
    "в",
    "на",
    "по",
    "с",
    "для",
    "из",
    "от",
    "до",
    "при",
    "или",
    "но",
    "а",
    "не",
    "что",
    "как",
    "так",
    "к",
    "о",
    "за",
    "the",
    "and",
    "or",
    "for",
    "of",
    "to",
    "in",
    "is",
    "a",
    "файл",
    "папка",
    "документ",
    "doc",
    "file",
}

# ─────────────────────────── этапы индексирования ─────────────────────────────
# Индексирование разбито на этапы: сначала быстрое наполнение индекса, затем
# прогрессивное «догонение» содержимого. Это даёт пользователю работоспособный
# поиск через минуты, а не сутки.
#
#   1. metadata — индексируется ТОЛЬКО имя/путь/размер/mtime всех файлов.
#      58 тыс. файлов за ~5 минут. Поиск по именам работает сразу.
#   2. small    — быстрый проход по небольшим файлам: первые N чанков на файл.
#      Даёт ранний поиск по содержимому без повторного чтения тяжёлых документов.
#   3. large    — полный проход: обрабатывает тяжёлые файлы и догружает чанки,
#      оставшиеся после small, затем закрывает файл как fully indexed.
#
# Порядок важен: чем меньше число, тем «старше» этап (больше информации).
STAGES = ("metadata", "small", "large")
STAGE_RANK = {name: i for i, name in enumerate(STAGES)}

# Пороги размера сохраняются для OCR-кандидатов и совместимости старых тестов.
DEFAULT_SMALL_OFFICE_MB = 20.0
DEFAULT_SMALL_PDF_MB = 2.0


def _file_category(filepath: Path, small_office_mb: float, small_pdf_mb: float) -> str:
    """
    Категория размера для прогрессивных этапов small/large.

    Изображения всегда в категории «large» — OCR CPU-intensive.
    """
    try:
        size_mb = filepath.stat().st_size / 1_048_576
    except OSError:
        return "large"  # не можем прочитать stat — кидаем в «медленный» этап
    ext = filepath.suffix.lower()
    if ext in (".txt", ".csv", ".rtf", ".pptx"):
        return "small"
    if ext in (".doc", ".docx", ".xlsx", ".xls", ".xlsm") and size_mb < small_office_mb:
        return "small"
    if ext == ".pdf" and size_mb < small_pdf_mb:
        return "small"
    if ext in IMAGE_EXTENSIONS:
        return "large"  # OCR изображений CPU-intensive — всегда в large
    return "large"


def _generate_tags(
    filepath: Path,
    relative_path: Path,
    full_text: str,
    synonym_map: Optional[Dict[str, List[str]]] = None,
) -> List[str]:
    """
    Генерирует список тегов для файла на основе:
      1. Компонентов пути (папки + имя файла без расширения)
      2. Синонимов для найденных аббревиатур/терминов
      3. Ключевых слов из текста содержимого (первые 2000 символов)
      4. Метатегов: тип документа, год, расширение

    Возвращает дедуплицированный отсортированный список тегов.
    """
    tags: Set[str] = set()
    smap = {**DEFAULT_SYNONYM_MAP, **(synonym_map or {})}

    def _add_token(tok: str) -> None:
        """Добавить токен и его синонимы в tags."""
        tok = tok.strip().lower()
        if len(tok) < 2 or tok in _TAG_STOPWORDS:
            return
        tags.add(tok)
        # Проверяем синонимы по точному совпадению
        if tok in smap:
            for syn in smap[tok]:
                if syn:
                    tags.add(syn.lower())

    # ── 1. Токены из пути (папки + имя файла) ─────────────────────────
    parts = list(relative_path.parts)
    # Имя файла без расширения тоже включаем
    stem = filepath.stem
    for part in [*parts, stem]:
        # Разбиваем на токены по пробелам, подчёркиваниям, дефисам, точкам
        tokens = re.split(r"[\s_\-\.\(\)\[\]]+", part)
        for tok in tokens:
            _add_token(tok)
        # Добавляем всё слово целиком (папка или имя), если оно значимое
        clean = re.sub(r"[\s_\-\.\(\)\[\]]+", " ", part).strip().lower()
        if len(clean) >= 2 and clean not in _TAG_STOPWORDS:
            tags.add(clean)

    # ── 2. Метатег: расширение файла ──────────────────────────────────
    ext_clean = filepath.suffix.lower().lstrip(".")
    if ext_clean:
        tags.add(ext_clean)
        # Карта расширений → понятные метатеги
        EXT_LABELS = {
            "pdf": "PDF документ",
            "docx": "Word документ",
            "xlsx": "Excel таблица",
            "xls": "Excel таблица",
            "jpg": "фотография",
            "jpeg": "фотография",
            "png": "изображение",
            "gif": "изображение",
            "tif": "скан",
            "tiff": "скан",
            "bmp": "изображение",
            "webp": "изображение",
        }
        if ext_clean in EXT_LABELS:
            tags.add(EXT_LABELS[ext_clean])

    # ── 3. Год из пути или имени файла ────────────────────────────────
    years = re.findall(r"\b(20\d{2}|19\d{2})\b", str(relative_path))
    for y in years:
        tags.add(y)

    # ── 4. Ключевые слова из текста содержимого ───────────────────────
    if full_text:
        # Берём первые 2000 символов — для скорости и памяти
        sample = full_text[:2000]
        # Кириллические и латинские слова длиной 3+
        content_words = re.findall(r"[а-яёa-z][а-яёa-z0-9\-]{2,}", sample.lower())
        # Считаем частоту, берём топ-20
        freq: Dict[str, int] = {}
        for w in content_words:
            if w not in _TAG_STOPWORDS and len(w) >= 3:
                freq[w] = freq.get(w, 0) + 1
        top_words = sorted(freq, key=lambda x: -freq[x])[:20]
        for w in top_words:
            _add_token(w)

    # ── 5. Детектируем тип документа по имени ────────────────────────
    name_lower = filepath.name.lower()
    DOC_TYPE_MAP = {
        "акт": "акт",
        "договор": "договор",
        "счёт": "счёт",
        "счет": "счёт",
        "накладная": "накладная",
        "паспорт": "паспорт",
        "псм": "паспорт самоходной машины",
        "птс": "паспорт транспортного средства",
        "техпаспорт": "технический паспорт",
        "спецификация": "спецификация",
        "инструкция": "инструкция",
        "отчёт": "отчёт",
        "отчет": "отчёт",
        "протокол": "протокол",
        "приказ": "приказ",
        "заявка": "заявка",
        "сертификат": "сертификат",
        "лицензия": "лицензия",
        "страховой": "страховой полис",
        "полис": "страховой полис",
        "фото": "фотография",
        "photo": "фотография",
        "скан": "скан документа",
        "scan": "скан документа",
    }
    for key, label in DOC_TYPE_MAP.items():
        if key in name_lower:
            tags.add(label)
            break

    # Убираем слишком длинные теги (>50 символов) и пустые
    result = sorted(t for t in tags if 1 < len(t) <= 50 and t.strip())
    return result


# ═══════════════════════════ RAGIndexer ════════════════════════════════


class RAGIndexer:
    """
    Индексирует файлы DOCX / XLSX / XLS / PDF в векторную базу Qdrant.

    Особенности:
    - Инкрементальное индексирование (пропускает неизменённые файлы).
    - Транзакционное состояние в SQLite (index_state.db).
    - При --recreate очищает state БД вместе с коллекцией.
    - При изменении файла — сначала удаляет старые векторы из Qdrant.
    - При удалении файла — чистит векторы из Qdrant и из state БД.
    - .xls читается через xlrd (старый формат Excel).
    - PDF: сначала pdfplumber (текстовый слой), затем OCR (если слой пуст).
    """

    def __init__(
        self,
        catalog_path: str,
        qdrant_db_path: str,
        embedding_model: str,
        collection_name: str,
        vector_size: int,
        chunk_size: int,
        chunk_overlap: int,
        batch_size: int,
        chunk_group_size: int = 4,
        recreate_collection: bool = False,
        skip_ocr: bool = False,
        max_chunks_per_file: int = 0,
        read_workers: int = 4,
        use_onnx: bool = False,
        qdrant_url: str = "",
        ollama_url: str = "http://localhost:11434",
        metadata_only_extensions: Optional[set] = None,
        telemetry_db_path: str = "",
        small_office_mb: Optional[float] = None,
        small_pdf_mb: Optional[float] = None,
        synonym_map: Optional[Dict[str, List[str]]] = None,
        ocr_tesseract_cmd: str = "",
        ocr_poppler_bin: str = "",
        ocr_engine: str = "tesseract",
        ocr_pdf_batch_pages: int = 8,
        ocr_rapid_fallback_enabled: bool = True,
        qdrant_timeout_sec: int = 60,
        exclude_patterns: Optional[List[str]] = None,
        only_paths: Optional[set[str]] = None,
        ocr_max_image_pages: int = MAX_IMAGE_PAGES,
        catalog_wait_attempts: int = 10,
        catalog_wait_seconds: int = 60,
        min_chunk_chars: int = 120,
        fulltext_enabled: bool = False,
        embedding_backend: str = "",
        embedding_onnx_provider: str = "",
        embedding_onnx_file_name: str = "",
    ) -> None:
        # current_stage выставляется при каждом запуске index_directory(stage=...)
        # и определяет поведение skip-логики и экстракции содержимого.
        self.current_stage: str = "content"  # legacy-совместимый дефолт
        self.catalog_path = Path(catalog_path)
        self.catalog_wait_seconds = max(1, int(catalog_wait_seconds or 60))
        self.catalog_wait_attempts = max(
            0,
            int(catalog_wait_attempts if catalog_wait_attempts is not None else 10),
        )
        self._ensure_catalog_available()

        self.qdrant_db_path = Path(qdrant_db_path)
        self.collection_name = collection_name
        self.embedding_model = str(embedding_model or "")
        self.vector_size = vector_size
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_chars = max(20, min(int(chunk_size or 500), int(min_chunk_chars or 120)))
        self.fulltext_enabled = bool(fulltext_enabled)
        self.chunk_group_size = max(1, int(chunk_group_size or 4))
        self.batch_size = batch_size
        self.recreate = recreate_collection
        self.skip_ocr = skip_ocr
        self._ocr_context = threading.local()
        self.dry_run = False
        self.max_chunks_per_file = max_chunks_per_file  # 0 = без ограничений
        self.read_workers = max(1, int(read_workers or 4))
        self.qdrant_timeout_sec = max(5, int(qdrant_timeout_sec or 60))
        self.exclude_patterns = self._normalize_exclude_patterns(exclude_patterns or [])
        self.only_paths = {str(path).strip() for path in (only_paths or set()) if str(path).strip()}
        self.ocr_max_image_pages = max(1, int(ocr_max_image_pages or MAX_IMAGE_PAGES))
        self.small_office_mb = float(DEFAULT_SMALL_OFFICE_MB if small_office_mb is None else small_office_mb)
        self.small_pdf_mb = float(DEFAULT_SMALL_PDF_MB if small_pdf_mb is None else small_pdf_mb)
        # Таблица синонимов для генерации тегов (дополняет DEFAULT_SYNONYM_MAP)
        self.synonym_map: Dict[str, List[str]] = synonym_map or {}
        self.ocr_tesseract_cmd = str(ocr_tesseract_cmd or "").strip()
        self.ocr_poppler_bin = str(ocr_poppler_bin or "").strip()
        self.ocr_engine = str(ocr_engine or "tesseract").strip().lower()
        self.ocr_pdf_batch_pages = max(1, int(ocr_pdf_batch_pages or 8))
        self.ocr_rapid_fallback_enabled = bool(ocr_rapid_fallback_enabled)
        self._use_rapid_ocr = self.ocr_engine == "rapidocr"
        if not self.ocr_tesseract_cmd or not self.ocr_poppler_bin:
            runtime = resolve_ocr_runtime(
                {
                    "ocr_tesseract_cmd": self.ocr_tesseract_cmd,
                    "ocr_poppler_bin": self.ocr_poppler_bin,
                }
            )
            self.ocr_tesseract_cmd = self.ocr_tesseract_cmd or runtime.get("tesseract_cmd", "")
            self.ocr_poppler_bin = self.ocr_poppler_bin or runtime.get("poppler_bin", "")
        if self.skip_ocr:
            logger.info("Inline OCR отключён (--no-ocr).")
        elif self._use_rapid_ocr:
            logger.info("OCR engine: RapidOCR + DirectML/GPU (poppler=%s)", self.ocr_poppler_bin or "auto")
        elif self.ocr_tesseract_cmd and self.ocr_poppler_bin:
            logger.info(
                "OCR runtime: tesseract=%s, poppler=%s",
                self.ocr_tesseract_cmd,
                self.ocr_poppler_bin,
            )
        else:
            logger.warning(
                "OCR runtime не полностью настроен. "
                "Ожидались tools/tesseract и tools/poppler (или ocr_* в config/env). "
                "tesseract=%s, poppler=%s",
                self.ocr_tesseract_cmd or "MISSING",
                self.ocr_poppler_bin or "MISSING",
            )
        # Расширения, для которых индексируется только метадата (без чтения содержимого).
        # Полезно для быстрого первого прохода по скан-PDF: имя/путь/размер — за секунды.
        self.metadata_only_extensions = {
            e.lower() if e.startswith(".") else f".{e.lower()}" for e in (metadata_only_extensions or set())
        }
        self.force_replace_extensions: set[str] = set()
        telemetry_path = telemetry_db_path.strip() if telemetry_db_path else ""
        if not telemetry_path:
            telemetry_path = str(self.qdrant_db_path / "rag_telemetry.db")
        self.telemetry = TelemetryDB(telemetry_path)
        self.run_id: str = ""
        self._run_deleted_files = 0
        self.payload_schema_version = PAYLOAD_SCHEMA_VERSION

        if embedding_model.startswith("ollama:"):
            from .llm import OllamaEmbedder  # noqa: PLC0415

            ollama_model_name = embedding_model[len("ollama:") :]
            logger.info("Загрузка OllamaEmbedder: %s (%s)", ollama_model_name, ollama_url)
            self.embedder = OllamaEmbedder(model=ollama_model_name, ollama_url=ollama_url)
        elif use_onnx or str(embedding_backend or "").strip().lower() == "onnx":
            model_kwargs = {
                key: value
                for key, value in {
                    "provider": str(embedding_onnx_provider or "").strip(),
                    "file_name": str(embedding_onnx_file_name or "").strip(),
                }.items()
                if value
            }
            logger.info(
                "Загрузка модели эмбеддинга: %s (backend=onnx, provider=%s, file=%s)",
                embedding_model,
                model_kwargs.get("provider") or "auto",
                model_kwargs.get("file_name") or "auto",
            )
            try:
                self.embedder = SentenceTransformer(
                    embedding_model,
                    backend="onnx",
                    model_kwargs=model_kwargs,
                    local_files_only=True,
                )
                logger.info("ONNX backend загружен успешно")
            except Exception as exc:
                logger.warning("ONNX backend недоступен (%s), использую PyTorch", exc)
                self.embedder = SentenceTransformer(embedding_model, local_files_only=True)
        else:
            logger.info("Загрузка модели эмбеддинга: %s", embedding_model)
            self.embedder = SentenceTransformer(embedding_model)

        # Режим подключения: сервер (Docker) или локальный SQLite
        if qdrant_url:
            logger.info("Подключение к Qdrant серверу: %s", qdrant_url)
            self.qdrant = QdrantClient(url=qdrant_url, timeout=self.qdrant_timeout_sec)
            # state БД рядом с qdrant_db_path и в серверном режиме тоже локально.
            self.qdrant_db_path.mkdir(parents=True, exist_ok=True)
        else:
            logger.info("Qdrant локальный режим: %s", qdrant_db_path)
            self.qdrant = QdrantClient(path=str(self.qdrant_db_path), timeout=self.qdrant_timeout_sec)
        self.state_db = IndexStateDB(str(self.qdrant_db_path / "index_state.db"))
        legacy_state_file = self.qdrant_db_path / "index_state.json"
        imported = self.state_db.bootstrap_from_json(legacy_state_file)
        if imported:
            logger.info("Импортировано %d записей legacy state.json в index_state.db", imported)

        self._setup_collection()
        self.state_db.validate_embedding_config(
            embedding_model=embedding_model,
            vector_size=vector_size,
            collection_name=self.collection_name,
            recreate=self.recreate,
        )
        self._ensure_payload_schema_version()

        self.point_count = 0

    def set_run_id(self, run_id: str) -> None:
        self.run_id = run_id or ""

    def _ensure_catalog_available(self) -> None:
        """Wait briefly for a disconnected catalog before aborting the run."""
        catalog_path = getattr(self, "catalog_path", None)
        if catalog_path is None:
            return

        def is_available() -> bool:
            try:
                return bool(catalog_path.exists())
            except OSError:
                return False

        if is_available():
            return

        attempts = max(0, int(getattr(self, "catalog_wait_attempts", 0) or 0))
        wait_seconds = max(1, int(getattr(self, "catalog_wait_seconds", 60) or 60))
        logger.warning(
            "Папка каталога недоступна: %s — жду появления (%d попыток, каждые %ds)…",
            catalog_path,
            attempts,
            wait_seconds,
        )
        for attempt in range(1, attempts + 1):
            time.sleep(wait_seconds)
            if is_available():
                logger.info("Каталог снова доступен: %s", catalog_path)
                return
            logger.warning(
                "Каталог всё ещё недоступен: %s (попытка %d/%d)",
                catalog_path,
                attempt,
                attempts,
            )
        raise RuntimeError(f"Папка каталога недоступна после {attempts} попыток: {catalog_path}")

    def _check_indexer_control(self, *, stage: str, stage_stats: Dict[str, int]) -> None:
        """Apply cooperative pause/cancel commands written by the UI."""
        command = str(read_indexer_control().get("command") or "running").lower()
        if command == "cancel":
            raise IndexerCancelled("Индексация отменена пользователем.")
        if command != "pause":
            self._ensure_catalog_available()
            return

        logger.info("Индексация поставлена на паузу пользователем.")
        if getattr(self, "run_id", ""):
            try:
                self.telemetry.update_stage(
                    run_id=self.run_id,
                    stage=stage,
                    processed_files=int(stage_stats.get("processed_files", 0)),
                    added_files=int(stage_stats.get("added_files", 0)),
                    updated_files=int(stage_stats.get("updated_files", 0)),
                    skipped_files=int(stage_stats.get("skipped_files", 0)),
                    error_files=int(stage_stats.get("error_files", 0)),
                    points_added=int(stage_stats.get("points_added", 0)),
                    status="paused",
                )
            except Exception:
                logger.debug("Не удалось обновить telemetry stage status=paused", exc_info=True)
        while command == "pause":
            time.sleep(1.0)
            command = str(read_indexer_control().get("command") or "running").lower()
            if command == "cancel":
                raise IndexerCancelled("Индексация отменена пользователем.")
        if getattr(self, "run_id", ""):
            try:
                self.telemetry.update_stage(
                    run_id=self.run_id,
                    stage=stage,
                    processed_files=int(stage_stats.get("processed_files", 0)),
                    added_files=int(stage_stats.get("added_files", 0)),
                    updated_files=int(stage_stats.get("updated_files", 0)),
                    skipped_files=int(stage_stats.get("skipped_files", 0)),
                    error_files=int(stage_stats.get("error_files", 0)),
                    points_added=int(stage_stats.get("points_added", 0)),
                    status="running",
                )
            except Exception:
                logger.debug("Не удалось обновить telemetry stage status=running", exc_info=True)
        self._ensure_catalog_available()

    # ── collection setup ───────────────────────────────────────────────

    def _setup_collection(self) -> None:
        recreated = ensure_collection(
            self.qdrant,
            collection_name=self.collection_name,
            vector_size=self.vector_size,
            recreate=self.recreate,
            fulltext_enabled=self.fulltext_enabled,
        )
        if recreated:
            self.state_db.clear()
            logger.info("state_entries очищен (--recreate)")

    def _ensure_payload_schema_version(self) -> int:
        desired = str(int(getattr(self, "payload_schema_version", PAYLOAD_SCHEMA_VERSION) or PAYLOAD_SCHEMA_VERSION))
        current = str((self.state_db.get_config() or {}).get("payload_schema_version") or "")
        if current == desired:
            return 0
        changed = 0
        has_existing_state = False
        try:
            has_existing_state = self.state_db.count() > 0
        except Exception:
            has_existing_state = False
        if has_existing_state and not getattr(self, "recreate", False):
            changed = self.state_db.mark_all_for_reindex(stage="metadata")
            if changed:
                logger.warning(
                    "Payload schema changed: stored=%s current=%s; %d files marked for reindex",
                    current or "(unset)",
                    desired,
                    changed,
                )
        self.state_db.set_config_many({"payload_schema_version": desired})
        return changed

    # ── fingerprint ────────────────────────────────────────────────────

    def _normalize_exclude_patterns(self, patterns: List[str]) -> List[str]:
        normalized: List[str] = []
        for pattern in patterns:
            value = str(pattern or "").strip().replace("\\", "/")
            if value:
                normalized.append(value)
        return normalized

    def _is_excluded_path(self, filepath: Path) -> bool:
        if not getattr(self, "exclude_patterns", None):
            return False
        try:
            rel = filepath.relative_to(self.catalog_path).as_posix()
        except ValueError:
            rel = filepath.as_posix()
        name = filepath.name
        for pattern in self.exclude_patterns:
            variants = [pattern]
            if pattern.startswith("**/"):
                variants.append(pattern[3:])
            if any(fnmatch.fnmatch(rel, item) or fnmatch.fnmatch(name, item) for item in variants):
                return True
        return False

    def _get_file_fingerprint(self, filepath: Path) -> Tuple[str, float]:
        """Быстрый fingerprint файла: размер + mtime."""
        stat = filepath.stat()
        fingerprint = f"{stat.st_size}_{stat.st_mtime}"
        return fingerprint, stat.st_mtime

    # ── stage helpers ──────────────────────────────────────────────────

    def _should_skip_for_stage(
        self,
        file_key: str,
        fingerprint: str,
        *,
        existing_entry: Optional[Dict[str, Any]] = None,
        entry_loaded: bool = False,
    ) -> bool:
        """
        True если файл уже покрыт текущим или более полным этапом.

        Правило: stage "metadata" покрывается любым current_stage != metadata
        только если файл УЖЕ имеет полное содержимое (т.е. state.stage == content).
        Если state.stage == metadata и мы сейчас на small/large — нужно ПРОАПГРЕЙДИТЬ.
        """
        existing = existing_entry if entry_loaded else self._get_state_entry(file_key)
        if not existing:
            return False
        if str(existing.get("fingerprint") or "") != fingerprint:
            return False  # файл изменился — обязательно переиндексируем

        existing_stage = str(existing.get("stage") or "content")  # backward compat
        existing_status = str(existing.get("status") or ("error" if existing_stage == "error" else "ok"))
        extension = str(existing.get("extension") or Path(file_key).suffix or "").lower()
        if (
            self.current_stage in {"small", "large"}
            and bool(getattr(self, "skip_ocr", False))
            and extension in {".pdf", *IMAGE_EXTENSIONS}
            and existing_status in {"deferred_ocr", "empty"}
            and existing_stage in {"metadata", "empty"}
            and str(existing.get("indexed_stage") or "") in {"small", "large"}
        ):
            return True
        # Если текущий этап = metadata, а файл уже проиндексирован (любым этапом) —
        # можем пропустить: мета-запись у файла уже есть.
        if self.current_stage == "metadata":
            return True
        if self.current_stage == "small":
            return existing_stage in ("content", "partial", "small")
        if self.current_stage == "large":
            if existing_stage != "content":
                return False
            indexed_stage = str(existing.get("indexed_stage") or "")
            try:
                indexed_chunks = int(existing.get("indexed_chunks") or 0)
                total_chunks = int(existing.get("total_chunks") or 0)
            except (TypeError, ValueError):
                indexed_chunks = 0
                total_chunks = 0
            # Старые quick/small записи могли быть ошибочно помечены как content
            # без счетчиков покрытия. Full-проход должен перепроверить их.
            if indexed_stage == "small" and (indexed_chunks <= 0 or (total_chunks > 0 and indexed_chunks < total_chunks)):
                return False
            return True
        return existing_stage in ("content", self.current_stage)

    def _get_state_entry(self, full_path: str) -> Optional[Dict[str, Any]]:
        """Read state entry from SQLite."""
        if hasattr(self, "state_db"):
            try:
                return self.state_db.get_entry(full_path)
            except Exception:
                return None
        return None

    def _upsert_state_entry(self, entry: Dict[str, Any]) -> None:
        """Write state entry to SQLite."""
        if hasattr(self, "state_db"):
            self.state_db.upsert_many([entry])

    # ── Qdrant vector deletion ─────────────────────────────────────────

    def _delete_file_vectors(self, filepath: Path, *, payload_match: Optional[Dict[str, Any]] = None) -> None:
        """Удалить все векторы в Qdrant, связанные с данным файлом или payload identity."""
        try:
            delete_file_vectors(
                self.qdrant,
                collection_name=self.collection_name,
                filepath=filepath,
                timeout_sec=self.qdrant_timeout_sec,
                payload_match=payload_match,
            )
            logger.debug("Удалены старые векторы для: %s", filepath)
        except Exception as exc:
            logger.warning("Не удалось удалить векторы для %s: %s", filepath, exc)

    # ── text extraction ────────────────────────────────────────────────

    def _extract_docx(self, filepath: Path) -> str:
        """Извлечь текст из DOCX (параграфы + таблицы)."""
        return extract_docx(filepath)

    def _extract_doc(self, filepath: Path) -> str:
        return extract_doc(filepath, max_chars=self._extractor_max_chars())

    def _extract_rtf(self, filepath: Path) -> str:
        return extract_rtf(filepath, max_chars=self._extractor_max_chars())

    def _extract_pptx(self, filepath: Path) -> str:
        return extract_pptx(filepath, max_chars=self._extractor_max_chars())

    def _extract_pptx_document(self, filepath: Path) -> ExtractedDocument:
        return extract_pptx_document(filepath, max_chars=self._extractor_max_chars())

    def _extract_spreadsheet(self, filepath: Path) -> str:
        """
        Точка входа для всех табличных форматов.
        Явно маршрутизирует .xls → xlrd, .xlsx → openpyxl.
        Раньше маршрутизация была внутри _extract_xlsx, что приводило
        к неочевидным варнингам при смешении форматов.
        """
        return extract_spreadsheet(filepath, max_chars=self._extractor_max_chars())

    def _extract_spreadsheet_document(self, filepath: Path) -> ExtractedDocument:
        return extract_spreadsheet_document(filepath, max_chars=self._extractor_max_chars())

    def _extract_text(self, filepath: Path) -> str:
        return extract_text(filepath, max_chars=self._extractor_max_chars())

    def _extract_csv(self, filepath: Path) -> str:
        return extract_csv(filepath, max_chars=self._extractor_max_chars())

    def _extract_html(self, filepath: Path) -> str:
        return extract_html(filepath, max_chars=self._extractor_max_chars())

    def _extractor_max_chars(self) -> int:
        if self.current_stage != "small":
            return 0
        return self.max_chunks_per_file * self.chunk_size if self.max_chunks_per_file else 0

    def _extract_pdf(self, filepath: Path) -> str:
        """
        Извлечь текст из PDF.
        Использует pymupdf (fitz) — в 3-5x быстрее pdfplumber.
        Fallback на pdfplumber если pymupdf не установлен.
        При пустом текстовом слое — OCR (если не --no-ocr).
        """
        text = extract_pdf(filepath, skip_ocr=self.skip_ocr, ocr=self._ocr_pdf)
        if text or not self.skip_ocr:
            return text
        return self._cached_ocr_text(filepath)

    def _extract_pdf_document(self, filepath: Path) -> ExtractedDocument:
        doc = extract_pdf_document(filepath, skip_ocr=self.skip_ocr, ocr=self._ocr_pdf)
        if doc.blocks or not self.skip_ocr:
            return doc
        return document_from_legacy_text(self._cached_ocr_text(filepath))

    def _ocr_result_identity(self, filepath: Path) -> tuple[str, float]:
        """Return the stable source identity, including members extracted to temp files."""
        context = getattr(self, "_ocr_context", None)
        logical_path = str(getattr(context, "logical_path", "") or filepath)
        logical_mtime = getattr(context, "logical_mtime", None)
        if logical_mtime is None:
            logical_mtime = float(filepath.stat().st_mtime)
        return logical_path, float(logical_mtime)

    def _get_cached_ocr_result(self, filepath: Path) -> Optional[Dict[str, Any]]:
        try:
            logical_path, logical_mtime = self._ocr_result_identity(filepath)
            cached = self.telemetry.get_ocr_file_result(logical_path, logical_mtime)
            if isinstance(cached, dict):
                if str(cached.get("status") or "").lower() != "error":
                    return cached
                logger.info("Повтор OCR после кэшированной ошибки: %s", logical_path)
        except Exception:
            pass
        return None

    def _cached_ocr_text(self, filepath: Path) -> str:
        cached = self._get_cached_ocr_result(filepath)
        if cached is not None:
            logger.info("OCR из кэша без запуска OCR: %s", filepath.name)
            return str(cached.get("extracted_text") or "")
        return ""

    def _ocr_pdf(self, filepath: Path) -> str:
        """OCR сканированного PDF через pytesseract + pdf2image, с кэшем в telemetry DB."""
        logical_path, logical_mtime = self._ocr_result_identity(filepath)
        cached = self._get_cached_ocr_result(filepath)
        if cached is not None:
            logger.info("OCR из кэша: %s", filepath.name)
            return str(cached.get("extracted_text") or "")

        diagnostics: Dict[str, Any] = {}
        started = time.perf_counter()
        try:
            text = ocr_pdf(
                filepath,
                tesseract_cmd=getattr(self, "ocr_tesseract_cmd", ""),
                poppler_bin=getattr(self, "ocr_poppler_bin", ""),
                use_rapid=getattr(self, "_use_rapid_ocr", False),
                raise_on_failure=True,
                batch_pages=getattr(self, "ocr_pdf_batch_pages", 8),
                rapid_fallback_enabled=getattr(self, "ocr_rapid_fallback_enabled", True),
                diagnostics=diagnostics,
            )
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            try:
                self.telemetry.save_ocr_file_result(
                    logical_path,
                    logical_mtime,
                    status="error",
                    error=str(exc),
                    requested_engine=str(diagnostics.get("requested_engine") or getattr(self, "ocr_engine", "")),
                    engine=str(diagnostics.get("engine") or getattr(self, "ocr_engine", "")),
                    fallback_used=bool(diagnostics.get("fallback_used")),
                    duration_ms=duration_ms,
                )
            except Exception:
                pass
            raise

        duration_ms = int((time.perf_counter() - started) * 1000)
        try:
            pages = text.count("Страница:") if text else 0
            if pages == 0 and text.strip():
                pages = 1
            chars = len(text.strip())
            self.telemetry.save_ocr_file_result(
                logical_path,
                logical_mtime,
                text=text,
                pages=pages,
                chars=chars,
                status="ok" if chars > 0 else "empty",
                requested_engine=str(diagnostics.get("requested_engine") or getattr(self, "ocr_engine", "")),
                engine=str(diagnostics.get("engine") or getattr(self, "ocr_engine", "")),
                fallback_used=bool(diagnostics.get("fallback_used")),
                duration_ms=duration_ms,
            )
        except Exception:
            pass

        return text

    def _extract_image(self, filepath: Path) -> str:
        """
        Извлечь текст из изображения через pytesseract (OCR).
        Поддерживает JPEG, PNG, GIF, BMP, TIFF, WEBP.
        Возвращает пустую строку если pytesseract не установлен или текст не найден.
        """
        logical_path, logical_mtime = self._ocr_result_identity(filepath)
        cached = self._get_cached_ocr_result(filepath)
        if cached is not None:
            logger.info("OCR из кэша: %s", filepath.name)
            return str(cached.get("extracted_text") or "")

        try:
            text = extract_image(
                filepath,
                tesseract_cmd=getattr(self, "ocr_tesseract_cmd", ""),
                max_pages=int(getattr(self, "ocr_max_image_pages", MAX_IMAGE_PAGES) or MAX_IMAGE_PAGES),
                use_rapid=getattr(self, "_use_rapid_ocr", False),
                raise_on_failure=True,
            )
        except Exception as exc:
            try:
                self.telemetry.save_ocr_file_result(
                    logical_path,
                    logical_mtime,
                    status="error",
                    error=str(exc),
                )
            except Exception:
                pass
            raise

        try:
            chars = len(text.strip())
            self.telemetry.save_ocr_file_result(
                logical_path,
                logical_mtime,
                text=text,
                pages=1 if chars > 0 else 0,
                chars=chars,
                status="ok" if chars > 0 else "empty",
            )
        except Exception:
            pass

        return text

    # ── chunking ───────────────────────────────────────────────────────

    def _chunk_text(self, text: str) -> List[str]:
        """Разбить текст на перекрывающиеся чанки."""
        return chunk_text(text, chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap)

    def _chunk_text_with_provenance(self, text: str | ExtractedDocument) -> List[Dict[str, Any]]:
        """Chunk structured blocks while coalescing rows and rejecting fragments."""
        items: List[Dict[str, Any]] = []
        blocks = list(text.blocks) if isinstance(text, ExtractedDocument) else blocks_from_legacy_text(str(text or ""))
        min_chunk_chars = max(20, int(getattr(self, "min_chunk_chars", 20) or 20))
        total_document_chars = sum(len(str(block.text or "").strip()) for block in blocks)
        allow_short_document = 0 < total_document_chars < min_chunk_chars
        pending: List[TextBlock] = []
        pending_chars = 0

        def group_key(block: TextBlock) -> tuple[Any, ...]:
            if block.sheet:
                return ("sheet", block.sheet)
            if block.page is not None:
                return ("page", block.page)
            if block.slide is not None:
                return ("slide", block.slide)
            if block.section:
                return ("section", block.section)
            return ("document",)

        def flush() -> None:
            nonlocal pending, pending_chars
            if not pending:
                return
            combined = "\n".join(
                str(block.text or "").strip()
                for block in pending
                if str(block.text or "").strip()
            ).strip()
            first = pending[0]
            last = pending[-1]
            row_values = [
                value
                for block in pending
                for value in (block.row_start, block.row_end)
                if value is not None
            ]
            merged_block = TextBlock(
                text=combined,
                page=first.page,
                sheet=first.sheet,
                row_start=min(row_values) if row_values else first.row_start,
                row_end=max(row_values) if row_values else last.row_end,
                slide=first.slide,
                section=first.section,
                metadata=dict(first.metadata or {}),
            )
            for chunk in self._chunk_text(combined):
                clean = str(chunk or "").strip()
                if len(clean) < min_chunk_chars and not allow_short_document:
                    continue
                items.append({"text": clean, "block": merged_block})
            pending = []
            pending_chars = 0

        for block in blocks:
            clean = str(block.text or "").strip()
            if not clean:
                continue
            same_group = not pending or group_key(pending[-1]) == group_key(block)
            projected = pending_chars + (1 if pending else 0) + len(clean)
            if pending and (
                not same_group
                or (projected > self.chunk_size and pending_chars >= self.min_chunk_chars)
            ):
                flush()
            pending.append(block)
            pending_chars += (1 if len(pending) > 1 else 0) + len(clean)
        flush()
        return items

    def _content_hash(self, text: str) -> str:
        normalized = "\n".join(line.strip() for line in str(text or "").splitlines() if line.strip())
        if not normalized:
            return ""
        return hashlib.sha256(normalized[:1_000_000].encode("utf-8", errors="ignore")).hexdigest()

    def _semantic_chunk_end(self, text: str, start: int, max_end: int) -> int:
        return semantic_chunk_end(text, start=start, max_end=max_end, chunk_size=self.chunk_size)

    @staticmethod
    def _strip_provenance_markers(text: str) -> str:
        lines = []
        for line in str(text or "").splitlines():
            if re.match(r"^\s*(Страница|Лист|Строка):", line, flags=re.IGNORECASE):
                continue
            lines.append(line)
        return "\n".join(lines).strip()

    def _doc_id(self, state_key: str, relative_path: Path, payload_extra: Optional[Dict[str, Any]] = None) -> str:
        if payload_extra:
            cloud_id = str(payload_extra.get("cloud_file_id") or "").strip()
            if cloud_id:
                return f"cloud:{cloud_id}"
        key = str(state_key or relative_path)
        return "file:" + hashlib.sha1(key.encode("utf-8", errors="ignore")).hexdigest()

    def _base_provenance(
        self,
        *,
        filepath: Path,
        relative_path: Path,
        state_key: Optional[str],
        payload_extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        doc_id = self._doc_id(str(state_key or filepath), relative_path, payload_extra)
        return {
            "doc_id": doc_id,
            "parent_id": doc_id,
            "section": "",
            "page": None,
            "sheet": "",
            "row_start": None,
            "row_end": None,
            "provenance": {
                "doc_id": doc_id,
                "path": str(relative_path),
                "full_path": str(filepath),
            },
        }

    def _chunk_provenance(
        self,
        *,
        chunk: str,
        chunk_index: int,
        doc_id: str,
        block: Optional[TextBlock] = None,
    ) -> Dict[str, Any]:
        page = block.page if block and block.page is not None else self._extract_marker_int(chunk, r"Страница:\s*(\d+)")
        row = (
            block.row_start
            if block and block.row_start is not None
            else self._extract_marker_int(chunk, r"Строка:\s*(\d+)")
        )
        sheet = block.sheet if block and block.sheet else self._extract_marker_text(chunk, r"Лист:\s*([^\n\r]+)")
        slide = block.slide if block and block.slide is not None else None
        section = self._extract_section_title(chunk)
        group_size = max(1, int(getattr(self, "chunk_group_size", 4) or 4))
        parent_id = f"{doc_id}:chunk-group:{chunk_index // group_size}"
        return {
            "parent_id": parent_id,
            "section": section,
            "page": page,
            "sheet": sheet,
            "slide": slide,
            "row_start": row,
            "row_end": row,
            "provenance": {
                "doc_id": doc_id,
                "parent_id": parent_id,
                "section": section,
                "page": page,
                "sheet": sheet,
                "slide": slide,
                "row_start": row,
                "row_end": row,
            },
        }

    @staticmethod
    def _extract_marker_int(text: str, pattern: str) -> Optional[int]:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        if not match:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_marker_text(text: str, pattern: str) -> str:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        return str(match.group(1)).strip()[:160] if match else ""

    @staticmethod
    def _extract_section_title(text: str) -> str:
        for line in str(text or "").splitlines():
            title = line.strip()
            if not title:
                continue
            if re.match(r"^(Страница|Лист|Строка):", title, flags=re.IGNORECASE):
                continue
            if len(title) <= 120 and (title.isupper() or re.match(r"^\d+(?:\.\d+)*[.)]?\s+\S+", title)):
                return title[:120]
            break
        return ""

    # ── single file ────────────────────────────────────────────────────

    def process_file(
        self,
        filepath: Path,
        *,
        logical_path: Optional[str] = None,
        state_key: Optional[str] = None,
        payload_extra: Optional[Dict[str, Any]] = None,
        fingerprint_override: str = "",
        delete_payload_match: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Обработать один файл: извлечь текст, нарезать и записать батчем."""
        relative_path = Path(logical_path) if logical_path else filepath.relative_to(self.catalog_path)
        fingerprint, mtime = self._get_file_fingerprint(filepath)
        if fingerprint_override:
            fingerprint = fingerprint_override
        file_key = str(state_key or filepath)

        existing_entry = self._get_state_entry(file_key)
        if existing_entry:
            if str(existing_entry.get("fingerprint") or "") == fingerprint:
                logger.debug("Файл не изменился, пропуск: %s", filepath)
                return
            logger.info("Файл изменился, удаляю старые векторы: %s", filepath)
            self._delete_file_vectors(filepath, payload_match=delete_payload_match)

        logger.info("Индексирование: %s", filepath)
        doc_meta = extract_doc_meta(filepath)
        payload_extra = {**(payload_extra or {}), **doc_meta}

        ext = filepath.suffix.lower()
        full_text = ""
        extracted_doc: Optional[ExtractedDocument] = None
        file_type = ext.lstrip(".") or "file"

        if ext == ".docx":
            full_text = self._extract_docx(filepath)
            file_type = "docx"
        elif ext == ".doc":
            full_text = self._extract_doc(filepath)
            file_type = "doc"
        elif ext in (".xlsx", ".xlsm", ".xls"):
            extracted_doc = self._extract_spreadsheet_document(filepath)
            full_text = extracted_doc.text
            file_type = "xlsx"
        elif ext == ".rtf":
            full_text = self._extract_rtf(filepath)
            file_type = "rtf"
        elif ext == ".pptx":
            extracted_doc = self._extract_pptx_document(filepath)
            full_text = extracted_doc.text
            file_type = "pptx"
        elif ext == ".txt":
            full_text = self._extract_text(filepath)
            file_type = "txt"
        elif ext == ".csv":
            full_text = self._extract_csv(filepath)
            file_type = "csv"
        elif ext in (".html", ".htm"):
            full_text = self._extract_html(filepath)
            file_type = "html"
        elif ext == ".pdf":
            extracted_doc = self._extract_pdf_document(filepath)
            full_text = extracted_doc.text
            file_type = "pdf"
        elif ext in IMAGE_EXTENSIONS:
            if self.skip_ocr:
                full_text = self._cached_ocr_text(filepath)
            else:
                full_text = self._extract_image(filepath)
            file_type = "image"
        else:
            logger.debug("Неподдерживаемый формат (только метаданные): %s", ext)

        chunk_source = extracted_doc if extracted_doc is not None else full_text
        chunk_items = self._chunk_text_with_provenance(chunk_source) if full_text.strip() else []
        chunks = [str(item.get("text") or "") for item in chunk_items]
        total_chunks = len(chunks)
        content_hash = self._content_hash(full_text)
        duplicate_of = ""
        if content_hash and hasattr(self, "state_db"):
            duplicate = self.state_db.find_by_content_hash(content_hash, exclude_path=file_key)
            duplicate_of = str((duplicate or {}).get("full_path") or "")
        current_stage = str(getattr(self, "current_stage", "") or "content")
        stage_chunk_limit = int(self.max_chunks_per_file or 0) if current_stage == "small" else 0
        if stage_chunk_limit and len(chunks) >= stage_chunk_limit:
            logger.debug(
                "Файл %s: %d чанков, обрезано до %d (--max-chunks-per-file)",
                filepath.name,
                len(chunks),
                stage_chunk_limit,
            )
            chunk_items = chunk_items[:stage_chunk_limit]
            chunks = [str(item.get("text") or "") for item in chunk_items]
            total_chunks = max(total_chunks, len(chunks) + 1)
        if not chunks:
            logger.warning("Файл %s: контент пуст, сохраняю только metadata stage", filepath.name)

        try:
            size_bytes = int(filepath.stat().st_size)
        except OSError:
            size_bytes = 0
        stat = filepath.stat()
        tags = _generate_tags(filepath, relative_path, full_text, getattr(self, "synonym_map", {}) or {})
        meta_text = f"Файл: {filepath.name} | Путь: {relative_path} | Расширение: {filepath.suffix}"
        if doc_meta.get("doc_author"):
            meta_text += f" | Автор: {doc_meta['doc_author']}"
        if doc_meta.get("doc_last_editor"):
            meta_text += f" | Редактор: {doc_meta['doc_last_editor']}"
        if tags:
            meta_text += f" | Теги: {', '.join(tags[:30])}"

        base_provenance = self._base_provenance(
            filepath=filepath,
            relative_path=relative_path,
            state_key=file_key,
            payload_extra=payload_extra,
        )
        payload_schema_version = int(
            getattr(self, "payload_schema_version", PAYLOAD_SCHEMA_VERSION) or PAYLOAD_SCHEMA_VERSION
        )
        meta_payload: Dict[str, Any] = {
            "type": "file_metadata",
            "payload_schema_version": payload_schema_version,
            "text": meta_text,
            "filename": filepath.name,
            "extension": ext,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
            "path": str(relative_path),
            "full_path": str(filepath),
            "state_key": file_key,
            "tags": tags,
            "content_hash": content_hash,
            "is_duplicate": bool(duplicate_of),
            "duplicate_of": duplicate_of,
            **doc_meta,
            **base_provenance,
            **(payload_extra or {}),
        }
        add_numeric_tokens(meta_payload, meta_text, filepath.name, str(relative_path))

        clean_chunks = [self._strip_provenance_markers(chunk) or chunk for chunk in chunks]
        texts = [meta_text, *clean_chunks]
        payloads: List[Dict[str, Any]] = [meta_payload]
        doc_id = str(base_provenance["doc_id"])
        for idx, item in enumerate(chunk_items):
            chunk = str(item.get("text") or "")
            clean_chunk = clean_chunks[idx]
            block = item.get("block")
            chunk_payload = {
                    "type": f"{file_type}_content",
                    "payload_schema_version": payload_schema_version,
                    "text": clean_chunk,
                    "filename": filepath.name,
                    "extension": ext,
                    "path": str(relative_path),
                    "full_path": str(filepath),
                    "chunk_index": idx,
                    "state_key": file_key,
                    "tags": tags,
                    "content_hash": content_hash,
                    "is_duplicate": bool(duplicate_of),
                    "duplicate_of": duplicate_of,
                    **doc_meta,
                    **base_provenance,
                    **self._chunk_provenance(
                        chunk=chunk,
                        chunk_index=idx,
                        doc_id=doc_id,
                        block=block if isinstance(block, TextBlock) else None,
                    ),
                    **(payload_extra or {}),
                }
            add_numeric_tokens(chunk_payload, clean_chunk, filepath.name, str(relative_path))
            payloads.append(chunk_payload)
        vectors = self.embedder.encode(
            prepare_passage_texts(str(getattr(self, "embedding_model", "") or ""), texts),
            normalize_embeddings=True,
            batch_size=max(1, min(256, int(self.batch_size or 64))),
            show_progress_bar=False,
        )
        def _point_id(payload: Dict[str, Any]) -> str:
            doc_id = str(payload.get("doc_id") or payload.get("full_path") or "")
            if str(payload.get("type") or "") == "file_metadata":
                key = f"{doc_id}:metadata"
            else:
                key = f"{doc_id}:chunk:{int(payload.get('chunk_index') or 0)}"
            return str(uuid.uuid5(uuid.NAMESPACE_URL, key))

        points = [PointStruct(id=_point_id(p), vector=v.tolist(), payload=p) for v, p in zip(vectors, payloads)]
        written = upsert_points(
            self.qdrant,
            collection_name=self.collection_name,
            points=points,
            timeout_sec=self.qdrant_timeout_sec,
        )
        self.point_count += written
        stage = (
            "partial"
            if current_stage == "small" and chunks and total_chunks > len(chunks)
            else "content" if chunks else "metadata"
        )
        status = "ok" if chunks else "empty"
        self._upsert_state_entry(
            {
                "full_path": file_key,
                "fingerprint": fingerprint,
                "mtime": mtime,
                "stage": stage,
                "indexed_stage": str(getattr(self, "current_stage", "") or "content"),
                "status": status,
                "last_error": "",
                "next_retry_at": 0,
                "size_bytes": size_bytes,
                "extension": filepath.suffix.lower(),
                "content_hash": content_hash,
                "indexed_chunks": len(chunks),
                "total_chunks": total_chunks,
                **(payload_extra or {}),
            }
        )

    # ── directory scan ─────────────────────────────────────────────────

    def index_directory(self, stage: str = "content") -> Dict[str, int]:
        from .indexing.stage_runner import IndexStageRunner

        return IndexStageRunner(
            self,
            stages=STAGES,
            supported_extensions=SUPPORTED_EXTENSIONS,
            image_extensions=IMAGE_EXTENSIONS,
            file_category=_file_category,
            generate_tags=_generate_tags,
            logger=logger,
        ).run(stage)

    def index_all_stages(self, stages: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Прогоняет индексирование последовательно по всем этапам.

        По умолчанию: metadata → small → large.
        После каждого этапа индекс УЖЕ пригоден для поиска, качество растёт
        прогрессивно. Если процесс прерывать/перезапускать — продолжит с того
        этапа, на котором остановился (благодаря полю `stage` в state БД).
        """
        stages = list(stages) if stages else list(STAGES)
        logger.info("▶ Многоэтапная индексация: %s", " → ".join(stages))
        totals: Dict[str, int] = {
            "total_files": 0,
            "processed_files": 0,
            "added_files": 0,
            "updated_files": 0,
            "skipped_files": 0,
            "error_files": 0,
            "points_added": 0,
        }
        for stage in stages:
            # point_count — счётчик на сессию, сбрасываем между этапами чтобы
            # логи «итого» были понятнее.
            self.point_count = 0
            stage_stats = self.index_directory(stage=stage)
            totals["total_files"] = max(totals["total_files"], int(stage_stats.get("total_files", 0)))
            for k in (
                "processed_files",
                "added_files",
                "updated_files",
                "skipped_files",
                "error_files",
                "points_added",
            ):
                totals[k] += int(stage_stats.get(k, 0))
        logger.info("✔ Все этапы завершены: %s", ", ".join(stages))
        return totals

    def quality_report(self) -> Dict[str, Any]:
        """Return indexing quality counters derived from state and OCR telemetry."""
        stats = self.state_db.stats()
        total = int(stats.get("total") or 0)
        by_stage = dict(stats.get("by_stage") or {})
        by_status = dict(stats.get("by_status") or {})
        by_indexed_stage = dict(stats.get("by_indexed_stage") or {})
        content_files = int(by_stage.get("content") or 0)
        error_files = int((by_status.get("error") if by_status else by_stage.get("error")) or 0)
        empty_files = int((by_status.get("empty") if by_status else by_stage.get("empty")) or 0)
        metadata_files = int(by_stage.get("metadata") or 0)
        report: Dict[str, Any] = {
            "total_files": total,
            "stage_distribution": by_stage,
            "status_distribution": by_status,
            "indexed_stage_distribution": by_indexed_stage,
            "by_extension": stats.get("by_ext") or {},
            "content_coverage_pct": round((content_files / total) * 100, 2) if total else 0.0,
            "metadata_only_files": metadata_files,
            "empty_files": empty_files,
            "error_files": error_files,
            "error_pct": round((error_files / total) * 100, 2) if total else 0.0,
            "failed_paths": int(stats.get("failed_paths") or 0),
            "duplicate_groups": int(stats.get("duplicate_groups") or 0),
            "duplicate_files": int(stats.get("duplicate_files") or 0),
        }
        try:
            rows = self.telemetry.fetch_dicts(
                """
                SELECT status, COUNT(*) AS cnt, COALESCE(SUM(chars), 0) AS chars
                FROM ocr_file_results
                GROUP BY status
                """
            )
            report["ocr_results"] = {
                str(row.get("status") or "unknown"): {
                    "files": int(row.get("cnt") or 0),
                    "chars": int(row.get("chars") or 0),
                }
                for row in rows
            }
        except Exception:
            report["ocr_results"] = {}
        return report

    def _deleted_file_candidates(
        self,
        existing_files: List[Path | str],
        *,
        preserve_members_of_existing_archives: bool = False,
        allow_empty_inventory: bool = False,
    ) -> List[str]:
        """Return state keys absent from the current filesystem inventory.

        A lightweight cleanup inventory contains archive files but not their
        logical ``archive::member`` entries. Preserve those entries while the
        parent archive still exists; full metadata/content inventory passes
        enumerate members and remove stale members precisely.
        """
        existing_paths = {str(f) for f in existing_files}
        if not existing_paths and not allow_empty_inventory:
            state_total = int((self.state_db.stats() or {}).get("total") or 0)
            if state_total:
                raise RuntimeError(
                    "Cleanup отменён: filesystem inventory пуст, но state содержит "
                    f"{state_total} записей. Проверьте доступность каталога или повторите "
                    "с явным --allow-empty-cleanup."
                )
        deleted_keys = self.state_db.list_deleted_candidates(existing_paths)
        if preserve_members_of_existing_archives and deleted_keys:
            archive_exists: Dict[str, bool] = {}
            retained_archive_members = 0
            filtered_keys: List[str] = []
            for key in deleted_keys:
                archive_path, separator, _member = str(key).partition("::")
                if not separator:
                    filtered_keys.append(key)
                    continue
                if archive_path not in archive_exists:
                    archive_exists[archive_path] = Path(archive_path).is_file()
                exists = archive_exists[archive_path]
                if exists:
                    retained_archive_members += 1
                else:
                    filtered_keys.append(key)
            if retained_archive_members:
                logger.info(
                    "Cleanup сохраняет %d элементов существующих архивов; "
                    "точная очистка элементов выполняется полным inventory",
                    retained_archive_members,
                )
            deleted_keys = filtered_keys
        return deleted_keys

    def _cleanup_deleted_files(
        self,
        existing_files: List[Path | str],
        *,
        preserve_members_of_existing_archives: bool = False,
        allow_empty_inventory: bool = False,
    ) -> int:
        """Удалить из state БД и Qdrant файлы, которых больше нет на диске."""
        deleted_keys = self._deleted_file_candidates(
            existing_files,
            preserve_members_of_existing_archives=preserve_members_of_existing_archives,
            allow_empty_inventory=allow_empty_inventory,
        )
        if not deleted_keys:
            return 0
        logger.info("Удаление %d удалённых файлов из индекса…", len(deleted_keys))
        for key in deleted_keys:
            self._delete_file_vectors(Path(key))
        self.state_db.delete_entries(deleted_keys)
        return len(deleted_keys)

    def process_index_queue_once(self, *, limit: int = 10, lease_seconds: int = 300) -> Dict[str, int]:
        """Process a small batch from durable index_queue."""
        if not hasattr(self, "state_db"):
            return {"leased": 0, "completed": 0, "failed": 0, "missing": 0}
        self.state_db.requeue_expired_index_tasks()
        tasks = self.state_db.lease_index_tasks(limit=limit, lease_seconds=lease_seconds)
        stats = {"leased": len(tasks), "completed": 0, "failed": 0, "missing": 0}
        for task in tasks:
            task_id = int(task["id"])
            path = Path(str(task.get("full_path") or ""))
            stage = str(task.get("stage") or "content")
            try:
                if not path.exists():
                    self._delete_file_vectors(path)
                    self.state_db.delete_entries([str(path)])
                    self.state_db.complete_index_task(task_id)
                    stats["missing"] += 1
                    continue
                if self._is_excluded_path(path):
                    self.state_db.complete_index_task(task_id)
                    stats["completed"] += 1
                    continue
                if path.suffix.lower() == ".zip":
                    if stage == "all":
                        self.index_all_stages()
                    else:
                        self.index_directory(stage=stage)
                else:
                    self.process_file(path)
                self.state_db.complete_index_task(task_id)
                stats["completed"] += 1
            except Exception as exc:
                logger.warning("Queue task failed for %s: %s", path, exc)
                self.state_db.fail_index_task(task_id, error=str(exc))
                stats["failed"] += 1
        return stats

    def drain_index_queue(
        self,
        *,
        limit: Optional[int] = None,
        max_batches: int = 10,
        lease_seconds: int = 300,
    ) -> Dict[str, int]:
        """Drain bounded queue batches without letting watch loop spin forever."""
        batch_limit = max(1, int(limit or getattr(self, "read_workers", 1) or 1))
        totals = {"leased": 0, "completed": 0, "failed": 0, "missing": 0}
        for _ in range(max(1, int(max_batches))):
            stats = self.process_index_queue_once(limit=batch_limit, lease_seconds=lease_seconds)
            for key in totals:
                totals[key] += int(stats.get(key) or 0)
            if int(stats.get("leased") or 0) < batch_limit:
                break
        return totals

    def watch(self, stage: str = "content") -> None:
        """Watch catalog changes and incrementally reindex changed files."""
        try:
            from watchdog.events import FileSystemEventHandler  # type: ignore
            from watchdog.observers import Observer  # type: ignore
        except ImportError as exc:
            raise RuntimeError("Для --watch установите зависимость watchdog") from exc

        wake_queue: "queue.Queue[None]" = queue.Queue(maxsize=1)
        watch_stage = "small" if stage == "all" else stage
        zip_debounce_seconds = 5.0

        def _wake() -> None:
            try:
                wake_queue.put_nowait(None)
            except queue.Full:
                pass

        class _Handler(FileSystemEventHandler):
            def on_created(self, event):  # type: ignore[no-untyped-def]
                self._enqueue(event, "created")

            def on_modified(self, event):  # type: ignore[no-untyped-def]
                self._enqueue(event, "changed")

            def on_moved(self, event):  # type: ignore[no-untyped-def]
                self._enqueue(event, "moved")

            def on_deleted(self, event):  # type: ignore[no-untyped-def]
                self._enqueue(event, "deleted")

            def _enqueue(self, event, reason: str):  # type: ignore[no-untyped-def]
                if getattr(event, "is_directory", False):
                    return
                raw = str(getattr(event, "dest_path", "") or getattr(event, "src_path", "") or "")
                if not raw:
                    return
                path = Path(raw)
                if path.suffix.lower() in SUPPORTED_EXTENSIONS and not path.name.startswith("~$"):
                    available_at = time.time() + zip_debounce_seconds if path.suffix.lower() == ".zip" else None
                    self_indexer.state_db.enqueue_index_task(
                        str(path),
                        stage=watch_stage,
                        reason=f"watch:{reason}",
                        priority=20,
                        available_at=available_at,
                    )
                    _wake()

        self_indexer = self
        observer = Observer()
        observer.schedule(_Handler(), str(self.catalog_path), recursive=True)
        observer.start()
        logger.info("Watch mode запущен: %s", self.catalog_path)
        try:
            while True:
                try:
                    wake_queue.get(timeout=5.0)
                except queue.Empty:
                    pass
                time.sleep(0.5)
                stats = self.drain_index_queue(limit=max(1, self.read_workers), max_batches=10)
                if stats.get("leased"):
                    logger.info("Watch queue processed: %s", stats)
        finally:
            observer.stop()
            observer.join(timeout=10)


# ─────────────────────────── CLI entry point ───────────────────────────


def _parse_extension_csv(value: str) -> set[str]:
    return {
        item.strip().lower() if item.strip().startswith(".") else "." + item.strip().lower()
        for item in str(value or "").split(",")
        if item.strip()
    }


def _configure_forced_replacement(
    indexer: "RAGIndexer",
    *,
    mark_stage_metadata_for: str,
    force_replace_for: str,
    dry_run: bool,
) -> None:
    forced_extensions = _parse_extension_csv(force_replace_for)
    marked_extensions = _parse_extension_csv(mark_stage_metadata_for)
    if marked_extensions:
        if dry_run:
            logger.info("--dry-run: миграция state --mark-stage-metadata-for не выполняется.")
        else:
            changed = indexer.state_db.update_stage_for_extensions(marked_extensions, stage="metadata")
            forced_extensions.update(marked_extensions)
            logger.info(
                "Миграция state: %d записей с расширениями %s помечены stage=metadata",
                changed,
                sorted(marked_extensions),
            )

    indexer.force_replace_extensions = forced_extensions
    if forced_extensions:
        logger.info("Полная замена старых векторов включена для расширений: %s", sorted(forced_extensions))


def main() -> None:
    cfg = load_config()

    # Добавляем FileHandler с UTF-8 (путь к логу из конфига)
    log_file = cfg.get("log_file")
    if log_file:
        try:
            fh = build_log_handler(log_file, label="index_rag")
            logging.getLogger().addHandler(fh)
        except Exception as exc:
            logger.warning("Не удалось открыть лог-файл %s: %s", log_file, exc)

    parser = argparse.ArgumentParser(
        description="RAG Indexer для DOCX/XLSX/XLS/PDF файлов",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Режимы работы:\n"
            "  (по умолчанию)        — многоэтапно: metadata → small → large\n"
            "  --stage metadata      — только имена/пути/размеры всех файлов (минуты)\n"
            "  --stage small         — быстрый проход: все файлы, первые --max-chunks чанков\n"
            "  --stage large         — полный проход: догрузить оставшиеся чанки всех файлов\n"
            "  --stage all           — все этапы подряд (поведение по умолчанию)\n"
            "  --watch               — после первичного прохода следить за изменениями каталога\n"
            "  --recreate            — пересоздать коллекцию и очистить state\n"
            "  --cleanup             — только удалить из индекса файлы, которых нет на диске\n"
        ),
    )
    parser.add_argument("--catalog", default=cfg["catalog_path"], help="Папка для индексирования")
    parser.add_argument("--db", default=cfg["qdrant_db_path"], help="Путь к локальной базе Qdrant (SQLite режим)")
    parser.add_argument(
        "--url",
        default=str(cfg.get("qdrant_url") or ""),
        dest="qdrant_url",
        help="URL Qdrant-сервера (например http://localhost:6333). Если указан — используется вместо --db",
    )
    parser.add_argument("--model", default=cfg["embedding_model"], help="Модель эмбеддинга")
    parser.add_argument("--collection", default=cfg["collection_name"], help="Имя коллекции")
    parser.add_argument("--recreate", action="store_true", help="Пересоздать коллекцию и очистить state")
    parser.add_argument(
        "--ocr-engine",
        default=str(cfg.get("ocr_engine") or "tesseract"),
        dest="ocr_engine",
        choices=("tesseract", "rapidocr"),
        help="OCR движок: tesseract (CPU, по умолчанию) или rapidocr (GPU/DirectML)",
    )
    parser.add_argument(
        "--no-ocr",
        action="store_true",
        dest="no_ocr",
        help="Пропускать OCR для сканированных PDF (быстрее, текст не извлекается)",
    )
    parser.add_argument(
        "--force-ocr",
        action="store_true",
        dest="force_ocr",
        help="Принудительно выполнять OCR, даже если в config включён index_skip_ocr.",
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=int(cfg.get("index_max_chunks", 5)),
        dest="max_chunks",
        help="Лимит чанков для --stage small (по умолчанию 5; 0 = без ограничений). --stage large всегда без лимита.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(cfg.get("index_read_workers", 4)),
        dest="workers",
        help="Число параллельных потоков для чтения файлов (по умолчанию 4)",
    )
    parser.add_argument(
        "--onnx",
        action="store_true",
        dest="use_onnx",
        help="Использовать ONNX Runtime для encode (быстрее, но может не работать на Python 3.14)",
    )
    default_stage = str(cfg.get("index_default_stage", "all")).strip().lower()
    if default_stage not in ("all", "full", *STAGES):
        default_stage = "all"
    parser.add_argument(
        "--stage",
        default=default_stage,
        choices=("all", "full", *STAGES),
        help="Этап индексирования. По умолчанию 'all'/'full' — прогон всех этапов "
        "(metadata → small → large). Можно запустить отдельный этап для "
        "дробного прогресса или для тонкой настройки фоновых задач.",
    )
    parser.add_argument(
        "--metadata-only-for",
        default="",
        dest="metadata_only_for",
        help="[legacy] Список расширений через запятую для индексирования ТОЛЬКО "
        "метаданных (например: .pdf). Используется в рамках одного stage. "
        "Современный эквивалент: --stage metadata.",
    )
    parser.add_argument(
        "--metadata-only", action="store_true", dest="metadata_only", help="[legacy] Псевдоним для --stage metadata."
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help=("Только очистить индекс от файлов, которые удалены с диска, без полного сканирования. Быстро (~1 мин)."),
    )
    parser.add_argument(
        "--allow-empty-cleanup",
        action="store_true",
        help="Разрешить --cleanup удалить непустой state при пустом filesystem inventory. "
        "Использовать только для подтверждённо пустого каталога.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать файлы, которые будут обработаны, и причины без записи в Qdrant/state.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="После первичного прогона следить за изменениями каталога и индексировать события.",
    )
    parser.add_argument(
        "--quality-report",
        action="store_true",
        help="Вывести JSON-отчёт качества индексирования и выйти без запуска индексации.",
    )
    parser.add_argument(
        "--mark-stage-metadata-for",
        default="",
        dest="mark_stage_metadata_for",
        help="Перед индексированием пометить уже имеющиеся в state записи указанных расширений "
        "как stage=metadata (чтобы они переиндексировались на этапах small/large). "
        "Пример: --mark-stage-metadata-for .pdf. Пригодится после legacy-прохода --metadata-only-for.",
    )
    parser.add_argument(
        "--force-replace-for",
        default="",
        dest="force_replace_for",
        help="При индексировании полностью заменить старые векторы для указанных расширений, "
        "не изменяя текущий stage в state. Используйте для продолжения прерванной миграции, "
        "которую ранее запустили с --mark-stage-metadata-for.",
    )
    parser.add_argument(
        "--only-paths-file",
        default="",
        dest="only_paths_file",
        help="Обработать только пути из UTF-8 файла (по одному full_path/state_key на строку).",
    )
    args = parser.parse_args()
    if args.force_ocr:
        args.no_ocr = False
    elif str(args.stage or "").lower() in {"all", "full", "small", "large"}:
        args.no_ocr = True
    elif bool(cfg.get("index_skip_ocr", False)) and "--no-ocr" not in sys.argv:
        args.no_ocr = True
    args.collection = resolve_embedding_collection_name(
        args.collection,
        args.model,
        enabled=bool(cfg.get("embedding_collection_versioning", False)),
        suffix=str(cfg.get("embedding_collection_suffix") or ""),
    )

    # Разбор stage и legacy-флагов
    metadata_only_extensions: set = set()
    stage = args.stage
    if args.metadata_only:
        # legacy: --metadata-only → --stage metadata
        stage = "metadata"
        logger.info("Legacy --metadata-only: использую --stage metadata")
    elif args.metadata_only_for:
        metadata_only_extensions = {
            e.strip().lower() if e.strip().startswith(".") else "." + e.strip().lower()
            for e in args.metadata_only_for.split(",")
            if e.strip()
        }
        logger.info(
            "Legacy --metadata-only-for для расширений: %s (в рамках --stage %s)",
            sorted(metadata_only_extensions),
            stage,
        )

    telemetry_path = (cfg.get("telemetry_db_path") or "").strip()
    if not telemetry_path:
        telemetry_path = str(Path(args.db) / "rag_telemetry.db")
    only_paths: set[str] = set()
    if args.only_paths_file:
        only_paths_path = Path(str(args.only_paths_file))
        only_paths = {
            line.strip()
            for line in only_paths_path.read_text(encoding="utf-8", errors="replace").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        logger.info("Ограничение индексации по файлу %s: %d путей", only_paths_path, len(only_paths))

    indexer = RAGIndexer(
        catalog_path=args.catalog,
        qdrant_db_path=args.db,
        embedding_model=args.model,
        collection_name=args.collection,
        vector_size=cfg["vector_size"],
        chunk_size=cfg["chunk_size"],
        chunk_overlap=cfg["chunk_overlap"],
        chunk_group_size=int(cfg.get("chunk_group_size", 4) or 4),
        batch_size=cfg["batch_size"],
        recreate_collection=args.recreate,
        skip_ocr=args.no_ocr,
        max_chunks_per_file=args.max_chunks,
        read_workers=args.workers,
        use_onnx=args.use_onnx,
        qdrant_url=args.qdrant_url,
        metadata_only_extensions=metadata_only_extensions,
        telemetry_db_path=telemetry_path,
        small_office_mb=float(cfg.get("small_office_mb", DEFAULT_SMALL_OFFICE_MB)),
        small_pdf_mb=float(cfg.get("small_pdf_mb", DEFAULT_SMALL_PDF_MB)),
        synonym_map=cfg.get("synonym_map") or {},
        ollama_url=str(cfg.get("ollama_url") or "http://localhost:11434"),
        ocr_tesseract_cmd=str(cfg.get("ocr_tesseract_cmd") or ""),
        ocr_poppler_bin=str(cfg.get("ocr_poppler_bin") or ""),
        ocr_engine=str(getattr(args, "ocr_engine", None) or cfg.get("ocr_engine") or "tesseract"),
        ocr_pdf_batch_pages=int(cfg.get("ocr_pdf_batch_pages", 8) or 8),
        ocr_rapid_fallback_enabled=bool(cfg.get("ocr_rapid_fallback_enabled", True)),
        qdrant_timeout_sec=int(cfg.get("qdrant_timeout_sec", 60) or 60),
        min_chunk_chars=int(cfg.get("index_min_chunk_chars", 120) or 120),
        fulltext_enabled=(
            bool(cfg.get("retrieval_fulltext_enabled", False))
            and not bool(cfg.get("index_defer_fulltext", False))
        ),
        embedding_backend=str(cfg.get("index_embedding_backend") or cfg.get("embedding_backend") or ""),
        embedding_onnx_provider=str(
            cfg.get("index_embedding_onnx_provider") or cfg.get("embedding_onnx_provider") or ""
        ),
        embedding_onnx_file_name=str(
            cfg.get("index_embedding_onnx_file_name") or cfg.get("embedding_onnx_file_name") or ""
        ),
        exclude_patterns=list(cfg.get("index_exclude_patterns") or []),
        only_paths=only_paths,
        ocr_max_image_pages=int(cfg.get("ocr_max_image_pages", MAX_IMAGE_PAGES) or MAX_IMAGE_PAGES),
        catalog_wait_attempts=int(cfg.get("catalog_wait_attempts", 10) or 10),
        catalog_wait_seconds=int(cfg.get("catalog_wait_seconds", 60) or 60),
    )
    if args.quality_report:
        print(json.dumps(indexer.quality_report(), ensure_ascii=False, indent=2))
        return

    run_id = indexer.telemetry.start_index_run(
        catalog_path=args.catalog,
        collection_name=args.collection,
        recreate=bool(args.recreate),
        note=f"stage={stage}",
        worker_pid=os.getpid(),
    )
    indexer.set_run_id(run_id)
    run_totals: Dict[str, int] = {
        "total_files": 0,
        "added_files": 0,
        "updated_files": 0,
        "skipped_files": 0,
        "error_files": 0,
        "points_added": 0,
    }

    # Миграция: пометить указанные расширения в state как stage=metadata
    # (это нужно, если раньше был прогон --metadata-only-for без поддержки stage,
    # и мы хотим, чтобы на этапах small/large эти файлы переиндексировались).
    _configure_forced_replacement(
        indexer,
        mark_stage_metadata_for=args.mark_stage_metadata_for,
        force_replace_for=args.force_replace_for,
        dry_run=bool(args.dry_run),
    )

    try:
        if args.cleanup:
            # Режим только очистки: сканируем диск, удаляем «фантомы» и выходим
            logger.info("Режим --cleanup: поиск и удаление удалённых файлов из индекса…")
            all_files = [
                f
                for f in indexer.catalog_path.rglob("*")
                if f.is_file()
                and f.suffix.lower() in SUPPORTED_EXTENSIONS
                and not f.name.startswith("~$")
                and not indexer._is_excluded_path(f)
            ]
            logger.info("Файлов на диске: %d", len(all_files))
            if args.dry_run:
                candidates = indexer._deleted_file_candidates(
                    all_files,
                    preserve_members_of_existing_archives=True,
                    allow_empty_inventory=bool(args.allow_empty_cleanup),
                )
                logger.info(
                    "--dry-run cleanup: найдено %d кандидатов; удаление из индекса не выполняется.",
                    len(candidates),
                )
                for candidate in candidates[:20]:
                    logger.info("--dry-run cleanup candidate: %s", candidate)
                deleted = 0
            else:
                deleted = indexer._cleanup_deleted_files(
                    all_files,
                    preserve_members_of_existing_archives=True,
                    allow_empty_inventory=bool(args.allow_empty_cleanup),
                )
            indexer._run_deleted_files += deleted
            run_totals["total_files"] = len(all_files)
            logger.info("Очистка завершена. Удалено из индекса: %d", deleted)
        else:
            if args.dry_run:
                indexer.dry_run = True
            if stage in {"all", "full"}:
                totals = indexer.index_all_stages()
            else:
                totals = indexer.index_directory(stage=stage)
            if args.dry_run:
                logger.info("--dry-run завершён: %s", totals)
                indexer.telemetry.finish_index_run(
                    run_id=run_id,
                    status="completed",
                    total_files=int(totals.get("total_files", 0)),
                    added_files=0,
                    updated_files=0,
                    skipped_files=int(totals.get("skipped_files", 0)),
                    deleted_files=0,
                    error_files=0,
                    points_added=0,
                    note="dry-run",
                )
                return
            if args.watch:
                indexer.watch(stage=stage)
            run_totals["total_files"] = max(run_totals["total_files"], int(totals.get("total_files", 0)))
            for k in ("added_files", "updated_files", "skipped_files", "error_files", "points_added"):
                run_totals[k] += int(totals.get(k, 0))
        indexer.telemetry.finish_index_run(
            run_id=run_id,
            status="completed",
            total_files=run_totals["total_files"],
            added_files=run_totals["added_files"],
            updated_files=run_totals["updated_files"],
            skipped_files=run_totals["skipped_files"],
            deleted_files=indexer._run_deleted_files,
            error_files=run_totals["error_files"],
            points_added=run_totals["points_added"],
            note="ok",
        )
    except (KeyboardInterrupt, IndexerCancelled) as exc:
        indexer.telemetry.finish_index_run(
            run_id=run_id,
            status="cancelled",
            total_files=run_totals["total_files"],
            added_files=run_totals["added_files"],
            updated_files=run_totals["updated_files"],
            skipped_files=run_totals["skipped_files"],
            deleted_files=indexer._run_deleted_files,
            error_files=run_totals["error_files"],
            points_added=run_totals["points_added"],
            note=str(exc) or "interrupted by user",
        )
        logger.warning("Индексация прервана: %s", exc)
        raise
    except Exception as exc:
        logger.exception("Индексация завершилась с ошибкой: %s", exc)
        indexer.telemetry.finish_index_run(
            run_id=run_id,
            status="failed",
            total_files=run_totals["total_files"],
            added_files=run_totals["added_files"],
            updated_files=run_totals["updated_files"],
            skipped_files=run_totals["skipped_files"],
            deleted_files=indexer._run_deleted_files,
            error_files=run_totals["error_files"] + 1,
            points_added=run_totals["points_added"],
            note=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
