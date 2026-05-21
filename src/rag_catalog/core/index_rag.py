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
import logging
import os
import re
import sys
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
from .extractors import (
    extract_doc_meta,
    extract_csv,
    extract_docx,
    extract_image,
    extract_pdf,
    extract_spreadsheet,
    extract_text,
    ocr_pdf,
)
from .index_state_db import IndexStateDB
from .indexing import delete_file_vectors, ensure_collection, upsert_points
from .log_history import build_log_handler, install_env_log_handler
from .ocr_runtime import resolve_ocr_runtime
from .rag_core import load_config
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
    ".docx", ".xlsx", ".xls", ".pdf", ".txt", ".csv",
    # Изображения — OCR если есть текст
    ".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".bmp", ".webp",
}

# Расширения изображений (подмножество SUPPORTED_EXTENSIONS)
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".bmp", ".webp"}

# Максимум страниц/кадров при OCR многостраничного изображения (TIFF, GIF).
# Защита от случайных файлов с тысячами кадров, которые зависнут индексатор.
MAX_IMAGE_PAGES: int = 50

# ─────────────────────────── таблица синонимов ────────────────────────────────
# Сокращение → список синонимов/расшифровок.
# Встроенная база для строительной/горной техники; дополняется через config.json
# ("synonym_map": {"ключ": ["синоним1", "синоним2"]}).
DEFAULT_SYNONYM_MAP: Dict[str, List[str]] = {
    # Документы на технику
    "псм":  ["паспорт самоходной машины", "техпаспорт самоходной машины"],
    "птс":  ["паспорт транспортного средства", "техпаспорт"],
    "сос":  ["свидетельство о собственности"],
    "сти":  ["свидетельство о технической исправности"],
    "осаго": ["обязательное страхование автогражданской ответственности", "страховой полис"],
    "ки":   ["карточка инвентаризации", "инвентарная карточка"],
    # Типы документов
    "акт":  ["акт приёма", "акт сдачи", "акт передачи", "акт выполненных работ"],
    "ттн":  ["товарно-транспортная накладная", "накладная"],
    "тн":   ["товарная накладная"],
    "тзт":  ["технико-эксплуатационный паспорт"],
    "сп":   ["спецификация"],
    "кп":   ["коммерческое предложение"],
    "до":   ["дополнительное соглашение"],
    # Марки техники
    "cat":        ["caterpillar", "кэт", "кэтерпиллар"],
    "komatsu":    ["комацу"],
    "hitachi":    ["хитачи"],
    "liebherr":   ["либхер", "либхерр"],
    "volvo":      ["вольво"],
    "hyundai":    ["хёндэ", "хундай"],
    "doosan":     ["дусан"],
    "jcb":        ["джэйсиби"],
    "tadano":     ["тадано"],
    "xcmg":       ["иксцмг"],
    "sany":       ["сани"],
    "zoomlion":   ["зумлион"],
    # Виды техники
    "экскаватор": ["экскаватор гусеничный", "гусеничный экскаватор", "экскаватор-погрузчик"],
    "пк":         ["погрузчик колёсный", "фронтальный погрузчик"],
    "пг":         ["погрузчик гусеничный"],
    "мтп":        ["машина технологического транспорта"],
    "атз":        ["автотопливозаправщик"],
    "кму":        ["краноманипуляторная установка", "манипулятор"],
    "ав":         ["автовышка", "вышка автомобильная", "подъёмник"],
    "аутп":       ["автомобильный утилизатор твёрдых отходов"],
    # Операции / процессы
    "ремонт":     ["техническое обслуживание", "то", "тр", "капитальный ремонт"],
    "то":         ["техническое обслуживание"],
    "тр":         ["текущий ремонт"],
    "зч":         ["запасные части", "запчасти"],
    # Контрагенты / документы
    "инн":        ["идентификационный номер налогоплательщика"],
    "огрн":       ["основной государственный регистрационный номер"],
    "кпп":        ["код причины постановки на учёт"],
}

# Стоп-слова для тегов (не добавляем в список тегов)
_TAG_STOPWORDS: Set[str] = {
    "и", "в", "на", "по", "с", "для", "из", "от", "до", "при",
    "или", "но", "а", "не", "что", "как", "так", "к", "о", "за",
    "the", "and", "or", "for", "of", "to", "in", "is", "a",
    "файл", "папка", "документ", "doc", "file",
}

# ─────────────────────────── этапы индексирования ─────────────────────────────
# Индексирование разбито на этапы: сначала быстрое наполнение индекса, затем
# прогрессивное «догонение» содержимого. Это даёт пользователю работоспособный
# поиск через минуты, а не сутки.
#
#   1. metadata — индексируется ТОЛЬКО имя/путь/размер/mtime всех файлов.
#      58 тыс. файлов за ~5 минут. Поиск по именам работает сразу.
#   2. small    — полное содержимое быстрых файлов: docx/xlsx/xls любого
#      размера + PDF < 2 МБ. Обычно 10-20 минут на 50k файлов.
#   3. large    — содержимое тяжёлых файлов: крупные xlsx/docx + большие PDF
#      + сканированные PDF (медленно, часы). Запускается отдельным проходом
#      когда поиск по метаданным уже работает.
#
# Порядок важен: чем меньше число, тем «старше» этап (больше информации).
STAGES = ("metadata", "small", "large")
STAGE_RANK = {name: i for i, name in enumerate(STAGES)}

# Пороги для категоризации файлов между small/large (дефолты из config).
DEFAULT_SMALL_OFFICE_MB = 20.0
DEFAULT_SMALL_PDF_MB = 2.0


def _file_category(filepath: Path, small_office_mb: float, small_pdf_mb: float) -> str:
    """
    Возвращает «small» или «large» для данного файла.
    Используется для разделения файлов между этапами small и large.

    Изображения всегда в категории «large» — OCR CPU-intensive.
    """
    try:
        size_mb = filepath.stat().st_size / 1_048_576
    except OSError:
        return "large"  # не можем прочитать stat — кидаем в «медленный» этап
    ext = filepath.suffix.lower()
    if ext in (".txt", ".csv"):
        return "small"
    if ext in (".docx", ".xlsx", ".xls") and size_mb < small_office_mb:
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
        "акт":        "акт",
        "договор":    "договор",
        "счёт":       "счёт",
        "счет":       "счёт",
        "накладная":  "накладная",
        "паспорт":    "паспорт",
        "псм":        "паспорт самоходной машины",
        "птс":        "паспорт транспортного средства",
        "техпаспорт": "технический паспорт",
        "спецификация": "спецификация",
        "инструкция": "инструкция",
        "отчёт":      "отчёт",
        "отчет":      "отчёт",
        "протокол":   "протокол",
        "приказ":     "приказ",
        "заявка":     "заявка",
        "сертификат": "сертификат",
        "лицензия":   "лицензия",
        "страховой":  "страховой полис",
        "полис":      "страховой полис",
        "фото":       "фотография",
        "photo":      "фотография",
        "скан":       "скан документа",
        "scan":       "скан документа",
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
        qdrant_timeout_sec: int = 60,
        exclude_patterns: Optional[List[str]] = None,
        ocr_max_image_pages: int = MAX_IMAGE_PAGES,
        catalog_wait_attempts: int = 10,
        catalog_wait_seconds: int = 60,
    ) -> None:
        # current_stage выставляется при каждом запуске index_directory(stage=...)
        # и определяет поведение skip-логики и экстракции содержимого.
        self.current_stage: str = "content"  # legacy-совместимый дефолт
        self.catalog_path = Path(catalog_path)
        if not self.catalog_path.exists():
            import time as _time
            wait_seconds = max(1, int(catalog_wait_seconds or 60))
            attempts = max(0, int(catalog_wait_attempts if catalog_wait_attempts is not None else 10))
            logger.warning(
                "Папка каталога недоступна: %s — жду появления (%d попыток, каждые %ds)…",
                catalog_path, attempts, wait_seconds,
            )
            for attempt in range(1, attempts + 1):
                _time.sleep(wait_seconds)
                if self.catalog_path.exists():
                    break
                logger.warning("Каталог всё ещё недоступен: %s (попытка %d/%d)", catalog_path, attempt, attempts)
            if not self.catalog_path.exists():
                raise RuntimeError(f"Папка каталога недоступна после {attempts} попыток: {catalog_path}")
            logger.info("Каталог доступен: %s", catalog_path)

        self.qdrant_db_path = Path(qdrant_db_path)
        self.collection_name = collection_name
        self.vector_size = vector_size
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.chunk_group_size = max(1, int(chunk_group_size or 4))
        self.batch_size = batch_size
        self.recreate = recreate_collection
        self.skip_ocr = skip_ocr
        self.max_chunks_per_file = max_chunks_per_file  # 0 = без ограничений
        self.read_workers = read_workers
        self.qdrant_timeout_sec = max(5, int(qdrant_timeout_sec or 60))
        self.exclude_patterns = self._normalize_exclude_patterns(exclude_patterns or [])
        self.ocr_max_image_pages = max(1, int(ocr_max_image_pages or MAX_IMAGE_PAGES))
        self.small_office_mb = float(
            DEFAULT_SMALL_OFFICE_MB if small_office_mb is None else small_office_mb
        )
        self.small_pdf_mb = float(
            DEFAULT_SMALL_PDF_MB if small_pdf_mb is None else small_pdf_mb
        )
        # Таблица синонимов для генерации тегов (дополняет DEFAULT_SYNONYM_MAP)
        self.synonym_map: Dict[str, List[str]] = synonym_map or {}
        self.ocr_tesseract_cmd = str(ocr_tesseract_cmd or "").strip()
        self.ocr_poppler_bin = str(ocr_poppler_bin or "").strip()
        self.ocr_engine = str(ocr_engine or "tesseract").strip().lower()
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
            e.lower() if e.startswith(".") else f".{e.lower()}"
            for e in (metadata_only_extensions or set())
        }
        telemetry_path = telemetry_db_path.strip() if telemetry_db_path else ""
        if not telemetry_path:
            telemetry_path = str(self.qdrant_db_path / "rag_telemetry.db")
        self.telemetry = TelemetryDB(telemetry_path)
        self.run_id: str = ""
        self._run_deleted_files = 0

        if embedding_model.startswith("ollama:"):
            from .llm import OllamaEmbedder  # noqa: PLC0415
            ollama_model_name = embedding_model[len("ollama:"):]
            logger.info("Загрузка OllamaEmbedder: %s (%s)", ollama_model_name, ollama_url)
            self.embedder = OllamaEmbedder(model=ollama_model_name, ollama_url=ollama_url)
        elif use_onnx:
            logger.info("Загрузка модели эмбеддинга: %s (backend=onnx)", embedding_model)
            try:
                self.embedder = SentenceTransformer(embedding_model, backend="onnx")
                logger.info("ONNX backend загружен успешно")
            except Exception as exc:
                logger.warning("ONNX backend недоступен (%s), использую PyTorch", exc)
                self.embedder = SentenceTransformer(embedding_model)
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

        self.point_count = 0

    def set_run_id(self, run_id: str) -> None:
        self.run_id = run_id or ""

    # ── collection setup ───────────────────────────────────────────────

    def _setup_collection(self) -> None:
        recreated = ensure_collection(
            self.qdrant,
            collection_name=self.collection_name,
            vector_size=self.vector_size,
            recreate=self.recreate,
        )
        if recreated:
            self.state_db.clear()
            logger.info("state_entries очищен (--recreate)")

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

    def _should_skip_for_stage(self, file_key: str, fingerprint: str) -> bool:
        """
        True если файл уже покрыт текущим или более полным этапом.

        Правило: stage "metadata" покрывается любым current_stage != metadata
        только если файл УЖЕ имеет полное содержимое (т.е. state.stage == content).
        Если state.stage == metadata и мы сейчас на small/large — нужно ПРОАПГРЕЙДИТЬ.
        """
        existing = self._get_state_entry(file_key)
        if not existing:
            return False
        if str(existing.get("fingerprint") or "") != fingerprint:
            return False  # файл изменился — обязательно переиндексируем

        existing_stage = str(existing.get("stage") or "content")  # backward compat
        # Если текущий этап = metadata, а файл уже проиндексирован (любым этапом) —
        # можем пропустить: мета-запись у файла уже есть.
        if self.current_stage == "metadata":
            return True
        # Для small/large пропускаем если у файла уже есть "content" или тот же этап
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

    def _extract_spreadsheet(self, filepath: Path) -> str:
        """
        Точка входа для всех табличных форматов.
        Явно маршрутизирует .xls → xlrd, .xlsx → openpyxl.
        Раньше маршрутизация была внутри _extract_xlsx, что приводило
        к неочевидным варнингам при смешении форматов.
        """
        return extract_spreadsheet(filepath, max_chars=self._extractor_max_chars())

    def _extract_text(self, filepath: Path) -> str:
        return extract_text(filepath, max_chars=self._extractor_max_chars())

    def _extract_csv(self, filepath: Path) -> str:
        return extract_csv(filepath, max_chars=self._extractor_max_chars())

    def _extractor_max_chars(self) -> int:
        return self.max_chunks_per_file * self.chunk_size if self.max_chunks_per_file else 0

    def _extract_pdf(self, filepath: Path) -> str:
        """
        Извлечь текст из PDF.
        Использует pymupdf (fitz) — в 3-5x быстрее pdfplumber.
        Fallback на pdfplumber если pymupdf не установлен.
        При пустом текстовом слое — OCR (если не --no-ocr).
        """
        return extract_pdf(filepath, skip_ocr=self.skip_ocr, ocr=self._ocr_pdf)

    def _ocr_pdf(self, filepath: Path) -> str:
        """OCR сканированного PDF через pytesseract + pdf2image, с кэшем в telemetry DB."""
        try:
            mtime = float(filepath.stat().st_mtime)
            cached = self.telemetry.get_ocr_file_result(str(filepath), mtime)
            if cached is not None:
                logger.info("OCR из кэша: %s", filepath.name)
                return str(cached.get("extracted_text") or "")
        except Exception:
            pass

        text = ocr_pdf(
            filepath,
            tesseract_cmd=getattr(self, "ocr_tesseract_cmd", ""),
            poppler_bin=getattr(self, "ocr_poppler_bin", ""),
            use_rapid=getattr(self, "_use_rapid_ocr", False),
        )

        try:
            mtime = float(filepath.stat().st_mtime)
            pages = text.count("Страница:") if text else 0
            if pages == 0 and text.strip():
                pages = 1
            chars = len(text.strip())
            self.telemetry.save_ocr_file_result(
                str(filepath), mtime,
                text=text, pages=pages, chars=chars,
                status="ok" if chars > 0 else "empty",
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
        try:
            mtime = float(filepath.stat().st_mtime)
            cached = self.telemetry.get_ocr_file_result(str(filepath), mtime)
            if cached is not None:
                logger.info("OCR из кэша: %s", filepath.name)
                return str(cached.get("extracted_text") or "")
        except Exception:
            pass

        text = extract_image(
            filepath,
            tesseract_cmd=getattr(self, "ocr_tesseract_cmd", ""),
            max_pages=int(getattr(self, "ocr_max_image_pages", MAX_IMAGE_PAGES) or MAX_IMAGE_PAGES),
            use_rapid=getattr(self, "_use_rapid_ocr", False),
        )

        try:
            mtime = float(filepath.stat().st_mtime)
            chars = len(text.strip())
            self.telemetry.save_ocr_file_result(
                str(filepath), mtime,
                text=text, pages=1 if chars > 0 else 0, chars=chars,
                status="ok" if chars > 0 else "empty",
            )
        except Exception:
            pass

        return text

    # ── chunking ───────────────────────────────────────────────────────

    def _chunk_text(self, text: str) -> List[str]:
        """Разбить текст на перекрывающиеся чанки."""
        return chunk_text(text, chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap)

    def _semantic_chunk_end(self, text: str, start: int, max_end: int) -> int:
        return semantic_chunk_end(text, start=start, max_end=max_end, chunk_size=self.chunk_size)

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
    ) -> Dict[str, Any]:
        page = self._extract_marker_int(chunk, r"Страница:\s*(\d+)")
        row = self._extract_marker_int(chunk, r"Строка:\s*(\d+)")
        sheet = self._extract_marker_text(chunk, r"Лист:\s*([^\n\r]+)")
        section = self._extract_section_title(chunk)
        group_size = max(1, int(getattr(self, "chunk_group_size", 4) or 4))
        parent_id = f"{doc_id}:chunk-group:{chunk_index // group_size}"
        return {
            "parent_id": parent_id,
            "section": section,
            "page": page,
            "sheet": sheet,
            "row_start": row,
            "row_end": row,
            "provenance": {
                "doc_id": doc_id,
                "parent_id": parent_id,
                "section": section,
                "page": page,
                "sheet": sheet,
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
        file_type = ext.lstrip(".") or "file"

        if ext == ".docx":
            full_text = self._extract_docx(filepath)
            file_type = "docx"
        elif ext in (".xlsx", ".xls"):
            full_text = self._extract_spreadsheet(filepath)
            file_type = "xlsx"
        elif ext == ".txt":
            full_text = self._extract_text(filepath)
            file_type = "txt"
        elif ext == ".csv":
            full_text = self._extract_csv(filepath)
            file_type = "csv"
        elif ext == ".pdf":
            full_text = self._extract_pdf(filepath)
            file_type = "pdf"
        elif ext in IMAGE_EXTENSIONS:
            if not self.skip_ocr:
                full_text = self._extract_image(filepath)
            file_type = "image"
        else:
            logger.debug("Неподдерживаемый формат (только метаданные): %s", ext)

        chunks = self._chunk_text(full_text) if full_text.strip() else []
        if self.max_chunks_per_file and len(chunks) > self.max_chunks_per_file:
            logger.warning(
                "Файл %s: %d чанков, обрезано до %d (--max-chunks-per-file)",
                filepath.name,
                len(chunks),
                self.max_chunks_per_file,
            )
            chunks = chunks[: self.max_chunks_per_file]
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
        meta_payload: Dict[str, Any] = {
            "type": "file_metadata",
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
            **doc_meta,
            **base_provenance,
            **(payload_extra or {}),
        }

        texts = [meta_text, *chunks]
        payloads: List[Dict[str, Any]] = [meta_payload]
        doc_id = str(base_provenance["doc_id"])
        for idx, chunk in enumerate(chunks):
            payloads.append(
                {
                    "type": f"{file_type}_content",
                    "text": chunk,
                    "filename": filepath.name,
                    "extension": ext,
                    "path": str(relative_path),
                    "full_path": str(filepath),
                    "chunk_index": idx,
                    "state_key": file_key,
                    "tags": tags,
                    **doc_meta,
                    **base_provenance,
                    **self._chunk_provenance(chunk=chunk, chunk_index=idx, doc_id=doc_id),
                    **(payload_extra or {}),
                }
            )
        vectors = self.embedder.encode(
            texts,
            normalize_embeddings=True,
            batch_size=max(1, min(256, int(self.batch_size or 64))),
            show_progress_bar=False,
        )
        points = [
            PointStruct(id=str(uuid.uuid4()), vector=v.tolist(), payload=p)
            for v, p in zip(vectors, payloads)
        ]
        written = upsert_points(
            self.qdrant,
            collection_name=self.collection_name,
            points=points,
            timeout_sec=self.qdrant_timeout_sec,
        )
        self.point_count += written
        stage = "content" if chunks else "metadata"
        self._upsert_state_entry(
            {
                "full_path": file_key,
                "fingerprint": fingerprint,
                "mtime": mtime,
                "stage": stage,
                "size_bytes": size_bytes,
                "extension": filepath.suffix.lower(),
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
            for k in ("processed_files", "added_files", "updated_files", "skipped_files", "error_files", "points_added"):
                totals[k] += int(stage_stats.get(k, 0))
        logger.info("✔ Все этапы завершены: %s", ", ".join(stages))
        return totals

    def _cleanup_deleted_files(self, existing_files: List[Path]) -> int:
        """Удалить из state БД и Qdrant файлы, которых больше нет на диске."""
        existing_paths = {str(f) for f in existing_files}
        deleted_keys = self.state_db.list_deleted_candidates(existing_paths)
        if not deleted_keys:
            return 0
        logger.info("Удаление %d удалённых файлов из индекса…", len(deleted_keys))
        for key in deleted_keys:
            self._delete_file_vectors(Path(key))
        self.state_db.delete_entries(deleted_keys)
        return len(deleted_keys)


# ─────────────────────────── CLI entry point ───────────────────────────

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
            "  --stage small         — содержимое docx/xlsx/xls + мелких PDF (десятки минут)\n"
            "  --stage large         — содержимое больших файлов + сканированные PDF (часы)\n"
            "  --stage all           — все этапы подряд (поведение по умолчанию)\n"
            "  --recreate            — пересоздать коллекцию и очистить state\n"
            "  --cleanup             — только удалить из индекса файлы, которых нет на диске\n"
        ),
    )
    parser.add_argument("--catalog", default=cfg["catalog_path"], help="Папка для индексирования")
    parser.add_argument("--db", default=cfg["qdrant_db_path"], help="Путь к локальной базе Qdrant (SQLite режим)")
    parser.add_argument("--url", default="", dest="qdrant_url",
                        help="URL Qdrant-сервера (например http://localhost:6333). Если указан — используется вместо --db")
    parser.add_argument("--model", default=cfg["embedding_model"], help="Модель эмбеддинга")
    parser.add_argument("--collection", default=cfg["collection_name"], help="Имя коллекции")
    parser.add_argument("--recreate", action="store_true", help="Пересоздать коллекцию и очистить state")
    parser.add_argument("--ocr-engine", default=str(cfg.get("ocr_engine") or "tesseract"),
                        dest="ocr_engine", choices=("tesseract", "rapidocr"),
                        help="OCR движок: tesseract (CPU, по умолчанию) или rapidocr (GPU/DirectML)")
    parser.add_argument("--no-ocr", action="store_true", dest="no_ocr",
                        help="Пропускать OCR для сканированных PDF (быстрее, текст не извлекается)")
    parser.add_argument("--max-chunks", type=int, default=int(cfg.get("index_max_chunks", 2000)), dest="max_chunks",
                        help="Максимум чанков с одного файла (по умолчанию 2000; 0 = без ограничений)")
    parser.add_argument("--workers", type=int, default=int(cfg.get("index_read_workers", 4)), dest="workers",
                        help="Число параллельных потоков для чтения файлов (по умолчанию 4)")
    parser.add_argument("--onnx", action="store_true", dest="use_onnx",
                        help="Использовать ONNX Runtime для encode (быстрее, но может не работать на Python 3.14)")
    default_stage = str(cfg.get("index_default_stage", "all")).strip().lower()
    if default_stage not in ("all", *STAGES):
        default_stage = "all"
    parser.add_argument("--stage", default=default_stage, choices=("all", *STAGES),
                        help="Этап индексирования. По умолчанию 'all' — прогон всех этапов "
                             "(metadata → small → large). Можно запустить отдельный этап для "
                             "дробного прогресса или для тонкой настройки фоновых задач.")
    parser.add_argument("--metadata-only-for", default="", dest="metadata_only_for",
                        help="[legacy] Список расширений через запятую для индексирования ТОЛЬКО "
                             "метаданных (например: .pdf). Используется в рамках одного stage. "
                             "Современный эквивалент: --stage metadata.")
    parser.add_argument("--metadata-only", action="store_true", dest="metadata_only",
                        help="[legacy] Псевдоним для --stage metadata.")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help=(
            "Только очистить индекс от файлов, которые удалены с диска, "
            "без полного сканирования. Быстро (~1 мин)."
        ),
    )
    parser.add_argument(
        "--mark-stage-metadata-for", default="", dest="mark_stage_metadata_for",
        help="Перед индексированием пометить уже имеющиеся в state записи указанных расширений "
             "как stage=metadata (чтобы они переиндексировались на этапах small/large). "
             "Пример: --mark-stage-metadata-for .pdf. Пригодится после legacy-прохода --metadata-only-for.")
    args = parser.parse_args()
    if bool(cfg.get("index_skip_ocr", False)) and "--no-ocr" not in sys.argv:
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
            for e in args.metadata_only_for.split(",") if e.strip()
        }
        logger.info("Legacy --metadata-only-for для расширений: %s (в рамках --stage %s)",
                    sorted(metadata_only_extensions), stage)

    telemetry_path = (cfg.get("telemetry_db_path") or "").strip()
    if not telemetry_path:
        telemetry_path = str(Path(args.db) / "rag_telemetry.db")

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
        qdrant_timeout_sec=int(cfg.get("qdrant_timeout_sec", 60) or 60),
        exclude_patterns=list(cfg.get("index_exclude_patterns") or []),
        ocr_max_image_pages=int(cfg.get("ocr_max_image_pages", MAX_IMAGE_PAGES) or MAX_IMAGE_PAGES),
        catalog_wait_attempts=int(cfg.get("catalog_wait_attempts", 10) or 10),
        catalog_wait_seconds=int(cfg.get("catalog_wait_seconds", 60) or 60),
    )
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
    if args.mark_stage_metadata_for:
        exts = {
            e.strip().lower() if e.strip().startswith(".") else "." + e.strip().lower()
            for e in args.mark_stage_metadata_for.split(",") if e.strip()
        }
        changed = indexer.state_db.update_stage_for_extensions(exts, stage="metadata")
        logger.info("Миграция state: %d записей с расширениями %s помечены stage=metadata",
                    changed, sorted(exts))

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
            deleted = indexer._cleanup_deleted_files(all_files)
            indexer._run_deleted_files += deleted
            run_totals["total_files"] = len(all_files)
            logger.info("Очистка завершена. Удалено из индекса: %d", deleted)
        else:
            if stage == "all":
                totals = indexer.index_all_stages()
            else:
                totals = indexer.index_directory(stage=stage)
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
    except KeyboardInterrupt:
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
            note="interrupted by user",
        )
        logger.warning("Индексация прервана пользователем.")
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
