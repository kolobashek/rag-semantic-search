"""
rag_core.py — Общее ядро RAG-системы.

Предоставляет:
  - load_config() / save_config()  — загрузка/сохранение config.json
  - RAGSearcher                    — единый класс семантического поиска
                                     (используется app_ui, windows_app, rag_search)
"""

import json
import logging
import os
import re
import sqlite3
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._platform_compat import apply_windows_platform_workarounds

apply_windows_platform_workarounds()

from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchAny, MatchText, MatchValue

from .embedding_collections import resolve_collection_name_from_config
from .exact_tokens import numeric_exact_tokens, query_numeric_tokens, repair_mojibake_text
from .index_state_db import IndexStateDB
from .retrieval import bm25_rank_items, prepare_bm25_items, prepare_query_text, rrf_fuse, tokenize
from .telemetry_db import TelemetryDB

# SentenceTransformer импортируется ЛЕНИВО внутри RAGSearcher.embedder.
# НЕ импортировать здесь — import тянет torch (~5 сек, 500+ МБ RAM).

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _resolve_config_file() -> Path:
    explicit_path = str(os.environ.get("RAG_CONFIG_PATH") or "").strip()
    if explicit_path:
        return Path(explicit_path).expanduser().resolve()
    # Nested .claude/.codex worktrees should reuse the nearest ancestor config.
    for base in [PROJECT_ROOT, *PROJECT_ROOT.parents]:
        candidate = base / "config.json"
        if candidate.exists():
            return candidate
    return PROJECT_ROOT / "config.json"


CONFIG_FILE = _resolve_config_file()

DEFAULT_CONFIG: Dict[str, Any] = {
    "catalog_path": r"O:\Обмен",
    "qdrant_db_path": r"D:\qdrant_state",
    "qdrant_url": "http://localhost:6333",   # Docker-сервер (приоритет над db_path)
    "log_file": r"O:\rag_automation.log",
    "collection_name": "catalog",
    "embedding_model": "intfloat/multilingual-e5-small",
    "embedding_backend": "",
    "embedding_onnx_provider": "",
    "embedding_onnx_file_name": "",
    "index_embedding_backend": "",
    "index_embedding_onnx_provider": "",
    "index_embedding_onnx_file_name": "",
    "index_defer_fulltext": False,
    "embedding_collection_versioning": False,
    "embedding_collection_suffix": "",
    "retrieval_preset": "release_v2",  # legacy|release_v2
    "vector_size": 384,
    "chunk_size": 500,
    "chunk_overlap": 100,
    "index_min_chunk_chars": 120,
    "chunk_group_size": 4,
    "batch_size": 1000,
    "index_read_workers": 0,
    "index_max_chunks": 5,
    "index_skip_ocr": False,
    "index_default_stage": "all",
    "index_exclude_patterns": [],
    "ocr_max_image_pages": 50,
    "catalog_wait_attempts": 10,
    "catalog_wait_seconds": 60,
    "small_office_mb": 20.0,
    "small_pdf_mb": 2.0,
    "qdrant_timeout_sec": 60,
    "qdrant_scroll_limit": 256,
    "search_warmup_enabled": True,
    "metadata_needle_cache_size": 512,
    # OCR runtime: можно оставить пустым и использовать bundled tools/
    "ocr_tesseract_cmd": "",
    "ocr_poppler_bin": "",
    "telegram_enabled": False,
    "telegram_bot_token": "",
    "telegram_allowed_chat_id": "",
    "telegram_bot_link": "",
    "users_db_path": "",
    "telemetry_db_path": "",
    # ── Cloud drive foundation ──────────────────────────────────────────
    "cloud_drive_enabled": False,
    "cloud_drive_public_links_enabled": False,
    "cloud_drive_db_path": "",
    "cloud_drive_storage": "local",
    "cloud_drive_storage_root": "",
    "cloud_drive_bucket": "",
    "cloud_drive_s3_endpoint": "",
    "cloud_drive_s3_region": "",
    "cloud_drive_s3_access_key": "",
    "cloud_drive_s3_secret_key": "",
    # ── LLM / Ollama ──────────────────────────────────────────────────────
    "ollama_url": "http://localhost:11434",
    "llm_enabled": False,            # включить RAG Q&A и расширение запроса
    "llm_search_expand_enabled": False,  # расширять запрос внутри core search pipeline
    "llm_expand_model": "phi3:mini", # модель для расширения запроса
    "llm_rag_model": "qwen3:8b",     # модель для RAG Q&A
    "llm_answer_top_k": 5,
    # Ранжирование результатов поиска
    "rank_feedback_step": 0.02,      # шаг буста/штрафа за +1/-1 feedback
    "rank_feedback_cap": 0.18,       # максимум абсолютного влияния feedback
    "rank_recency_enabled": True,    # учитывать свежесть modified
    "rank_recency_half_life_days": 180.0,  # через сколько дней буст в 2 раза меньше
    "rank_recency_max_boost": 0.03,  # максимум буста за самый свежий документ
    "rank_max_chunks_per_document": 3,
    "retrieval_pipeline": "v2",  # legacy|v2
    "retrieval_dense_top_k": 50,
    "retrieval_lexical_top_k": 50,
    "retrieval_bm25_enabled": True,
    "retrieval_bm25_top_k": 50,
    "retrieval_fulltext_enabled": True,
    "retrieval_fulltext_top_k": 100,
    "retrieval_final_top_k": 10,
    "retrieval_relevance_gate_enabled": True,
    "retrieval_min_dense_score": 0.78,
    "retrieval_single_term_min_dense_score": 0.80,
    "retrieval_min_content_chars": 120,
    "retrieval_reranker_enabled": False,
    "retrieval_reranker_model": "",
    "retrieval_reranker_backend": "",
    "retrieval_reranker_onnx_provider": "",
    "retrieval_reranker_onnx_file_name": "",
    "retrieval_reranker_top_n": 30,
    "retrieval_reranker_weight": 0.65,
    "retrieval_reranker_min_score": -4.0,
}

logger = logging.getLogger(__name__)
MAX_QUERY_LEN = 2000
FS_CACHE_TTL_SEC = 300
FS_CACHE_MAX_ITEMS = 250_000

_ENTITY_RE = re.compile(r"\b[a-zа-я]*\d+[a-zа-я0-9\-]*\b", re.IGNORECASE)
_TERM_ALIASES = {
    "touareg": ["туарег", "volkswagen", "фольксваген", "vw"],
    "туарег": ["touareg", "volkswagen", "фольксваген", "vw"],
    "volkswagen": ["фольксваген", "vw"],
    "фольксваген": ["volkswagen", "vw"],
    "обслуживания": ["обслуживание", "техническое обслуживание", "услуги", "ремонт", "сервис"],
    "технических": ["технические", "техническое обслуживание", "услуги", "ремонт", "сервис"],
}
_WEIGHT_LINE_RE = re.compile(
    r"(масса|вес|снаряженн\w*\s+масса|разрешенн\w*\s+максимальн\w*\s+масса)[^\n\r]{0,80}",
    re.IGNORECASE,
)
_NUMBER_UNIT_RE = re.compile(
    r"(\d[\d\s.,]{1,15})\s*(кг|килограмм(?:а|ов)?|т|тн|тонн(?:а|ы|)?)\b",
    re.IGNORECASE,
)
_UNIT_NUMBER_RE = re.compile(
    r"(кг|килограмм(?:а|ов)?|т|тн|тонн(?:а|ы|)?)\)?\s*(\d[\d\s.,]{1,15})\b",
    re.IGNORECASE,
)



RETRIEVAL_PRESETS: Dict[str, Dict[str, Any]] = {
    "release_v2": {
        "retrieval_pipeline": "v2",
        "retrieval_bm25_enabled": True,
        "retrieval_dense_top_k": 50,
        "retrieval_bm25_top_k": 50,
        "retrieval_fulltext_enabled": True,
        "retrieval_fulltext_top_k": 100,
        "retrieval_lexical_top_k": 50,
        "retrieval_final_top_k": 10,
        "retrieval_relevance_gate_enabled": True,
        "retrieval_min_dense_score": 0.78,
        "retrieval_single_term_min_dense_score": 0.80,
        "retrieval_min_content_chars": 120,
        # Reranker stays opt-in until latency and eval thresholds are agreed.
        "retrieval_reranker_enabled": False,
        "retrieval_reranker_top_n": 30,
        "retrieval_reranker_weight": 0.65,
    },
}


def apply_retrieval_preset(config: Dict[str, Any], explicit_keys: set[str] | None = None) -> Dict[str, Any]:
    """Apply named retrieval presets while preserving explicitly provided keys."""
    out = dict(config)
    preset = str(out.get("retrieval_preset") or "legacy").strip().lower()
    values = RETRIEVAL_PRESETS.get(preset)
    if not values:
        return out
    explicit = explicit_keys or set()
    for key, value in values.items():
        if key not in explicit:
            out[key] = value
    return out

# ─────────────────────────── config helpers ────────────────────────────

def load_config() -> Dict[str, Any]:
    """
    Загрузить конфигурацию из config.json.
    Недостающие ключи берутся из DEFAULT_CONFIG.
    """
    config_file = _resolve_config_file()
    if config_file.exists():
        try:
            with open(config_file, "r", encoding="utf-8") as fh:
                user_cfg = json.load(fh)
            return apply_retrieval_preset({**DEFAULT_CONFIG, **user_cfg}, set(user_cfg.keys()))
        except Exception as exc:
            logger.warning("Не удалось загрузить config.json: %s. Используются значения по умолчанию.", exc)
    return apply_retrieval_preset(dict(DEFAULT_CONFIG), set())


def save_config(config: Dict[str, Any]) -> None:
    """Сохранить конфигурацию в config.json."""
    config_file = _resolve_config_file()
    try:
        with open(config_file, "w", encoding="utf-8") as fh:
            json.dump(config, fh, indent=2, ensure_ascii=False)
        logger.info("Конфигурация сохранена: %s", config_file)
    except Exception as exc:
        logger.error("Не удалось сохранить config.json: %s", exc)


# ──────────────────────────── RAGSearcher ──────────────────────────────

class RAGSearcher:
    """
    Единый клиент семантического поиска по векторной базе Qdrant.

    Используется во всех трёх интерфейсах (Streamlit, PyQt6, CLI).
    Модель эмбеддинга загружается лениво при первом обращении.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = apply_retrieval_preset(dict(config), set(config.keys()))
        self.embedding_model_name = str(self.config.get("embedding_model") or "")
        self.collection_name = resolve_collection_name_from_config(self.config)
        self.connected = False
        self._fulltext_available = False
        self._embedder: Optional[Any] = None  # SentenceTransformer, загружается лениво
        self._reranker: Optional[Any] = None  # CrossEncoder, загружается лениво
        self._fs_cache: Dict[str, Any] = {"ts": 0.0, "items": []}
        self._metadata_index_source = 0
        self._metadata_token_docs: Dict[str, tuple[int, ...]] = {}
        self._metadata_needle_docs: OrderedDict[str, tuple[int, ...]] = OrderedDict()
        self._metadata_corpus_size = 0
        self._metadata_average_doc_length = 0.0
        telemetry_path = (self.config.get("telemetry_db_path") or "").strip()
        if not telemetry_path:
            telemetry_path = str(Path(self.config["qdrant_db_path"]) / "rag_telemetry.db")
        self.telemetry = TelemetryDB(telemetry_path)

        # Подключение: сервер (Docker) имеет приоритет над локальным SQLite
        qdrant_url = self.config.get("qdrant_url", "")
        qdrant_path = Path(self.config["qdrant_db_path"])
        qdrant_timeout = int(self.config.get("qdrant_timeout_sec", 60) or 60)
        try:
            if qdrant_url:
                self.qdrant = QdrantClient(url=qdrant_url, timeout=qdrant_timeout)
                logger.info("Подключено к Qdrant-серверу: %s", qdrant_url)
            else:
                self.qdrant = QdrantClient(path=str(qdrant_path), timeout=qdrant_timeout)
                logger.info("Подключено к Qdrant локально: %s", qdrant_path)
            collection_info = self.qdrant.get_collection(self.collection_name)
            schema = getattr(collection_info, "payload_schema", None) or {}
            self._fulltext_available = "text" in schema
            self.connected = True
        except Exception as exc:
            logger.error("Не удалось подключиться к Qdrant: %s", exc)

    # ── lazy embedder ──────────────────────────────────────────────────

    @property
    def embedder(self) -> Any:
        """Ленивая загрузка модели эмбеддинга.

        Если embedding_model начинается с ``"ollama:"`` — использует OllamaEmbedder
        (nomic-embed-text и аналоги через Ollama API). Иначе — SentenceTransformer.
        """
        if self._embedder is None:
            model_name = self.config["embedding_model"]
            if model_name.startswith("ollama:"):
                from .llm import OllamaEmbedder  # noqa: PLC0415
                ollama_model = model_name[len("ollama:"):]
                ollama_url = self.config.get("ollama_url", "http://localhost:11434")
                logger.info("Загрузка OllamaEmbedder: %s (%s)", ollama_model, ollama_url)
                self._embedder = OllamaEmbedder(model=ollama_model, ollama_url=ollama_url)
            else:
                from sentence_transformers import SentenceTransformer  # noqa: PLC0415
                backend = str(self.config.get("embedding_backend") or "").strip().lower()
                if backend == "onnx":
                    model_kwargs = {
                        key: value
                        for key, value in {
                            "provider": str(self.config.get("embedding_onnx_provider") or "").strip(),
                            "file_name": str(self.config.get("embedding_onnx_file_name") or "").strip(),
                        }.items()
                        if value
                    }
                    logger.info(
                        "Загрузка модели эмбеддинга: %s (backend=onnx, provider=%s, file=%s)",
                        model_name,
                        model_kwargs.get("provider") or "auto",
                        model_kwargs.get("file_name") or "auto",
                    )
                    self._embedder = SentenceTransformer(
                        model_name,
                        backend="onnx",
                        model_kwargs=model_kwargs,
                        local_files_only=True,
                    )
                else:
                    logger.info("Загрузка модели эмбеддинга: %s", model_name)
                    self._embedder = SentenceTransformer(model_name, local_files_only=True)
        return self._embedder

    @property
    def reranker(self) -> Optional[Any]:
        """Lazy CrossEncoder reranker for retrieval v2, enabled by config only."""
        model_name = str(self.config.get("retrieval_reranker_model") or "").strip()
        if not model_name:
            return None
        if getattr(self, "_reranker", None) is None:
            from sentence_transformers import CrossEncoder  # noqa: PLC0415

            backend = str(self.config.get("retrieval_reranker_backend") or "").strip().lower()
            if backend == "onnx":
                model_kwargs = {
                    key: value
                    for key, value in {
                        "provider": str(self.config.get("retrieval_reranker_onnx_provider") or "").strip(),
                        "file_name": str(self.config.get("retrieval_reranker_onnx_file_name") or "").strip(),
                    }.items()
                    if value
                }
                logger.info(
                    "Загрузка reranker-модели: %s (backend=onnx, provider=%s, file=%s)",
                    model_name,
                    model_kwargs.get("provider") or "auto",
                    model_kwargs.get("file_name") or "auto",
                )
                self._reranker = CrossEncoder(
                    model_name,
                    backend="onnx",
                    model_kwargs=model_kwargs,
                    local_files_only=True,
                )
            else:
                logger.info("Загрузка reranker-модели: %s", model_name)
                self._reranker = CrossEncoder(model_name, local_files_only=True)
        return self._reranker

    # ── search ────────────────────────────────────────────────────────

    def search(
        self,
        query: str,
        limit: int = 10,
        file_type: Optional[str] = None,
        content_only: bool = False,
        title_only: bool = False,
        query_original: str = "",
        source: str = "unknown",
        username: str = "",
    ) -> List[Dict[str, Any]]:
        """
        Семантический поиск по индексированным файлам.

        Фильтры передаются напрямую в Qdrant (payload filter), а не
        обрабатываются на стороне Python — это гарантирует точный `limit`
        даже при большом количестве точек в базе.

        Args:
            query:        Строка поискового запроса.
            limit:        Максимальное количество результатов.
            file_type:    Фильтр по расширению файла, например '.docx', '.pdf'.
            content_only: Если True — исключить точки типа file_metadata.
            title_only:   Если True — вернуть только metadata-результаты по имени/пути.

        Returns:
            Список словарей с ключами:
            score, type, text, filename, path, full_path, size_mb, modified, extension.
        """
        started = time.perf_counter()
        raw_query = query or ""
        raw_original = query_original if query_original else raw_query
        if title_only and content_only:
            content_only = False

        # Expand synonyms/aliases using the ORIGINAL (pre-LLM) query so that
        # alias groups match user intent, not the LLM-rewritten text.
        _alias_exp = self._search_alias_expansion(raw_original[:MAX_QUERY_LEN])
        _alias_terms: List[str] = [
            str(a) for a in (_alias_exp.get("aliases") or []) if str(a).strip()
        ]
        # Append alias terms that aren't already present in the incoming query.
        _raw_lower = raw_query.lower()
        _extra = [a for a in _alias_terms if a.lower() not in _raw_lower]
        query_with_aliases = (
            (raw_query + " " + " ".join(_extra)).strip() if _extra else raw_query
        )[:MAX_QUERY_LEN]

        # Apply LLM expansion ONLY when the query wasn't already pre-expanded
        # externally (e.g. by nice_app.py).  Detected by comparing raw_query to
        # raw_original: if they differ, LLM expansion already happened upstream.
        _externally_expanded = raw_query.strip() != raw_original.strip()
        if _externally_expanded:
            query_used = query_with_aliases
        else:
            query_used = self._expand_query_for_search(query_with_aliases)[:MAX_QUERY_LEN]
        query_original_used = raw_original
        truncated_note = ""
        if len(raw_query) > MAX_QUERY_LEN:
            truncated_note = f"truncated_from={len(raw_query)}"

        if not self.connected:
            self.telemetry.log_search(
                source=source,
                query=query_original_used,
                limit_value=limit,
                file_type=file_type,
                content_only=content_only,
                results_count=0,
                duration_ms=0,
                ok=False,
                error="not_connected",
                username=username,
                query_original=query_original_used,
                query_used=query_used,
            )
            raise ConnectionError("Нет подключения к Qdrant")

        lexical_query = query_original_used or raw_query
        if not title_only:
            early_numeric_tokens = [
                token for token in query_numeric_tokens(lexical_query) if len(token) >= 5
            ]
            early_numeric_results = (
                self._spreadsheet_numeric_exact_scan(
                    query=lexical_query,
                    tokens=early_numeric_tokens,
                    limit=limit,
                    file_type=file_type,
                )
                if early_numeric_tokens
                else []
            )
            if early_numeric_results:
                results = self._merge_ranked_results(
                    early_numeric_results,
                    [],
                    limit=limit,
                    query=query_used,
                )
                results = [self._repair_result_display_fields(item) for item in results]
                self.telemetry.log_search(
                    source=source,
                    query=query_original_used,
                    limit_value=limit,
                    file_type=file_type,
                    content_only=content_only,
                    results_count=len(results),
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    ok=True,
                    error=truncated_note,
                    username=username,
                    query_original=query_original_used,
                    query_used=query_used,
                )
                return results

        try:
            query_vector = self.embedder.encode(
                prepare_query_text(
                    str(getattr(self, "embedding_model_name", "") or self.config.get("embedding_model") or ""),
                    query_used,
                ),
                normalize_embeddings=True,
            ).tolist()
        except Exception as exc:
            logger.error("Не удалось построить эмбеддинг запроса: %s", exc)
            self.telemetry.log_search(
                source=source,
                query=query_original_used,
                limit_value=limit,
                file_type=file_type,
                content_only=content_only,
                results_count=0,
                duration_ms=int((time.perf_counter() - started) * 1000),
                ok=False,
                error=f"embed_error: {exc}",
                username=username,
                query_original=query_original_used,
                query_used=query_used,
            )
            raise RuntimeError(f"Не удалось построить эмбеддинг запроса: {exc}") from exc

        # ── Строим фильтр Qdrant ───────────────────────────────────────
        must_conditions = []

        if file_type:
            must_conditions.append(
                FieldCondition(
                    key="extension",
                    match=MatchValue(value=file_type.lower()),
                )
            )
        should_conditions = []
        if title_only:
            if file_type:
                must_conditions.append(
                    FieldCondition(
                        key="type",
                        match=MatchValue(value="file_metadata"),
                    )
                )
            else:
                should_conditions.append(
                    FieldCondition(
                        key="type",
                        match=MatchAny(any=["file_metadata", "folder_metadata"]),
                    )
                )

        must_not_conditions = []
        if content_only:
            must_not_conditions.append(
                FieldCondition(
                    key="type",
                    match=MatchValue(value="file_metadata"),
                )
            )

        if must_conditions or must_not_conditions or should_conditions:
            qdrant_filter = Filter(
                should=should_conditions if should_conditions else None,
                must=must_conditions if must_conditions else None,
                must_not=must_not_conditions if must_not_conditions else None,
            )
        else:
            qdrant_filter = None

        try:
            # qdrant-client >= 1.10: query_points заменяет устаревший search()
            response = self.qdrant.query_points(
                collection_name=self.collection_name,
                query=query_vector,
                query_filter=qdrant_filter,
                limit=limit,
                with_payload=True,
            )
            raw = response.points
        except Exception as exc:
            logger.error("Ошибка поиска в Qdrant: %s", exc)
            self.telemetry.log_search(
                source=source,
                query=query_original_used,
                limit_value=limit,
                file_type=file_type,
                content_only=content_only,
                results_count=0,
                duration_ms=int((time.perf_counter() - started) * 1000),
                ok=False,
                error=f"qdrant_error: {exc}",
                username=username,
                query_original=query_original_used,
                query_used=query_used,
            )
            raise RuntimeError(f"Ошибка поиска в Qdrant: {exc}") from exc

        results: List[Dict[str, Any]] = []
        metadata_types = {"file_metadata", "folder_metadata"}
        for hit in raw:
            payload = hit.payload or {}
            results.append(
                {
                    "score": round(hit.score, 3),
                    "type": payload.get("type", ""),
                    "text": payload.get("text", ""),
                    "filename": payload.get("filename", ""),
                    "path": payload.get("path", ""),
                    "full_path": payload.get("full_path", ""),
                    "size_mb": payload.get("size_mb"),
                    "modified": payload.get("modified"),
                    "created": payload.get("created"),
                    "extension": payload.get("extension", ""),
                    "doc_author": payload.get("doc_author", ""),
                    "doc_last_editor": payload.get("doc_last_editor", ""),
                    "doc_top_editor": payload.get("doc_top_editor", ""),
                    "doc_created": payload.get("doc_created", ""),
                    "chunk_index": payload.get("chunk_index"),
                    "cloud_file_id": payload.get("cloud_file_id", ""),
                    "cloud_version_id": payload.get("cloud_version_id", ""),
                    "cloud_path": payload.get("cloud_path", ""),
                    "storage_key": payload.get("storage_key", ""),
                    "doc_id": payload.get("doc_id", ""),
                    "parent_id": payload.get("parent_id", ""),
                    "section": payload.get("section", ""),
                    "page": payload.get("page"),
                    "sheet": payload.get("sheet", ""),
                    "row_start": payload.get("row_start"),
                    "row_end": payload.get("row_end"),
                    "provenance": payload.get("provenance") or {},
                    "dense_score": round(hit.score, 6),
                    "retrieval_source": "dense",
                }
            )
        if title_only:
            results = [item for item in results if str(item.get("type") or "") in metadata_types]

        numeric_exact_results = self._numeric_exact_search(
            query=lexical_query,
            limit=max(limit * 2, 20),
            file_type=file_type,
            content_only=content_only,
            title_only=title_only,
        )
        lexical_results = self._lexical_catalog_search(
            query=lexical_query,
            limit=max(limit * 4, 40),
            file_type=file_type,
            content_only=content_only,
            title_only=title_only,
        )
        fulltext_results = self._fulltext_content_search(
            query=lexical_query,
            limit=int(self.config.get("retrieval_fulltext_top_k", max(limit * 4, 40)) or max(limit * 4, 40)),
            file_type=file_type,
            content_only=content_only,
            title_only=title_only,
        )
        if str(self.config.get("retrieval_pipeline") or "legacy").lower() == "v2":
            rerank_top_n = max(limit, int(self.config.get("retrieval_reranker_top_n", max(limit * 3, 30)) or limit))
            bm25_results = self._bm25_catalog_search(
                query=lexical_query,
                limit=int(self.config.get("retrieval_bm25_top_k", max(limit * 4, 40)) or max(limit * 4, 40)),
                file_type=file_type,
                content_only=content_only,
                title_only=title_only,
            )
            channels = [numeric_exact_results, lexical_results, bm25_results, fulltext_results, results]
            fused = rrf_fuse(channels, limit=max(limit * 4, 40))
            results = self._merge_ranked_results([], fused, limit=rerank_top_n, query=query_used)
            if bool(self.config.get("retrieval_reranker_enabled", False)):
                results = self._rerank_results(query_used, results, limit=rerank_top_n)
            else:
                results = self._apply_relevance_gate(lexical_query, results)
        else:
            results = self._merge_ranked_results(
                [*numeric_exact_results, *lexical_results, *fulltext_results],
                results,
                limit=limit,
                query=query_used,
            )
        results = self._apply_relevance_gate(lexical_query, results)
        results = results[:limit]
        results = [self._repair_result_display_fields(item) for item in results]

        self.telemetry.log_search(
            source=source,
            query=query_original_used,
            limit_value=limit,
            file_type=file_type,
            content_only=content_only,
            results_count=len(results),
            duration_ms=int((time.perf_counter() - started) * 1000),
            ok=True,
            error=truncated_note,
            username=username,
            query_original=query_original_used,
            query_used=query_used,
        )
        return results

    def _search_alias_expansion(self, query: str) -> Dict[str, Any]:
        telemetry = getattr(self, "telemetry", None)
        if telemetry is None or not hasattr(telemetry, "expand_search_query"):
            return {"expanded_query": query or "", "aliases": [], "groups": []}
        try:
            return telemetry.expand_search_query(query)
        except Exception as exc:
            logger.debug("Search alias expansion failed: %s", exc)
            return {"expanded_query": query or "", "aliases": [], "groups": []}

    def _expand_query_for_search(self, query: str) -> str:
        config = getattr(self, "config", {}) or {}
        if not (bool(config.get("llm_enabled", False)) and bool(config.get("llm_search_expand_enabled", False))):
            return query
        if not query.strip():
            return query
        try:
            from .llm import expand_query  # noqa: PLC0415

            return expand_query(
                query,
                model=str(config.get("llm_expand_model") or "phi3:mini"),
                ollama_url=str(config.get("ollama_url") or "http://localhost:11434"),
                timeout=int(config.get("llm_expand_timeout_sec", 15) or 15),
            )
        except Exception as exc:
            logger.warning("Core query expansion failed, using original query: %s", exc)
            return query

    def _terms_from_text(self, text: str) -> List[str]:
        terms = [
            t.lower().replace("ё", "е")
            for t in re.findall(r"[a-zа-яё0-9\-]{2,}", text or "", flags=re.IGNORECASE)
        ]
        stop = {"и", "или", "по", "на", "в", "во", "от", "для", "мне", "нужен", "нужна"}
        out: List[str] = []
        seen = set()
        for term in terms:
            if term in stop or term in seen:
                continue
            seen.add(term)
            out.append(term)
        return out

    def _query_terms(self, query: str) -> List[str]:
        expansion = self._search_alias_expansion(query)
        expanded_query = str(expansion.get("expanded_query") or query or "")
        return self._terms_from_text(expanded_query)

    def _term_matches(self, haystack: str, term: str) -> bool:
        for candidate in self._term_variants(term):
            if candidate in haystack:
                return True
            if len(candidate) >= 5:
                stem = candidate.rstrip("аеиоуыьъйяю")
                if len(stem) >= 4 and stem in haystack:
                    return True
        return False

    def _term_variants(self, term: str) -> List[str]:
        clean = str(term or "").lower().replace("ё", "е")
        variants = [clean]
        for alias in _TERM_ALIASES.get(clean, []):
            alias_norm = alias.lower().replace("ё", "е")
            if alias_norm and alias_norm not in variants:
                variants.append(alias_norm)
        if "0" in clean or re.search(r"[oо].*\d|\d.*[oо]", clean, flags=re.IGNORECASE):
            for src, dst in (("o", "0"), ("о", "0"), ("0", "o"), ("0", "о")):
                alt = clean.replace(src, dst)
                if alt and alt not in variants:
                    variants.append(alt)
            for idx, char in enumerate(clean):
                if char == "0":
                    for dst in ("o", "о"):
                        alt = f"{clean[:idx]}{dst}{clean[idx + 1:]}"
                        if alt and alt not in variants:
                            variants.append(alt)
        return variants

    def _modified_to_ts(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            return None

    def _recency_adjustment(self, item: Dict[str, Any], now_ts: float) -> tuple[float, float]:
        config = getattr(self, "config", {}) or {}
        if not bool(config.get("rank_recency_enabled", True)):
            return 0.0, 0.0
        half_life_days = float(config.get("rank_recency_half_life_days", 180.0) or 180.0)
        max_boost = float(config.get("rank_recency_max_boost", 0.03) or 0.03)
        if half_life_days <= 0.0 or max_boost <= 0.0:
            return 0.0, 0.0
        modified_ts = self._modified_to_ts(item.get("modified"))
        if modified_ts is None:
            return 0.0, 0.0
        age_days = max(0.0, (now_ts - modified_ts) / 86_400.0)
        freshness = 0.5 ** (age_days / half_life_days)
        adjustment = max(0.0, min(max_boost, max_boost * freshness))
        return freshness, adjustment

    def _refresh_fs_cache(self) -> List[Dict[str, Any]]:
        now = time.time()
        cache = getattr(self, "_fs_cache", {"ts": 0.0, "items": []})
        cached = cache.get("items") or []
        if cached and now - float(cache.get("ts") or 0) < FS_CACHE_TTL_SEC:
            return cached

        config = getattr(self, "config", {})
        catalog_path = str(config.get("catalog_path") or "").strip()
        if not catalog_path:
            self._fs_cache = {"ts": now, "items": []}
            return []
        root = Path(catalog_path)
        if not root.exists():
            self._fs_cache = {"ts": now, "items": []}
            return []

        items: List[Dict[str, Any]] = []
        max_items = int(config.get("filesystem_search_max_items", FS_CACHE_MAX_ITEMS))
        state_db_path = self._state_db_path()
        if state_db_path and state_db_path.exists():
            try:
                entries = IndexStateDB(str(state_db_path)).iter_entries()
                folder_seen: set[str] = set()

                def add_folder(rel_folder: str) -> None:
                    rel_clean = str(rel_folder or "").strip()
                    if not rel_clean or rel_clean in folder_seen or len(items) >= max_items:
                        return
                    folder_seen.add(rel_clean)
                    folder_name = rel_clean.replace("/", "\\").rsplit("\\", 1)[-1]
                    items.append(
                        {
                            "kind": "folder",
                            "filename": folder_name,
                            "path": rel_clean,
                            "full_path": str(root / Path(rel_clean)),
                            "extension": "",
                        }
                    )

                for entry in entries:
                    full_path_str = str(entry.get("full_path") or "").strip()
                    if not full_path_str:
                        continue
                    full_path = Path(full_path_str)
                    filename = full_path.name
                    ext = str(entry.get("extension") or full_path.suffix.lower() or "")
                    try:
                        rel = str(full_path.relative_to(root))
                    except ValueError:
                        rel = str(full_path)
                    size_b = int(entry.get("size_bytes") or 0)
                    mtime = float(entry.get("mtime") or 0.0)
                    items.append(
                        {
                            "kind": "file",
                            "filename": filename,
                            "path": rel,
                            "full_path": full_path_str,
                            "extension": ext,
                            "size_mb": round(size_b / 1_048_576, 2) if size_b > 0 else None,
                            "modified": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(mtime)) if mtime > 0 else None,
                        }
                    )
                    parent_parts = [part for part in re.split(r"[\\/]+", rel)[:-1] if part]
                    for idx in range(1, len(parent_parts) + 1):
                        add_folder("\\".join(parent_parts[:idx]))
                    if len(items) >= max_items:
                        self._fs_cache = {"ts": now, "items": items}
                        return items
                if items:
                    self._fs_cache = {"ts": now, "items": items}
                    return items
                for dirpath, dirnames, _filenames in os.walk(root):
                    base = Path(dirpath)
                    for dirname in dirnames:
                        add_folder(str((base / dirname).relative_to(root)))
                        if len(items) >= max_items:
                            self._fs_cache = {"ts": now, "items": items}
                            return items
                if items:
                    self._fs_cache = {"ts": now, "items": items}
                    return items
            except Exception:
                items = []

        for dirpath, dirnames, filenames in os.walk(root):
            base = Path(dirpath)
            for dirname in dirnames:
                p = base / dirname
                rel = str(p.relative_to(root))
                items.append({
                    "kind": "folder",
                    "filename": dirname,
                    "path": rel,
                    "full_path": str(p),
                    "extension": "",
                })
                if len(items) >= max_items:
                    self._fs_cache = {"ts": now, "items": items}
                    return items
            for filename in filenames:
                p = base / filename
                ext = p.suffix.lower()
                rel = str(p.relative_to(root))
                try:
                    stat = p.stat()
                    size_mb = round(stat.st_size / 1_048_576, 2)
                    modified = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(stat.st_mtime))
                except OSError:
                    size_mb = None
                    modified = None
                items.append({
                    "kind": "file",
                    "filename": filename,
                    "path": rel,
                    "full_path": str(p),
                    "extension": ext,
                    "size_mb": size_mb,
                    "modified": modified,
                })
                if len(items) >= max_items:
                    self._fs_cache = {"ts": now, "items": items}
                    return items
        self._fs_cache = {"ts": now, "items": items}
        return items

    def _state_db_path(self) -> Optional[Path]:
        config = getattr(self, "config", {}) or {}
        candidates: list[Path] = []
        qdrant_db_path = str(config.get("qdrant_db_path") or "").strip()
        if qdrant_db_path:
            candidates.append(Path(qdrant_db_path) / "index_state.db")
        catalog_path = str(config.get("catalog_path") or "").strip()
        default_catalog = str(DEFAULT_CONFIG.get("catalog_path") or "").strip()
        if catalog_path and os.path.normcase(os.path.normpath(catalog_path)) == os.path.normcase(os.path.normpath(default_catalog)):
            candidates.append(PROJECT_ROOT / "data" / "index_state.db")
        for path in candidates:
            if path.exists():
                return path
        return None

    def clear_filesystem_cache(self) -> None:
        """Force the next lexical catalog search to rescan the filesystem."""
        self._fs_cache = {"ts": 0.0, "items": []}
        self._metadata_index_source = 0
        self._metadata_token_docs = {}
        self._metadata_needle_docs = OrderedDict()
        self._metadata_corpus_size = 0
        self._metadata_average_doc_length = 0.0

    def _prepare_metadata_search_item(self, item: Dict[str, Any]) -> None:
        if isinstance(item.get("_search_name_terms"), set):
            return
        name = str(item.get("filename") or "").lower().replace("ё", "е")
        path = str(item.get("path") or "").lower().replace("ё", "е")
        item["_search_name"] = name
        item["_search_path"] = path
        item["_search_hay"] = f"{name} {path}"
        item["_search_path_parts"] = tuple(part for part in re.split(r"[\\/]+", path) if part)
        item["_search_name_terms"] = set(self._terms_from_text(name))

    def _build_metadata_token_index(self, items: List[Dict[str, Any]]) -> None:
        if int(getattr(self, "_metadata_index_source", 0) or 0) == id(items):
            return
        token_docs: Dict[str, list[int]] = {}
        total_doc_length = 0
        corpus_size = 0
        for index, item in enumerate(items):
            tokens = item.get("_bm25_tokens") or []
            if tokens:
                corpus_size += 1
                total_doc_length += len(tokens)
            for token in tokens:
                token_docs.setdefault(str(token), []).append(index)
        self._metadata_token_docs = {token: tuple(indices) for token, indices in token_docs.items()}
        self._metadata_needle_docs = OrderedDict()
        self._metadata_corpus_size = corpus_size
        self._metadata_average_doc_length = total_doc_length / max(1, corpus_size)
        self._metadata_index_source = id(items)

    def _metadata_candidates(self, items: List[Dict[str, Any]], terms: List[str]) -> List[Dict[str, Any]]:
        if int(getattr(self, "_metadata_index_source", 0) or 0) != id(items):
            return items
        token_docs = getattr(self, "_metadata_token_docs", {}) or {}
        needle_docs = getattr(self, "_metadata_needle_docs", None)
        if not isinstance(needle_docs, OrderedDict):
            needle_docs = OrderedDict(needle_docs or {})
            self._metadata_needle_docs = needle_docs
        cache_size = max(16, int(self.config.get("metadata_needle_cache_size", 512) or 512))
        indices: set[int] = set()
        for term in terms:
            needles: list[str] = []
            for variant in self._term_variants(term):
                if variant and variant not in needles:
                    needles.append(variant)
                if len(variant) >= 5:
                    stem = variant.rstrip("аеиоуыьъйяю")
                    if len(stem) >= 4 and stem not in needles:
                        needles.append(stem)
            for needle in needles:
                matched = needle_docs.get(needle)
                if matched is None:
                    found: set[int] = set()
                    for token, document_indices in token_docs.items():
                        if needle in token:
                            found.update(document_indices)
                    matched = tuple(found)
                    needle_docs[needle] = matched
                    while len(needle_docs) > cache_size:
                        needle_docs.popitem(last=False)
                else:
                    needle_docs.move_to_end(needle)
                indices.update(matched)
        return [items[index] for index in sorted(indices)]

    def warm_retrieval_cache(self) -> int:
        """Prepare reusable metadata structures for warm-search latency."""
        items = self._refresh_fs_cache()
        for item in items:
            self._prepare_metadata_search_item(item)
        prepare_bm25_items(items)
        self._build_metadata_token_index(items)
        return len(items)

    def _fulltext_content_search(
        self,
        *,
        query: str,
        limit: int,
        file_type: Optional[str],
        content_only: bool,
        title_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """Retrieve exact/stemmed content matches from the Qdrant text index."""
        if title_only or not bool(self.config.get("retrieval_fulltext_enabled", False)):
            return []
        if not bool(getattr(self, "_fulltext_available", False)):
            return []
        terms = tokenize(query)
        if not terms:
            return []
        must: List[Any] = [FieldCondition(key="text", match=MatchText(text=" ".join(terms)))]
        if file_type:
            must.append(FieldCondition(key="extension", match=MatchValue(value=file_type.lower())))
        must_not = [
            FieldCondition(
                key="type",
                match=MatchAny(any=["file_metadata", "folder_metadata"]),
            )
        ]
        try:
            points, _offset = self.qdrant.scroll(
                collection_name=self.collection_name,
                scroll_filter=Filter(must=must, must_not=must_not),
                limit=max(1, int(limit)),
                with_payload=True,
                with_vectors=False,
            )
        except Exception as exc:
            logger.warning("Полнотекстовый канал недоступен: %s", exc)
            return []

        query_norm = " ".join(terms)
        ranked: List[Dict[str, Any]] = []
        for point in points:
            payload = dict(getattr(point, "payload", None) or {})
            text = str(payload.get("text") or "")
            normalized = " ".join(tokenize(text))
            occurrences = sum(max(1, normalized.count(term)) for term in terms)
            exact_phrase = bool(query_norm and query_norm in normalized)
            score = min(0.999, 0.96 + (0.025 if exact_phrase else 0.0) + min(0.014, occurrences * 0.002))
            result = self._result_from_payload(
                payload,
                score=score,
                rank_reason="точное совпадение в содержимом",
                retrieval_source="fulltext",
            )
            result["fulltext_matched_terms"] = len(terms)
            result["fulltext_query_terms"] = len(terms)
            result["fulltext_occurrences"] = occurrences
            ranked.append(result)
        ranked.sort(
            key=lambda item: (
                float(item.get("score") or 0),
                int(item.get("fulltext_occurrences") or 0),
                -len(str(item.get("text") or "")),
            ),
            reverse=True,
        )
        return ranked[: max(1, int(limit))]

    def _bm25_catalog_search(
        self,
        *,
        query: str,
        limit: int,
        file_type: Optional[str],
        content_only: bool,
        title_only: bool = False,
    ) -> List[Dict[str, Any]]:
        _ = title_only  # BM25 channel is metadata/title search by design.
        if content_only or not bool(self.config.get("retrieval_bm25_enabled", True)):
            return []
        if not str(self.config.get("catalog_path") or "").strip():
            return []
        terms = self._query_terms(query)
        if not terms:
            return []
        metadata_items = self._refresh_fs_cache()
        indexed_candidates = self._metadata_candidates(metadata_items, terms) if not file_type else metadata_items
        candidates: List[Dict[str, Any]] = []
        for item in indexed_candidates:
            if file_type and item.get("kind") == "folder":
                continue
            if file_type and str(item.get("extension") or "").lower() != file_type.lower():
                continue
            candidates.append(item)

        use_corpus_stats = not file_type and len(indexed_candidates) != len(metadata_items)
        ranked = bm25_rank_items(
            candidates,
            terms,
            limit=limit,
            corpus_size=int(getattr(self, "_metadata_corpus_size", 0) or 0) if use_corpus_stats else None,
            average_doc_length=(
                float(getattr(self, "_metadata_average_doc_length", 0.0) or 0.0) if use_corpus_stats else None
            ),
        )
        out: List[Dict[str, Any]] = []
        for item in ranked:
            is_folder = item.get("kind") == "folder"
            out.append(
                {
                    "score": item.get("score", 0),
                    "type": "folder_metadata" if is_folder else "file_metadata",
                    "text": (
                        f"Каталог: {item.get('filename')} | Путь: {item.get('path')}"
                        if is_folder
                        else f"Файл: {item.get('filename')} | Путь: {item.get('path')} | Расширение: {item.get('extension')}"
                    ),
                    "filename": item.get("filename", ""),
                    "path": item.get("path", ""),
                    "full_path": item.get("full_path", ""),
                    "size_mb": item.get("size_mb"),
                    "modified": item.get("modified"),
                    "extension": item.get("extension", ""),
                    "chunk_index": None,
                    "rank_reason": item.get("rank_reason", "BM25 совпадение в имени/пути"),
                    "retrieval_source": "bm25",
                    "bm25_matched_terms": item.get("bm25_matched_terms", 0),
                    "bm25_query_terms": len(terms),
                }
            )
        return out

    def _numeric_exact_search(
        self,
        *,
        query: str,
        limit: int,
        file_type: Optional[str],
        content_only: bool,
        title_only: bool = False,
    ) -> List[Dict[str, Any]]:
        if title_only or not getattr(self, "connected", False):
            return []
        tokens = query_numeric_tokens(query)
        if not tokens:
            return []

        strong_tokens = sorted({token for token in tokens if len(token) >= 5}, key=len, reverse=True)
        if not strong_tokens:
            return []

        filters: list[tuple[list[str], float]] = []
        for token in strong_tokens[:4]:
            filters.append(([token], 0.9995))
        query_groups = re.findall(r"\d{3,}", str(query or ""))
        if len(query_groups) >= 2:
            filters.append((query_groups[:4], 0.9985))

        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in self._spreadsheet_numeric_exact_scan(
            query=query,
            tokens=strong_tokens,
            limit=limit,
            file_type=file_type,
        ):
            key = f"{item.get('full_path')}::{item.get('chunk_index')}::{item.get('type')}"
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
            if len(out) >= limit:
                return out
        if out:
            return out

        scroll_timeout = int((getattr(self, "config", {}) or {}).get("numeric_exact_qdrant_timeout_sec", 2) or 2)
        for required_tokens, score in filters:
            must = [
                FieldCondition(key="numeric_tokens", match=MatchValue(value=token))
                for token in required_tokens
            ]
            if file_type:
                must.append(FieldCondition(key="extension", match=MatchValue(value=file_type.lower())))
            if content_only:
                must.append(
                    FieldCondition(
                        key="type",
                        match=MatchAny(any=["docx_content", "doc_content", "xlsx_content", "pdf_content", "txt_content", "csv_content", "rtf_content", "pptx_content", "image_content"]),
                    )
                )
            try:
                points, _offset = self.qdrant.scroll(
                    collection_name=self.collection_name,
                    scroll_filter=Filter(must=must),
                    limit=max(1, min(limit, 50)),
                    with_payload=True,
                    with_vectors=False,
                    timeout=scroll_timeout,
                )
            except Exception as exc:
                logger.debug("Numeric exact search failed for %s: %s", required_tokens, exc)
                continue
            for point in points:
                payload = point.payload or {}
                key = f"{payload.get('full_path')}::{payload.get('chunk_index')}::{payload.get('type')}"
                if key in seen:
                    continue
                seen.add(key)
                out.append(self._result_from_payload(payload, score=score, rank_reason="точное совпадение номера", retrieval_source="numeric_exact"))
                if len(out) >= limit:
                    return out
        return out

    def _spreadsheet_numeric_exact_scan(
        self,
        *,
        query: str,
        tokens: List[str],
        limit: int,
        file_type: Optional[str],
    ) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        if file_type and file_type.lower() not in {".xlsx", ".xls", ".csv"}:
            return []
        query_groups = re.findall(r"\d{3,}", str(query or ""))
        if not query_groups:
            return []
        text_terms = [term for term in self._terms_from_text(query) if not term.isdigit()]
        if not text_terms:
            return []

        started = time.perf_counter()
        max_seconds = float((getattr(self, "config", {}) or {}).get("numeric_exact_fs_scan_seconds", 4.0) or 4.0)
        max_candidates = int((getattr(self, "config", {}) or {}).get("numeric_exact_fs_scan_candidates", 200) or 200)
        candidates = self._spreadsheet_numeric_candidates(
            text_terms=text_terms,
            file_type=file_type,
            max_candidates=max_candidates,
        )

        cache = getattr(self, "_numeric_file_cache", {})
        out: list[dict[str, Any]] = []
        for item in candidates:
            if time.perf_counter() - started > max_seconds:
                break
            full_path = str(item.get("full_path") or "")
            path_obj = Path(full_path)
            if not path_obj.exists() or not path_obj.is_file():
                continue
            try:
                mtime = path_obj.stat().st_mtime
            except OSError:
                continue
            cached = cache.get(full_path)
            if cached and cached.get("mtime") == mtime:
                file_tokens = set(cached.get("tokens") or [])
                text = str(cached.get("text") or "")
            else:
                try:
                    if path_obj.suffix.lower() == ".csv":
                        from .extractors import extract_csv  # noqa: PLC0415

                        text = extract_csv(path_obj)
                    else:
                        from .extractors import extract_spreadsheet_document  # noqa: PLC0415

                        doc = extract_spreadsheet_document(path_obj)
                        text = doc.text
                except Exception as exc:
                    logger.debug("Spreadsheet numeric scan failed for %s: %s", full_path, exc)
                    continue
                file_tokens = set(numeric_exact_tokens(text, max_tokens=5000))
                cache[full_path] = {"mtime": mtime, "tokens": sorted(file_tokens), "text": text}
            if not file_tokens:
                continue
            matched_joined = any(token in file_tokens for token in tokens if len(token) >= 5)
            matched_groups = all(group in file_tokens for group in query_groups[:4])
            if not (matched_joined or matched_groups):
                continue
            snippet_tokens = [token for token in tokens if len(token) >= 5] or query_groups
            snippet = self._numeric_snippet(text, snippet_tokens)
            result = {
                "score": 0.999,
                "type": f"{path_obj.suffix.lower().lstrip('.')}_content",
                "text": snippet or f"Точное совпадение номера в файле: {item.get('filename')}",
                "filename": item.get("filename", path_obj.name),
                "path": item.get("path", str(path_obj)),
                "full_path": full_path,
                "size_mb": item.get("size_mb"),
                "modified": item.get("modified"),
                "extension": path_obj.suffix.lower(),
                "chunk_index": None,
                "rank_reason": "точное совпадение номера в таблице",
                "retrieval_source": "numeric_fs_exact",
            }
            out.append(self._repair_result_display_fields(result))
            if len(out) >= limit or matched_joined:
                break
        self._numeric_file_cache = cache
        return out

    def _spreadsheet_numeric_candidates(
        self,
        *,
        text_terms: List[str],
        file_type: Optional[str],
        max_candidates: int,
    ) -> List[Dict[str, Any]]:
        exts = {".xlsx", ".xls", ".csv"}
        if file_type:
            exts = {file_type.lower()} & exts
        if not exts:
            return []

        def term_hit(haystack: str) -> bool:
            return any(
                self._term_matches(haystack, term)
                or (term == "стс" and "тс" in haystack)
                for term in text_terms
            )

        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        state_db_path = self._state_db_path()
        root = Path(str((getattr(self, "config", {}) or {}).get("catalog_path") or ""))
        if state_db_path:
            try:
                con = sqlite3.connect(state_db_path)
                con.row_factory = sqlite3.Row
                placeholders = ",".join("?" for _ in sorted(exts))
                rows = con.execute(
                    f"""
                    SELECT full_path, extension, size_bytes, mtime, stage
                    FROM state_entries
                    WHERE extension IN ({placeholders})
                      AND status = 'ok'
                    ORDER BY
                      CASE WHEN stage = 'content' THEN 0 ELSE 1 END,
                      length(full_path) ASC
                    """,
                    tuple(sorted(exts)),
                ).fetchall()
            except sqlite3.Error as exc:
                logger.debug("Spreadsheet candidate state DB query failed: %s", exc)
                rows = []
            finally:
                try:
                    con.close()  # type: ignore[name-defined]
                except Exception:
                    pass

            for row in rows:
                full_path = str(row["full_path"] or "")
                if not full_path or full_path in seen:
                    continue
                hay = full_path.lower().replace("ё", "е")
                if not term_hit(hay):
                    continue
                path_obj = Path(full_path)
                try:
                    rel = str(path_obj.relative_to(root)) if root else full_path
                except ValueError:
                    rel = full_path
                mtime = float(row["mtime"] or 0.0)
                size_b = int(row["size_bytes"] or 0)
                candidates.append(
                    {
                        "kind": "file",
                        "filename": path_obj.name,
                        "path": rel,
                        "full_path": full_path,
                        "extension": str(row["extension"] or path_obj.suffix.lower()),
                        "size_mb": round(size_b / 1_048_576, 2) if size_b > 0 else None,
                        "modified": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(mtime)) if mtime > 0 else None,
                    }
                )
                seen.add(full_path)
                if len(candidates) >= max_candidates:
                    return candidates

        for item in self._refresh_fs_cache():
            if item.get("kind") != "file":
                continue
            ext = str(item.get("extension") or "").lower()
            if ext not in {".xlsx", ".xls", ".csv"}:
                continue
            if file_type and ext != file_type.lower():
                continue
            full_path = str(item.get("full_path") or "")
            if not full_path or full_path in seen:
                continue
            hay = f"{item.get('filename') or ''} {item.get('path') or ''}".lower().replace("ё", "е")
            if not term_hit(hay):
                continue
            candidates.append(item)
            seen.add(full_path)
            if len(candidates) >= max_candidates:
                break
        return candidates

    def _numeric_snippet(self, text: str, tokens: List[str]) -> str:
        value = str(text or "")
        matches = list(re.finditer(r"\d{3,}", value))
        token_set = set(tokens)
        for idx, match in enumerate(matches):
            nearby = "".join(
                item.group(0)
                for item in matches[max(0, idx - 1) : min(len(matches), idx + 2)]
            )
            if match.group(0) in token_set or any(token in nearby for token in token_set):
                start = max(0, match.start() - 220)
                end = min(len(value), match.end() + 420)
                return value[start:end].strip()
        return value[:600].strip()

    def _result_from_payload(
        self,
        payload: Dict[str, Any],
        *,
        score: float,
        rank_reason: str = "",
        retrieval_source: str = "",
    ) -> Dict[str, Any]:
        result = {
            "score": round(float(score), 6),
            "type": payload.get("type", ""),
            "text": payload.get("text", ""),
            "filename": payload.get("filename", ""),
            "path": payload.get("path", ""),
            "full_path": payload.get("full_path", ""),
            "size_mb": payload.get("size_mb"),
            "modified": payload.get("modified"),
            "created": payload.get("created"),
            "extension": payload.get("extension", ""),
            "doc_author": payload.get("doc_author", ""),
            "doc_last_editor": payload.get("doc_last_editor", ""),
            "doc_top_editor": payload.get("doc_top_editor", ""),
            "doc_created": payload.get("doc_created", ""),
            "chunk_index": payload.get("chunk_index"),
            "cloud_file_id": payload.get("cloud_file_id", ""),
            "cloud_version_id": payload.get("cloud_version_id", ""),
            "cloud_path": payload.get("cloud_path", ""),
            "storage_key": payload.get("storage_key", ""),
            "doc_id": payload.get("doc_id", ""),
            "parent_id": payload.get("parent_id", ""),
            "section": payload.get("section", ""),
            "page": payload.get("page"),
            "sheet": payload.get("sheet", ""),
            "row_start": payload.get("row_start"),
            "row_end": payload.get("row_end"),
            "provenance": payload.get("provenance") or {},
        }
        if rank_reason:
            result["rank_reason"] = rank_reason
        if retrieval_source:
            result["retrieval_source"] = retrieval_source
        return self._repair_result_display_fields(result)

    def _repair_result_display_fields(self, item: Dict[str, Any]) -> Dict[str, Any]:
        repaired = dict(item)
        for key in ("filename", "path"):
            value = str(repaired.get(key) or "")
            fixed = repair_mojibake_text(value)
            if fixed != value:
                repaired[key] = fixed
        text = str(repaired.get("text") or "")
        fixed_text = repair_mojibake_text(text)
        if fixed_text != text:
            repaired["text"] = fixed_text
        return repaired

    def _lexical_catalog_search(
        self,
        *,
        query: str,
        limit: int,
        file_type: Optional[str],
        content_only: bool,
        title_only: bool = False,
    ) -> List[Dict[str, Any]]:
        _ = title_only  # lexical path already returns only metadata entries
        if content_only:
            return []
        terms = self._query_terms(query)
        if not terms:
            return []
        raw_terms = self._terms_from_text(query)
        expansion = self._search_alias_expansion(query)
        alias_phrases = [
            str(alias or "").lower().replace("ё", "е")
            for alias in expansion.get("aliases", [])
            if str(alias or "").strip()
        ]
        alias_groups = [
            str(group.get("label") or "").lower().replace("ё", "е")
            for group in expansion.get("groups", [])
            if isinstance(group, dict) and str(group.get("label") or "").strip()
        ]
        wants_machine_passport = any(
            term in {"паспорт", "паспорта", "паспорты", "псм", "птс", "стс", "техпаспорт"}
            for term in raw_terms
        ) or any("паспорт техники" in label for label in alias_groups)
        wants_vin_plate = "vin" in raw_terms
        vin_context_terms = [term for term in raw_terms if term != "vin"]
        vin_vehicle_doc_context_terms = [term for term in raw_terms if term not in {"vin", "птс", "стс"}]
        entity_terms = _extract_entities(query)
        query_norm = " ".join(terms)
        variants: Dict[str, tuple[str, ...]] = {}

        def term_matches(haystack: str, term: str) -> bool:
            candidates = variants.get(term)
            if candidates is None:
                expanded: list[str] = []
                for candidate in self._term_variants(term):
                    if candidate not in expanded:
                        expanded.append(candidate)
                    if len(candidate) >= 5:
                        stem = candidate.rstrip("аеиоуыьъйяю")
                        if len(stem) >= 4 and stem not in expanded:
                            expanded.append(stem)
                candidates = tuple(expanded)
                variants[term] = candidates
            return any(candidate in haystack for candidate in candidates)

        out: List[Dict[str, Any]] = []
        metadata_items = self._refresh_fs_cache()
        for item in self._metadata_candidates(metadata_items, terms):
            if file_type and item.get("kind") == "file" and item.get("extension") != file_type.lower():
                continue
            if file_type and item.get("kind") == "folder":
                continue
            self._prepare_metadata_search_item(item)
            name = str(item.get("_search_name") or "")
            path = str(item.get("_search_path") or "")
            hay = str(item.get("_search_hay") or "")
            path_parts = item.get("_search_path_parts") or ()
            parent_name = path_parts[-2] if len(path_parts) >= 2 else ""
            name_terms = item.get("_search_name_terms") or set()
            if entity_terms and not any(term_matches(hay, e) for e in entity_terms):
                continue
            matched = sum(1 for t in terms if term_matches(hay, t))
            if matched == 0:
                continue
            is_folder = item.get("kind") == "folder"
            first_term = terms[0] if terms else ""
            first_stem = first_term.rstrip("аеиоуыьъйяю") if len(first_term) >= 5 else first_term
            if is_folder and first_stem and name.startswith(first_stem):
                score = 0.999
            elif first_stem and parent_name.startswith(first_stem):
                score = 0.997
            elif query_norm and query_norm in name:
                score = 0.995
            elif all(term_matches(name, t) for t in terms):
                score = 0.975
            elif all(term_matches(path, t) for t in terms):
                score = 0.955
            else:
                score = 0.86 + min(0.08, matched / max(1, len(terms)) * 0.08)
            raw_matched = 0
            if len(raw_terms) > 1:
                raw_matched = sum(1 for t in raw_terms if term_matches(hay, t))
                if raw_matched < len(raw_terms):
                    score = min(score, 0.91 + min(0.04, raw_matched / max(1, len(raw_terms)) * 0.04))
            if alias_groups:
                for label in alias_groups:
                    label_terms = [
                        t
                        for t in re.findall(r"[a-zа-яё0-9\-]{2,}", label, flags=re.IGNORECASE)
                        if t not in {"и", "или", "по", "на", "в", "во", "от", "для"}
                    ]
                    if (not raw_terms or raw_matched > 0) and label_terms and all(term_matches(hay, t) for t in label_terms):
                        score = max(score, 0.972)
                        break
            if (not raw_terms or raw_matched > 0) and alias_phrases and any(phrase and phrase in hay for phrase in alias_phrases):
                score = max(score, 0.965)
            if wants_vin_plate and not is_folder and (
                "шильдик" in hay
                or "табличка" in hay
            ):
                if vin_context_terms and all(term_matches(hay, term) for term in vin_context_terms):
                    score = max(score, 0.985)
                elif not vin_context_terms:
                    score = max(score, 0.965)
            if wants_vin_plate and not is_folder and (
                "паспорт транспортного средства" in hay
                or "свидетельство о регистрации" in hay
                or "птс" in hay
                or "стс" in hay
            ) and vin_vehicle_doc_context_terms and all(
                term_matches(hay, term) for term in vin_vehicle_doc_context_terms
            ):
                score = max(score, 0.985)
            if wants_machine_passport and not is_folder and (
                "выписка из электронного паспорта" in hay
                or "электронного паспорта" in hay
            ):
                score = max(score, 0.998)
            elif wants_machine_passport and not is_folder and "паспорт" in name and (
                not entity_terms or any(term_matches(hay, e) for e in entity_terms)
            ):
                score = max(score, 0.992)
            elif wants_machine_passport and not is_folder and ("псм" in hay or "птс" in hay):
                score = max(score, 0.996)
            elif (
                wants_machine_passport
                and not is_folder
                and str(item.get("extension") or "").lower() == ".pdf"
                and not any(noisy in hay for noisy in ("осаго", "страхов", "полис"))
                and entity_terms
                and any(any(variant in name_terms for variant in self._term_variants(e)) for e in entity_terms)
            ):
                score = max(score, 0.9996)
            elif not is_folder and ("документы на технику" in hay or "док-ты техника" in hay):
                score = min(0.94, score + 0.04)
            if query_norm and query_norm in name:
                score += 0.0025
            elif query_norm and query_norm in path:
                score += 0.0015
            if raw_terms:
                exact_terms_in_name = sum(1 for t in raw_terms if t and term_matches(name, t))
                score += min(0.002, exact_terms_in_name * 0.0004)
            out.append({
                "score": round(score, 6),
                "type": "folder_metadata" if is_folder else "file_metadata",
                "text": (
                    f"Каталог: {item.get('filename')} | Путь: {item.get('path')}"
                    if is_folder
                    else f"Файл: {item.get('filename')} | Путь: {item.get('path')} | Расширение: {item.get('extension')}"
                ),
                "filename": item.get("filename", ""),
                "path": item.get("path", ""),
                "full_path": item.get("full_path", ""),
                "size_mb": item.get("size_mb"),
                "modified": item.get("modified"),
                "extension": item.get("extension", ""),
                "chunk_index": None,
                "rank_reason": "совпадение в имени/пути",
                "retrieval_source": "lexical",
                "lexical_matched_terms": matched,
                "lexical_query_terms": len(terms),
            })
        out.sort(
            key=lambda x: (
                float(x.get("score") or 0),
                1 if str(x.get("type") or "") == "file_metadata" else 0,
                -len(str(x.get("path") or "")),
            ),
            reverse=True,
        )
        return out[:limit]

    def _merge_ranked_results(
        self,
        lexical: List[Dict[str, Any]],
        semantic: List[Dict[str, Any]],
        *,
        limit: int,
        query: str = "",
    ) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for item in [*lexical, *semantic]:
            key = f"{item.get('full_path')}::{item.get('chunk_index')}::{item.get('type')}"
            existing = merged.get(key)
            if existing is None or float(item.get("score") or 0) > float(existing.get("score") or 0):
                merged[key] = item
        if hasattr(self.telemetry, "get_search_feedback_scores"):
            feedback = self.telemetry.get_search_feedback_scores(
                query=query,
                paths=[str(item.get("full_path") or "") for item in merged.values()],
            )
        else:
            feedback = {}
        config = getattr(self, "config", {}) or {}
        feedback_step = float(config.get("rank_feedback_step", 0.02) or 0.02)
        feedback_cap = float(config.get("rank_feedback_cap", 0.18) or 0.18)
        now_ts = time.time()
        for item in merged.values():
            path = str(item.get("full_path") or "")
            signal = int(feedback.get(path, 0))
            if str(item.get("fusion") or "") == "rrf":
                base_score = float(item.get("rank_score", item.get("score") or 0) or 0)
            else:
                base_score = float(item.get("score") or 0)
            if signal:
                adjustment = max(-feedback_cap, min(feedback_cap, signal * feedback_step))
            else:
                adjustment = 0.0
            freshness, recency_adj = self._recency_adjustment(item, now_ts)
            if str(item.get("fusion") or "") == "rrf":
                recency_adj *= base_score
            if not signal:
                item["feedback_score"] = 0
            else:
                item["feedback_score"] = signal
            item["recency_score"] = round(freshness, 4)
            item["rank_score"] = max(0.0, min(1.0, base_score + adjustment + recency_adj))
        ranked = sorted(
            merged.values(),
            key=lambda x: (
                float(x.get("rank_score", x.get("score") or 0) or 0),
                float(x.get("score") or 0),
                -len(str(x.get("path") or x.get("full_path") or "")),
            ),
            reverse=True,
        )
        folder_cap = max(3, min(5, limit // 2))
        chunk_cap = max(1, int(config.get("rank_max_chunks_per_document", 3) or 3))
        balanced: List[Dict[str, Any]] = []
        deferred_folders: List[Dict[str, Any]] = []
        deferred_chunks: List[Dict[str, Any]] = []
        chunks_by_path: Dict[str, int] = {}
        folder_count = 0
        for item in ranked:
            if item.get("type") == "folder_metadata" and folder_count >= folder_cap:
                deferred_folders.append(item)
                continue
            if item.get("type") == "folder_metadata":
                folder_count += 1
            chunk_index = item.get("chunk_index")
            if item.get("type") not in {"file_metadata", "folder_metadata"} and chunk_index is not None:
                path_key = str(item.get("full_path") or item.get("path") or "")
                path_count = chunks_by_path.get(path_key, 0)
                if path_key and path_count >= chunk_cap:
                    deferred_chunks.append(item)
                    continue
                if path_key:
                    chunks_by_path[path_key] = path_count + 1
            balanced.append(item)
            if len(balanced) >= limit:
                break
        if len(balanced) < limit:
            deferred = [*deferred_folders, *deferred_chunks]
            balanced.extend(deferred[: limit - len(balanced)])
        return balanced[:limit]

    def _apply_relevance_gate(
        self,
        query: str,
        results: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Drop weak dense-only candidates and malformed content fragments."""
        if not bool(self.config.get("retrieval_relevance_gate_enabled", False)):
            return results
        query_terms = tokenize(query)
        dense_floor = float(self.config.get("retrieval_min_dense_score", 0.78) or 0.78)
        if len(query_terms) == 1:
            dense_floor = max(
                dense_floor,
                float(self.config.get("retrieval_single_term_min_dense_score", 0.80) or 0.80),
            )
        min_content_chars = max(20, int(self.config.get("retrieval_min_content_chars", 120) or 120))
        reranker_floor = float(self.config.get("retrieval_reranker_min_score", -4.0) or -4.0)
        gated: List[Dict[str, Any]] = []
        for item in results:
            item_type = str(item.get("type") or "")
            sources = {
                str(source)
                for source in (item.get("retrieval_sources") or [])
                if str(source).strip()
            }
            source = str(item.get("retrieval_source") or "").strip()
            if source:
                sources.add(source)

            if item_type not in {"file_metadata", "folder_metadata"}:
                clean_text = " ".join(str(item.get("text") or "").split())
                if len(clean_text) < min_content_chars and "fulltext" not in sources:
                    continue

            strong_lexical = "fulltext" in sources
            numeric_sources = {"numeric_exact", "numeric_fs_exact", "spreadsheet_numeric_exact"}
            numeric_context_terms = [term for term in query_terms if not any(char.isdigit() for char in term)]
            numeric_context_only = not numeric_context_terms or all(
                term in {"vin", "птс", "стс", "псм", "утм"} for term in numeric_context_terms
            )
            if sources & numeric_sources and numeric_context_only:
                strong_lexical = True
            lexical_matched = int(item.get("lexical_matched_terms") or 0)
            lexical_total = int(item.get("lexical_query_terms") or len(query_terms) or 0)
            bm25_matched = int(item.get("bm25_matched_terms") or 0)
            bm25_total = int(item.get("bm25_query_terms") or len(query_terms) or 0)
            if lexical_total and lexical_matched >= lexical_total:
                strong_lexical = True
            if bm25_total and bm25_matched >= bm25_total:
                strong_lexical = True

            raw_reranker = item.get("reranker_score")
            if raw_reranker is not None and not strong_lexical and float(raw_reranker) < reranker_floor:
                continue
            reranker_pass = raw_reranker is not None and float(raw_reranker) >= reranker_floor
            dense_score = float(item.get("dense_score") or (item.get("score") if sources == {"dense"} else 0) or 0)
            if not strong_lexical and not reranker_pass and dense_score < dense_floor:
                continue
            updated = dict(item)
            updated["relevance_evidence"] = (
                "lexical" if strong_lexical else "reranker" if reranker_pass else "dense"
            )
            updated["relevance_floor"] = dense_floor
            gated.append(updated)
        return gated

    def _rerank_results(self, query: str, results: List[Dict[str, Any]], *, limit: int) -> List[Dict[str, Any]]:
        if not results:
            return []
        config = getattr(self, "config", {}) or {}
        if not bool(config.get("retrieval_reranker_enabled", False)):
            return results[:limit]
        model = self.reranker
        if model is None:
            return results[:limit]

        candidates = results[: max(limit, int(config.get("retrieval_reranker_top_n", limit) or limit))]
        pairs = [(query, self._rerank_text(item)) for item in candidates]
        try:
            raw_scores = list(model.predict(pairs))
        except Exception as exc:
            logger.warning("Reranker failed, using fused ranking: %s", exc)
            return results[:limit]

        if not raw_scores:
            return results[:limit]
        lo = min(float(score) for score in raw_scores)
        hi = max(float(score) for score in raw_scores)
        span = max(hi - lo, 1e-9)
        weight = max(0.0, min(1.0, float(config.get("retrieval_reranker_weight", 0.65) or 0.65)))
        reranked: List[Dict[str, Any]] = []
        for item, raw_score in zip(candidates, raw_scores):
            reranker_score = (float(raw_score) - lo) / span
            base_score = float(item.get("rank_score", item.get("score") or 0) or 0)
            updated = dict(item)
            updated["reranker_score"] = round(float(raw_score), 6)
            updated["rank_score"] = round((1.0 - weight) * base_score + weight * reranker_score, 6)
            updated["retrieval_reranked"] = True
            reranked.append(updated)

        reranked.sort(
            key=lambda item: (
                float(item.get("rank_score") or 0),
                float(item.get("reranker_score") or 0),
                float(item.get("score") or 0),
            ),
            reverse=True,
        )
        return reranked[:limit]

    def _rerank_text(self, item: Dict[str, Any]) -> str:
        parts = [
            str(item.get("filename") or ""),
            str(item.get("path") or ""),
            str(item.get("text") or ""),
        ]
        return "\n".join(part for part in parts if part).strip()[:4000]

    def _content_chunks_for_paths(self, paths: List[str], max_chunks: int = 100) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen = set()
        for full_path in paths:
            if not full_path or full_path in seen:
                continue
            seen.add(full_path)
            try:
                points, _offset = self.qdrant.scroll(
                    collection_name=self.collection_name,
                    scroll_filter=Filter(
                        must=[
                            FieldCondition(
                                key="full_path",
                                match=MatchValue(value=full_path),
                            )
                        ],
                        must_not=[
                            FieldCondition(
                                key="type",
                                match=MatchValue(value="file_metadata"),
                            )
                        ],
                    ),
                    limit=max_chunks,
                    with_payload=True,
                    with_vectors=False,
                )
            except Exception as exc:
                logger.debug("Не удалось получить content chunks для %s: %s", full_path, exc)
                continue
            for point in points:
                payload = point.payload or {}
                out.append({
                    "score": 0.92,
                    "type": payload.get("type", ""),
                    "text": payload.get("text", ""),
                    "filename": payload.get("filename", ""),
                    "path": payload.get("path", ""),
                    "full_path": payload.get("full_path", ""),
                    "size_mb": payload.get("size_mb"),
                    "modified": payload.get("modified"),
                    "created": payload.get("created"),
                    "extension": payload.get("extension", ""),
                    "doc_author": payload.get("doc_author", ""),
                    "doc_last_editor": payload.get("doc_last_editor", ""),
                    "doc_top_editor": payload.get("doc_top_editor", ""),
                    "doc_created": payload.get("doc_created", ""),
                    "chunk_index": payload.get("chunk_index"),
                    "cloud_file_id": payload.get("cloud_file_id", ""),
                    "cloud_version_id": payload.get("cloud_version_id", ""),
                    "cloud_path": payload.get("cloud_path", ""),
                    "storage_key": payload.get("storage_key", ""),
                    "doc_id": payload.get("doc_id", ""),
                    "parent_id": payload.get("parent_id", ""),
                    "section": payload.get("section", ""),
                    "page": payload.get("page"),
                    "sheet": payload.get("sheet", ""),
                    "row_start": payload.get("row_start"),
                    "row_end": payload.get("row_end"),
                    "provenance": payload.get("provenance") or {},
                    "rank_reason": "content by exact entity path",
                })
        return out

    # ── stats ─────────────────────────────────────────────────────────

    def get_collection_stats(self) -> Dict[str, Any]:
        """Вернуть базовую статистику коллекции Qdrant."""
        if not self.connected:
            return {}
        try:
            info = self.qdrant.get_collection(self.collection_name)
            return {
                "points_count": info.points_count,
                "status": str(info.status),
            }
        except Exception as exc:
            logger.error("Не удалось получить статистику: %s", exc)
            return {}

    def answer_documents(
        self,
        question: str,
        *,
        limit: int = 20,
        source: str = "rag_answer",
        username: str = "",
    ) -> Dict[str, Any]:
        """Generate a RAG answer with explicit source citations.

        This is a structured backend wrapper around `llm.rag_answer()` for UI,
        CLI and Telegram. It keeps the "не знаю" behavior deterministic when no
        textual context is available.
        """
        started = time.perf_counter()
        q = (question or "").strip()
        if not q:
            return {"ok": False, "answer": "Пустой вопрос.", "sources": [], "error": "empty_question"}
        if not self.connected:
            return {"ok": False, "answer": "Нет подключения к Qdrant.", "sources": [], "error": "not_connected"}

        try:
            results = self.search(
                q,
                limit=limit,
                file_type=None,
                content_only=True,
                query_original=q,
                source=f"{source}:search",
                username=username,
            )
        except Exception as exc:
            return {"ok": False, "answer": f"Ошибка поиска: {exc}", "sources": [], "error": f"search_error: {exc}"}

        sources = self._rag_sources(results, max_sources=int(self.config.get("llm_answer_top_k", 5) or 5))
        if not sources:
            answer = "В документах не нашёл подтверждённого ответа."
            self._log_rag_answer(q, answer, False, started, "no_text_sources")
            return {"ok": False, "answer": answer, "sources": [], "results": results, "error": "no_text_sources"}

        try:
            from .llm import rag_answer  # noqa: PLC0415

            answer = rag_answer(
                q,
                results,
                model=str(self.config.get("llm_rag_model") or "qwen3:8b"),
                ollama_url=str(self.config.get("ollama_url") or "http://localhost:11434"),
                top_k=int(self.config.get("llm_answer_top_k", 5) or 5),
                timeout=int(self.config.get("llm_rag_timeout_sec", 90) or 90),
            )
        except Exception as exc:
            answer = f"Ошибка генерации: {exc}"
            self._log_rag_answer(q, answer, False, started, f"rag_answer_error: {exc}")
            return {"ok": False, "answer": answer, "sources": sources, "results": results, "error": f"rag_answer_error: {exc}"}

        verification = self._verify_rag_answer(answer, sources)
        ok = bool(answer and "нет данных для ответа" not in answer.lower() and verification["ok"])
        error = "" if ok else str(verification.get("error") or "weak_answer")
        safe_answer = answer or "Модель не дала ответа."
        if not ok:
            verification["model_answer"] = safe_answer
            safe_answer = self._grounded_fallback_answer(verification)
        self._log_rag_answer(q, safe_answer, ok, started, error)
        return {
            "ok": ok,
            "question": q,
            "answer": safe_answer,
            "sources": sources,
            "results": results,
            "duration_ms": int((time.perf_counter() - started) * 1000),
            "verification": verification,
            "error": "" if ok else error,
        }

    def _rag_sources(self, results: List[Dict[str, Any]], *, max_sources: int = 5) -> List[Dict[str, Any]]:
        sources: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for item in results:
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            key = str(item.get("full_path") or item.get("path") or item.get("filename") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            source_number = len(sources) + 1
            sources.append(
                {
                    "source_id": f"S{source_number}",
                    "citation": self._source_citation(item, source_number),
                    "filename": item.get("filename", ""),
                    "path": item.get("path", ""),
                    "full_path": item.get("full_path", ""),
                    "chunk_index": item.get("chunk_index"),
                    "score": item.get("rank_score", item.get("score")),
                    "doc_id": item.get("doc_id", ""),
                    "parent_id": item.get("parent_id", ""),
                    "section": item.get("section", ""),
                    "page": item.get("page"),
                    "sheet": item.get("sheet", ""),
                    "row_start": item.get("row_start"),
                    "row_end": item.get("row_end"),
                    "provenance": item.get("provenance") or {},
                    "excerpt": text[:500],
                }
            )
            if len(sources) >= max_sources:
                break
        return sources

    def _log_rag_answer(self, question: str, answer: str, ok: bool, started: float, error: str) -> None:
        try:
            self.telemetry.log_fact(
                source="rag_answer",
                question=question,
                ok=ok,
                answer=answer,
                source_type="rag",
                value_kg=None,
                duration_ms=int((time.perf_counter() - started) * 1000),
                error=error,
            )
        except Exception:
            logger.debug("Failed to log rag answer telemetry", exc_info=True)

    def _verify_rag_answer(self, answer: str, sources: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Lightweight grounding gate for numeric/date facts in generated answers."""
        answer_facts = self._extract_verifiable_facts(answer)
        if not answer_facts:
            return {"ok": True, "checked_facts": [], "missing_facts": []}
        source_text = "\n".join(str(src.get("excerpt") or "") for src in sources)
        source_facts = self._extract_verifiable_facts(source_text)
        missing = sorted(answer_facts - source_facts)
        conflicts = self._conflicting_answer_facts(answer_facts, source_facts)
        if missing:
            return {
                "ok": False,
                "checked_facts": sorted(answer_facts),
                "missing_facts": missing,
                "error": "unsupported_facts",
            }
        if conflicts:
            return {
                "ok": False,
                "checked_facts": sorted(answer_facts),
                "missing_facts": [],
                "conflicting_facts": conflicts,
                "error": "conflicting_facts",
            }
        return {"ok": True, "checked_facts": sorted(answer_facts), "missing_facts": []}

    def _extract_verifiable_facts(self, text: str) -> set[str]:
        facts: set[str] = set()
        normalized = re.sub(r"\[(?:s|источник)\s*\d+\]", "", str(text or "").lower(), flags=re.IGNORECASE)
        normalized = normalized.replace(",", ".")
        for match in re.finditer(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b", normalized):
            facts.add(f"date:{match.group(0).replace('/', '.').replace('-', '.')}")
        for match in re.finditer(
            r"\b(\d[\d\s.]{0,15})\s*(кг|килограмм(?:а|ов)?|т|тн|тонн(?:а|ы|)?|руб(?:\.|лей|ля|ль)?|₽|%)\b",
            normalized,
            flags=re.IGNORECASE,
        ):
            value = self._normalize_fact_number(match.group(1))
            unit = self._normalize_fact_unit(match.group(2))
            if value:
                facts.add(f"{unit}:{value}")
        for match in re.finditer(r"\b\d{1,4}(?:\.\d+)?\b", normalized):
            facts.add(f"num:{match.group(0).rstrip('.')}")
        return facts

    def _normalize_fact_number(self, value: str) -> str:
        clean = re.sub(r"\s+", "", str(value or "").strip()).rstrip(".")
        if not clean:
            return ""
        try:
            as_float = float(clean)
        except ValueError:
            return clean
        if as_float.is_integer():
            return str(int(as_float))
        return f"{as_float:.4f}".rstrip("0").rstrip(".")

    def _normalize_fact_unit(self, unit: str) -> str:
        clean = str(unit or "").lower().strip().rstrip(".")
        if clean in {"кг", "килограмм", "килограмма", "килограммов"}:
            return "weight_kg"
        if clean in {"т", "тн", "тонн", "тонна", "тонны"}:
            return "weight_t"
        if clean in {"₽", "руб", "рублей", "рубля", "рубль"}:
            return "money_rub"
        if clean == "%":
            return "percent"
        return clean or "num"

    def _conflicting_answer_facts(self, answer_facts: set[str], source_facts: set[str]) -> Dict[str, List[str]]:
        conflicts: Dict[str, List[str]] = {}
        for fact in answer_facts:
            if ":" not in fact:
                continue
            kind, value = fact.split(":", 1)
            if kind in {"num", "date"}:
                continue
            source_values = sorted({src.split(":", 1)[1] for src in source_facts if src.startswith(f"{kind}:")})
            if value in source_values and len(source_values) > 1:
                conflicts[kind] = source_values
        return conflicts

    def _grounded_fallback_answer(self, verification: Dict[str, Any]) -> str:
        if verification.get("conflicting_facts"):
            return "Нашёл противоречивые данные в источниках. Не могу дать подтверждённый ответ без ручной проверки."
        return "Не нашёл подтверждения этому ответу в найденных фрагментах документов."

    def _source_citation(self, item: Dict[str, Any], source_number: int) -> str:
        label = f"S{source_number}"
        filename = str(item.get("filename") or item.get("path") or "источник")
        parts = [f"[{label}] {filename}"]
        page = item.get("page")
        if page not in (None, ""):
            parts.append(f"стр. {page}")
        sheet = str(item.get("sheet") or "").strip()
        if sheet:
            parts.append(f"лист {sheet}")
        row_start = item.get("row_start")
        row_end = item.get("row_end")
        if row_start not in (None, ""):
            row_label = f"строка {row_start}"
            if row_end not in (None, "") and row_end != row_start:
                row_label += f"-{row_end}"
            parts.append(row_label)
        chunk_index = item.get("chunk_index")
        if chunk_index not in (None, ""):
            parts.append(f"chunk {chunk_index}")
        return " · ".join(parts)

    def answer_fact_question(self, question: str, limit: int = 20) -> Dict[str, Any]:
        """
        Извлечь факт-ответ из документов.

        Основной кейс: "Сколько весит PC300?" -> "3400 кг согласно ПСМ" + ссылка.
        """
        started = time.perf_counter()
        if not self.connected:
            self.telemetry.log_fact(
                source="fact",
                question=question,
                ok=False,
                answer="",
                source_type="",
                value_kg=None,
                duration_ms=0,
                error="not_connected",
            )
            return {"ok": False, "error": "Нет подключения к Qdrant"}

        q = (question or "").strip()
        if not q:
            self.telemetry.log_fact(
                source="fact",
                question=question,
                ok=False,
                answer="",
                source_type="",
                value_kg=None,
                duration_ms=0,
                error="empty_question",
            )
            return {"ok": False, "error": "Пустой вопрос"}

        try:
            candidates = self.search(
                q, limit=limit, file_type=None, content_only=True, source="fact_search"
            )
        except Exception as exc:
            self.telemetry.log_fact(
                source="fact",
                question=q,
                ok=False,
                answer="",
                source_type="",
                value_kg=None,
                duration_ms=int((time.perf_counter() - started) * 1000),
                error=f"fact_search_error: {exc}",
            )
            return {"ok": False, "error": f"Ошибка поиска: {exc}"}

        entities = _extract_entities(q)
        alias_entities = self._discover_entity_aliases(entities)
        entities = list(dict.fromkeys([*entities, *alias_entities]))
        if entities:
            metadata_hits = self._lexical_catalog_search(
                query=" ".join(entities),
                limit=30,
                file_type=None,
                content_only=False,
            )
            paths = [
                str(x.get("full_path") or "")
                for x in metadata_hits
                if x.get("type") == "file_metadata" and x.get("full_path")
            ]
            candidates.extend(self._content_chunks_for_paths(paths[:10], max_chunks=120))
        if not candidates:
            self.telemetry.log_fact(
                source="fact",
                question=q,
                ok=False,
                answer="",
                source_type="",
                value_kg=None,
                duration_ms=int((time.perf_counter() - started) * 1000),
                error="no_candidates",
            )
            return {"ok": False, "error": "Ничего не найдено"}

        ranked = sorted(
            candidates,
            key=lambda item: _answer_rank(item, entities),
            reverse=True,
        )

        for item in ranked:
            text = item.get("text") or ""
            match = _extract_weight(text)
            if not match:
                continue
            source_type = _detect_source_type(item)
            value_kg = match["value_kg"]
            answer = f"{value_kg} кг согласно {source_type}"
            self.telemetry.log_fact(
                source="fact",
                question=q,
                ok=True,
                answer=answer,
                source_type=source_type,
                value_kg=value_kg,
                duration_ms=int((time.perf_counter() - started) * 1000),
                error="",
            )
            return {
                "ok": True,
                "question": q,
                "answer": answer,
                "value_kg": value_kg,
                "source_type": source_type,
                "source": {
                    "filename": item.get("filename", ""),
                    "path": item.get("path", ""),
                    "full_path": item.get("full_path", ""),
                    "text_excerpt": match["line"],
                },
                "search_result": item,
            }

        best = ranked[0]
        out = {
            "ok": False,
            "error": "В найденных документах не удалось извлечь массу/вес",
            "best_source": {
                "filename": best.get("filename", ""),
                "path": best.get("path", ""),
                "full_path": best.get("full_path", ""),
            },
        }
        self.telemetry.log_fact(
            source="fact",
            question=q,
            ok=False,
            answer="",
            source_type="",
            value_kg=None,
            duration_ms=int((time.perf_counter() - started) * 1000),
            error=out["error"],
        )
        return out

    def _discover_entity_aliases(self, entities: List[str]) -> List[str]:
        aliases: List[str] = []
        for entity in entities:
            if not entity:
                continue
            try:
                for item in self.search(entity, limit=20, content_only=False, source="alias_lookup"):
                    bag = " ".join(
                        str(item.get(k, "") or "")
                        for k in ("filename", "path", "text")
                    )
                    if item.get("type") == "file_metadata":
                        bag = f"{bag} {self._read_lightweight_file_text(str(item.get('full_path') or ''))[:3000]}"
                    for m in re.finditer(
                        rf"{re.escape(entity)}[^\d]{{0,12}}\((\d{{4,6}})\)",
                        bag,
                        flags=re.IGNORECASE,
                    ):
                        aliases.append(m.group(1))
                    for m in re.finditer(
                        rf"\b(\d{{4,6}})\b[^\n\r]{{0,25}}{re.escape(entity)}",
                        bag,
                        flags=re.IGNORECASE,
                    ):
                        aliases.append(m.group(1))
            except Exception as exc:
                logger.debug("Alias lookup failed for %s: %s", entity, exc)
        return [x for x in dict.fromkeys(aliases) if x]

    def _read_lightweight_file_text(self, full_path: str) -> str:
        path = Path(full_path)
        if not path.exists() or path.suffix.lower() != ".pdf":
            return ""
        try:
            import pdfplumber  # noqa: PLC0415
            parts: List[str] = []
            with pdfplumber.open(str(path)) as pdf:
                for page in pdf.pages[:3]:
                    parts.append(page.extract_text() or "")
            return "\n".join(parts)
        except Exception as exc:
            logger.debug("Lightweight PDF read failed for %s: %s", full_path, exc)
            return ""


def _extract_entities(query: str) -> List[str]:
    raw = _ENTITY_RE.findall(query or "")
    return [x.lower() for x in raw if x]


def _answer_rank(item: Dict[str, Any], entities: List[str]) -> float:
    score = float(item.get("score", 0.0))
    text = (item.get("text") or "").lower()
    filename = (item.get("filename") or "").lower()
    path = (item.get("path") or "").lower()
    bag = " ".join([filename, path, text[:4000]])

    bonus = 0.0
    for e in entities:
        if e in bag:
            bonus += 1.2
    if "псм" in bag:
        bonus += 1.0
    if "птс" in bag:
        bonus += 0.8
    if "масса" in bag or "вес" in bag:
        bonus += 0.7
    return score + bonus


def _extract_weight(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None

    lines = [x.strip() for x in re.split(r"[\r\n]+", text) if x.strip()]
    weighted_lines = []

    for i, ln in enumerate(lines):
        if _WEIGHT_LINE_RE.search(ln):
            weighted_lines.append(" ".join(lines[i : i + 4]))
    if not weighted_lines:
        return None

    for ln in weighted_lines:
        m = _NUMBER_UNIT_RE.search(ln)
        if m:
            raw_number = m.group(1)
            raw_unit = m.group(2).lower()
        else:
            m = _UNIT_NUMBER_RE.search(ln)
            if not m:
                continue
            raw_unit = m.group(1).lower()
            raw_number = m.group(2)
        value = _parse_number(raw_number)
        if value is None:
            continue
        if raw_unit.startswith("т"):
            value_kg = int(round(value * 1000))
        else:
            value_kg = int(round(value))
        return {"value_kg": value_kg, "line": ln[:240]}
    return None


def _parse_number(raw: str) -> Optional[float]:
    cleaned = (raw or "").replace(" ", "")
    if not cleaned:
        return None
    # 1) десятичный разделитель через запятую
    if "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    # 2) если и точка и запятая — удаляем разделители тысяч
    cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _detect_source_type(item: Dict[str, Any]) -> str:
    bag = " ".join(
        [
            (item.get("filename") or "").lower(),
            (item.get("path") or "").lower(),
            (item.get("text") or "").lower()[:2000],
        ]
    )
    if "псм" in bag:
        return "ПСМ"
    if "птс" in bag:
        return "ПТС"
    if "электронного паспорта" in bag or "выписка из электронного паспорта" in bag:
        return "электронному паспорту"
    return "документа"
