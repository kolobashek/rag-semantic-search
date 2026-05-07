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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._platform_compat import apply_windows_platform_workarounds

apply_windows_platform_workarounds()

from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchAny, MatchValue

from .embedding_collections import resolve_collection_name_from_config
from .index_state_db import IndexStateDB
from .retrieval import bm25_rank_items, rrf_fuse
from .telemetry_db import TelemetryDB

# SentenceTransformer импортируется ЛЕНИВО внутри RAGSearcher.embedder.
# НЕ импортировать здесь — import тянет torch (~5 сек, 500+ МБ RAM).

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _resolve_config_file() -> Path:
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
    "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
    "embedding_collection_versioning": False,
    "embedding_collection_suffix": "",
    "vector_size": 384,
    "chunk_size": 500,
    "chunk_overlap": 100,
    "batch_size": 1000,
    "index_read_workers": 4,
    "index_max_chunks": 2000,
    "index_skip_ocr": False,
    "index_default_stage": "all",
    "small_office_mb": 20.0,
    "small_pdf_mb": 2.0,
    "qdrant_timeout_sec": 60,
    "qdrant_scroll_limit": 256,
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
    "retrieval_pipeline": "legacy",  # legacy|v2
    "retrieval_dense_top_k": 50,
    "retrieval_lexical_top_k": 50,
    "retrieval_bm25_enabled": True,
    "retrieval_bm25_top_k": 50,
    "retrieval_final_top_k": 10,
    "retrieval_reranker_enabled": False,
    "retrieval_reranker_model": "",
    "retrieval_reranker_top_n": 30,
    "retrieval_reranker_weight": 0.65,
}

logger = logging.getLogger(__name__)
MAX_QUERY_LEN = 2000
FS_CACHE_TTL_SEC = 300
FS_CACHE_MAX_ITEMS = 250_000

_ENTITY_RE = re.compile(r"\b[a-zа-я]*\d+[a-zа-я0-9\-]*\b", re.IGNORECASE)
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
            return {**DEFAULT_CONFIG, **user_cfg}
        except Exception as exc:
            logger.warning("Не удалось загрузить config.json: %s. Используются значения по умолчанию.", exc)
    return dict(DEFAULT_CONFIG)


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
        self.config = config
        self.collection_name = resolve_collection_name_from_config(config)
        self.connected = False
        self._embedder: Optional[Any] = None  # SentenceTransformer, загружается лениво
        self._reranker: Optional[Any] = None  # CrossEncoder, загружается лениво
        self._fs_cache: Dict[str, Any] = {"ts": 0.0, "items": []}
        telemetry_path = (config.get("telemetry_db_path") or "").strip()
        if not telemetry_path:
            telemetry_path = str(Path(config["qdrant_db_path"]) / "rag_telemetry.db")
        self.telemetry = TelemetryDB(telemetry_path)

        # Подключение: сервер (Docker) имеет приоритет над локальным SQLite
        qdrant_url = config.get("qdrant_url", "")
        qdrant_path = Path(config["qdrant_db_path"])
        qdrant_timeout = int(config.get("qdrant_timeout_sec", 60) or 60)
        try:
            if qdrant_url:
                self.qdrant = QdrantClient(url=qdrant_url, timeout=qdrant_timeout)
                logger.info("Подключено к Qdrant-серверу: %s", qdrant_url)
            else:
                self.qdrant = QdrantClient(path=str(qdrant_path), timeout=qdrant_timeout)
                logger.info("Подключено к Qdrant локально: %s", qdrant_path)
            self.qdrant.get_collection(self.collection_name)
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
                logger.info("Загрузка модели эмбеддинга: %s", model_name)
                self._embedder = SentenceTransformer(model_name)
        return self._embedder

    @property
    def reranker(self) -> Optional[Any]:
        """Lazy CrossEncoder reranker for retrieval v2, enabled by config only."""
        model_name = str(self.config.get("retrieval_reranker_model") or "").strip()
        if not model_name:
            return None
        if getattr(self, "_reranker", None) is None:
            from sentence_transformers import CrossEncoder  # noqa: PLC0415

            logger.info("Загрузка reranker-модели: %s", model_name)
            self._reranker = CrossEncoder(model_name)
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
        query_used = self._expand_query_for_search(raw_query[:MAX_QUERY_LEN])[:MAX_QUERY_LEN]
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

        try:
            query_vector = self.embedder.encode(
                query_used, normalize_embeddings=True
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
                    "extension": payload.get("extension", ""),
                    "chunk_index": payload.get("chunk_index"),
                    "cloud_file_id": payload.get("cloud_file_id", ""),
                    "cloud_version_id": payload.get("cloud_version_id", ""),
                    "cloud_path": payload.get("cloud_path", ""),
                    "storage_key": payload.get("storage_key", ""),
                }
            )
        if title_only:
            results = [item for item in results if str(item.get("type") or "") in metadata_types]

        lexical_results = self._lexical_catalog_search(
            query=query_used,
            limit=max(limit * 4, 40),
            file_type=file_type,
            content_only=content_only,
            title_only=title_only,
        )
        if str(self.config.get("retrieval_pipeline") or "legacy").lower() == "v2":
            rerank_top_n = max(limit, int(self.config.get("retrieval_reranker_top_n", max(limit * 3, 30)) or limit))
            bm25_results = self._bm25_catalog_search(
                query=query_used,
                limit=int(self.config.get("retrieval_bm25_top_k", max(limit * 4, 40)) or max(limit * 4, 40)),
                file_type=file_type,
                content_only=content_only,
                title_only=title_only,
            )
            channels = [lexical_results, bm25_results, results]
            fused = rrf_fuse(channels, limit=max(limit * 4, 40))
            results = self._merge_ranked_results([], fused, limit=rerank_top_n, query=query_used)
            results = self._rerank_results(query_used, results, limit=limit)
        else:
            results = self._merge_ranked_results(lexical_results, results, limit=limit, query=query_used)

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
        if term in haystack:
            return True
        if len(term) >= 5:
            stem = term.rstrip("аеиоуыьъйяю")
            return len(stem) >= 4 and stem in haystack
        return False

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
        root = Path(config.get("catalog_path", ""))
        if not root.exists():
            self._fs_cache = {"ts": now, "items": []}
            return []

        items: List[Dict[str, Any]] = []
        max_items = int(config.get("filesystem_search_max_items", FS_CACHE_MAX_ITEMS))
        qdrant_db_path = str(config.get("qdrant_db_path") or "").strip()
        state_db_path = Path(qdrant_db_path) / "index_state.db" if qdrant_db_path else Path()
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
        candidates: List[Dict[str, Any]] = []
        for item in self._refresh_fs_cache():
            if file_type and item.get("kind") == "folder":
                continue
            if file_type and str(item.get("extension") or "").lower() != file_type.lower():
                continue
            candidates.append(item)

        ranked = bm25_rank_items(candidates, terms, limit=limit)
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
                }
            )
        return out

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
        entity_terms = _extract_entities(query)
        query_norm = " ".join(terms)
        out: List[Dict[str, Any]] = []
        for item in self._refresh_fs_cache():
            if file_type and item.get("kind") == "file" and item.get("extension") != file_type.lower():
                continue
            if file_type and item.get("kind") == "folder":
                continue
            name = str(item.get("filename") or "").lower().replace("ё", "е")
            path = str(item.get("path") or "").lower().replace("ё", "е")
            hay = f"{name} {path}"
            path_parts = [p for p in re.split(r"[\\/]+", path) if p]
            parent_name = path_parts[-2] if len(path_parts) >= 2 else ""
            if entity_terms and not any(self._term_matches(hay, e) for e in entity_terms):
                continue
            matched = sum(1 for t in terms if self._term_matches(hay, t))
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
            elif all(self._term_matches(name, t) for t in terms):
                score = 0.975
            elif all(self._term_matches(path, t) for t in terms):
                score = 0.955
            else:
                score = 0.86 + min(0.08, matched / max(1, len(terms)) * 0.08)
            raw_matched = 0
            if len(raw_terms) > 1:
                raw_matched = sum(1 for t in raw_terms if self._term_matches(hay, t))
                if raw_matched < len(raw_terms):
                    score = min(score, 0.91 + min(0.04, raw_matched / max(1, len(raw_terms)) * 0.04))
            if alias_groups:
                for label in alias_groups:
                    label_terms = [
                        t
                        for t in re.findall(r"[a-zа-яё0-9\-]{2,}", label, flags=re.IGNORECASE)
                        if t not in {"и", "или", "по", "на", "в", "во", "от", "для"}
                    ]
                    if (not raw_terms or raw_matched > 0) and label_terms and all(self._term_matches(hay, t) for t in label_terms):
                        score = max(score, 0.972)
                        break
            if (not raw_terms or raw_matched > 0) and alias_phrases and any(phrase and phrase in hay for phrase in alias_phrases):
                score = max(score, 0.965)
            if wants_machine_passport and not is_folder and (
                "выписка из электронного паспорта" in hay
                or "электронного паспорта" in hay
            ):
                score = max(score, 0.998)
            elif wants_machine_passport and not is_folder and ("псм" in hay or "птс" in hay):
                score = max(score, 0.996)
            elif not is_folder and ("документы на технику" in hay or "док-ты техника" in hay):
                score = min(0.94, score + 0.04)
            if query_norm and query_norm in name:
                score += 0.0025
            elif query_norm and query_norm in path:
                score += 0.0015
            if raw_terms:
                exact_terms_in_name = sum(1 for t in raw_terms if t and self._term_matches(name, t))
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
                    "extension": payload.get("extension", ""),
                    "chunk_index": payload.get("chunk_index"),
                    "cloud_file_id": payload.get("cloud_file_id", ""),
                    "cloud_version_id": payload.get("cloud_version_id", ""),
                    "cloud_path": payload.get("cloud_path", ""),
                    "storage_key": payload.get("storage_key", ""),
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
        self._log_rag_answer(q, answer, ok, started, error)
        return {
            "ok": ok,
            "question": q,
            "answer": answer or "Модель не дала ответа.",
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
            sources.append(
                {
                    "filename": item.get("filename", ""),
                    "path": item.get("path", ""),
                    "full_path": item.get("full_path", ""),
                    "chunk_index": item.get("chunk_index"),
                    "score": item.get("rank_score", item.get("score")),
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
        if missing:
            return {
                "ok": False,
                "checked_facts": sorted(answer_facts),
                "missing_facts": missing,
                "error": "unsupported_facts",
            }
        return {"ok": True, "checked_facts": sorted(answer_facts), "missing_facts": []}

    def _extract_verifiable_facts(self, text: str) -> set[str]:
        facts: set[str] = set()
        normalized = str(text or "").lower().replace(",", ".")
        for match in re.finditer(r"\b\d{1,4}(?:\.\d+)?\b", normalized):
            facts.add(match.group(0).rstrip("."))
        for match in re.finditer(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b", normalized):
            facts.add(match.group(0).replace("/", ".").replace("-", "."))
        return facts

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
