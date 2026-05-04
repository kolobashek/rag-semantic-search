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
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._platform_compat import apply_windows_platform_workarounds
apply_windows_platform_workarounds()

from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchAny, MatchValue
from .telemetry_db import TelemetryDB
# SentenceTransformer импортируется ЛЕНИВО внутри RAGSearcher.embedder.
# НЕ импортировать здесь — import тянет torch (~5 сек, 500+ МБ RAM).

# config.json remains in the project root for backward-compatible launches.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_FILE = PROJECT_ROOT / "config.json"

DEFAULT_CONFIG: Dict[str, Any] = {
    "catalog_path": r"O:\Обмен",
    "qdrant_db_path": r"D:\qdrant_state",
    "qdrant_url": "http://localhost:6333",   # Docker-сервер (приоритет над db_path)
    "log_file": r"O:\rag_automation.log",
    "collection_name": "catalog",
    "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
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
    "telegram_enabled": False,
    "telegram_bot_token": "",
    "telegram_allowed_chat_id": "",
    "telegram_bot_link": "",
    "users_db_path": "",
    "telemetry_db_path": "",
    # ── LLM / Ollama ──────────────────────────────────────────────────────
    "ollama_url": "http://localhost:11434",
    "llm_enabled": False,            # включить RAG Q&A и расширение запроса
    "llm_expand_model": "phi3:mini", # модель для расширения запроса
    "llm_rag_model": "qwen3:8b",     # модель для RAG Q&A
}

logger = logging.getLogger(__name__)
MAX_QUERY_LEN = 2000
FS_CACHE_TTL_SEC = 300
FS_CACHE_MAX_ITEMS = 250_000

_ENTITY_RE = re.compile(r"\b[a-zа-я]*\d+[a-zа-я0-9\-]*\b", re.IGNORECASE)

# Русские морфологические суффиксы для упрощённого стемминга.
# Упорядочены от длиннейших к коротким — применяется первый подходящий.
_RU_SUFFIXES: tuple[str, ...] = (
    # падежные окончания множественного числа
    "ового", "овой", "овых", "овыми", "ового",
    "ного", "ной", "ных", "ными",
    "ского", "ской", "ских", "скими",
    # именительный/родительный/дательный
    "ами", "ями", "ов", "ев", "ей", "ях", "ам", "ям",
    # падежные окончания единственного числа
    "ому", "ему", "ого", "его", "ой", "ей",
    "ом", "ем", "ым", "им",
    # мужской/женский/средний род именительный
    "ый", "ий", "ая", "яя", "ое", "ее",
    # инфинитив и глагольные формы
    "ать", "ять", "ить", "еть", "оть",
    "ается", "яется", "ется", "ится",
    "ающий", "яющий", "ущий", "ющий",
    # существительные
    "ости", "ость", "ение", "ания", "ание",
    "тель", "ник", "ница",
    # короткие падежные окончания — добавляются последними (пробуются только если ничего длиннее не подошло)
    "у", "ю",      # дательный/винительный: паспорту, технику
    "е",           # предложный: экскаваторе
    "а", "я",      # родительный: экскаватора, погрузчика
    "и", "ы",      # родительный ж.р.: техники, системы
)


def _ru_stem(term: str) -> str:
    """Убрать один русский суффикс, если остаток >= 4 символа. Иначе вернуть term."""
    for suffix in _RU_SUFFIXES:
        if term.endswith(suffix) and len(term) - len(suffix) >= 4:
            return term[: -len(suffix)]
    return term


class LexicalScore:
    """Именованные константы скоринга для лексического поиска по ФС.

    Тиры упорядочены по убыванию точности совпадения.
    Изменяйте только здесь — не в теле _lexical_catalog_search.
    """
    FOLDER_PREFIX    = 0.999  # папка, чьё имя начинается с первого термина запроса
    PARENT_PREFIX    = 0.997  # родительская папка файла начинается с первого термина
    FULL_QUERY_IN_NAME = 0.995  # полный нормализованный запрос входит в имя файла
    ALL_TERMS_IN_NAME  = 0.975  # все термины найдены в имени файла
    ALL_TERMS_IN_PATH  = 0.955  # все термины найдены в пути (но не только в имени)
    PARTIAL_BASE       = 0.86   # частичное совпадение; delta добавляется пропорционально
    PARTIAL_MAX_DELTA  = 0.08   # максимальная прибавка к PARTIAL_BASE


# Доменные правила буста для лексического поиска.
# Каждая запись: (trigger_terms, file_keywords, score_floor).
# Если запрос содержит хотя бы один trigger_term, а имя/путь файла содержит file_keyword,
# score поднимается до score_floor (без снижения, если текущий выше).
# Добавляйте новые правила сюда — не меняйте _lexical_catalog_search.
_DOMAIN_BOOST_RULES: List[tuple[frozenset[str], List[str], float]] = [
    (
        frozenset({"паспорт", "паспорта", "паспорты", "псм", "птс", "стс", "техпаспорт"}),
        ["выписка из электронного паспорта", "электронного паспорта"],
        0.998,
    ),
    (
        frozenset({"паспорт", "паспорта", "паспорты", "псм", "птс", "стс", "техпаспорт"}),
        ["псм", "птс"],
        0.996,
    ),
]


# ─────────────────────────── config helpers ────────────────────────────

def load_config() -> Dict[str, Any]:
    """
    Загрузить конфигурацию из config.json.
    Недостающие ключи берутся из DEFAULT_CONFIG.
    """
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
                user_cfg = json.load(fh)
            return {**DEFAULT_CONFIG, **user_cfg}
        except Exception as exc:
            logger.warning("Не удалось загрузить config.json: %s. Используются значения по умолчанию.", exc)
    return dict(DEFAULT_CONFIG)


def save_config(config: Dict[str, Any]) -> None:
    """Сохранить конфигурацию в config.json."""
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as fh:
            json.dump(config, fh, indent=2, ensure_ascii=False)
        logger.info("Конфигурация сохранена: %s", CONFIG_FILE)
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
        self.collection_name = config["collection_name"]
        self.connected = False
        self._embedder: Optional[Any] = None  # SentenceTransformer, загружается лениво
        self._fs_cache: Dict[str, Any] = {"ts": 0.0, "items": []}
        telemetry_path = (config.get("telemetry_db_path") or "").strip()
        if not telemetry_path:
            telemetry_path = str(Path(config["qdrant_db_path"]) / "rag_telemetry.db")
        self.telemetry = TelemetryDB(telemetry_path)

        # Подключение: сервер (Docker) имеет приоритет над локальным SQLite
        qdrant_url = config.get("qdrant_url", "")
        qdrant_path = Path(config["qdrant_db_path"])
        try:
            if qdrant_url:
                self.qdrant = QdrantClient(url=qdrant_url)
                logger.info("Подключено к Qdrant-серверу: %s", qdrant_url)
            else:
                self.qdrant = QdrantClient(path=str(qdrant_path))
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
        query_used = raw_query[:MAX_QUERY_LEN]
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

        must_not_conditions = []
        if content_only:
            must_not_conditions.append(
                FieldCondition(
                    key="type",
                    match=MatchValue(value="file_metadata"),
                )
            )

        # title_only: ограничить поиск только metadata-точками прямо в Qdrant,
        # чтобы limit работал корректно (не терять metadata, находящиеся за пределами top-N).
        if title_only:
            must_conditions.append(
                FieldCondition(
                    key="type",
                    match=MatchAny(any=["file_metadata", "folder_metadata"]),
                )
            )

        if must_conditions or must_not_conditions:
            qdrant_filter = Filter(
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
                }
            )

        lexical_results = self._lexical_catalog_search(
            query=query_used,
            limit=max(limit * 4, 40),
            file_type=file_type,
            content_only=content_only,
            title_only=title_only,
        )
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

    # Паттерн границы слова для русских/латинских символов (включая цифры для артикулов).
    # Используется только для стемминга — точное совпадение проверяется быстрым `in`.
    _WB_TMPL = r"(?<![а-яёa-z0-9\-])%s(?![а-яёa-z0-9\-])"

    def _term_matches(self, haystack: str, term: str) -> bool:
        """Проверить, встречается ли term в haystack как целое слово (с учётом морфологии).

        Стратегия двух шагов:
        1. Быстрый `in` — точное вхождение подстроки (O(n), без regex-оверхеда).
        2. Regex с word-boundary только для стемминга, когда точное вхождение не нашлось.
        """
        if term in haystack:
            return True
        if len(term) >= 5:
            stem = _ru_stem(term)
            if stem != term and len(stem) >= 4:
                # Word-boundary необходим при поиске основы, чтобы "экскаватор"
                # не матчился как подстрока в "экскаваторный" без учёта контекста.
                return bool(re.search(self._WB_TMPL % re.escape(stem), haystack, re.IGNORECASE))
        return False

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
        # Определяем, какие доменные правила активны для данного запроса.
        active_boost_rules = [
            (keywords, score_floor)
            for (trigger_terms, keywords, score_floor) in _DOMAIN_BOOST_RULES
            if any(t in trigger_terms for t in raw_terms)
            or any(
                any(t in trigger_terms for t in re.findall(r"[а-яёa-z]{3,}", label, re.IGNORECASE))
                for label in alias_groups
            )
        ]
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
            first_stem = _ru_stem(first_term) if len(first_term) >= 5 else first_term
            if is_folder and first_stem and name.startswith(first_stem):
                score = LexicalScore.FOLDER_PREFIX
            elif first_stem and parent_name.startswith(first_stem):
                score = LexicalScore.PARENT_PREFIX
            elif query_norm and query_norm in name:
                score = LexicalScore.FULL_QUERY_IN_NAME
            elif all(self._term_matches(name, t) for t in terms):
                score = LexicalScore.ALL_TERMS_IN_NAME
            elif all(self._term_matches(path, t) for t in terms):
                score = LexicalScore.ALL_TERMS_IN_PATH
            else:
                score = LexicalScore.PARTIAL_BASE + min(
                    LexicalScore.PARTIAL_MAX_DELTA,
                    matched / max(1, len(terms)) * LexicalScore.PARTIAL_MAX_DELTA,
                )
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
            if not is_folder and active_boost_rules:
                for keywords, score_floor in active_boost_rules:
                    if any(kw in hay for kw in keywords):
                        score = max(score, score_floor)
                        break
            if not is_folder and ("документы на технику" in hay or "док-ты техника" in hay):
                score = min(0.94, score + 0.04)
            out.append({
                "score": round(score, 3),
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
        out.sort(key=lambda x: (float(x.get("score") or 0), -len(str(x.get("path") or ""))), reverse=True)
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
        for item in merged.values():
            path = str(item.get("full_path") or "")
            signal = int(feedback.get(path, 0))
            if not signal:
                item["feedback_score"] = 0
                item["rank_score"] = float(item.get("score") or 0)
                continue
            base_score = float(item.get("score") or 0)
            adjustment = max(-0.18, min(0.18, signal * 0.02))
            item["feedback_score"] = signal
            item["rank_score"] = max(0.0, min(1.0, base_score + adjustment))
        ranked = sorted(
            merged.values(),
            key=lambda x: (
                float(x.get("rank_score", x.get("score") or 0) or 0),
                1 if x.get("type") == "folder_metadata" else 0,
            ),
            reverse=True,
        )
        folder_cap = max(3, min(5, limit // 2))
        balanced: List[Dict[str, Any]] = []
        deferred_folders: List[Dict[str, Any]] = []
        folder_count = 0
        for item in ranked:
            if item.get("type") == "folder_metadata" and folder_count >= folder_cap:
                deferred_folders.append(item)
                continue
            if item.get("type") == "folder_metadata":
                folder_count += 1
            balanced.append(item)
            if len(balanced) >= limit:
                break
        if len(balanced) < limit:
            balanced.extend(deferred_folders[: limit - len(balanced)])
        return balanced[:limit]

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

    def answer_fact_question(self, question: str, limit: int = 20) -> Dict[str, Any]:
        """Ответить на произвольный фактологический вопрос по документам каталога.

        Использует LLM (Ollama) для извлечения ответа из найденных чанков —
        работает для любых параметров: масса, возраст, дата, номер, стоимость и т.д.

        Если llm_enabled=False в конфиге — возвращает топ-1 фрагмент без интерпретации.
        """
        started = time.perf_counter()
        if not self.connected:
            self.telemetry.log_fact(
                source="fact", question=question, ok=False,
                answer="", source_type="", value_kg=None, duration_ms=0,
                error="not_connected",
            )
            return {"ok": False, "error": "Нет подключения к Qdrant"}

        q = (question or "").strip()
        if not q:
            self.telemetry.log_fact(
                source="fact", question=question, ok=False,
                answer="", source_type="", value_kg=None, duration_ms=0,
                error="empty_question",
            )
            return {"ok": False, "error": "Пустой вопрос"}

        # ── 1. Поиск кандидатов ───────────────────────────────────────
        try:
            candidates = self.search(
                q, limit=limit, file_type=None, content_only=True, source="fact_search"
            )
        except Exception as exc:
            self.telemetry.log_fact(
                source="fact", question=q, ok=False, answer="", source_type="",
                value_kg=None, duration_ms=int((time.perf_counter() - started) * 1000),
                error=f"fact_search_error: {exc}",
            )
            return {"ok": False, "error": f"Ошибка поиска: {exc}"}

        # ── 2. Расширяем кандидатами по сущностям и алиасам ─────────
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
                source="fact", question=q, ok=False, answer="", source_type="",
                value_kg=None, duration_ms=int((time.perf_counter() - started) * 1000),
                error="no_candidates",
            )
            return {"ok": False, "error": "Ничего не найдено"}

        # ── 3. Ранжируем по релевантности к сущностям запроса ────────
        ranked = sorted(
            candidates,
            key=lambda item: _answer_rank(item, entities),
            reverse=True,
        )

        # ── 4. Генерируем ответ ──────────────────────────────────────
        llm_enabled = bool(self.config.get("llm_enabled"))
        if llm_enabled:
            from .llm import rag_answer  # noqa: PLC0415 — ленивый импорт
            try:
                answer = rag_answer(
                    q,
                    ranked,
                    model=str(self.config.get("llm_rag_model") or "qwen3:8b"),
                    ollama_url=str(self.config.get("ollama_url") or "http://localhost:11434"),
                    top_k=6,
                    max_chars_per_chunk=1200,
                    timeout=120,
                )
                source_type = "llm"
            except Exception as exc:
                logger.error("answer_fact_question LLM failed: %s", exc)
                answer = ""
                source_type = "error"
        else:
            # Fallback без LLM: отдаём лучший фрагмент напрямую
            best_text = (ranked[0].get("text") or "")[:800]
            answer = best_text if best_text else "LLM не включён; текстовые фрагменты найдены, но интерпретация недоступна."
            source_type = "no_llm"

        ok = bool(answer and source_type != "error")
        sources = [
            {
                "filename": r.get("filename", ""),
                "path": r.get("path", ""),
                "full_path": r.get("full_path", ""),
            }
            for r in ranked[:5]
            if r.get("full_path")
        ]
        # Убираем дубли по full_path
        seen_paths: set[str] = set()
        unique_sources = []
        for s in sources:
            fp = s["full_path"]
            if fp not in seen_paths:
                seen_paths.add(fp)
                unique_sources.append(s)

        self.telemetry.log_fact(
            source="fact", question=q, ok=ok, answer=answer[:500], source_type=source_type,
            value_kg=None, duration_ms=int((time.perf_counter() - started) * 1000),
            error="" if ok else source_type,
        )
        return {
            "ok": ok,
            "question": q,
            "answer": answer,
            "source_type": source_type,
            "sources": unique_sources,
            # value_kg оставлен для обратной совместимости (Telegram-бот и др.)
            "value_kg": None,
        }

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
    """Ранговый балл для fact-поиска.

    Первичный критерий — насколько полно сущности запроса (артикулы, модели, имена)
    представлены в тексте/пути документа. Вторичный — семантический score из Qdrant.
    Нет domain-специфичных бонусов: функция работает для любых параметров и сущностей.
    """
    score = float(item.get("rank_score") or item.get("score") or 0.0)
    text = (item.get("text") or "").lower()
    filename = (item.get("filename") or "").lower()
    path = (item.get("path") or "").lower()
    bag = " ".join([filename, path, text[:4000]])

    entity_bonus = 0.0
    n_entities = max(len(entities), 1)
    for e in entities:
        if e in bag:
            entity_bonus += 1.0 / n_entities  # нормализуем: сумма бонусов ≤ 1.0

    # entity_bonus * 10 → первичный ключ; score → тай-брейкер
    return entity_bonus * 10.0 + score
