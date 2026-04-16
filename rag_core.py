"""
rag_core.py — Общее ядро RAG-системы.

Предоставляет:
  - load_config() / save_config()  — загрузка/сохранение config.json
  - RAGSearcher                    — единый класс семантического поиска
                                     (используется app_ui, windows_app, rag_search_fixed)
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Workaround: зависание при импорте torch на Windows ───────────────────────
import platform as _p
# Python 3.14: _wmi_query зависает
if hasattr(_p, '_wmi_query'):
    _p._wmi_query = lambda *a, **kw: ('10.0.19041', '1', 'Multiprocessor Free', '0', '0')
# Python 3.11+: platform.processor() может зависать при ограниченном WMI
_p.processor = lambda: 'Intel64 Family 6 Model 165 Stepping 2, GenuineIntel'
# ─────────────────────────────────────────────────────────────────────────────

from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue
# SentenceTransformer импортируется ЛЕНИВО внутри RAGSearcher.embedder.
# НЕ импортировать здесь — import тянет torch (~5 сек, 500+ МБ RAM).

# config.json лежит рядом с этим файлом
CONFIG_FILE = Path(__file__).parent / "config.json"

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
}

logger = logging.getLogger(__name__)


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
        """Ленивая загрузка модели эмбеддинга (импорт sentence_transformers при первом запросе)."""
        if self._embedder is None:
            from sentence_transformers import SentenceTransformer  # noqa: PLC0415
            model_name = self.config["embedding_model"]
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

        Returns:
            Список словарей с ключами:
            score, type, text, filename, path, full_path, size_mb, modified, extension.
        """
        if not self.connected:
            return []

        query_vector = self.embedder.encode(query, normalize_embeddings=True).tolist()

        # ── Строим фильтр Qdrant ───────────────────────────────────────
        must_conditions = []

        if content_only:
            # Исключаем записи с type == "file_metadata"
            # В qdrant-client 1.10+ MatchExcept убран — используем must_not
            pass  # обрабатывается через must_not ниже

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
            return []

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
                }
            )

        return results

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
