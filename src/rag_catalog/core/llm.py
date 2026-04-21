"""
llm.py — Интеграция с Ollama для расширенных функций RAG.

Предоставляет:
  - expand_query()   — расширение поискового запроса через LLM
  - rag_answer()     — генерация ответа по найденным документам (RAG Q&A)
  - OllamaEmbedder   — эмбеддер через Ollama API (nomic-embed-text и др.)

Все вызовы синхронные; для UI использовать в io_bound / threadpool.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_OLLAMA_URL = "http://localhost:11434"
DEFAULT_EXPAND_MODEL = "phi3:mini"
DEFAULT_RAG_MODEL = "qwen3:8b"
DEFAULT_EMBED_MODEL = "nomic-embed-text"
NOMIC_VECTOR_SIZE = 768


# ─────────────────────────── HTTP helper ───────────────────────────────────

def _ollama_post(url: str, payload: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    """POST-запрос к Ollama API. Возвращает распарсенный JSON."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise ConnectionError(f"Ollama недоступен ({url}): {exc}") from exc


# ─────────────────────────── query expansion ───────────────────────────────

_EXPAND_SYSTEM = (
    "Ты — ассистент для улучшения поисковых запросов. "
    "Пользователь ищет документы в корпоративном архиве (договоры, паспорта техники, "
    "реестры, акты, счета, накладные). "
    "Расширь запрос: добавь синонимы, раскрой аббревиатуры, исправь опечатки. "
    "Верни ТОЛЬКО расширенный запрос — одной строкой, без пояснений и кавычек."
)


def expand_query(
    query: str,
    *,
    model: str = DEFAULT_EXPAND_MODEL,
    ollama_url: str = DEFAULT_OLLAMA_URL,
    timeout: int = 15,
) -> str:
    """
    Расширить поисковый запрос через LLM.

    Возвращает расширенный запрос или исходный query при любой ошибке.
    """
    if not query.strip():
        return query
    prompt = f"{_EXPAND_SYSTEM}\n\nЗапрос: {query}\nРасширенный запрос:"
    try:
        t0 = time.perf_counter()
        result = _ollama_post(
            f"{ollama_url}/api/generate",
            {"model": model, "prompt": prompt, "stream": False, "options": {"temperature": 0.2, "num_predict": 120}},
            timeout=timeout,
        )
        expanded = str(result.get("response") or "").strip().splitlines()[0].strip()
        elapsed = int((time.perf_counter() - t0) * 1000)
        logger.debug("expand_query (%dms): '%s' → '%s'", elapsed, query, expanded)
        if expanded and expanded.lower() != query.lower():
            return expanded
    except Exception as exc:
        logger.warning("expand_query failed (%s): %s", model, exc)
    return query


# ─────────────────────────── RAG answer ────────────────────────────────────

_RAG_SYSTEM = (
    "Ты — корпоративный ассистент. Отвечай строго на основе предоставленных фрагментов документов. "
    "Если ответа в документах нет — скажи об этом честно. "
    "Отвечай на русском языке, кратко и по делу. "
    "В конце укажи источники (имена файлов) в формате «Источники: файл1, файл2»."
)

_RAG_PROMPT_TMPL = """\
Вопрос: {query}

Фрагменты из документов:
{context}

Ответ:"""


def rag_answer(
    query: str,
    results: List[Dict[str, Any]],
    *,
    model: str = DEFAULT_RAG_MODEL,
    ollama_url: str = DEFAULT_OLLAMA_URL,
    top_k: int = 5,
    max_chars_per_chunk: int = 800,
    timeout: int = 90,
) -> str:
    """
    Сгенерировать ответ на вопрос по найденным документам.

    Args:
        query:   Исходный запрос пользователя.
        results: Список результатов из RAGSearcher.search() (отсортированных по score).
        model:   Ollama-модель для генерации.
        top_k:   Сколько лучших результатов передать в контекст.

    Returns:
        Текст ответа или сообщение об ошибке.
    """
    if not results:
        return "Документы не найдены — нет данных для ответа."

    # Собираем контекст из top_k уникальных файлов
    seen_files: set[str] = set()
    context_parts: List[str] = []
    for r in results[:top_k * 2]:  # берём с запасом на дубли
        fname = str(r.get("filename") or r.get("path") or "")
        text = str(r.get("text") or "").strip()
        if not text or fname in seen_files:
            continue
        seen_files.add(fname)
        excerpt = text[:max_chars_per_chunk]
        context_parts.append(f"[{fname}]\n{excerpt}")
        if len(context_parts) >= top_k:
            break

    if not context_parts:
        return "В найденных документах нет текстового содержимого для анализа."

    context = "\n\n".join(context_parts)
    prompt = f"{_RAG_SYSTEM}\n\n{_RAG_PROMPT_TMPL.format(query=query, context=context)}"

    try:
        t0 = time.perf_counter()
        result = _ollama_post(
            f"{ollama_url}/api/generate",
            {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 512},
            },
            timeout=timeout,
        )
        answer = str(result.get("response") or "").strip()
        elapsed = int((time.perf_counter() - t0) * 1000)
        logger.info("rag_answer (%dms, %d docs, model=%s)", elapsed, len(context_parts), model)
        return answer or "Модель не дала ответа."
    except ConnectionError as exc:
        logger.error("rag_answer: Ollama недоступен: %s", exc)
        return f"Ollama недоступен: {exc}"
    except Exception as exc:
        logger.error("rag_answer failed: %s", exc)
        return f"Ошибка генерации: {exc}"


# ─────────────────────────── OllamaEmbedder ────────────────────────────────

class OllamaEmbedder:
    """
    Эмбеддер с интерфейсом, совместимым с SentenceTransformer.

    Использует Ollama /api/embeddings endpoint.
    По умолчанию — nomic-embed-text (768 измерений).

    Пример использования:
        embedder = OllamaEmbedder("nomic-embed-text")
        vec = embedder.encode("текст запроса", normalize_embeddings=True)
    """

    def __init__(
        self,
        model: str = DEFAULT_EMBED_MODEL,
        ollama_url: str = DEFAULT_OLLAMA_URL,
        timeout: int = 30,
    ) -> None:
        self.model = model
        self.ollama_url = ollama_url
        self.timeout = timeout
        # Проверяем доступность при инициализации
        logger.info("OllamaEmbedder: модель=%s, url=%s", model, ollama_url)

    def _embed_one(self, text: str) -> List[float]:
        result = _ollama_post(
            f"{self.ollama_url}/api/embeddings",
            {"model": self.model, "prompt": text},
            timeout=self.timeout,
        )
        vec = result.get("embedding")
        if not vec:
            raise ValueError(f"Ollama не вернул embedding для модели {self.model}")
        return vec

    def encode(
        self,
        texts: Any,
        normalize_embeddings: bool = True,
        batch_size: int = 32,
        show_progress_bar: bool = False,
        **kwargs: Any,
    ) -> np.ndarray:
        """
        Совместимый с SentenceTransformer метод encode.

        texts: str или List[str]
        Возвращает np.ndarray shape (dim,) для строки или (N, dim) для списка.
        """
        single = isinstance(texts, str)
        items: List[str] = [texts] if single else list(texts)

        vecs: List[List[float]] = []
        for i, text in enumerate(items):
            try:
                vec = self._embed_one(text)
            except Exception as exc:
                logger.warning("OllamaEmbedder: ошибка на тексте %d: %s", i, exc)
                # Заглушка нулевым вектором при ошибке
                dim = len(vecs[0]) if vecs else NOMIC_VECTOR_SIZE
                vec = [0.0] * dim
            vecs.append(vec)

        arr = np.array(vecs, dtype="float32")

        if normalize_embeddings:
            norms = np.linalg.norm(arr, axis=-1, keepdims=True)
            norms = np.where(norms == 0, 1.0, norms)
            arr = arr / norms

        return arr[0] if single else arr
