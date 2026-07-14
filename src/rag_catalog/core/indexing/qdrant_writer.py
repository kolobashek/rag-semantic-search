"""Qdrant write helpers for indexers.

This module keeps collection lifecycle and point deletion/upsert mechanics out
of the high-level indexer pipeline. It is intentionally small and synchronous:
batching, embedding, and state updates remain owned by the caller.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    FilterSelector,
    MatchValue,
    PayloadSchemaType,
    PointStruct,
    Snowball,
    SnowballLanguage,
    SnowballParams,
    TextIndexParams,
    TextIndexType,
    TokenizerType,
    VectorParams,
)

logger = logging.getLogger(__name__)


def _is_payload_too_large_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "larger than allowed" in message
        or "payload too large" in message
        or "request entity too large" in message
        or "status code 413" in message
        or "413 (payload too large)" in message
    )


def ensure_collection(
    client: Any,
    *,
    collection_name: str,
    vector_size: int,
    recreate: bool = False,
    fulltext_enabled: bool = False,
) -> bool:
    """Ensure the target collection exists.

    Returns `True` when the collection was recreated and state should be cleared
    by the caller.
    """
    existing = [c.name for c in client.get_collections().collections]
    if collection_name in existing:
        if recreate:
            logger.info("Пересоздание коллекции %s…", collection_name)
            client.delete_collection(collection_name)
            create_collection(
                client,
                collection_name=collection_name,
                vector_size=vector_size,
                fulltext_enabled=fulltext_enabled,
            )
            return True
        logger.info("Коллекция %s уже существует.", collection_name)
        ensure_payload_indexes(
            client,
            collection_name=collection_name,
            fulltext_enabled=fulltext_enabled,
        )
        return False

    create_collection(
        client,
        collection_name=collection_name,
        vector_size=vector_size,
        fulltext_enabled=fulltext_enabled,
    )
    return False


def create_collection(
    client: Any,
    *,
    collection_name: str,
    vector_size: int,
    fulltext_enabled: bool = False,
) -> None:
    logger.info("Создание коллекции %s…", collection_name)
    client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
    )
    ensure_payload_indexes(
        client,
        collection_name=collection_name,
        fulltext_enabled=fulltext_enabled,
    )


def ensure_payload_indexes(
    client: Any,
    *,
    collection_name: str,
    fulltext_enabled: bool = False,
) -> None:
    """Best-effort payload indexes for filters and Russian full-text retrieval."""
    for field_name in ("numeric_tokens", "type", "extension", "full_path"):
        try:
            client.create_payload_index(
                collection_name=collection_name,
                field_name=field_name,
                field_schema=PayloadSchemaType.KEYWORD,
                wait=False,
            )
        except Exception as exc:
            message = str(exc).lower()
            if "already exists" not in message and "exists" not in message:
                logger.debug("Не удалось создать payload index %s: %s", field_name, exc)

    if not fulltext_enabled:
        return
    try:
        client.create_payload_index(
            collection_name=collection_name,
            field_name="text",
            field_schema=TextIndexParams(
                type=TextIndexType.TEXT,
                tokenizer=TokenizerType.WORD,
                min_token_len=2,
                max_token_len=64,
                lowercase=True,
                phrase_matching=True,
                on_disk=True,
                stemmer=SnowballParams(
                    type=Snowball.SNOWBALL,
                    language=SnowballLanguage.RUSSIAN,
                ),
            ),
            wait=False,
        )
    except Exception as exc:
        message = str(exc).lower()
        if "already exists" not in message and "exists" not in message:
            logger.warning("Не удалось создать полнотекстовый index text: %s", exc)


def delete_file_vectors(
    client: Any,
    *,
    collection_name: str,
    filepath: Path,
    timeout_sec: int,
    payload_match: Mapping[str, Any] | None = None,
    retries: int = 2,
) -> None:
    """Delete vectors by explicit payload identity or by `full_path` fallback."""
    must = []
    if payload_match:
        for key, value in payload_match.items():
            if value not in (None, ""):
                must.append(FieldCondition(key=str(key), match=MatchValue(value=value)))
    if not must:
        must.append(FieldCondition(key="full_path", match=MatchValue(value=str(filepath))))

    last_error: Exception | None = None
    for attempt in range(max(1, int(retries) + 1)):
        try:
            client.delete(
                collection_name=collection_name,
                wait=False,
                timeout=timeout_sec,
                points_selector=FilterSelector(filter=Filter(must=must)),
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt >= int(retries):
                break
            delay = min(5.0, 0.75 * (attempt + 1))
            logger.warning(
                "Qdrant delete timeout/error, retry %d/%d in %.1fs: %s",
                attempt + 1,
                int(retries),
                delay,
                exc,
            )
            time.sleep(delay)
    if last_error is not None:
        raise last_error


def upsert_points(
    client: Any,
    *,
    collection_name: str,
    points: Sequence[PointStruct],
    timeout_sec: int = 60,
    retries: int = 2,
) -> int:
    """Upsert points, splitting batches that exceed Qdrant's HTTP body limit."""
    if not points:
        return 0
    timeout = max(5, int(timeout_sec or 60))

    def write_batch(prepared: list[PointStruct]) -> int:
        last_error: Exception | None = None
        for attempt in range(max(1, int(retries) + 1)):
            try:
                try:
                    client.upsert(
                        collection_name,
                        points=prepared,
                        wait=False,
                        timeout=timeout,
                    )
                except TypeError as type_error:
                    if "unexpected keyword" not in str(type_error):
                        raise
                    client.upsert(collection_name, points=prepared)
                return len(prepared)
            except Exception as exc:
                last_error = exc
                if _is_payload_too_large_error(exc):
                    if len(prepared) <= 1:
                        raise RuntimeError(
                            "Одна точка Qdrant превышает допустимый размер payload; "
                            "содержимое документа нужно сократить перед индексацией."
                        ) from exc
                    midpoint = len(prepared) // 2
                    logger.warning(
                        "Qdrant batch из %d точек превышает лимит payload; делю на %d + %d",
                        len(prepared),
                        midpoint,
                        len(prepared) - midpoint,
                    )
                    return write_batch(prepared[:midpoint]) + write_batch(prepared[midpoint:])
                if attempt >= int(retries):
                    break
                delay = min(5.0, 0.75 * (attempt + 1))
                logger.warning(
                    "Qdrant upsert timeout/error, retry %d/%d in %.1fs: %s",
                    attempt + 1,
                    int(retries),
                    delay,
                    exc,
                )
                time.sleep(delay)
        if last_error is not None:
            raise last_error
        return len(prepared)

    return write_batch(list(points))
