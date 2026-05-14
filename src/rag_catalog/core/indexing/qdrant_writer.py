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
    PointStruct,
    VectorParams,
)

logger = logging.getLogger(__name__)


def ensure_collection(
    client: Any,
    *,
    collection_name: str,
    vector_size: int,
    recreate: bool = False,
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
            create_collection(client, collection_name=collection_name, vector_size=vector_size)
            return True
        logger.info("Коллекция %s уже существует.", collection_name)
        return False

    create_collection(client, collection_name=collection_name, vector_size=vector_size)
    return False


def create_collection(client: Any, *, collection_name: str, vector_size: int) -> None:
    logger.info("Создание коллекции %s…", collection_name)
    client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
    )


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
    """Upsert a non-empty point sequence and return the number of points written."""
    if not points:
        return 0
    prepared = list(points)
    last_error: Exception | None = None
    for attempt in range(max(1, int(retries) + 1)):
        try:
            try:
                client.upsert(collection_name, points=prepared, wait=False, timeout=max(5, int(timeout_sec or 60)))
            except TypeError as type_error:
                if "unexpected keyword" not in str(type_error):
                    raise
                client.upsert(collection_name, points=prepared)
            return len(prepared)
        except Exception as exc:
            last_error = exc
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
    return len(points)
