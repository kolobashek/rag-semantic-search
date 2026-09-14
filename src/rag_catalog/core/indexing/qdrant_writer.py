"""Qdrant write helpers for indexers.

This module keeps collection lifecycle and point deletion/upsert mechanics out
of the high-level indexer pipeline. It is intentionally small and synchronous:
batching, embedding, and state updates remain owned by the caller.
"""

from __future__ import annotations

import json
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

_DEFAULT_MAX_UPSERT_BODY_BYTES = 28 * 1024 * 1024

# Обрыв соединения с Qdrant (WinError 10054, httpx.ReadError/ConnectError,
# ResponseHandlingException): 5 попыток с экспоненциальной паузой 2 → 32 с,
# независимо от `retries`. Обычные таймауты Qdrant остаются на коротком ретрае.
TRANSIENT_CONNECTION_RETRIES = 5
_TRANSIENT_DELAY_BASE_SEC = 2.0
_TRANSIENT_DELAY_MAX_SEC = 32.0

_TRANSIENT_MESSAGE_MARKERS = (
    "10054",
    "10053",
    "10061",
    "connection reset",
    "connection refused",
    "connection aborted",
    "forcibly closed",
    "удаленный хост",
    "удалённый хост",
    "server disconnected",
    "readerror",
    "connecterror",
    "remoteprotocolerror",
)


def _transient_exception_types() -> tuple[type[BaseException], ...]:
    types: list[type[BaseException]] = [ConnectionError]  # + ConnectionResetError/Refused/Aborted
    try:
        from qdrant_client.http.exceptions import ResponseHandlingException  # noqa: PLC0415

        types.append(ResponseHandlingException)
    except Exception:  # pragma: no cover - qdrant_client без http.exceptions
        pass
    try:
        import httpx  # noqa: PLC0415

        # NetworkError: ReadError, WriteError, ConnectError, CloseError.
        types.extend([httpx.NetworkError, httpx.RemoteProtocolError])
    except Exception:  # pragma: no cover
        pass
    try:
        import httpcore  # noqa: PLC0415

        types.extend([httpcore.NetworkError, httpcore.RemoteProtocolError])
    except Exception:  # pragma: no cover
        pass
    return tuple(types)


_TRANSIENT_TYPES: tuple[type[BaseException], ...] = _transient_exception_types()


def is_transient_connection_error(exc: BaseException) -> bool:
    """Обрыв/сброс соединения с Qdrant, который имеет смысл повторить после паузы.

    Проверяется сама ошибка и цепочка причин (`__cause__`/`__context__`): qdrant-client
    заворачивает httpx.ReadError в ResponseHandlingException.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    depth = 0
    while current is not None and id(current) not in seen and depth < 8:
        seen.add(id(current))
        depth += 1
        if isinstance(current, _TRANSIENT_TYPES):
            return True
        message = str(current).lower()
        if any(marker in message for marker in _TRANSIENT_MESSAGE_MARKERS):
            return True
        current = current.__cause__ or current.__context__
    return False


def _transient_delay(attempt: int) -> float:
    """2, 4, 8, 16, 32 с для попыток 0..4."""
    return float(min(_TRANSIENT_DELAY_MAX_SEC, _TRANSIENT_DELAY_BASE_SEC * (2 ** max(0, int(attempt)))))


def _retry_plan(exc: Exception, attempt: int, retries: int) -> tuple[bool, float, int]:
    """(повторять?, пауза, лимит попыток) для ошибки `exc` на попытке `attempt` (с 0)."""
    if is_transient_connection_error(exc):
        limit = max(int(retries), TRANSIENT_CONNECTION_RETRIES)
        return attempt < limit, _transient_delay(attempt), limit
    limit = int(retries)
    return attempt < limit, min(5.0, 0.75 * (attempt + 1)), limit


def _is_payload_too_large_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "larger than allowed" in message
        or "payload too large" in message
        or "request entity too large" in message
        or "status code 413" in message
        or "413 (payload too large)" in message
    )


def _estimated_point_json_size(point: PointStruct) -> int:
    """Estimate the UTF-8 JSON bytes Qdrant will receive for one point."""
    if hasattr(point, "model_dump"):
        value = point.model_dump(mode="json", exclude_none=True)
    else:  # pragma: no cover - compatibility with qdrant-client on Pydantic v1
        value = point.dict(exclude_none=True)
    return len(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    )


def _partition_upsert_batches(
    points: Sequence[PointStruct],
    *,
    max_body_bytes: int,
) -> list[list[PointStruct]]:
    # Reserve space for the request wrapper and JSON separators. The default
    # target also leaves headroom below Qdrant's 32 MiB HTTP body limit.
    wrapper_bytes = 512
    batches: list[list[PointStruct]] = []
    current: list[PointStruct] = []
    current_bytes = wrapper_bytes
    limit = max(wrapper_bytes + 1, int(max_body_bytes))

    for point in points:
        point_bytes = _estimated_point_json_size(point) + 1
        if current and current_bytes + point_bytes > limit:
            batches.append(current)
            current = []
            current_bytes = wrapper_bytes
        current.append(point)
        current_bytes += point_bytes
    if current:
        batches.append(current)
    return batches


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
    for attempt in range(4):
        try:
            existing = [c.name for c in client.get_collections().collections]
            break
        except Exception as exc:
            transient = isinstance(exc, TimeoutError) or is_transient_connection_error(exc)
            if not transient or attempt == 3:
                raise
            logger.warning("Qdrant collection probe failed; retry %d/3", attempt + 1)
            time.sleep(_transient_delay(attempt))
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
    wait: bool = False,
    timeout_sec: int = 300,
) -> None:
    """Best-effort payload indexes for filters and Russian full-text retrieval."""
    for field_name in ("numeric_tokens", "type", "extension", "full_path"):
        try:
            client.create_payload_index(
                collection_name=collection_name,
                field_name=field_name,
                field_schema=PayloadSchemaType.KEYWORD,
                wait=wait,
                timeout=max(5, int(timeout_sec or 300)),
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
            wait=wait,
            timeout=max(5, int(timeout_sec or 300)),
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
    attempt = 0
    while True:
        try:
            client.delete(
                collection_name=collection_name,
                wait=True,
                timeout=timeout_sec,
                points_selector=FilterSelector(filter=Filter(must=must)),
            )
            return
        except Exception as exc:
            last_error = exc
            should_retry, delay, limit = _retry_plan(exc, attempt, int(retries))
            if not should_retry:
                break
            logger.warning(
                "Qdrant delete timeout/error, retry %d/%d in %.1fs: %s",
                attempt + 1,
                limit,
                delay,
                exc,
            )
            time.sleep(delay)
            attempt += 1
    if last_error is not None:
        raise last_error


def upsert_points(
    client: Any,
    *,
    collection_name: str,
    points: Sequence[PointStruct],
    timeout_sec: int = 60,
    retries: int = 2,
    max_body_bytes: int = _DEFAULT_MAX_UPSERT_BODY_BYTES,
) -> int:
    """Upsert points in body-limited batches, retaining reactive splitting."""
    if not points:
        return 0
    timeout = max(5, int(timeout_sec or 60))

    def write_batch(prepared: list[PointStruct]) -> int:
        last_error: Exception | None = None
        attempt = 0
        while True:
            try:
                try:
                    client.upsert(
                        collection_name,
                        points=prepared,
                        wait=True,
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
                should_retry, delay, limit = _retry_plan(exc, attempt, int(retries))
                if not should_retry:
                    break
                logger.warning(
                    "Qdrant upsert timeout/error, retry %d/%d in %.1fs: %s",
                    attempt + 1,
                    limit,
                    delay,
                    exc,
                )
                time.sleep(delay)
                attempt += 1
        if last_error is not None:
            raise last_error
        return len(prepared)

    prepared_batches = _partition_upsert_batches(points, max_body_bytes=max_body_bytes)
    if len(prepared_batches) > 1:
        logger.info(
            "Qdrant batch из %d точек заранее разделён по размеру JSON на %d частей",
            len(points),
            len(prepared_batches),
        )
    return sum(write_batch(batch) for batch in prepared_batches)
