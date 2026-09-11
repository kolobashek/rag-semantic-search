"""Устойчивость записи в Qdrant к обрыву соединения (WinError 10054, ReadError).

Ночной прогон дважды падал с ResponseHandlingException после двух коротких
ретраев. Теперь обрыв соединения повторяется 5 раз с паузой 2 → 32 с; обычные
ошибки/таймауты Qdrant остаются на прежнем коротком ретрае.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from qdrant_client.http.exceptions import ResponseHandlingException
from qdrant_client.models import PointStruct

from rag_catalog.core.indexing import qdrant_writer
from rag_catalog.core.indexing.qdrant_writer import (
    TRANSIENT_CONNECTION_RETRIES,
    delete_file_vectors,
    is_transient_connection_error,
    upsert_points,
)

WINERR = "[WinError 10054] Удаленный хост принудительно разорвал существующее подключение"


def _reset_error() -> ResponseHandlingException:
    return ResponseHandlingException(httpx.ReadError(WINERR))


class _FlakyClient:
    """Первые `failures` вызовов upsert/delete бросают `error_factory()`, дальше — успех."""

    def __init__(self, failures: int, error_factory=_reset_error) -> None:
        self.failures = failures
        self.error_factory = error_factory
        self.upserted: list[int] = []
        self.deleted = 0

    def _maybe_fail(self) -> None:
        if self.failures > 0:
            self.failures -= 1
            raise self.error_factory()

    def upsert(self, collection_name, points, **kwargs):
        self._maybe_fail()
        self.upserted.append(len(points))

    def delete(self, **kwargs):
        self._maybe_fail()
        self.deleted += 1


@pytest.fixture
def delays(monkeypatch) -> list[float]:
    seen: list[float] = []
    monkeypatch.setattr(qdrant_writer.time, "sleep", lambda delay: seen.append(delay))
    return seen


def test_transient_detection_covers_reset_and_httpx_errors() -> None:
    assert is_transient_connection_error(_reset_error())
    assert is_transient_connection_error(httpx.ReadError(WINERR))
    assert is_transient_connection_error(httpx.ConnectError("connection refused"))
    assert is_transient_connection_error(ConnectionResetError(10054, "reset"))
    assert is_transient_connection_error(RuntimeError(WINERR))  # по тексту
    wrapped = RuntimeError("wrapped")
    wrapped.__cause__ = httpx.ReadError("boom")
    assert is_transient_connection_error(wrapped)  # по цепочке причин
    assert not is_transient_connection_error(TimeoutError("timed out"))
    assert not is_transient_connection_error(RuntimeError("payload too large"))


def test_upsert_retries_connection_reset_with_exponential_backoff(delays) -> None:
    client = _FlakyClient(failures=2)
    points = [PointStruct(id="p1", vector=[0.1, 0.2], payload={"x": 1})]

    written = upsert_points(client, collection_name="catalog", points=points, retries=2)

    assert written == 1
    assert client.upserted == [1]
    assert delays == [2.0, 4.0]


def test_upsert_gives_up_after_five_connection_failures(delays) -> None:
    client = _FlakyClient(failures=TRANSIENT_CONNECTION_RETRIES + 1)
    points = [PointStruct(id="p1", vector=[0.1, 0.2], payload={"x": 1})]

    with pytest.raises(ResponseHandlingException):
        upsert_points(client, collection_name="catalog", points=points, retries=2)

    assert client.upserted == []
    assert delays == [2.0, 4.0, 8.0, 16.0, 32.0]


def test_upsert_recovers_on_the_fifth_retry(delays) -> None:
    client = _FlakyClient(failures=TRANSIENT_CONNECTION_RETRIES)
    points = [PointStruct(id="p1", vector=[0.1, 0.2], payload={"x": 1})]

    assert upsert_points(client, collection_name="catalog", points=points, retries=2) == 1
    assert delays == [2.0, 4.0, 8.0, 16.0, 32.0]


def test_upsert_keeps_short_retry_for_ordinary_errors(delays) -> None:
    client = _FlakyClient(failures=1, error_factory=lambda: TimeoutError("timed out"))
    points = [PointStruct(id="p1", vector=[0.1, 0.2], payload={"x": 1})]

    assert upsert_points(client, collection_name="catalog", points=points, retries=2) == 1
    assert delays == [0.75]


def test_delete_retries_connection_reset_then_succeeds(delays) -> None:
    client = _FlakyClient(failures=2)

    delete_file_vectors(client, collection_name="catalog", filepath=Path("doc.pdf"), timeout_sec=5, retries=2)

    assert client.deleted == 1
    assert delays == [2.0, 4.0]


def test_delete_raises_after_exhausted_connection_retries(delays) -> None:
    client = _FlakyClient(failures=TRANSIENT_CONNECTION_RETRIES + 1)

    with pytest.raises(ResponseHandlingException):
        delete_file_vectors(client, collection_name="catalog", filepath=Path("doc.pdf"), timeout_sec=5)

    assert client.deleted == 0
    assert len(delays) == TRANSIENT_CONNECTION_RETRIES
