"""Приём клиентских журналов: очистка секретов, лимиты, выгрузка."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import pytest

from rag_catalog.core import client_logs


class _FakeTelemetry:
    def __init__(self) -> None:
        self.events: List[Dict[str, Any]] = []

    def log_app_event(self, **kwargs: Any) -> None:
        self.events.append(kwargs)


# ── очистка секретов ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "leaked"),
    [
        ("GET /api/files Authorization: Bearer abc123XYZ._-", "abc123XYZ"),
        ('connect token="s3cr3t-value"', "s3cr3t-value"),
        ("password=hunter2 for user kris", "hunter2"),
        ("https://rag.local/api/files?token=deadbeef&path=/x", "deadbeef"),
        ("https://kris:hunter2@rag.local/api", "hunter2"),
        ("api_key: AKIA1234567890", "AKIA1234567890"),
        ('{"password":"private value with spaces"}', "private"),
        ('{"access_token":"sensitive-token"}', "sensitive-token"),
        ('{"refresh_token":"secret-value"}', "secret-value"),
    ],
)
def test_secrets_are_redacted(raw: str, leaked: str) -> None:
    cleaned = client_logs.redact_secrets(raw)
    assert leaked not in cleaned
    assert "скрыто" in cleaned


def test_uploader_redacts_before_queueing_and_sending():
    sent = []
    uploader = client_logs.ClientLogUploader(sent.append, client="sync")
    uploader.emit(logging.LogRecord("test", logging.ERROR, "", 1,
                                    '{"password":"private password"} Bearer private-token', (), None))
    assert "private" not in str(list(uploader._queue))
    uploader.flush()
    assert "private" not in str(sent)
    uploader.close()


def test_client_log_endpoint_rejects_oversized_stream_before_json(monkeypatch):
    import asyncio

    from fastapi import HTTPException

    from rag_catalog.ui import api
    monkeypatch.setattr(api, "load_config", lambda: {})
    monkeypatch.setattr(api, "_require_cloud_drive_api_user", lambda *a, **kw: {"username": "test"})
    class Request:
        async def stream(self):
            yield b"x" * (1024 * 1024 + 1)
            raise AssertionError("must stop consuming the request")
    with pytest.raises(HTTPException) as error:
        asyncio.run(api.api_client_logs(Request(), authorization="test"))
    assert error.value.status_code == 413


def test_redaction_keeps_useful_context() -> None:
    cleaned = client_logs.redact_secrets("Не удалось синхронизировать Demo/local.txt (HTTP 500)")
    assert cleaned == "Не удалось синхронизировать Demo/local.txt (HTTP 500)"


# ── нормализация событий ───────────────────────────────────────────────


def test_normalise_event_fills_defaults_and_trims() -> None:
    event = client_logs.normalise_event(
        {"message": "x" * 5000, "level": "warning", "logger": "rag_sync", "ts": 1700000000}
    )
    assert event is not None
    assert event["level"] == "WARNING"
    assert len(event["message"]) == client_logs.MAX_MESSAGE_CHARS
    assert event["logger"] == "rag_sync"
    assert event["ts"].startswith("2023-")


def test_normalise_event_defaults_unknown_level_to_error() -> None:
    event = client_logs.normalise_event({"message": "boom", "level": "TRACE"})
    assert event is not None and event["level"] == "ERROR"


def test_normalise_event_drops_empty_records() -> None:
    assert client_logs.normalise_event({"message": "   "}) is None
    assert client_logs.normalise_event({}) is None


def test_normalise_event_accepts_plain_string() -> None:
    event = client_logs.normalise_event("provider stopped")
    assert event is not None and event["message"] == "provider stopped"


# ── запись в телеметрию ────────────────────────────────────────────────


def test_ingest_writes_events_with_client_feature() -> None:
    telemetry = _FakeTelemetry()

    stored = client_logs.ingest_client_events(
        telemetry,
        username="kris",
        client="sync",
        device_id="win-ABC",
        events=[{"level": "ERROR", "message": "Не удалось синхронизировать Demo/local.txt"}],
        client_host="10.0.0.5",
        app_version="1.1.0",
    )

    assert stored == 1
    event = telemetry.events[0]
    assert event["feature"] == client_logs.CLIENT_FEATURE
    assert event["screen"] == "sync"
    assert event["action"] == "client_error"
    assert event["ok"] is False
    assert event["username"] == "kris"
    assert event["details"]["device_id"] == "win-ABC"
    assert event["details"]["app_version"] == "1.1.0"
    assert event["details"]["client_host"] == "10.0.0.5"


def test_ingest_marks_warnings_as_ok_but_errors_as_not_ok() -> None:
    telemetry = _FakeTelemetry()
    client_logs.ingest_client_events(
        telemetry,
        username="kris",
        client="sync",
        events=[{"level": "WARNING", "message": "retry"}, {"level": "CRITICAL", "message": "die"}],
    )
    assert [e["ok"] for e in telemetry.events] == [True, False]
    assert [e["action"] for e in telemetry.events] == ["client_warning", "client_critical"]


def test_ingest_redacts_secrets_before_storing() -> None:
    telemetry = _FakeTelemetry()
    client_logs.ingest_client_events(
        telemetry,
        username="kris",
        client="sync",
        events=[{"message": "auth failed: Bearer topsecrettoken"}],
    )
    assert "topsecrettoken" not in telemetry.events[0]["details"]["message"]


def test_ingest_caps_batch_size() -> None:
    telemetry = _FakeTelemetry()
    stored = client_logs.ingest_client_events(
        telemetry,
        username="kris",
        client="sync",
        events=[{"message": f"e{i}"} for i in range(client_logs.MAX_EVENTS_PER_BATCH + 50)],
    )
    assert stored == client_logs.MAX_EVENTS_PER_BATCH


def test_ingest_maps_unknown_client_to_other() -> None:
    telemetry = _FakeTelemetry()
    client_logs.ingest_client_events(
        telemetry, username="kris", client="totally-made-up", events=[{"message": "x"}]
    )
    assert telemetry.events[0]["screen"] == "other"


def test_ingest_survives_broken_telemetry() -> None:
    class _Broken:
        def log_app_event(self, **kwargs: Any) -> None:
            raise RuntimeError("db is locked")

    assert client_logs.ingest_client_events(
        _Broken(), username="kris", client="sync", events=[{"message": "x"}]
    ) == 0


# ── выгрузка с клиента ─────────────────────────────────────────────────


def test_uploader_batches_records_and_keeps_payload_shape() -> None:
    sent: List[Dict[str, Any]] = []
    handler = client_logs.ClientLogUploader(
        sent.append, client="sync", device_id="win-ABC", app_version="1.1.0"
    )
    log = logging.getLogger("test.uploader")
    log.addHandler(handler)
    log.propagate = False
    try:
        log.error("Не удалось синхронизировать %s", "Demo/local.txt")
        log.info("это не должно уехать — уровень ниже WARNING")
        handler.flush()
    finally:
        log.removeHandler(handler)

    assert len(sent) == 1
    payload = sent[0]
    assert payload["client"] == "sync"
    assert payload["device_id"] == "win-ABC"
    assert len(payload["events"]) == 1
    assert payload["events"][0]["message"] == "Не удалось синхронизировать Demo/local.txt"


def test_uploader_requeues_batch_when_server_is_unreachable() -> None:
    attempts: List[int] = []

    def _flaky(payload: Dict[str, Any]) -> None:
        attempts.append(len(payload["events"]))
        if len(attempts) == 1:
            raise ConnectionError("server is down")

    handler = client_logs.ClientLogUploader(_flaky, client="sync")
    log = logging.getLogger("test.uploader.retry")
    log.addHandler(handler)
    log.propagate = False
    try:
        log.error("boom")
        handler.flush()  # падает — запись возвращается в очередь
        handler.flush()  # вторая попытка проходит
    finally:
        log.removeHandler(handler)

    assert attempts == [1, 1]


def test_uploader_never_raises_from_emit() -> None:
    handler = client_logs.ClientLogUploader(lambda payload: None, client="sync")
    record = logging.LogRecord("x", logging.ERROR, __file__, 1, "%d", ("not-an-int",), None)
    handler.emit(record)  # аргументы не совпадают с шаблоном — не должно бросить


def test_uploader_queue_is_bounded() -> None:
    sent: List[Dict[str, Any]] = []
    handler = client_logs.ClientLogUploader(
        sent.append, client="sync", queue_size=10, batch_size=100
    )
    log = logging.getLogger("test.uploader.bounded")
    log.addHandler(handler)
    log.propagate = False
    try:
        for i in range(200):
            log.error("event %d", i)
        handler.flush()
    finally:
        log.removeHandler(handler)

    assert len(sent[0]["events"]) == 10
    assert sent[0]["events"][-1]["message"] == "event 199"


def test_telemetry_handler_writes_directly() -> None:
    telemetry = _FakeTelemetry()
    handler = client_logs.TelemetryLogHandler(
        telemetry, client="desktop", username="kris", device_id="GOD"
    )
    log = logging.getLogger("test.telemetry.handler")
    log.addHandler(handler)
    log.propagate = False
    try:
        log.error("поиск упал")
        handler.flush()
    finally:
        log.removeHandler(handler)

    assert len(telemetry.events) == 1
    assert telemetry.events[0]["screen"] == "desktop"
    assert telemetry.events[0]["details"]["message"] == "поиск упал"


def test_local_file_log_is_rotating(tmp_path) -> None:
    path = tmp_path / "logs" / "client.log"
    handler = client_logs.install_local_file_log(path, max_bytes=1024, backups=2)
    assert handler is not None
    try:
        logging.getLogger("test.localfile").error("hello")
        handler.flush()
        assert path.exists()
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()


def test_default_client_log_path_uses_localappdata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOCALAPPDATA", r"C:\Users\kris\AppData\Local")
    path = client_logs.default_client_log_path("RAGDesktop", "desktop.log")
    assert path.as_posix().endswith("RAGDesktop/logs/desktop.log")
