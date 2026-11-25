"""Slow disk/SQLite operations must not stall the HTTP event loop."""

import asyncio
import io
import threading
from types import SimpleNamespace

import httpx
import anyio
import pytest
from fastapi import FastAPI

from rag_catalog.ui import api


def test_slow_page_badges_allow_range_download_and_ignore_removed_elements(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from rag_catalog.ui.file_job_badges import FileJobBadges

    sample = tmp_path / "file.bin"
    sample.write_bytes(b"A" * 8192)
    monkeypatch.setattr(api, "load_config", lambda: {})
    monkeypatch.setattr(api, "_require_cloud_drive_api_user", lambda *a, **kw: {"username": "test"})
    monkeypatch.setattr(api, "_require_cloud_drive_path_access", lambda *a, **kw: None)
    monkeypatch.setattr(api, "_audit_cloud_drive_api_event", lambda *a, **kw: None)
    service = SimpleNamespace(
        get_download_descriptor=lambda path: dict(
            mode="local_file", file_path=sample, mime_type="application/octet-stream", filename=sample.name
        )
    )
    monkeypatch.setattr(api.CloudDriveService, "from_config", lambda _: service)
    app = FastAPI()
    app.get("/ping")(api.api_ping)
    app.get("/download")(api.api_cloud_drive_download)
    entered, release = threading.Event(), threading.Event()
    calls, painted = [], []

    def fetch(ids):
        calls.append(ids)
        entered.set()
        assert release.wait(5)
        return {"a": {"status": "pending"}}

    async def exercise():
        badges = FileJobBadges()
        old, current = nullcontext(), nullcontext()
        old.is_deleted = current.is_deleted = False
        badges.targets = [("a", old, lambda _: pytest.fail("painted removed element")), ("a", current, painted.append)]
        pending = asyncio.create_task(badges.load(fetch))
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            old.is_deleted = True
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
                ping, first = await asyncio.wait_for(
                    asyncio.gather(
                        client.get("/ping"), client.get("/download?path=file.bin", headers={"Range": "bytes=0-4095"})
                    ),
                    1,
                )
                assert ping.status_code == 200 and first.status_code == 206 and first.content == b"A" * 4096
                assert not pending.done()
        finally:
            release.set()
            await pending
        assert calls == [["a"]] and painted == [{"status": "pending"}]

    asyncio.run(exercise())


def test_search_usage_record_does_not_copy_query_to_technical_log(monkeypatch, caplog):
    from rag_catalog.ui import state

    events = []
    monkeypatch.setattr(state, "_get_telemetry", lambda _: SimpleNamespace(log_app_event=lambda **kw: events.append(kw)))
    page = state.PageState(cfg={"usage_analytics_enabled": False})
    with caplog.at_level("INFO", logger=state._APP_EVENT_LOGGER.name):
        state._log_app_event(page, "search", "run_full", details={"query": "private-query", "duration_ms": 17})
    assert events[0]["details"]["query"] == "private-query"  # Used by the user's search suggestions.
    assert '"duration_ms": 17' in caplog.text and "private-query" not in caplog.text


def test_browser_diagnostics_allowlist_precedes_both_storage_and_logging(monkeypatch, caplog):
    marker = "private-document-fixture"
    events = []
    monkeypatch.setattr(api, "load_config", lambda: {})
    monkeypatch.setattr(api, "_telemetry_db_path", lambda _: "mock")
    monkeypatch.setattr(api, "TelemetryDB", lambda _: SimpleNamespace(log_app_event=lambda **kw: events.append(kw)))
    app = FastAPI()
    app.post("/api/ui-events")(api.api_ui_events)

    async def post():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            for path, action in (("/search?q=" + marker, "javascript_error"), ("/" + marker, marker)):
                response = await client.post("/api/ui-events", json={
                    "action": action, "path": path, "url": "https://example/#" + marker,
                    "search": {"query": marker}, "session_id": marker, "elapsed_ms": 23,
                    "details": {"excerpt": marker, "message": marker, "duration_ms": 14,
                                "attempt": marker, "method": [marker]},
                }, headers={"Referer": "https://example/?q=" + marker, "User-Agent": marker})
                assert response.json() == {"ok": True}
    with caplog.at_level("INFO", logger=api.logger.name):
        asyncio.run(post())
    assert [(e["screen"], e["action"]) for e in events] == [
        ("/search", "javascript_error"), ("browser", "client_event")]
    assert all(e["details"] == {"elapsed_ms": 23, "duration_ms": 14} for e in events)
    assert "browser_event action=javascript_error" in caplog.text
    assert marker not in str(events) and marker not in caplog.text


@pytest.mark.parametrize('stage', [
    'heartbeat_auth', 'heartbeat_store', 'diagnostics_auth', 'diagnostics_store',
    'ui_store', 'logs_auth', 'logs_store', 'upload_auth', 'upload_copy', 'upload_commit',
])
def test_slow_storage_does_not_block_other_requests(monkeypatch, tmp_path, stage):
    cfg = {'cloud_drive_db_path': str(tmp_path / 'cloud.db'),
           'telemetry_db_path': str(tmp_path / 'telemetry.db')}
    monkeypatch.setattr(api, 'load_config', lambda: cfg)
    monkeypatch.setattr(api, '_diagnostics_access', lambda *a, **k: {'username': 'test'})
    monkeypatch.setattr(api, '_require_cloud_drive_api_user', lambda *a, **k: {'username': 'test'})
    store = SimpleNamespace(heartbeat=lambda *a, **k: None,
                            read=lambda *a: {'request_id': ''},
                            poll_recovery=lambda *a: '', submit=lambda *a, **k: None)
    monkeypatch.setattr(api.ClientDiagnosticsDB, 'from_config', lambda cfg: store)
    monkeypatch.setattr(api, 'TelemetryDB', lambda *a: SimpleNamespace(log_app_event=lambda **k: None))
    monkeypatch.setattr(api, 'ingest_client_events', lambda *a, **k: 1)
    service = SimpleNamespace(registry=SimpleNamespace(get_file_by_path=lambda *a: None),
                              upload_file=lambda **k: {'name': 'test.txt', 'path': 'test.txt'})
    monkeypatch.setattr(api.CloudDriveService, 'from_config', lambda cfg: service)
    monkeypatch.setattr(api, '_require_cloud_drive_path_access', lambda *a, **k: None)
    monkeypatch.setattr(api, '_audit_cloud_drive_api_event', lambda *a, **k: None)

    routes = {
        'heartbeat': (api.api_client_diagnostics_heartbeat, {'json': dict(
            app_version='test', phase='running', state='UpToDate', last_error='')}),
        'diagnostics': (api.api_client_diagnostics_upload, {'json': {'log_text': 'test'}}),
        'ui': (api.api_ui_events, {'json': {'action': 'page_open'}}),
        'logs': (api.api_client_logs, {'json': {'events': [{'message': 'test'}]}}),
        'upload': (api.api_cloud_drive_upload, {'files': {'file': ('test.txt', b'test')}}),
    }
    targets = {
        'heartbeat_auth': (api, '_diagnostics_access'),
        'heartbeat_store': (store, 'heartbeat'),
        'diagnostics_auth': (api, '_diagnostics_access'),
        'diagnostics_store': (store, 'submit'),
        'ui_store': (api, 'TelemetryDB'),
        'logs_auth': (api, '_require_cloud_drive_api_user'),
        'logs_store': (api, 'TelemetryDB'),
        'upload_auth': (api, '_require_cloud_drive_api_user'),
        'upload_copy': (api, '_write_bounded_upload'),
        'upload_commit': (service, 'upload_file'),
    }
    entered, release = threading.Event(), threading.Event()
    owner, name = targets[stage]
    original = getattr(owner, name)
    worker_threads = []

    def slow(*args, **kwargs):
        worker_threads.append(threading.get_ident())
        entered.set()
        release.wait(2)  # Bound the failure case when testing a blocking implementation.
        return original(*args, **kwargs)

    monkeypatch.setattr(owner, name, slow)
    endpoint, request = routes[stage.split('_')[0]]
    app = FastAPI()
    app.post('/slow')(endpoint)
    app.get('/ping')(api.api_ping)

    async def exercise():
        loop_thread = threading.get_ident()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
            pending = asyncio.create_task(client.post('/slow', params={'client_id': 'test'}, **request))
            try:
                async def wait_entered():
                    while not entered.is_set():
                        await asyncio.sleep(.005)
                await asyncio.wait_for(wait_entered(), 3)
                assert worker_threads and loop_thread not in worker_threads
                assert not pending.done(), 'Slow storage blocked the entire event loop'
                response = await asyncio.wait_for(client.get('/ping'), .5)
                assert response.json()['ok'] is True
            finally:
                release.set()
                response = await asyncio.wait_for(pending, 5)
            assert response.status_code == 200

    asyncio.run(exercise())


def test_bounded_upload_streams_without_reading_whole_file():
    source = io.BytesIO(b'a' * 17)
    target = io.BytesIO()
    with pytest.raises(api.HTTPException) as exc:
        api._write_bounded_upload(SimpleNamespace(file=source), target, max_bytes=16)
    assert exc.value.status_code == 413
    assert target.getvalue() == b''


def test_ping_does_not_need_a_free_storage_worker():
    async def exercise():
        app = FastAPI()
        app.get('/ping')(api.api_ping)
        limiter = anyio.to_thread.current_default_thread_limiter()
        original_tokens = limiter.total_tokens
        limiter.total_tokens = 1
        await limiter.acquire()
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                response = await asyncio.wait_for(client.get('/ping'), .5)
                assert response.json()['ok'] is True
        finally:
            limiter.release()
            limiter.total_tokens = original_tokens
    asyncio.run(exercise())
