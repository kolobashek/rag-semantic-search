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
