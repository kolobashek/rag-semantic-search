import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rag_catalog.core.client_diagnostics import MAX_LOG_BYTES, ClientDiagnosticsDB, clean_log
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.user_auth_db import UserAuthDB
from rag_catalog.ui import api


def test_snapshot_bound_redacted_and_overwritten(tmp_path):
    db = ClientDiagnosticsDB(str(tmp_path / 'logs.db'))
    db.submit('pc', 'Bearer secret-123 password=secret-456 code ABCD-1234 https://s/?X-Amz-Signature=secret-789')
    text = db.read('pc')['log_text']
    assert 'secret-' not in text and 'ABCD-1234' not in text
    db.submit('pc', '\u0430' * MAX_LOG_BYTES)
    assert len(db.read('pc')['log_text'].encode()) <= MAX_LOG_BYTES
    db.submit('pc', 'new')
    assert db.read('pc')['log_text'] == 'new'
    assert 'pass' not in clean_log('https://user:pass@host/')


def test_request_is_durable_idempotent_and_acknowledged_only_by_matching_id(tmp_path):
    path = str(tmp_path / 'logs.db')
    db = ClientDiagnosticsDB(path)
    request = db.request('pc', 'admin')
    assert request['request_id']
    assert ClientDiagnosticsDB(path).request('pc', 'admin')['request_id'] == request['request_id']
    db.submit('pc', 'automatic', request_id='stale')
    assert db.read('pc')['request_id'] == request['request_id']
    db.submit('pc', 'fresh', request_id=request['request_id'])
    assert db.read('pc')['request_id'] == ''
    assert db.read('pc')['log_text'] == 'fresh'


def test_retention_prunes_snapshots_and_old_requests(tmp_path, monkeypatch):
    db = ClientDiagnosticsDB(str(tmp_path / 'logs.db'))
    db.submit('pc', 'old')
    db.request('offline', 'admin')
    now = time.time()
    monkeypatch.setattr('rag_catalog.core.client_diagnostics.time.time', lambda: now + 15 * 86400)
    assert db.read('pc')['uploaded_at'] == 0
    assert db.read('offline')['request_id'] == ''


@pytest.fixture
def diagnostic_api(tmp_path, monkeypatch):
    cfg = {'cloud_drive_db_path': str(tmp_path / 'cloud.db'), 'cloud_drive_storage': 'local',
           'cloud_drive_storage_root': str(tmp_path / 'storage'),
           'users_db_path': str(tmp_path / 'users.db'), 'telemetry_db_path': str(tmp_path / 'telemetry.db')}
    users = UserAuthDB(cfg['users_db_path'])
    tokens = {}
    for username, role in [('admin', 'admin'), ('veronika', 'user'), ('other', 'user')]:
        users.admin_create_user(username=username, password='Test-12345678', role=role, status='active')
        tokens[username] = {'Authorization': 'Bearer ' + users.create_session(username=username)}
    service = CloudDriveService.from_config(cfg)
    client_id = service.register_sync_client(username='veronika', device_id='pc-1')['id']
    monkeypatch.setattr(api, 'load_config', lambda: cfg)
    app = FastAPI()
    app.get('/pending')(api.api_client_diagnostics_pending)
    app.post('/request')(api.api_client_diagnostics_request)
    app.post('/upload')(api.api_client_diagnostics_upload)
    app.get('/status')(api.api_client_diagnostics_status)
    app.get('/download')(api.api_client_diagnostics_download)
    app.post('/update')(api.api_client_update_request)
    app.get('/update-pending')(api.api_client_update_pending)
    with TestClient(app) as client:
        yield client, tokens, {'client_id': client_id}, cfg


def test_diagnostics_authenticated_round_trip(diagnostic_api):
    client, tokens, params, cfg = diagnostic_api
    assert client.get('/download', params=params, headers=tokens['admin']).status_code == 404
    requested = client.post('/request', params=params, headers=tokens['admin'])
    assert requested.status_code == 200
    pending = client.get('/pending', params=params, headers=tokens['veronika']).json()
    assert pending['request_id'] == requested.json()['request_id']
    response = client.post('/upload', params=params, headers=tokens['veronika'], json={
        'request_id': pending['request_id'], 'app_version': '0.6.3',
        'log_text': 'ERROR hydration\nBearer private-token', 'username': 'admin'})
    assert response.status_code == 200
    status = client.get('/status', params=params, headers=tokens['admin']).json()
    assert status['request_id'] == '' and status['uploaded_at'] > 0
    assert 'log_text' not in status
    response = client.get('/download', params=params, headers=tokens['admin'])
    assert response.status_code == 200
    assert 'hydration' in response.text and 'private-token' not in response.text
    assert response.headers['cache-control'] == 'no-store'


def test_diagnostics_acl_and_body_limits(diagnostic_api):
    client, tokens, params, cfg = diagnostic_api
    for endpoint, method in [('/request', 'post'), ('/status', 'get'), ('/download', 'get')]:
        assert getattr(client, method)(endpoint, params=params, headers=tokens['veronika']).status_code == 403
        assert getattr(client, method)(endpoint, params=params).status_code == 401
    assert client.get('/pending', params=params, headers=tokens['other']).status_code == 403
    assert client.post('/upload', params=params, headers=tokens['other'], json={'log_text': 'bad'}).status_code == 403
    assert client.post('/upload', params=params, headers=tokens['veronika'],
                       json={'log_text': 'x' * (MAX_LOG_BYTES + 1)}).status_code == 413
    assert client.post('/upload', params=params, headers=tokens['veronika'],
                       content=b'x' * (1024 * 1024 + 1)).status_code == 413
    for payload in [[], {'log_text': []}, {'log_text': None}]:
        assert client.post('/upload', params=params, headers=tokens['veronika'], json=payload).status_code == 400
    assert client.get('/status', params={'client_id': 'missing'}, headers=tokens['admin']).status_code == 404


def test_remote_update_owner_and_admin_only(diagnostic_api):
    client, tokens, params, cfg = diagnostic_api
    assert client.post('/update', params=params).status_code == 401
    assert client.post('/update', params=params, headers=tokens['veronika']).status_code == 403
    assert client.get('/update-pending', params=params, headers=tokens['other']).status_code == 403
    assert client.get('/update-pending', params=params, headers=tokens['admin']).status_code == 403
    result = client.post('/update', params=params, headers=tokens['admin'])
    assert result.status_code == 200
    assert result.json()['target_version'] == api._CLOUD_FILES_VERSION
    query = {**params, 'app_version': '0.6.3'}
    assert client.get('/update-pending', params=query, headers=tokens['veronika']).json()['requested']
    query['app_version'] = api._CLOUD_FILES_VERSION
    assert not client.get('/update-pending', params=query, headers=tokens['veronika']).json()['requested']
    assert ClientDiagnosticsDB.from_config(cfg).read_update(params['client_id'])['completed_at'] > 0


def test_update_request_survives_restart_and_invalid_version(tmp_path):
    path = str(tmp_path / 'logs.db')
    ClientDiagnosticsDB(path).request_update('pc', 'admin', '0.6.4')
    db = ClientDiagnosticsDB(path)
    assert not db.poll_update('pc', 'invalid')['completed_at']
    assert not db.poll_update('pc', '0.6.3')['completed_at']
    assert db.poll_update('pc', '0.6.5')['completed_at']
