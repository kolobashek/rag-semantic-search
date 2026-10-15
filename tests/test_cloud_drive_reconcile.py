from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.cloud_drive.storage import LocalStorageAdapter


@pytest.fixture
def setup(tmp_path):
    catalog = tmp_path / 'catalog'
    catalog.mkdir()
    source = catalog / 'report.txt'
    source.write_bytes(b'old content')
    os.utime(source, (1700000000, 1700000000))
    service = CloudDriveService(
        registry=CloudDriveRegistryDB(str(tmp_path / 'cloud.db')),
        storage=LocalStorageAdapter(str(tmp_path / 'storage')),
    )
    service.bootstrap_from_catalog(str(catalog), import_files=True)
    return service, source


@pytest.mark.parametrize(('delta', 'action', 'current_content'), [
    (60, 'updated', b'new content'),
    (-60, 'archived_older', b'old content'),
    (0, 'archived_equal_date', b'old content'),
])
def test_reconcile_chooses_newest_and_preserves_both_copies(setup, delta, action, current_content):
    service, source = setup
    original = service.registry.get_file_by_path('report.txt')
    source.write_bytes(b'new content')
    os.utime(source, (1700000000 + delta, 1700000000 + delta))
    result = service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True, actor='admin')
    assert result['action'] == action
    current = service.registry.get_file_by_path('report.txt')
    assert current.id == original.id
    assert Path(service.storage.resolve_path(current.storage_key)).read_bytes() == current_content
    versions = service.registry.list_file_versions(path='report.txt')
    assert len(versions) == 2
    assert sum(v['is_current'] for v in versions) == 1
    assert {Path(service.storage.resolve_path(v['storage_key'])).read_bytes() for v in versions} == {b'old content', b'new content'}
    incoming = next(v for v in versions if v['created_by'] == 'admin')
    assert incoming['source_mtime'] == 1700000000 + delta
    service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True, actor='admin')
    assert len(service.registry.list_file_versions(path='report.txt')) == 2


def test_preview_does_not_read_or_write_contents(setup, monkeypatch):
    service, source = setup
    monkeypatch.setattr(service.storage, 'download_file', lambda *a: pytest.fail('unexpected download'))
    result = service.reconcile_file_from_source(path='report.txt', source_path=str(source))
    assert result['applied'] is False
    assert len(service.registry.list_file_versions(path='report.txt')) == 1


def test_same_content_does_not_duplicate_history(setup):
    service, source = setup
    os.utime(source, (1700000100, 1700000100))
    result = service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert result['action'] == 'unchanged'
    assert len(service.registry.list_file_versions(path='report.txt')) == 1
    assert service.registry.get_file_by_path('report.txt').source_mtime == 1700000100


def test_upload_save_time_is_used_not_later_metadata_update(setup):
    service, source = setup
    row = service.registry.get_file_by_path('report.txt')
    with service.registry._connect() as db:
        db.execute("UPDATE cloud_files SET source_mtime=0,updated_at='2099-01-01T00:00:00+00:00' WHERE id=?", (row.id,))
        db.execute("UPDATE cloud_file_versions SET source_mtime=0,created_at='2020-01-01T00:00:00+00:00' WHERE id=?", (row.current_version_id,))
    source.write_bytes(b'newer than saved copy')
    os.utime(source, (1700000000, 1700000000))
    result = service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert result['action'] == 'updated'
    assert result['cloud_time_basis'] == 'version_saved_at'


def test_unknown_cloud_time_archives_without_guessing(setup):
    service, source = setup
    with service.registry._connect() as db:
        db.execute('UPDATE cloud_files SET source_mtime=0')
        db.execute("UPDATE cloud_file_versions SET source_mtime=0,created_at='unknown'")
    source.write_bytes(b'different')
    result = service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert result['action'] == 'archived_unknown_date'
    assert len(service.registry.list_file_versions(path='report.txt')) == 2


def test_missing_or_damaged_history_prevents_overwrite(setup):
    service, source = setup
    original = service.registry.get_file_by_path('report.txt')
    Path(service.storage.resolve_path(original.storage_key)).write_bytes(b'damaged')
    source.write_bytes(b'new')
    with pytest.raises(RuntimeError, match='damaged'):
        service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert service.registry.get_file_by_path('report.txt').current_version_id == original.current_version_id
    assert len(service.registry.list_file_versions(path='report.txt')) == 1


def test_failed_incoming_verification_preserves_current(setup, monkeypatch):
    service, source = setup
    original = service.registry.get_file_by_path('report.txt')
    source.write_bytes(b'new')
    monkeypatch.setattr(service.storage, 'put_file', lambda p, k: Path(service.storage.resolve_path(original.storage_key)))
    with pytest.raises((RuntimeError, FileNotFoundError)):
        service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert service.registry.get_file_by_path('report.txt').current_version_id == original.current_version_id


def test_concurrent_cloud_edit_is_not_overwritten(setup, monkeypatch):
    service, source = setup
    original = service.registry.get_file_by_path('report.txt')
    source.write_bytes(b'incoming')
    put = service.storage.put_file

    def concurrent_upload(path, key):
        put(path, key)
        with service.registry._connect() as db:
            db.execute("UPDATE cloud_files SET updated_at='concurrent-edit' WHERE id=?", (original.id,))

    monkeypatch.setattr(service.storage, 'put_file', concurrent_upload)
    with pytest.raises(RuntimeError, match='changed during'):
        service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert service.registry.get_file_by_path('report.txt').current_version_id == original.current_version_id
    assert len(service.registry.list_file_versions(path='report.txt')) == 1


def test_source_changed_during_copy_is_not_committed(setup, monkeypatch):
    from rag_catalog.core.cloud_drive import service as module

    service, source = setup
    source.write_bytes(b'incoming')
    copy = module.shutil.copyfile

    def changed(path, destination, **kwargs):
        result = copy(path, destination, **kwargs)
        if Path(path) == source:
            source.write_bytes(b'concurrent source edit')
        return result

    monkeypatch.setattr(module.shutil, 'copyfile', changed)
    with pytest.raises(RuntimeError, match='Source changed'):
        service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert len(service.registry.list_file_versions(path='report.txt')) == 1


def test_v8_migration_backfills_only_known_current_mtime(setup):
    service, _ = setup
    db_path = service.registry.db_path
    with sqlite3.connect(db_path) as db:
        db.execute('ALTER TABLE cloud_file_versions DROP COLUMN source_mtime')
        db.execute("UPDATE schema_meta SET schema_version=8 WHERE db_kind='cloud_drive'")
    registry = CloudDriveRegistryDB(str(db_path))
    assert registry.list_file_versions(path='report.txt')[0]['source_mtime'] == 1700000000


def test_deleted_file_is_not_resurrected(setup):
    service, source = setup
    with service.registry._connect() as db:
        db.execute("UPDATE cloud_files SET deleted_at='2024-01-01T00:00:00+00:00'")
    with pytest.raises(RuntimeError, match='existing active'):
        service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)


def test_shared_folder_is_not_imported_outside_its_worker(setup):
    service, source = setup
    from types import SimpleNamespace
    service.shared_folders = SimpleNamespace(mount=lambda path: object())
    with pytest.raises(RuntimeError, match='shared folders'):
        service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)


def test_future_source_date_is_rejected(setup):
    import time

    service, source = setup
    future = time.time() + 86400
    os.utime(source, (future, future))
    with pytest.raises(RuntimeError, match='future'):
        service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)


def test_current_history_row_is_required(setup):
    service, source = setup
    row = service.registry.get_file_by_path('report.txt')
    with service.registry._connect() as db:
        db.execute("UPDATE cloud_file_versions SET checksum='wrong' WHERE id=?", (row.current_version_id,))
    source.write_bytes(b'new')
    with pytest.raises(RuntimeError, match='history is inconsistent'):
        service.reconcile_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert service.registry.get_file_by_path('report.txt').current_version_id == row.current_version_id


def test_cli_preview_and_apply(setup, monkeypatch, capsys):
    import json

    from rag_catalog.cli import cloud_drive

    service, source = setup
    source.write_bytes(b'new')
    monkeypatch.setattr(cloud_drive, 'load_config', lambda: {})
    monkeypatch.setattr(cloud_drive, '_cloud_service_from_cfg', lambda cfg: service)
    args = ['reconcile-file', '--path', 'report.txt', '--source', str(source)]
    assert cloud_drive.main(args) == 0
    assert json.loads(capsys.readouterr().out)['applied'] is False
    assert len(service.registry.list_file_versions(path='report.txt')) == 1
    assert cloud_drive.main(args + ['--apply']) == 0
    assert json.loads(capsys.readouterr().out)['action'] == 'updated'
    assert len(service.registry.list_file_versions(path='report.txt')) == 2
