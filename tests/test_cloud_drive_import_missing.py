from __future__ import annotations

import os
from pathlib import Path

import pytest

from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.cloud_drive.storage import LocalStorageAdapter


@pytest.fixture
def setup(tmp_path):
    service = CloudDriveService(
        registry=CloudDriveRegistryDB(str(tmp_path / 'cloud.db')),
        storage=LocalStorageAdapter(str(tmp_path / 'storage')),
    )
    service.registry.ensure_root_folder(root_name='Cloud')
    source = tmp_path / 'report.txt'
    source.write_bytes(b'new file')
    os.utime(source, (1700000000, 1700000000))
    return service, source


def test_import_creates_parents_history_and_verified_content(setup):
    service, source = setup
    result = service.import_missing_file_from_source(
        path='Staff/2026/report.txt', source_path=str(source), apply=True, actor='migration',
    )
    assert result['action'] == 'created'
    row = service.registry.get_file_by_path(result['path'])
    assert row.source_mtime == source.stat().st_mtime
    assert Path(service.storage.resolve_path(row.storage_key)).read_bytes() == source.read_bytes()
    versions = service.registry.list_file_versions(path=row.path)
    assert len(versions) == 1
    assert versions[0]['created_by'] == 'migration'
    assert versions[0]['source_mtime'] == source.stat().st_mtime
    assert service.registry.get_folder_by_path('Staff/2026') is not None


def test_preview_creates_nothing(setup):
    service, source = setup
    result = service.import_missing_file_from_source(path='Staff/report.txt', source_path=str(source))
    assert result['applied'] is False
    assert service.registry.get_folder_by_path('Staff') is None
    assert service.registry.get_file_by_path('Staff/report.txt') is None


def test_leading_space_in_filename_is_preserved(setup):
    service, source = setup
    result = service.import_missing_file_from_source(path='Staff/ report.txt', source_path=str(source), apply=True)
    assert result['path'] == 'Staff/ report.txt'
    row = service.registry.get_file_by_path(result['path'])
    assert row.name == ' report.txt'
    assert service.registry.get_file_by_path('Staff/report.txt') is None


@pytest.mark.parametrize('deleted', [False, True])
def test_existing_or_deleted_file_cannot_be_overwritten(setup, deleted):
    service, source = setup
    service.import_missing_file_from_source(path='report.txt', source_path=str(source), apply=True)
    original = service.registry.get_file_by_path('report.txt')
    if deleted:
        service.registry.delete_file('report.txt')
    source.write_bytes(b'replacement')
    with pytest.raises(RuntimeError, match='already exists or was deleted'):
        service.import_missing_file_from_source(path='report.txt', source_path=str(source), apply=True)
    row = service.registry.get_file_by_path('report.txt')
    assert row.current_version_id == original.current_version_id
    assert bool(row.deleted_at) == deleted
    assert len(service.registry.list_file_versions(path='report.txt')) == 1


def test_case_variants_reuse_folder_identity_and_permissions(setup):
    service, source = setup
    original = service.registry.create_folder(name='Staff')
    service.registry.grant_permission(subject_type='user', subject_id='kris', resource_type='folder',
                                      resource_id=original.id, access_level='write')
    result = service.import_missing_file_from_source(path='staff/report.txt', source_path=str(source), apply=True)
    assert result['path'] == 'Staff/report.txt'
    assert service.registry.get_file_by_path(result['path']).folder_id == original.id
    with service.registry._connect() as db:
        assert db.execute('SELECT count(*) FROM cloud_permissions WHERE resource_id=?', (original.id,)).fetchone()[0] == 1


def test_case_variant_filename_is_not_duplicated(setup):
    service, source = setup
    service.import_missing_file_from_source(path='REPORT.txt', source_path=str(source), apply=True)
    with pytest.raises(RuntimeError, match='already exists'):
        service.import_missing_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert service.registry.get_file_by_path('report.txt') is None


def test_deleted_parent_is_not_restored(setup):
    service, source = setup
    service.registry.create_folder(name='Staff')
    service.registry.delete_folder('Staff')
    with pytest.raises(RuntimeError, match='deleted folder'):
        service.import_missing_file_from_source(path='staff/report.txt', source_path=str(source), apply=True)
    assert service.registry.get_folder_by_path('Staff') is None


def test_file_cannot_become_parent_folder(setup):
    service, source = setup
    service.import_missing_file_from_source(path='staff', source_path=str(source), apply=True)
    with pytest.raises(RuntimeError, match='conflicts'):
        service.import_missing_file_from_source(path='Staff/report.txt', source_path=str(source), apply=True)


def test_concurrent_upload_wins_without_losing_history(setup, monkeypatch):
    service, source = setup
    put = service.storage.put_file

    def concurrent_upload(path, key):
        put(path, key)
        service.registry.upsert_file(
            folder_id=service.registry.get_root_folder().id, path='report.txt', name='report.txt',
            storage_key=key, mime_type='text/plain', size_bytes=source.stat().st_size, checksum='concurrent',
        )

    monkeypatch.setattr(service.storage, 'put_file', concurrent_upload)
    with pytest.raises(RuntimeError, match='already exists'):
        service.import_missing_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert service.registry.get_file_by_path('report.txt').checksum == 'concurrent'
    assert len(service.registry.list_file_versions(path='report.txt')) == 1


@pytest.mark.parametrize('operation', ['move', 'delete'])
def test_concurrent_parent_change_aborts_publication(setup, monkeypatch, operation):
    service, source = setup
    put = service.storage.put_file

    def change_parent(path, key):
        put(path, key)
        if operation == 'move':
            service.registry.rename_move_folder(source_path='Staff', new_name='Moved')
        else:
            service.registry.delete_folder('Staff')

    monkeypatch.setattr(service.storage, 'put_file', change_parent)
    with pytest.raises(RuntimeError, match='parent changed'):
        service.import_missing_file_from_source(path='Staff/report.txt', source_path=str(source), apply=True)
    assert service.registry.get_file_by_path('Staff/report.txt') is None


def test_bad_readback_never_publishes_file(setup, monkeypatch):
    service, source = setup
    monkeypatch.setattr(service.storage, 'download_file', lambda key, path: path.write_bytes(b'damaged'))
    with pytest.raises(RuntimeError, match='checksum verification'):
        service.import_missing_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert service.registry.get_file_by_path('report.txt') is None


def test_source_change_aborts_publication(setup, monkeypatch):
    from rag_catalog.core.cloud_drive import service as module

    service, source = setup
    copy = module.shutil.copyfile

    def changed(path, destination):
        copy(path, destination)
        source.write_bytes(b'another edit')

    monkeypatch.setattr(module.shutil, 'copyfile', changed)
    with pytest.raises(RuntimeError, match='Source changed'):
        service.import_missing_file_from_source(path='report.txt', source_path=str(source), apply=True)
    assert service.registry.get_file_by_path('report.txt') is None


def test_live_mount_is_not_imported(setup):
    from types import SimpleNamespace

    service, source = setup
    service.shared_folders = SimpleNamespace(mount=lambda path: object())
    with pytest.raises(RuntimeError, match='shared folders'):
        service.import_missing_file_from_source(path='Scans/report.txt', source_path=str(source), apply=True)
    assert service.registry.get_folder_by_path('Scans') is None


def test_ambiguous_casefold_folder_is_rejected(setup):
    service, _ = setup
    service.registry.create_folder(name='Staff')
    service.registry.create_folder(name='STAFF')
    with pytest.raises(RuntimeError, match='Ambiguous'):
        service.registry.ensure_import_folder(path='staff/2026')


@pytest.mark.parametrize('path', ['../file.txt', 'Staff/../file.txt', 'Staff/..', 'Staff/\x00'])
def test_invalid_destination_is_rejected(setup, path):
    service, source = setup
    with pytest.raises(ValueError):
        service.import_missing_file_from_source(path=path, source_path=str(source), apply=True)
