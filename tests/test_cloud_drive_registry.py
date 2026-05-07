from __future__ import annotations

from pathlib import Path

from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB
from rag_catalog.core.cloud_drive.service import CloudDriveJobCancelled, CloudDriveService
from rag_catalog.core.cloud_drive.storage import LocalStorageAdapter


def test_registry_root_folder_and_stats(tmp_path: Path) -> None:
    db_path = tmp_path / 'cloud_drive.db'
    registry = CloudDriveRegistryDB(str(db_path))

    root = registry.ensure_root_folder(root_name='Обмен', source_path='O:/Обмен')
    child = registry.upsert_folder(path='Contracts', name='Contracts', parent_id=root.id, depth=1, source_path='O:/Обмен/Contracts')
    registry.upsert_file(
        folder_id=child.id,
        path='Contracts/test.txt',
        name='test.txt',
        storage_key='Contracts/test.txt',
        mime_type='text/plain',
        size_bytes=12,
        checksum='abc',
        source_path='O:/Обмен/Contracts/test.txt',
    )
    registry.queue_job(job_type='reindex', payload={'path': 'Contracts/test.txt'})

    stats = registry.stats()
    assert stats.folders == 2
    assert stats.files == 1
    assert stats.versions == 1
    assert stats.pending_jobs == 1
    assert root.is_root is True


def test_service_bootstrap_from_catalog(tmp_path: Path) -> None:
    catalog = tmp_path / 'catalog'
    (catalog / 'Folder A').mkdir(parents=True)
    (catalog / 'Folder A' / 'hello.txt').write_text('hello', encoding='utf-8')
    (catalog / 'root.pdf').write_bytes(b'%PDF-1.4\n')

    storage_root = tmp_path / 'storage'
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(storage_root))
    service = CloudDriveService(registry=registry, storage=storage)

    stats = service.bootstrap_from_catalog(str(catalog), import_files=True)

    assert stats.folders >= 2
    assert stats.files == 2
    assert (storage_root / 'Folder A' / 'hello.txt').exists()
    assert (storage_root / 'root.pdf').exists()
    folder = registry.get_folder_by_path('Folder A')
    assert folder is not None
    file_row = registry.get_file_by_path('Folder A/hello.txt')
    assert file_row is not None
    assert file_row.size_bytes == 5


def test_registry_job_lifecycle(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    job = registry.queue_job(job_type='bootstrap', payload={'catalog_root': 'O:/Обмен', 'progress': {'status': 'pending'}})

    fetched = registry.get_job(job.id)
    assert fetched is not None
    assert fetched.status == 'pending'
    assert fetched.progress['status'] == 'pending'

    updated = registry.update_job(
        job.id,
        status='running',
        payload={'progress': {'status': 'running', 'imported_files': 10}},
    )
    assert updated.status == 'running'
    assert updated.progress['imported_files'] == 10

    latest = registry.get_latest_job(job_type='bootstrap')
    assert latest is not None
    assert latest.id == job.id


def test_service_bootstrap_job(tmp_path: Path) -> None:
    catalog = tmp_path / 'catalog'
    (catalog / 'Folder A').mkdir(parents=True)
    (catalog / 'Folder A' / 'hello.txt').write_text('hello', encoding='utf-8')
    (catalog / 'root.pdf').write_bytes(b'%PDF-1.4\n')

    storage_root = tmp_path / 'storage'
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(storage_root))
    service = CloudDriveService(registry=registry, storage=storage)

    job = service.create_bootstrap_job(catalog_root=str(catalog), import_files=True)
    stats = service.run_bootstrap_job(job.id)

    assert stats.files == 2
    saved = registry.get_job(job.id)
    assert saved is not None
    assert saved.status == 'completed'
    assert saved.progress['status'] == 'done'
    assert saved.progress['imported_files'] == 2


def test_service_cancel_pending_job(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)

    job = service.create_bootstrap_job(catalog_root='O:/Обмен', import_files=False)
    cancelled = service.cancel_job(job.id)
    assert cancelled.status == 'cancelled'
    assert cancelled.progress['status'] == 'cancelled'


def test_service_retry_and_recover_jobs(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)

    job = service.create_bootstrap_job(catalog_root='O:/Обмен', import_files=True)
    registry.update_job(job.id, status='running', payload={'progress': {'status': 'running'}})

    recovered = service.recover_bootstrap_jobs()
    assert recovered == 1
    stale = registry.get_job(job.id)
    assert stale is not None
    assert stale.status == 'failed'
    assert stale.progress['status'] == 'stale'

    retried = service.retry_bootstrap_job(job.id)
    assert retried.id != job.id
    assert retried.status == 'pending'


def test_bootstrap_from_catalog_can_be_cancelled(tmp_path: Path) -> None:
    catalog = tmp_path / 'catalog'
    catalog.mkdir()
    for idx in range(5):
        (catalog / f'{idx}.txt').write_text('hello', encoding='utf-8')
    storage_root = tmp_path / 'storage'
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(storage_root))
    service = CloudDriveService(registry=registry, storage=storage)

    calls = {'count': 0}

    def should_continue() -> bool:
        calls['count'] += 1
        return calls['count'] < 3

    try:
        service.bootstrap_from_catalog(str(catalog), should_continue=should_continue)
        assert False, 'expected cancellation'
    except CloudDriveJobCancelled:
        pass
