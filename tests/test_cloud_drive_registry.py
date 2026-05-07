from __future__ import annotations

from pathlib import Path

from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB
from rag_catalog.core.cloud_drive.service import CloudDriveService
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
