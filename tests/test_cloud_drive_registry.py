from __future__ import annotations

import sqlite3
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

    root_node = service.get_node('')
    assert root_node['node_type'] == 'folder'
    assert root_node['is_root'] is True

    folder_node = service.get_node('Folder A')
    assert folder_node['node_type'] == 'folder'
    assert folder_node['name'] == 'Folder A'

    file_node = service.get_node('Folder A/hello.txt')
    assert file_node['node_type'] == 'file'
    assert file_node['size_bytes'] == 5

    listing = service.list_directory('Folder A')
    assert listing['folder']['path'] == 'Folder A'
    assert listing['folders'] == []
    assert [item['name'] for item in listing['files']] == ['hello.txt']

    created = service.create_folder(parent_path='Folder A', name='Nested')
    assert created['node_type'] == 'folder'
    assert created['path'] == 'Folder A/Nested'
    assert registry.get_folder_by_path('Folder A/Nested') is not None

    descriptor = service.get_download_descriptor('Folder A/hello.txt')
    assert descriptor['mode'] == 'local_file'
    assert descriptor['filename'] == 'hello.txt'
    assert Path(descriptor['file_path']).exists()

    upload_source = tmp_path / 'new.txt'
    upload_source.write_text('new-content', encoding='utf-8')
    uploaded = service.upload_file(
        parent_path='Folder A',
        filename='new.txt',
        source_path=str(upload_source),
        mime_type='text/plain',
    )
    assert uploaded['node_type'] == 'file'
    assert uploaded['path'] == 'Folder A/new.txt'
    assert registry.get_file_by_path('Folder A/new.txt') is not None
    assert (storage_root / 'Folder A' / 'new.txt').exists()

    second_upload_source = tmp_path / 'new-v2.txt'
    second_upload_source.write_text('new-content-v2', encoding='utf-8')
    service.upload_file(
        parent_path='Folder A',
        filename='new.txt',
        source_path=str(second_upload_source),
        mime_type='text/plain',
    )
    versions = service.list_versions('Folder A/new.txt')
    assert versions['file']['path'] == 'Folder A/new.txt'
    assert len(versions['versions']) == 2
    assert versions['versions'][0]['is_current'] is True

    moved_file = service.move_node(source_path='Folder A/new.txt', dest_parent_path='', new_name='renamed.txt')
    assert moved_file['node_type'] == 'file'
    assert moved_file['path'] == 'renamed.txt'
    assert registry.get_file_by_path('renamed.txt') is not None
    assert (storage_root / 'renamed.txt').exists()

    moved_folder = service.move_node(source_path='Folder A', dest_parent_path='', new_name='Archive')
    assert moved_folder['node_type'] == 'folder'
    assert moved_folder['path'] == 'Archive'
    assert registry.get_folder_by_path('Archive') is not None
    assert registry.get_file_by_path('Archive/hello.txt') is not None
    assert (storage_root / 'Archive' / 'hello.txt').exists()

    deleted_file = service.delete_node('renamed.txt')
    assert deleted_file['node_type'] == 'file'
    assert registry.get_file_by_path('renamed.txt') is not None
    assert registry.get_file_by_path('renamed.txt').deleted_at != ''
    assert not (storage_root / 'renamed.txt').exists()

    deleted_folder = service.delete_node('Archive')
    assert deleted_folder['node_type'] == 'folder'
    assert registry.get_folder_by_path('Archive') is None
    assert registry.get_file_by_path('Archive/hello.txt') is None


def test_service_auto_queues_and_runs_reindex_jobs(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    registry.upsert_folder(path='Folder A', name='Folder A', parent_id=root.id, depth=1, source_path='')
    source = tmp_path / 'hello.txt'
    source.write_text('hello', encoding='utf-8')

    uploaded = service.upload_file(
        parent_path='Folder A',
        filename='hello.txt',
        source_path=str(source),
        mime_type='text/plain',
    )
    reindex_job = registry.get_latest_job(job_type='reindex')

    assert reindex_job is not None
    assert reindex_job.file_id == uploaded['id']
    assert reindex_job.progress['status'] == 'pending'

    completed = service.run_reindex_job(reindex_job.id)

    assert completed.status == 'completed'
    assert completed.progress['status'] == 'done'
    assert completed.progress['path'] == 'Folder A/hello.txt'

    service.delete_node('Folder A/hello.txt')
    cleanup_job = registry.get_latest_job(job_type='cleanup')

    assert cleanup_job is not None
    assert cleanup_job.progress['action'] == 'cleanup'
    assert cleanup_job.progress['path'] == 'Folder A/hello.txt'
    assert service.run_reindex_job(cleanup_job.id).status == 'completed'


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
    assert updated.started_at != ''
    assert updated.finished_at == ''

    completed = registry.update_job(
        job.id,
        status='completed',
        payload={'progress': {'status': 'done', 'imported_files': 10}},
    )
    assert completed.finished_at != ''

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


def test_registry_migrates_v1_cloud_jobs_to_v2(tmp_path: Path) -> None:
    db_path = tmp_path / 'registry.db'
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE schema_meta (
                db_kind TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                code_root TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            INSERT INTO schema_meta (db_kind, schema_version, updated_at, code_root)
            VALUES ('cloud_drive', 1, '2026-05-07T00:00:00+00:00', '')
            """
        )
        conn.execute(
            """
            CREATE TABLE cloud_jobs (
                id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                file_id TEXT NOT NULL DEFAULT '',
                version_id TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE cloud_folders (
                id TEXT PRIMARY KEY,
                parent_id TEXT,
                name TEXT NOT NULL,
                path TEXT NOT NULL UNIQUE,
                depth INTEGER NOT NULL DEFAULT 0,
                source_path TEXT NOT NULL DEFAULT '',
                is_root INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE cloud_files (
                id TEXT PRIMARY KEY,
                folder_id TEXT NOT NULL,
                name TEXT NOT NULL,
                path TEXT NOT NULL UNIQUE,
                storage_key TEXT NOT NULL,
                mime_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                size_bytes INTEGER NOT NULL DEFAULT 0,
                checksum TEXT NOT NULL DEFAULT '',
                source_path TEXT NOT NULL DEFAULT '',
                current_version_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE cloud_file_versions (
                id TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                storage_key TEXT NOT NULL,
                checksum TEXT NOT NULL DEFAULT '',
                size_bytes INTEGER NOT NULL DEFAULT 0,
                source_path TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE cloud_permissions (
                id TEXT PRIMARY KEY,
                subject_type TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                access_level TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

    registry = CloudDriveRegistryDB(str(db_path))
    migrated = registry.queue_job(job_type='bootstrap', payload={'progress': {'status': 'pending'}})
    fetched = registry.get_job(migrated.id)

    assert fetched is not None
    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(cloud_jobs)").fetchall()}
        version = conn.execute("SELECT schema_version FROM schema_meta WHERE db_kind='cloud_drive'").fetchone()[0]
    assert 'started_at' in columns
    assert 'finished_at' in columns
    assert int(version) == 2
