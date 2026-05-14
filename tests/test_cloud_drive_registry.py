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


def test_registry_upsert_file_is_idempotent_for_same_content(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    root = registry.ensure_root_folder(root_name='Обмен', source_path='O:/Обмен')

    first = registry.upsert_file(
        folder_id=root.id,
        path='same.txt',
        name='same.txt',
        storage_key='objects/sha256/ab/cd/abcd.txt',
        mime_type='text/plain',
        size_bytes=12,
        checksum='abcd',
        source_path='O:/Обмен/same.txt',
    )
    second = registry.upsert_file(
        folder_id=root.id,
        path='same.txt',
        name='same.txt',
        storage_key='objects/sha256/ab/cd/abcd.txt',
        mime_type='text/plain',
        size_bytes=12,
        checksum='abcd',
        source_path='O:/Обмен/same.txt',
        source_mtime=123.0,
    )

    versions = registry.list_file_versions(path='same.txt')

    assert first.id == second.id
    assert first.current_version_id == second.current_version_id
    assert len(versions) == 1
    assert registry.stats().versions == 1


def test_registry_compacts_duplicate_versions(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    root = registry.ensure_root_folder(root_name='Обмен', source_path='O:/Обмен')
    file_row = registry.upsert_file(
        folder_id=root.id,
        path='same.txt',
        name='same.txt',
        storage_key='objects/sha256/ab/cd/abcd.txt',
        mime_type='text/plain',
        size_bytes=12,
        checksum='abcd',
        source_path='O:/Обмен/same.txt',
    )
    with registry._connect() as conn:
        conn.execute(
            """
            INSERT INTO cloud_file_versions (id, file_id, storage_key, checksum, size_bytes, source_path, created_by, created_at)
            VALUES ('duplicate-version', ?, ?, ?, ?, '', '', '2026-05-13T00:00:00+00:00')
            """,
            (file_row.id, file_row.storage_key, file_row.checksum, file_row.size_bytes),
        )

    deleted = registry.compact_duplicate_versions()

    assert deleted == 1
    assert len(registry.list_file_versions(path='same.txt')) == 1
    assert registry.get_file_by_path('same.txt').current_version_id == file_row.current_version_id  # type: ignore[union-attr]


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
    folder = registry.get_folder_by_path('Folder A')
    assert folder is not None
    file_row = registry.get_file_by_path('Folder A/hello.txt')
    assert file_row is not None
    assert file_row.size_bytes == 5
    assert file_row.storage_key.startswith('objects/sha256/')
    assert Path(storage.resolve_path(file_row.storage_key)).exists()

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
    uploaded_row = registry.get_file_by_path('Folder A/new.txt')
    assert uploaded_row is not None
    assert uploaded_row.storage_key.startswith('objects/sha256/')
    assert Path(storage.resolve_path(uploaded_row.storage_key)).exists()

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
    current_storage_key = versions['versions'][0]['storage_key']

    moved_file = service.move_node(source_path='Folder A/new.txt', dest_parent_path='', new_name='renamed.txt')
    assert moved_file['node_type'] == 'file'
    assert moved_file['path'] == 'renamed.txt'
    moved_file_row = registry.get_file_by_path('renamed.txt')
    assert moved_file_row is not None
    assert moved_file_row.storage_key == current_storage_key
    assert Path(storage.resolve_path(moved_file_row.storage_key)).exists()

    moved_folder = service.move_node(source_path='Folder A', dest_parent_path='', new_name='Archive')
    assert moved_folder['node_type'] == 'folder'
    assert moved_folder['path'] == 'Archive'
    assert registry.get_folder_by_path('Archive') is not None
    archived_hello = registry.get_file_by_path('Archive/hello.txt')
    assert archived_hello is not None
    assert archived_hello.storage_key == file_row.storage_key

    deleted_file = service.delete_node('renamed.txt')
    assert deleted_file['node_type'] == 'file'
    assert registry.get_file_by_path('renamed.txt') is not None
    assert registry.get_file_by_path('renamed.txt').deleted_at != ''
    assert Path(storage.resolve_path(current_storage_key)).exists()

    deleted_folder = service.delete_node('Archive')
    assert deleted_folder['node_type'] == 'folder'
    assert registry.get_folder_by_path('Archive') is None
    deleted_child = registry.get_file_by_path('Archive/hello.txt')
    assert deleted_child is not None
    assert deleted_child.deleted_at != ''
    trash = service.list_trash()
    trash_paths = {item['path'] for item in trash['items']}
    assert {'renamed.txt', 'Archive', 'Archive/hello.txt'} <= trash_paths
    assert registry.list_file_versions(path='Archive/hello.txt')

    restored_folder = service.restore_node('Archive')
    assert restored_folder['node_type'] == 'folder'
    assert restored_folder['deleted_at'] == ''
    assert registry.get_folder_by_path('Archive') is not None
    assert registry.get_file_by_path('Archive/hello.txt').deleted_at == ''


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


def test_service_move_queues_cleanup_before_reindexing_new_path(tmp_path: Path, monkeypatch) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    registry.upsert_folder(path='Folder A', name='Folder A', parent_id=root.id, depth=1, source_path='')
    source = tmp_path / 'hello.txt'
    source.write_text('hello', encoding='utf-8')

    uploaded = service.upload_file(parent_path='Folder A', filename='hello.txt', source_path=str(source), mime_type='text/plain')
    moved = service.move_node(source_path='Folder A/hello.txt', dest_parent_path='', new_name='renamed.txt')
    pending = registry.list_pending_jobs(job_types=['reindex', 'cleanup'], limit=10)

    assert [job.job_type for job in pending] == ['reindex', 'cleanup', 'reindex']
    assert pending[1].progress['action'] == 'cleanup'
    assert pending[1].progress['path'] == 'Folder A/hello.txt'
    assert pending[2].progress['action'] == 'reindex'
    assert pending[2].progress['path'] == 'renamed.txt'

    reindexed_paths: list[str] = []
    cleanup_matches: list[dict[str, object]] = []

    def _fake_reindex(*, target_path, file_row, index_config):  # noqa: ANN001
        reindexed_paths.append(file_row.path)
        return True, 1

    def _fake_cleanup(*, index_config, payload_match):  # noqa: ANN001
        cleanup_matches.append(payload_match)
        return 1

    monkeypatch.setattr(service, '_run_indexer_for_file', _fake_reindex)
    monkeypatch.setattr(service, '_delete_index_vectors', _fake_cleanup)
    completed = service.run_pending_reindex_jobs(index_config={'qdrant_url': 'http://localhost:6333'}, limit=10)

    assert [job.status for job in completed] == ['completed', 'completed', 'completed']
    assert reindexed_paths == ['renamed.txt', 'renamed.txt']
    assert cleanup_matches == [{'cloud_file_id': uploaded['id']}]
    assert moved['path'] == 'renamed.txt'


def test_service_delete_restore_folder_queue_cleanup_and_reindex_children(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    registry.upsert_folder(path='Folder A', name='Folder A', parent_id=root.id, depth=1, source_path='')
    source = tmp_path / 'hello.txt'
    source.write_text('hello', encoding='utf-8')
    service.upload_file(parent_path='Folder A', filename='hello.txt', source_path=str(source), mime_type='text/plain')

    service.delete_node('Folder A')
    cleanup_jobs = [job for job in registry.list_jobs(limit=10) if job.job_type == 'cleanup']

    assert cleanup_jobs
    assert cleanup_jobs[0].progress['action'] == 'cleanup'
    assert cleanup_jobs[0].progress['path'] == 'Folder A/hello.txt'
    assert cleanup_jobs[0].progress['reason'] == 'delete_folder'

    service.restore_node('Folder A')
    reindex_jobs = [job for job in registry.list_jobs(limit=10) if job.job_type == 'reindex']

    assert any(job.progress.get('reason') == 'restore_folder' and job.progress.get('path') == 'Folder A/hello.txt' for job in reindex_jobs)


def test_service_lists_cloud_drive_changes_for_sync_clients(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    registry.upsert_folder(path='Folder A', name='Folder A', parent_id=root.id, depth=1, source_path='')
    source = tmp_path / 'hello.txt'
    source.write_text('hello', encoding='utf-8')
    service.upload_file(parent_path='Folder A', filename='hello.txt', source_path=str(source), mime_type='text/plain')

    initial = service.list_changes()
    paths = {item['path'] for item in initial['changes']}
    assert 'Folder A' in paths
    assert 'Folder A/hello.txt' in paths
    cursor = initial['next_cursor']

    service.delete_node('Folder A/hello.txt')
    delta = service.list_changes(since=cursor)

    assert any(item['path'] == 'Folder A/hello.txt' and item['deleted_at'] for item in delta['changes'])
    assert delta['next_cursor'] >= cursor


def test_cloud_drive_sync_clients_pairs_selective_and_conflicts(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    registry.upsert_folder(path='Folder A', name='Folder A', parent_id=root.id, depth=1, source_path='')

    client = service.register_sync_client(
        username='User',
        device_id='desktop-1',
        display_name='Office PC',
        platform='windows',
        metadata={'version': '0.1'},
    )
    same_client = service.register_sync_client(
        username='user',
        device_id='desktop-1',
        display_name='Office PC renamed',
        platform='windows',
        status='paused',
    )

    assert same_client['id'] == client['id']
    assert same_client['username'] == 'user'
    assert same_client['status'] == 'paused'
    assert service.list_sync_clients(username='user')[0]['display_name'] == 'Office PC renamed'

    pair = service.upsert_sync_pair(
        client_id=client['id'],
        local_path='D:/Sync/Folder A',
        cloud_path='Folder A',
        conflict_policy='newest_wins',
    )
    pairs = service.list_sync_pairs(username='user')

    assert len(pairs) == 1
    assert pairs[0]['id'] == pair['id']
    assert pairs[0]['cloud_path'] == 'Folder A'
    assert pairs[0]['conflict_policy'] == 'newest_wins'

    selective = service.set_selective_sync_paths(
        client_id=client['id'],
        paths=['Folder A', 'Folder A'],
        mode='include',
    )
    assert selective['count'] == 1
    assert selective['paths'][0]['cloud_path'] == 'Folder A'
    assert selective['paths'][0]['mode'] == 'include'

    conflict = service.record_sync_conflict(
        client_id=client['id'],
        pair_id=pair['id'],
        path='Folder A/hello.txt',
        local_path='D:/Sync/Folder A/hello.txt',
        cloud_path='Folder A/hello.txt',
        conflict_type='both_modified',
        local_version='local-1',
        cloud_version='cloud-1',
        details={'size_mismatch': True},
    )
    open_conflicts = service.list_sync_conflicts(username='user')

    assert open_conflicts == [conflict]
    assert conflict['status'] == 'open'
    assert conflict['details']['size_mismatch'] is True

    resolved = service.resolve_sync_conflict(
        conflict['id'],
        resolution='cloud_wins',
        resolved_by='admin',
    )

    assert resolved['status'] == 'resolved'
    assert resolved['resolution'] == 'cloud_wins'
    assert resolved['resolved_by'] == 'admin'
    assert service.list_sync_conflicts(username='user') == []
    assert len(service.list_sync_conflicts(username='user', status='resolved')) == 1

    assert service.delete_sync_pair(pair['id']) == {'ok': True}
    assert service.list_sync_pairs(username='user') == []


def test_service_reindex_job_passes_cloud_identity_to_indexer(tmp_path: Path, monkeypatch) -> None:
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
    job = registry.get_latest_job(job_type='reindex')
    assert job is not None
    calls: dict[str, object] = {}

    class _FakeIndexer:
        def __init__(self, **kwargs):
            self.point_count = 0
            calls['init'] = kwargs

        def _delete_file_vectors(self, filepath, *, payload_match=None):
            calls['delete'] = {'filepath': filepath, 'payload_match': payload_match}

        def process_file(self, filepath, **kwargs):
            calls['process'] = {'filepath': filepath, 'kwargs': kwargs}
            self.point_count = 3

        def _flush_buffer(self):
            calls['flushed'] = True

    import rag_catalog.core.index_rag as index_rag

    monkeypatch.setattr(index_rag, 'RAGIndexer', _FakeIndexer)
    completed = service.run_reindex_job(
        job.id,
        index_config={
            'catalog_path': str(tmp_path / 'catalog'),
            'qdrant_db_path': str(tmp_path / 'qdrant'),
            'collection_name': 'catalog',
        },
    )

    assert completed.status == 'completed'
    assert completed.progress['indexed'] is True
    assert completed.progress['points_added'] == 3
    assert calls['delete']['payload_match'] == {'cloud_file_id': uploaded['id']}  # type: ignore[index]
    process_kwargs = calls['process']['kwargs']  # type: ignore[index]
    assert process_kwargs['logical_path'] == 'Folder A/hello.txt'
    assert process_kwargs['state_key'] == f"cloud:{uploaded['id']}"
    assert process_kwargs['payload_extra']['cloud_file_id'] == uploaded['id']
    assert process_kwargs['payload_extra']['cloud_path'] == 'Folder A/hello.txt'


def test_service_runs_pending_reindex_jobs_fifo(tmp_path: Path, monkeypatch) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    registry.upsert_folder(path='Folder A', name='Folder A', parent_id=root.id, depth=1, source_path='')
    first_source = tmp_path / 'a.txt'
    second_source = tmp_path / 'b.txt'
    first_source.write_text('a', encoding='utf-8')
    second_source.write_text('b', encoding='utf-8')
    first = service.upload_file(parent_path='Folder A', filename='a.txt', source_path=str(first_source), mime_type='text/plain')
    second = service.upload_file(parent_path='Folder A', filename='b.txt', source_path=str(second_source), mime_type='text/plain')
    seen: list[str] = []

    class _FakeIndexer:
        def __init__(self, **_kwargs):
            self.point_count = 1

        def _delete_file_vectors(self, filepath, *, payload_match=None):
            pass

        def process_file(self, filepath, **kwargs):
            seen.append(str(kwargs.get('logical_path') or ''))

        def _flush_buffer(self):
            pass

    import rag_catalog.core.index_rag as index_rag

    monkeypatch.setattr(index_rag, 'RAGIndexer', _FakeIndexer)
    completed = service.run_pending_reindex_jobs(
        index_config={
            'catalog_path': str(tmp_path / 'catalog'),
            'qdrant_db_path': str(tmp_path / 'qdrant'),
            'collection_name': 'catalog',
        },
        limit=10,
    )

    assert [job.file_id for job in completed] == [first['id'], second['id']]
    assert seen == ['Folder A/a.txt', 'Folder A/b.txt']
    assert all(job.status == 'completed' for job in completed)


def test_service_retry_job_requeues_reindex(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    job = registry.queue_job(
        job_type='reindex',
        status='failed',
        file_id='file-1',
        version_id='version-1',
        payload={'path': 'Folder A/hello.txt', 'progress': {'status': 'failed'}},
    )

    retried = service.retry_job(job.id)

    assert retried.id != job.id
    assert retried.status == 'pending'
    assert retried.file_id == 'file-1'
    assert retried.version_id == 'version-1'
    assert retried.payload['retried_from_job_id'] == job.id
    assert retried.progress['status'] == 'pending'


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
        folder_columns = {row[1] for row in conn.execute("PRAGMA table_info(cloud_folders)").fetchall()}
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        version = conn.execute("SELECT schema_version FROM schema_meta WHERE db_kind='cloud_drive'").fetchone()[0]
    assert 'started_at' in columns
    assert 'finished_at' in columns
    assert 'deleted_at' in folder_columns
    assert 'cloud_sync_clients' in tables
    assert 'cloud_sync_pairs' in tables
    assert 'cloud_sync_selective_paths' in tables
    assert 'cloud_sync_conflicts' in tables
    assert int(version) == 4


def test_registry_repairs_current_version_missing_source_mtime_columns(tmp_path: Path) -> None:
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
            VALUES ('cloud_drive', 4, '2026-05-07T00:00:00+00:00', '')
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
                updated_at TEXT NOT NULL,
                deleted_at TEXT NOT NULL DEFAULT ''
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

    CloudDriveRegistryDB(str(db_path))

    with sqlite3.connect(db_path) as conn:
        folder_columns = {row[1] for row in conn.execute("PRAGMA table_info(cloud_folders)").fetchall()}
        file_columns = {row[1] for row in conn.execute("PRAGMA table_info(cloud_files)").fetchall()}

    assert 'source_mtime' in folder_columns
    assert 'source_mtime' in file_columns
