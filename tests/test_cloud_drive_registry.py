from __future__ import annotations

import sqlite3
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

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


def test_registry_folder_size_bytes_map_sums_descendants(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    root = registry.ensure_root_folder(root_name='Обмен', source_path='O:/Обмен')
    docs = registry.upsert_folder(path='Docs', name='Docs', parent_id=root.id, depth=1)
    archive = registry.upsert_folder(path='Docs/Archive', name='Archive', parent_id=docs.id, depth=2)
    empty = registry.upsert_folder(path='Empty', name='Empty', parent_id=root.id, depth=1)
    registry.upsert_file(
        folder_id=docs.id,
        path='Docs/current.pdf',
        name='current.pdf',
        storage_key='objects/current',
        mime_type='application/pdf',
        size_bytes=12,
        checksum='current',
    )
    registry.upsert_file(
        folder_id=archive.id,
        path='Docs/Archive/old.pdf',
        name='old.pdf',
        storage_key='objects/old',
        mime_type='application/pdf',
        size_bytes=8,
        checksum='old',
    )

    sizes = registry.folder_size_bytes_map([root.id, docs.id, archive.id, empty.id])

    assert sizes[root.id] == 20
    assert sizes[docs.id] == 20
    assert sizes[archive.id] == 8
    assert sizes[empty.id] == 0


def test_service_create_blank_files(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    registry.ensure_root_folder(root_name='Обмен', source_path='')

    word = service.create_blank_file(parent_path='', filename='Plan', file_type='word')
    excel = service.create_blank_file(parent_path='', filename='Budget', file_type='excel')
    text = service.create_blank_file(parent_path='', filename='Notes', file_type='text')

    assert word['name'] == 'Plan.docx'
    assert word['mime_type'] == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    assert excel['name'] == 'Budget.xlsx'
    assert excel['mime_type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    assert text['name'] == 'Notes.txt'
    assert text['mime_type'] == 'text/plain'
    assert text['size_bytes'] == 0
    with zipfile.ZipFile(storage.root / word['storage_key']) as archive:
        assert 'word/document.xml' in archive.namelist()
    with zipfile.ZipFile(storage.root / excel['storage_key']) as archive:
        assert 'xl/workbook.xml' in archive.namelist()
        assert 'xl/worksheets/sheet1.xml' in archive.namelist()


def test_service_create_blank_file_refuses_existing_path(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    service = CloudDriveService(registry=registry, storage=LocalStorageAdapter(str(tmp_path / 'storage')))
    registry.ensure_root_folder(root_name='Обмен', source_path='')
    service.create_blank_file(parent_path='', filename='Plan.docx', file_type='word')

    with pytest.raises(RuntimeError, match='уже существует'):
        service.create_blank_file(parent_path='', filename='Plan.docx', file_type='word')


def test_registry_search_nodes_page_filters_and_paginates(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    root = registry.ensure_root_folder(root_name='Обмен', source_path='O:/Обмен')
    docs = registry.upsert_folder(path='Docs', name='Docs', parent_id=root.id, depth=1)
    registry.upsert_file(
        folder_id=docs.id,
        path='Docs/Alpha Contract.pdf',
        name='Alpha Contract.pdf',
        storage_key='objects/a',
        mime_type='application/pdf',
        size_bytes=12,
        checksum='a',
    )
    registry.upsert_file(
        folder_id=docs.id,
        path='Docs/Alpha Budget.xlsx',
        name='Alpha Budget.xlsx',
        storage_key='objects/b',
        mime_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        size_bytes=8,
        checksum='b',
    )

    page = registry.search_nodes_page(query='Alpha', path='Docs', limit=1, node_type='file')

    assert page['total'] == 2
    assert page['count'] == 1
    assert page['next_offset'] == 1

    pdf_page = registry.search_nodes_page(query='Alpha', path='Docs', extension='pdf')

    assert pdf_page['total'] == 1
    assert pdf_page['items'][0]['path'] == 'Docs/Alpha Contract.pdf'


def test_registry_user_permissions_close_open_default_for_regular_users(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    root = registry.ensure_root_folder(root_name='Обмен', source_path='O:/Обмен')
    folder = registry.upsert_folder(path='Projects/A', name='A', parent_id=root.id, depth=2)
    file_row = registry.upsert_file(
        folder_id=folder.id,
        path='Projects/A/report.txt',
        name='report.txt',
        storage_key='objects/sha256/aa/bb/aabb.txt',
        mime_type='text/plain',
        size_bytes=12,
        checksum='aabb',
        source_path='O:/Обмен/Projects/A/report.txt',
    )

    assert registry.user_can_access(username='maria', role='viewer', path='Private/secret.txt')

    registry.grant_permission(
        subject_type='role',
        subject_id='viewer',
        resource_type='folder',
        resource_id=folder.id,
        access_level='viewer',
    )

    assert registry.user_can_access(username='maria', role='viewer', path='Projects/A/report.txt')
    assert registry.user_can_access(username='maria', role='viewer', file_id=file_row.id)
    assert not registry.user_can_access(username='maria', role='viewer', path='Private/secret.txt')
    assert not registry.user_can_access(username='maria', role='viewer', path='Projects/A/report.txt', required_level='editor')
    assert registry.user_can_access(username='root', role='admin', path='Private/secret.txt', required_level='admin')


def test_registry_group_permission_uses_explicit_membership(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    root = registry.ensure_root_folder(root_name='Обмен')
    folder = registry.upsert_folder(path='Finance', name='Finance', parent_id=root.id, depth=1)
    registry.grant_permission(
        subject_type='group',
        subject_id='group-finance',
        resource_type='folder',
        resource_id=folder.id,
        access_level='viewer',
    )

    assert registry.user_can_access(
        username='alice',
        role='user',
        groups=['group-finance'],
        path='Finance/report.pdf',
    )
    assert not registry.user_can_access(username='alice', role='user', groups=[], path='Finance/report.pdf')
    assert not registry.user_can_access(
        username='alice',
        role='user',
        groups=['another-group'],
        path='Finance/report.pdf',
    )


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


def test_registry_permissions_inherit_from_folder_and_preserve_open_default(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    folder = registry.upsert_folder(path='Projects', name='Projects', parent_id=root.id, depth=1, source_path='')
    file_row = registry.upsert_file(
        folder_id=folder.id,
        path='Projects/plan.docx',
        name='plan.docx',
        storage_key='objects/plan',
        mime_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        size_bytes=5,
        checksum='abc',
        source_path='',
    )

    assert registry.user_can_access(username='alice', role='user', path=file_row.path)

    registry.grant_permission(
        subject_type='user',
        subject_id='alice',
        resource_type='folder',
        resource_id=folder.id,
        access_level='viewer',
    )

    assert registry.user_can_access(username='alice', role='user', path=file_row.path)
    assert not registry.user_can_access(username='bob', role='user', path=file_row.path)
    assert not registry.user_can_access(username='alice', role='user', path=file_row.path, required_level='editor')
    assert registry.user_can_access(username='anyone', role='admin', path=file_row.path, required_level='admin')


def test_registry_user_home_folder_is_visible_but_private(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    alice_home = registry.ensure_user_home_folder(username='Alice')
    root = registry.get_root_folder()
    assert root is not None
    file_row = registry.upsert_file(
        folder_id=alice_home.id,
        path='alice/private.txt',
        name='private.txt',
        storage_key='objects/private',
        mime_type='text/plain',
        size_bytes=7,
        checksum='abc',
        source_path='',
    )

    assert alice_home.path == 'alice'
    assert registry.user_can_access(username='bob', role='user', path='')
    assert registry.user_can_access(username='bob', role='user', path='alice')
    assert not registry.user_can_access(username='bob', role='user', path=file_row.path)
    assert registry.user_can_access(username='alice', role='user', path=file_row.path, required_level='editor')
    assert registry.user_can_access(username='alice', role='user', path=file_row.path, required_level='admin')


def test_registry_user_home_folder_is_restored_after_rename(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    home = registry.ensure_user_home_folder(username='Alice')
    registry.upsert_file(
        folder_id=home.id,
        path='alice/doc.txt',
        name='doc.txt',
        storage_key='objects/doc',
        mime_type='text/plain',
        size_bytes=3,
        checksum='abc',
        source_path='',
    )

    registry.rename_move_folder(source_path='alice', new_name='alice_old')
    restored = registry.ensure_user_home_folder(username='Alice')

    assert restored.path == 'alice'
    assert registry.get_file_by_path('alice/doc.txt') is not None
    assert registry.get_folder_by_path('alice_old') is None


def test_registry_public_share_link_is_read_only(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    home = registry.ensure_user_home_folder(username='alice')
    file_row = registry.upsert_file(
        folder_id=home.id,
        path='alice/public.txt',
        name='public.txt',
        storage_key='objects/public',
        mime_type='text/plain',
        size_bytes=6,
        checksum='abc',
        source_path='',
    )

    link = registry.create_share_link(path=file_row.path, created_by='alice')

    assert registry.share_link_can_access(token=link['token'], path=file_row.path)
    assert not registry.share_link_can_access(token=link['token'], path=file_row.path, required_level='editor')
    assert not registry.share_link_can_access(token=link['token'], path='alice/other.txt')


def test_registry_public_share_link_lifecycle_and_expiry_validation(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'cloud_drive.db'))
    home = registry.ensure_user_home_folder(username='alice')
    file_row = registry.upsert_file(
        folder_id=home.id,
        path='alice/shared.txt',
        name='shared.txt',
        storage_key='objects/shared',
        mime_type='text/plain',
        size_bytes=6,
        checksum='abc',
        source_path='',
    )
    expires_at = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

    link = registry.create_share_link(path=file_row.path, created_by='alice', expires_at=expires_at)
    active = registry.list_share_links(path=file_row.path)

    assert [item['token'] for item in active] == [link['token']]
    assert active[0]['expires_at'] == expires_at
    assert registry.revoke_share_link(link['token']) is True
    assert registry.list_share_links(path=file_row.path) == []
    assert registry.list_share_links(path=file_row.path, include_inactive=True)[0]['revoked_at']
    assert not registry.share_link_can_access(token=link['token'], path=file_row.path)

    with pytest.raises(RuntimeError, match='ISO-формате'):
        registry.create_share_link(path=file_row.path, expires_at='завтра')
    with pytest.raises(RuntimeError, match='в будущем'):
        registry.create_share_link(path=file_row.path, expires_at='2020-01-01T00:00:00+00:00')


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


def test_cloud_drive_import_source_job_imports_files_and_queues_reindex(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    source_root = tmp_path / 'scanner'
    nested = source_root / 'Inbox'
    nested.mkdir(parents=True)
    (nested / 'scan.txt').write_text('scan payload', encoding='utf-8')

    source = service.upsert_import_source(
        name='Scanner inbox',
        source_path=str(source_root),
        target_path='Imports/Scanner',
        import_files=True,
        created_by='admin',
    )
    job = service.create_import_job(source_id=source['id'])
    stats = service.run_import_job(job.id)

    file_row = registry.get_file_by_path('Imports/Scanner/Inbox/scan.txt')
    assert file_row is not None
    assert file_row.source_path.endswith('scanner\\Inbox\\scan.txt') or file_row.source_path.endswith('scanner/Inbox/scan.txt')
    assert storage.exists(file_row.storage_key)
    assert stats['imported_files'] == 1
    assert stats['queued_reindex'] == 1

    reindex_jobs = [item for item in registry.list_jobs(limit=20) if item.job_type == 'reindex']
    assert any(item.progress.get('reason') == 'import' and item.progress.get('path') == 'Imports/Scanner/Inbox/scan.txt' for item in reindex_jobs)
    saved_source = registry.get_import_source(source['id'])
    assert saved_source is not None
    assert saved_source.last_status == 'completed'
    assert saved_source.stats['imported_files'] == 1


def test_cloud_drive_import_source_skips_unchanged_files(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    source_root = tmp_path / 'scanner'
    source_root.mkdir()
    source_file = source_root / 'scan.txt'
    source_file.write_text('same payload', encoding='utf-8')

    source = service.upsert_import_source(
        name='Scanner',
        source_path=str(source_root),
        target_path='Inbox',
        import_files=True,
    )
    service.run_import_job(service.create_import_job(source_id=source['id']).id)
    first_reindex_count = len([item for item in registry.list_jobs(limit=20) if item.job_type == 'reindex'])

    second = service.run_import_job(service.create_import_job(source_id=source['id']).id)

    assert second['imported_files'] == 0
    assert second['skipped_files'] == 1
    assert second['queued_reindex'] == 0
    assert len([item for item in registry.list_jobs(limit=20) if item.job_type == 'reindex']) == first_reindex_count


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


def test_service_runs_pending_reindex_jobs_past_failed_job(tmp_path: Path, monkeypatch) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))
    service = CloudDriveService(registry=registry, storage=storage)
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    folder = registry.upsert_folder(path='Folder A', name='Folder A', parent_id=root.id, depth=1, source_path='')
    first_source = tmp_path / 'a.txt'
    second_source = tmp_path / 'b.txt'
    first_source.write_text('a', encoding='utf-8')
    second_source.write_text('b', encoding='utf-8')
    first = service.upload_file(parent_path='Folder A', filename='a.txt', source_path=str(first_source), mime_type='text/plain')
    missing = registry.upsert_file(
        folder_id=folder.id,
        path='Folder A/missing.txt',
        name='missing.txt',
        storage_key='objects/missing.txt',
        mime_type='text/plain',
        size_bytes=1,
        checksum='missing',
        source_path=str(tmp_path / 'missing.txt'),
    )
    registry.queue_job(job_type='reindex', file_id=missing.id, version_id=missing.current_version_id, payload={'path': missing.path})
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

    assert [job.file_id for job in completed] == [first['id'], missing.id, second['id']]
    assert [job.status for job in completed] == ['completed', 'failed', 'completed']
    assert seen == ['Folder A/a.txt', 'Folder A/b.txt']
    assert registry.get_job(completed[1].id).last_error


def test_service_reindex_job_rejects_remote_storage_without_local_source(tmp_path: Path) -> None:
    class _RemoteOnlyStorage:
        def __init__(self) -> None:
            self.keys: set[str] = set()

        def put_file(self, source_path: Path, storage_key: str) -> None:
            self.keys.add(storage_key)

        def exists(self, storage_key: str) -> bool:
            return storage_key in self.keys

        def list_keys(self) -> set[str]:
            return set(self.keys)

        def move(self, old_storage_key: str, new_storage_key: str) -> None:
            self.keys.discard(old_storage_key)
            self.keys.add(new_storage_key)

        def delete(self, storage_key: str) -> None:
            self.keys.discard(storage_key)

        def resolve_path(self, storage_key: str) -> str:
            return f'https://storage.example/bucket/{storage_key}'

        def healthcheck(self) -> dict:
            return {'ok': True}

    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = _RemoteOnlyStorage()
    service = CloudDriveService(registry=registry, storage=storage)
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    registry.upsert_folder(path='Folder A', name='Folder A', parent_id=root.id, depth=1, source_path='')
    source = tmp_path / 'remote-only.txt'
    source.write_text('hello', encoding='utf-8')
    uploaded = service.upload_file(parent_path='Folder A', filename='hello.txt', source_path=str(source), mime_type='text/plain')
    job = registry.get_latest_job(job_type='reindex')
    assert job is not None

    with pytest.raises(RuntimeError, match='удалённом storage'):
        service.run_reindex_job(
            job.id,
            index_config={
                'catalog_path': str(tmp_path / 'catalog'),
                'qdrant_db_path': str(tmp_path / 'qdrant'),
                'collection_name': 'catalog',
            },
        )

    failed = registry.get_job(job.id)
    assert failed is not None
    assert failed.status == 'failed'
    assert failed.file_id == uploaded['id']
    assert failed.progress['status'] == 'failed'
    assert 'удалённом storage' in failed.last_error


def test_service_reindex_job_downloads_remote_storage_for_indexing(tmp_path: Path, monkeypatch) -> None:
    class _DownloadableRemoteStorage:
        def __init__(self) -> None:
            self.objects: dict[str, str] = {}

        def put_file(self, source_path: Path, storage_key: str) -> None:
            self.objects[storage_key] = source_path.read_text(encoding='utf-8')

        def download_file(self, storage_key: str, target_path: Path) -> None:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(self.objects[storage_key], encoding='utf-8')

        def exists(self, storage_key: str) -> bool:
            return storage_key in self.objects

        def list_keys(self) -> set[str]:
            return set(self.objects)

        def move(self, old_storage_key: str, new_storage_key: str) -> None:
            self.objects[new_storage_key] = self.objects.pop(old_storage_key)

        def delete(self, storage_key: str) -> None:
            self.objects.pop(storage_key, None)

        def resolve_path(self, storage_key: str) -> str:
            return f'https://storage.example/bucket/{storage_key}'

        def healthcheck(self) -> dict:
            return {'ok': True}

    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    storage = _DownloadableRemoteStorage()
    service = CloudDriveService(registry=registry, storage=storage)
    root = registry.ensure_root_folder(root_name='Обмен', source_path='')
    registry.upsert_folder(path='Folder A', name='Folder A', parent_id=root.id, depth=1, source_path='')
    source = tmp_path / 'remote.txt'
    source.write_text('remote payload', encoding='utf-8')
    uploaded = service.upload_file(parent_path='Folder A', filename='hello.txt', source_path=str(source), mime_type='text/plain')
    job = registry.get_latest_job(job_type='reindex')
    assert job is not None
    calls: dict[str, object] = {}

    class _FakeIndexer:
        def __init__(self, **_kwargs):
            self.point_count = 1

        def _delete_file_vectors(self, filepath, *, payload_match=None):
            calls['delete'] = {'filepath': filepath, 'payload_match': payload_match}

        def process_file(self, filepath, **kwargs):
            path = Path(filepath)
            calls['process'] = {'filepath': path, 'text': path.read_text(encoding='utf-8'), 'kwargs': kwargs}

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
    assert completed.file_id == uploaded['id']
    assert completed.progress['storage_origin'] == 'storage_download'
    assert completed.progress['indexed'] is True
    assert calls['process']['text'] == 'remote payload'  # type: ignore[index]
    assert not Path(completed.progress['storage_path']).exists()


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


def test_registry_claims_and_recovers_stale_cloud_jobs(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / 'registry.db'))
    first = registry.queue_job(job_type='reindex', file_id='file-1', payload={'progress': {'status': 'pending'}})
    registry.queue_job(job_type='cleanup', file_id='file-2', payload={'progress': {'status': 'pending'}})

    claimed = registry.claim_pending_job(job_types=['reindex'], worker_id='worker-a', lease_seconds=30)

    assert claimed is not None
    assert claimed.id == first.id
    assert claimed.status == 'running'
    assert claimed.attempts == 1
    assert claimed.lease_owner == 'worker-a'
    assert claimed.lease_until
    assert registry.claim_pending_job(job_types=['reindex'], worker_id='worker-b') is None

    with registry._connect() as conn:
        conn.execute(
            """
            UPDATE cloud_jobs
            SET lease_until='2000-01-01T00:00:00+00:00'
            WHERE id=?
            """,
            (first.id,),
        )

    assert registry.recover_stale_jobs(job_types=['reindex'], lease_timeout_seconds=1) == 1
    recovered = registry.get_job(first.id)
    assert recovered is not None
    assert recovered.status == 'pending'
    assert recovered.lease_owner == ''
    assert recovered.last_error == 'lease_expired'
    assert recovered.progress['recovered_reason'] == 'lease_expired'


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
    assert 'lease_owner' in columns
    assert 'lease_until' in columns
    assert 'next_run_at' in columns
    assert 'deleted_at' in folder_columns
    assert 'cloud_sync_clients' in tables
    assert 'cloud_sync_pairs' in tables
    assert 'cloud_sync_selective_paths' in tables
    assert 'cloud_sync_conflicts' in tables
    assert 'cloud_user_folders' in tables
    assert 'cloud_share_links' in tables
    assert 'cloud_import_sources' in tables
    assert int(version) == 7


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
