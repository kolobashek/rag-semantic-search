from __future__ import annotations

import os
import shutil
import time
from datetime import datetime, timedelta, timezone

import pytest

from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.cloud_drive.storage import LocalStorageAdapter


@pytest.fixture
def shared(tmp_path):
    root = tmp_path / 'smb'
    root.mkdir()
    service = CloudDriveService(
        registry=CloudDriveRegistryDB(str(tmp_path / 'registry.db')),
        storage=LocalStorageAdapter(str(tmp_path / 'objects')),
        shared_folders=[{'path': 'Scans', 'source_path': str(root), 'settle_seconds': 1}],
    )
    service.registry.ensure_root_folder(root_name='Cloud Drive')
    service._ensure_folder_path('Scans')
    return service, root


def settle(service, root):
    for parent, _, files in os.walk(root):
        for name in files:
            os.utime(os.path.join(parent, name), (time.time() - 120, time.time() - 120))
    bridge = service.shared_folders
    bridge.scan(service, bridge.shares[0])
    with bridge.connect() as conn:
        conn.execute('UPDATE observations SET since=?', (time.time() - 120,))
    return bridge.scan(service, bridge.shares[0])


def test_external_create_edit_delete_restore(shared):
    service, root = shared
    file = root / 'scan.txt'
    file.write_text('original')
    assert settle(service, root)['imported'] == 1
    original = service.registry.get_file_by_path('Scans/scan.txt')
    assert service.storage.exists(original.storage_key)
    file.write_text('edited on SMB')
    settle(service, root)
    assert service.registry.get_file_by_path(original.path).checksum != original.checksum
    file.unlink()
    service.shared_folders.scan(service, service.shared_folders.shares[0])
    assert service.list_trash()['count'] == 1
    service.restore_node(original.path)
    assert file.read_text() == 'edited on SMB'
    assert service.list_trash()['count'] == 0


def test_cloud_upload_move_delete_restore(shared, tmp_path):
    service, root = shared
    source = tmp_path / 'payload.txt'
    source.write_text('cloud content')
    service.create_folder(parent_path='Scans', name='team')
    result = service.upload_file(parent_path='Scans/team', filename='scan.txt', source_path=str(source))
    assert (root / 'team' / 'scan.txt').read_text() == 'cloud content'
    settle(service, root)
    service.move_node(source_path=result['path'], dest_parent_path='Scans/team', new_name='renamed.txt')
    assert not (root / 'team' / 'scan.txt').exists()
    service.delete_node('Scans/team')
    assert not (root / 'team').exists()
    assert list((root / '.rag-cloud').glob('*/old/renamed.txt'))
    service.restore_node('Scans/team')
    assert (root / 'team' / 'renamed.txt').read_text() == 'cloud content'


def test_unavailable_share_does_not_delete_registry(shared):
    service, root = shared
    (root / 'scan.txt').write_text('retained')
    settle(service, root)
    root.rename(root.with_name('offline'))
    with pytest.raises(OSError):
        service.shared_folders.scan(service, service.shared_folders.shares[0])
    assert not service.registry.get_file_by_path('Scans/scan.txt').deleted_at


def test_pending_printer_write_is_not_imported(shared):
    service, root = shared
    (root / 'scan.txt').write_text('partial')
    service.shared_folders.scan(service, service.shared_folders.shares[0])
    assert service.registry.get_file_by_path('Scans/scan.txt') is None


def test_unseen_smb_content_not_overwritten_or_deleted(shared, tmp_path):
    service, root = shared
    file = root / 'scan.txt'
    file.write_text('original')
    settle(service, root)
    file.write_text('external changes')
    source = tmp_path / 'new.txt'
    source.write_text('cloud changes')
    with pytest.raises(RuntimeError):
        service.upload_file(parent_path='Scans', filename='scan.txt', source_path=str(source))
    with pytest.raises(RuntimeError):
        service.delete_node('Scans/scan.txt')
    assert file.read_text() == 'external changes'


@pytest.mark.parametrize('path', ['Scans', 'Scans/../outside', 'Scans/.rag-cloud/item', './Scans/item'])
def test_protected_paths(shared, path):
    service, _ = shared
    with pytest.raises(RuntimeError):
        service.delete_node(path)


def test_expiry_keeps_tombstone_but_removes_content(shared):
    service, root = shared
    (root / 'scan.txt').write_text('retained')
    settle(service, root)
    before = service.registry.get_file_by_path('Scans/scan.txt')
    service.delete_node(before.path)
    bridge = service.shared_folders
    bridge.purge(service, bridge.shares[0])
    assert service.storage.exists(before.storage_key)
    assert service.list_trash()['count'] == 1
    with service.registry._connect() as conn:
        conn.execute('UPDATE cloud_files SET deleted_at=? WHERE id=?',
                     ((datetime.now(timezone.utc) - timedelta(days=15)).isoformat(), before.id))
    with bridge.connect() as conn:
        conn.execute('UPDATE operations SET expires=?', (time.time() - 1,))
    bridge.purge(service, bridge.shares[0])
    assert not service.storage.exists(before.storage_key)
    assert service.registry.get_file_by_path(before.path).deleted_at
    assert service.list_trash()['count'] == 0
    assert not list((root / '.rag-cloud').glob('*/old'))
    with pytest.raises(RuntimeError, match='expired'):
        service.restore_node(before.path)


def test_failed_mutation_fences_share_and_keeps_backup(shared, monkeypatch):
    service, root = shared
    (root / 'scan.txt').write_text('keep me')
    settle(service, root)
    monkeypatch.setattr(service.registry, 'delete_file', lambda *a: (_ for _ in ()).throw(RuntimeError('DB failure')))
    with pytest.raises(RuntimeError, match='DB failure'):
        service.delete_node('Scans/scan.txt')
    assert next((root / '.rag-cloud').glob('*/old')).read_text() == 'keep me'
    with pytest.raises(RuntimeError, match='recovery required'):
        service.shared_folders.scan(service, service.shared_folders.shares[0])


def test_restore_folder_does_not_resurrect_earlier_deleted_children(shared):
    service, root = shared
    (root / 'team').mkdir()
    (root / 'team' / 'old.txt').write_text('old')
    (root / 'team' / 'new.txt').write_text('new')
    settle(service, root)
    service.delete_node('Scans/team/old.txt')
    service.delete_node('Scans/team')
    service.restore_node('Scans/team')
    assert not (root / 'team' / 'old.txt').exists()
    assert service.registry.get_file_by_path('Scans/team/old.txt').deleted_at
    assert (root / 'team' / 'new.txt').exists()


def test_cloud_overwrite_and_immediate_rename(shared, tmp_path):
    service, root = shared
    source = tmp_path / 'data.txt'
    source.write_text('one')
    service.upload_file(parent_path='Scans', filename='scan.txt', source_path=str(source))
    source.write_text('two')
    service.upload_file(parent_path='Scans', filename='scan.txt', source_path=str(source))
    service.move_node(source_path='Scans/scan.txt', dest_parent_path='Scans', new_name='new.txt')
    assert (root / 'new.txt').read_text() == 'two'
    assert any(p.read_text() == 'one' for p in (root / '.rag-cloud').glob('*/old'))


def test_partial_enumeration_cannot_propagate_deletions(shared, monkeypatch):
    service, root = shared
    (root / 'scan.txt').write_text('keep')
    settle(service, root)

    def broken_walk(*args, **kwargs):
        yield str(root), [], []
        kwargs['onerror'](PermissionError('SMB ACL error'))

    monkeypatch.setattr(os, 'walk', broken_walk)
    with pytest.raises(PermissionError):
        service.shared_folders.scan(service, service.shared_folders.shares[0])
    assert not service.registry.get_file_by_path('Scans/scan.txt').deleted_at


def test_storage_failure_does_not_remove_smb_original(shared, tmp_path, monkeypatch):
    service, root = shared
    (root / 'scan.txt').write_text('keep')
    settle(service, root)
    source = tmp_path / 'data.txt'
    source.write_text('changed')
    monkeypatch.setattr(service.storage, 'put_file', lambda *a: (_ for _ in ()).throw(RuntimeError('offline')))
    with pytest.raises(RuntimeError, match='offline'):
        service.upload_file(parent_path='Scans', filename='scan.txt', source_path=str(source))
    assert (root / 'scan.txt').read_text() == 'keep'
    service.shared_folders._ready(service.shared_folders.shares[0])


def test_purge_preserves_objects_referenced_outside_share(shared):
    service, root = shared
    (root / 'scan.txt').write_text('shared content')
    settle(service, root)
    node = service.registry.get_file_by_path('Scans/scan.txt')
    service.upload_file(filename='other.txt', source_path=str(root / 'scan.txt'))
    service.delete_node(node.path)
    with service.registry._connect() as conn:
        conn.execute('UPDATE cloud_files SET deleted_at=? WHERE id=?',
                     ((datetime.now(timezone.utc) - timedelta(days=15)).isoformat(), node.id))
    service.shared_folders.purge(service, service.shared_folders.shares[0])
    assert service.storage.exists(node.storage_key)
    assert service.registry.get_file_by_path('other.txt').storage_key == node.storage_key


def test_role_permission_applies_to_current_and_future_users(shared):
    service, root = shared
    folder = service.registry.get_folder_by_path('Scans')
    service.grant_permission(subject_type='role', subject_id='user', resource_type='folder',
                             resource_id=folder.id, access_level='editor')
    for username in ['veronika', 'future-user']:
        assert service.registry.user_can_access(username=username, role='user', path='Scans/file.txt', required_level='editor')
        assert not service.registry.user_can_access(username=username, role='user', path='Other/file.txt', required_level='editor')


def test_delete_download_denied(shared):
    service, root = shared
    (root / 'scan.txt').write_text('secret')
    settle(service, root)
    service.delete_node('Scans/scan.txt')
    with pytest.raises(RuntimeError):
        service.get_download_descriptor('Scans/scan.txt')


def test_external_folder_delete_restores_whole_tree(shared):
    service, root = shared
    (root / 'folder' / 'nested').mkdir(parents=True)
    (root / 'folder' / 'nested' / 'scan.txt').write_text('restore together')
    settle(service, root)
    shutil.rmtree(root / 'folder')
    service.shared_folders.scan(service, service.shared_folders.shares[0])
    service.restore_node('Scans/folder')
    assert (root / 'folder' / 'nested' / 'scan.txt').read_text() == 'restore together'


def test_missing_delete_does_not_fence_share(shared):
    service, _ = shared
    with pytest.raises(RuntimeError, match='not found'):
        service.delete_node('Scans/missing.txt')
    service.shared_folders._ready(service.shared_folders.shares[0])


def test_excluded_entry_is_not_published(shared):
    service, root = shared
    (root / 'probe').mkdir()
    (root / 'probe' / 'private.txt').write_text('not published')
    other = CloudDriveService(registry=service.registry, storage=service.storage, shared_folders=[
        {'path': 'Scans', 'source_path': str(root), 'exclude_names': ['probe']}])
    other.shared_folders.scan(other, other.shared_folders.shares[0])
    assert service.registry.get_folder_by_path('Scans/probe') is None
    with pytest.raises(RuntimeError, match='excluded'):
        other.create_folder(parent_path='Scans/probe', name='child')
