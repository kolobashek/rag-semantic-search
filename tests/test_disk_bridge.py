from __future__ import annotations

import os
import time
from dataclasses import asdict
from pathlib import Path

import pytest
from filelock import Timeout

from rag_catalog.core.cloud_drive.disk_bridge import PRIVATE, DiskBridge, signature
from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.cloud_drive.storage import LocalStorageAdapter


@pytest.fixture
def setup(tmp_path):
    source = tmp_path / 'disk'
    folder = source / 'Shared'
    folder.mkdir(parents=True)
    file = folder / 'report.txt'
    file.write_bytes(b'initial')
    os.utime(file, (1700000000, 1700000000))
    service = CloudDriveService(registry=CloudDriveRegistryDB(str(tmp_path / 'cloud.db')),
                                storage=LocalStorageAdapter(str(tmp_path / 'storage')))
    service.bootstrap_from_catalog(str(source), import_files=True)
    shared = service.registry.get_folder_by_path('Shared')
    mapping = {'source': 'Shared', 'folder_id': shared.id}
    bridge = DiskBridge(service, source_root=str(source), mappings=[mapping],
                        state_path=str(tmp_path / 'bridge.db'), settle_seconds=0)
    return service, bridge, source, file


def contents(service, path='Shared/report.txt'):
    row = service.registry.get_file_by_path(path)
    return Path(service.storage.resolve_path(row.storage_key)).read_bytes()


def test_source_newer_preserves_history(setup):
    service, bridge, _, file = setup
    file.write_bytes(b'from disk')
    os.utime(file, (1700000200, 1700000200))
    report = bridge.run(apply=True)
    assert report['counts']['imported'] == 1
    assert contents(service) == b'from disk'
    assert len(service.registry.list_file_versions(path='Shared/report.txt')) == 2
    assert bridge.run(apply=True)['counts']['verified'] == 1


@pytest.mark.skipif(os.name != 'nt', reason='Windows non-overwriting publication')
def test_cloud_newer_is_exported_with_both_versions_retained(setup, tmp_path):
    service, bridge, _, file = setup
    incoming = tmp_path / 'upload.txt'
    incoming.write_bytes(b'from cloud')
    service.upload_file(parent_path='Shared', filename='report.txt', source_path=str(incoming))
    report = bridge.run(apply=True)
    assert report['counts']['exported'] == 1
    assert file.read_bytes() == b'from cloud'
    assert {Path(service.storage.resolve_path(v['storage_key'])).read_bytes()
            for v in service.registry.list_file_versions(path='Shared/report.txt')} == {b'initial', b'from cloud'}
    assert not report['errors']
    assert bridge.run(apply=True)['counts']['verified'] == 1


def test_equal_timestamp_conflict_never_overwrites(setup):
    service, bridge, _, file = setup
    file.write_bytes(b'different size and same time')
    os.utime(file, (1700000000, 1700000000))
    report = bridge.run(apply=True)
    assert report['counts']['conflict'] == 1
    assert report['complete'] is False
    assert contents(service) == b'initial'
    assert file.read_bytes() == b'different size and same time'
    assert bridge.run(apply=True)['counts']['conflict'] == 1
    assert len(service.registry.list_file_versions(path='Shared/report.txt')) == 2


def test_new_source_file_is_imported(setup):
    service, bridge, _, file = setup
    added = file.parent / 'new.txt'
    added.write_bytes(b'new')
    report = bridge.run(apply=True)
    assert report['counts']['imported'] == 1
    assert contents(service, 'Shared/new.txt') == b'new'


@pytest.mark.skipif(os.name != 'nt', reason='Windows non-overwriting publication')
def test_new_cloud_file_is_exported(setup, tmp_path):
    service, bridge, _, file = setup
    upload = tmp_path / 'upload'
    upload.write_bytes(b'new cloud')
    service.upload_file(parent_path='Shared', filename='new.txt', source_path=str(upload))
    report = bridge.run(apply=True)
    assert report['counts']['exported'] == 1
    assert (file.parent / 'new.txt').read_bytes() == b'new cloud'


def test_cloud_deletion_never_resurrects_or_deletes_local_file(setup):
    service, bridge, _, file = setup
    service.registry.delete_file('Shared/report.txt')
    assert bridge.run(apply=True)['counts']['cloud_deleted'] == 1
    assert file.read_bytes() == b'initial'
    assert service.registry.get_file_by_path('Shared/report.txt').deleted_at


def test_source_deletion_never_deletes_cloud_or_recreates_local_file(setup):
    service, bridge, _, file = setup
    bridge.run(apply=True)
    file.unlink()
    assert bridge.run(apply=True)['counts']['source_deleted'] == 1
    assert bridge.run(apply=True)['counts']['source_deleted'] == 1
    assert contents(service) == b'initial'
    assert not file.exists()


def test_first_pass_missing_imported_source_requires_review(setup):
    service, bridge, _, file = setup
    file.unlink()
    assert bridge.run(apply=True)['counts']['source_missing_review'] == 1
    assert not file.exists()


def test_unmounted_source_is_not_an_empty_disk(setup):
    service, bridge, source, file = setup
    source.rename(source.with_name('disconnected'))
    with pytest.raises(FileNotFoundError):
        bridge.run(apply=True)
    assert contents(service) == b'initial'


def test_home_mapping_is_rejected_and_not_exported(setup):
    service, bridge, source, _ = setup
    home = service.registry.ensure_user_home_folder(username='private')
    (source / 'private').mkdir()
    bridge.mappings = [{'source': 'private', 'folder_id': home.id}]
    report = bridge.run(apply=True)
    assert 'Personal' in report['errors'][0]['error']
    assert not list((source / 'private').iterdir())


def test_new_user_home_matching_source_alias_is_excluded(setup):
    service, bridge, _, _ = setup
    service.registry.ensure_user_home_folder(username='Shared')
    assert 'Personal' in bridge.run(apply=True)['errors'][0]['error']


def test_scope_is_explicit_and_limit_is_bounded(setup):
    service, bridge, source, file = setup
    private = source / 'private'
    private.mkdir()
    (private / 'secret.txt').write_bytes(b'private')
    for n in range(4):
        (file.parent / f'new{n}.txt').write_bytes(b'new')
    report = bridge.run(apply=True, max_files=2)
    assert report['counts']['imported'] == 2
    assert report['counts']['pending'] == 2
    assert service.registry.get_file_by_path('private/secret.txt') is None


def test_settle_observations_and_preview_do_not_write(setup):
    service, bridge, _, file = setup
    bridge.settle_seconds = 60
    file.write_bytes(b'new')
    report = bridge.run(apply=True)
    assert report['counts']['settling'] == 1
    with bridge.connect() as db:
        db.execute('UPDATE entries SET since=?', (time.time() - 120,))
    report = bridge.run()
    assert report['counts']['planned'] == 1
    assert contents(service) == b'initial'


def test_changed_mapping_cannot_reuse_state(setup):
    _, bridge, _, _ = setup
    with pytest.raises(ValueError, match='Mappings changed'):
        DiskBridge(bridge.service, source_root=str(bridge.root),
                   mappings=[{**bridge.mappings[0], 'source': 'Other'}],
                   state_path=str(bridge.state_path))


@pytest.mark.skipif(os.name != 'nt', reason='Windows sharing violation')
def test_locked_destination_is_preserved_and_can_retry_after_unlock(setup, tmp_path):
    service, bridge, source, file = setup
    upload = tmp_path / 'new'
    upload.write_bytes(b'new cloud')
    service.upload_file(parent_path='Shared', filename='report.txt', source_path=str(upload))
    with file.open('r+b'):
        report = bridge.run(apply=True)
    assert report['errors']
    assert file.read_bytes() == b'initial'
    assert contents(service) == b'new cloud'
    assert bridge.run(apply=True)['counts']['exported'] == 1
    assert (source / PRIVATE).is_dir()


def test_pending_operation_is_never_retried_blindly(setup):
    _, bridge, _, _ = setup
    with bridge.connect() as db:
        db.execute('INSERT INTO operations VALUES (?,?,?,?,?,?)',
                   ('op', bridge.mappings[0]['folder_id'], 'report.txt', '/staged', 'pending', time.time()))
    assert bridge.run(apply=True)['counts']['recovery_required'] == 1


def test_file_changed_after_comparison_is_deferred(setup):
    service, bridge, _, file = setup
    row = service.registry.get_file_by_path('Shared/report.txt')
    sig = signature(file)
    file.write_bytes(b'concurrent')
    assert bridge.exchange(bridge.mappings[0], 'Shared', 'report.txt', sig, asdict(row), set()) == 'source_busy'
    assert contents(service) == b'initial'


def test_deleted_parent_is_not_recreated(setup):
    service, bridge, _, file = setup
    service.registry.create_folder(parent_path='Shared', name='Old')
    service.registry.delete_folder('Shared/Old')
    folder = file.parent / 'Old'
    folder.mkdir()
    (folder / 'new.txt').write_bytes(b'new')
    report = bridge.run(apply=True)
    assert report['counts']['cloud_deleted'] == 1
    assert service.registry.get_folder_by_path('Shared/Old') is None


def test_internal_staging_is_not_imported(setup):
    service, bridge, _, file = setup
    (file.parent / '.rag-private').mkdir()
    (file.parent / '.rag-private' / 'old.txt').write_bytes(b'old')
    (file.parent / '~$office.docx').write_bytes(b'lock')
    bridge.run(apply=True)
    assert service.registry.get_file_by_path('Shared/.rag-private/old.txt') is None
    assert service.registry.get_file_by_path('Shared/~$office.docx') is None


def test_empty_source_folder_is_imported(setup):
    service, bridge, _, file = setup
    (file.parent / 'empty').mkdir()
    assert bridge.run(apply=True)['counts']['folders_created'] == 1
    assert service.registry.get_folder_by_path('Shared/empty')


def test_new_empty_cloud_folder_is_exported(setup):
    service, bridge, _, file = setup
    service.registry.create_folder(parent_path='Shared', name='new-empty')
    assert bridge.run(apply=True)['counts']['folders_created'] == 1
    assert (file.parent / 'new-empty').is_dir()


def test_preparing_cleanup_never_removes_an_old_copy(setup):
    _, bridge, source, _ = setup
    from rag_catalog.core.cloud_drive.disk_bridge import OWNER
    private = source / PRIVATE
    stage = private / ('f' * 32)
    stage.mkdir(parents=True)
    (private / 'owner.txt').write_text(OWNER, encoding='ascii')
    (stage / 'old').write_bytes(b'original')
    with bridge.connect() as db:
        db.execute('INSERT INTO operations VALUES (?,?,?,?,?,?)',
                   ('f' * 32, bridge.mappings[0]['folder_id'], 'report.txt', str(stage), 'preparing', time.time()))
    with pytest.raises(RuntimeError, match='Unexpected'):
        bridge.run(apply=True)
    assert (stage / 'old').read_bytes() == b'original'


def test_nested_personal_folder_fences_mapping(setup):
    service, bridge, _, _ = setup
    with service.registry._connect() as db:
        db.execute("INSERT INTO cloud_user_folders VALUES ('private','private-id','Shared/private','now','now')")
    assert 'Personal' in bridge.run(apply=True)['errors'][0]['error']


def test_metadata_preview_does_not_create_folders(setup):
    service, bridge, _, file = setup
    (file.parent / 'empty').mkdir()
    report = bridge.run()
    assert report['counts']['folder_pending'] == 1
    assert service.registry.get_folder_by_path('Shared/empty') is None


def test_second_bridge_instance_cannot_transfer(setup):
    service, bridge, _, _ = setup
    second = DiskBridge(service, source_root=str(bridge.root), mappings=bridge.mappings,
                        state_path=str(bridge.state_path), settle_seconds=0)
    with bridge.lock, pytest.raises(Timeout):
        second.run(apply=True)


@pytest.mark.skipif(os.name != 'nt', reason='Windows non-overwriting publication')
def test_cloud_changes_while_staging_never_overwrite_source(setup, tmp_path, monkeypatch):
    service, bridge, _, file = setup
    upload = tmp_path / 'new'
    upload.write_bytes(b'new cloud')
    service.upload_file(parent_path='Shared', filename='report.txt', source_path=str(upload))
    original = bridge.assert_current

    def changed(*args):
        upload.write_bytes(b'concurrent cloud')
        service.upload_file(parent_path='Shared', filename='report.txt', source_path=str(upload))
        return original(*args)

    monkeypatch.setattr(bridge, 'assert_current', changed)
    report = bridge.run(apply=True)
    assert 'Cloud version changed' in report['errors'][0]['error']
    assert file.read_bytes() == b'initial'
    monkeypatch.setattr(bridge, 'assert_current', original)
    assert bridge.run(apply=True)['counts']['exported'] == 1
    assert file.read_bytes() == b'concurrent cloud'


def test_failed_source_inventory_does_not_export_partial_view(setup, monkeypatch):
    service, bridge, _, file = setup

    def offline(*args, **kwargs):
        raise OSError('source disconnected midway')

    monkeypatch.setattr(bridge, 'inventory', offline)
    report = bridge.run(apply=True)
    assert report['errors']
    assert not report['changes']
    assert file.read_bytes() == contents(service) == b'initial'


def test_new_child_cannot_recreate_previously_missing_parent(setup, tmp_path):
    service, bridge, _, file = setup
    service.registry.create_folder(parent_path='Shared', name='old-missing')
    with service.registry._connect() as db:
        db.execute("UPDATE cloud_folders SET created_at='2020-01-01T00:00:00+00:00' "
                   "WHERE path='Shared/old-missing'")
    service.registry.create_folder(parent_path='Shared/old-missing', name='new-child')
    upload = tmp_path / 'upload'
    upload.write_bytes(b'new')
    service.upload_file(parent_path='Shared/old-missing/new-child', filename='new.txt', source_path=str(upload))
    report = bridge.run(apply=True)
    assert report['counts']['source_missing_review'] == 1
    assert report['counts']['folder_missing_review'] == 2
    assert not (file.parent / 'old-missing').exists()


@pytest.mark.skipif(os.name != 'nt', reason='Windows non-overwriting publication')
def test_cloud_write_after_import_is_not_marked_as_synced(setup, tmp_path, monkeypatch):
    service, bridge, _, file = setup
    file.write_bytes(b'new disk')
    os.utime(file, (1700000200, 1700000200))
    original = service.reconcile_file_from_source
    upload = tmp_path / 'concurrent'
    upload.write_bytes(b'newer cloud')

    def concurrent(**kwargs):
        result = original(**kwargs)
        service.upload_file(parent_path='Shared', filename='report.txt', source_path=str(upload))
        return result

    monkeypatch.setattr(service, 'reconcile_file_from_source', concurrent)
    assert bridge.run(apply=True)['counts']['imported'] == 1
    monkeypatch.setattr(service, 'reconcile_file_from_source', original)
    assert bridge.run(apply=True)['counts']['exported'] == 1
    assert file.read_bytes() == b'newer cloud'



@pytest.mark.skipif(os.name != 'nt', reason='Windows non-overwriting publication')
def test_delete_immediately_after_export_is_not_undone(setup, tmp_path):
    service, bridge, _, file = setup
    upload = tmp_path / 'new'
    upload.write_bytes(b'new cloud')
    service.upload_file(parent_path='Shared', filename='new.txt', source_path=str(upload))
    assert bridge.run(apply=True)['counts']['exported'] == 1
    (file.parent / 'new.txt').unlink()
    assert bridge.run(apply=True)['counts']['source_deleted'] == 1
    service.upload_file(parent_path='Shared', filename='new.txt', source_path=str(upload))
    bridge.run(apply=True)
    assert not (file.parent / 'new.txt').exists()
