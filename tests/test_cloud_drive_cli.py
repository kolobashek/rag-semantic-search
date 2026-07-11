from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

from rag_catalog.cli import cloud_drive
from rag_catalog.core.cloud_drive.operations import cloud_drive_backup_freshness
from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB


def test_cloud_drive_cli_init_and_stats(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / 'config.json'
    config_path.write_text(json.dumps({'qdrant_db_path': str(tmp_path / 'state')}, ensure_ascii=False), encoding='utf-8')
    monkeypatch.setattr(cloud_drive, 'load_config', lambda: json.loads(config_path.read_text(encoding='utf-8')))
    monkeypatch.setattr(cloud_drive, 'save_config', lambda cfg: config_path.write_text(json.dumps(cfg, ensure_ascii=False), encoding='utf-8'))

    rc = cloud_drive.main(['init', '--enable'])
    assert rc == 0

    cfg = json.loads(config_path.read_text(encoding='utf-8'))
    assert cfg['cloud_drive_enabled'] is True
    assert cfg['cloud_drive_db_path'].endswith('cloud_drive.db')

    rc = cloud_drive.main(['stats'])
    assert rc == 0


def test_cloud_drive_cli_bootstrap(tmp_path: Path, monkeypatch) -> None:
    catalog = tmp_path / 'catalog'
    catalog.mkdir()
    (catalog / 'hello.txt').write_text('hello', encoding='utf-8')
    config = {
        'catalog_path': str(catalog),
        'qdrant_db_path': str(tmp_path / 'state'),
        'cloud_drive_db_path': str(tmp_path / 'state' / 'cloud_drive.db'),
        'cloud_drive_storage': 'local',
        'cloud_drive_storage_root': str(tmp_path / 'state' / 'storage'),
    }
    monkeypatch.setattr(cloud_drive, 'load_config', lambda: dict(config))
    monkeypatch.setattr(cloud_drive, 'save_config', lambda cfg: None)

    rc = cloud_drive.main(['bootstrap', '--import-files'])
    assert rc == 0
    registry = CloudDriveRegistryDB(str(tmp_path / 'state' / 'cloud_drive.db'))
    row = registry.get_file_by_path('hello.txt')
    assert row is not None
    assert row.storage_key.startswith('objects/sha256/')
    assert (tmp_path / 'state' / 'storage' / row.storage_key).exists()


def test_cloud_drive_cli_compact_versions(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / 'state'
    config = {
        'qdrant_db_path': str(state_dir),
        'cloud_drive_db_path': str(state_dir / 'cloud_drive.db'),
        'cloud_drive_storage': 'local',
        'cloud_drive_storage_root': str(state_dir / 'storage'),
    }
    monkeypatch.setattr(cloud_drive, 'load_config', lambda: dict(config))
    monkeypatch.setattr(cloud_drive, 'save_config', lambda cfg: None)
    registry = CloudDriveRegistryDB(str(state_dir / 'cloud_drive.db'))
    root = registry.ensure_root_folder(root_name='root')
    file_row = registry.upsert_file(
        folder_id=root.id,
        path='hello.txt',
        name='hello.txt',
        storage_key='objects/sha256/aa/bb/aabb.txt',
        mime_type='text/plain',
        size_bytes=5,
        checksum='aabb',
    )
    with registry._connect() as conn:
        conn.execute(
            """
            INSERT INTO cloud_file_versions (id, file_id, storage_key, checksum, size_bytes, source_path, created_by, created_at)
            VALUES ('duplicate-version', ?, ?, ?, ?, '', '', '2026-05-13T00:00:00+00:00')
            """,
            (file_row.id, file_row.storage_key, file_row.checksum, file_row.size_bytes),
        )

    rc = cloud_drive.main(['compact-versions'])

    assert rc == 0
    assert len(registry.list_file_versions(path='hello.txt')) == 1


def test_cloud_drive_cli_backup_and_restore_to_target_dir(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / 'state'
    state_dir.mkdir()
    db_path = state_dir / 'cloud_drive.db'
    registry = CloudDriveRegistryDB(str(db_path))
    root = registry.ensure_root_folder(root_name='root')
    checksum = '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
    registry.upsert_file(
        folder_id=root.id,
        path='hello.txt',
        name='hello.txt',
        storage_key='objects/sha256/aa/bb/aabb.txt',
        mime_type='text/plain',
        size_bytes=5,
        checksum=checksum,
    )
    object_path = state_dir / 'storage' / 'objects' / 'sha256' / 'aa' / 'bb' / 'aabb.txt'
    object_path.parent.mkdir(parents=True)
    object_path.write_text('hello', encoding='utf-8')
    config = {
        'qdrant_db_path': str(state_dir),
        'cloud_drive_db_path': str(db_path),
        'cloud_drive_storage': 'local',
        'cloud_drive_storage_root': str(state_dir / 'storage'),
        'telegram_bot_token': 'must-not-leak',
    }
    monkeypatch.setattr(cloud_drive, 'load_config', lambda: dict(config))
    monkeypatch.setattr(cloud_drive, 'save_config', lambda cfg: None)
    backup_path = tmp_path / 'backup.zip'

    rc = cloud_drive.main(['backup', '--output', str(backup_path)])

    assert rc == 0
    with zipfile.ZipFile(backup_path) as zf:
        names = set(zf.namelist())
        assert 'manifest.json' in names
        assert 'config.snapshot.json' in names
        assert 'files/cloud_drive_db.db' in names
        assert 'storage/objects/sha256/aa/bb/aabb.txt' in names
        manifest = json.loads(zf.read('manifest.json'))
        assert manifest['version'] == 2
        assert manifest['storage_files'][0]['sha256'] == checksum
        snapshot = json.loads(zf.read('config.snapshot.json'))
        assert snapshot['telegram_bot_token'] == '[REDACTED]'
        assert 'must-not-leak' not in str(snapshot)

    assert cloud_drive.main(['verify-backup', str(backup_path)]) == 0
    assert cloud_drive.main(
        ['preflight', '--mode', 'upgrade', '--backup-dir', str(tmp_path), '--min-free-gb', '0']
    ) == 0

    restore_dir = tmp_path / 'restore'
    rc = cloud_drive.main(['restore', str(backup_path), '--target-dir', str(restore_dir)])

    assert rc == 0
    restored = CloudDriveRegistryDB(str(restore_dir / 'cloud_drive.db'))
    assert restored.get_file_by_path('hello.txt') is not None
    assert (restore_dir / 'cloud_storage' / 'objects' / 'sha256' / 'aa' / 'bb' / 'aabb.txt').read_text() == 'hello'

    drill_dir = tmp_path / 'drill'
    assert cloud_drive.main(['restore-drill', str(backup_path), '--target-dir', str(drill_dir)]) == 0
    assert (drill_dir / 'cloud_drive.db').is_file()
    drill_artifact = Path(f'{backup_path}.restore-drill.json')
    assert json.loads(drill_artifact.read_text(encoding='utf-8'))['ok'] is True


def test_cloud_drive_cli_fresh_install_preflight_accepts_empty_target(tmp_path: Path, monkeypatch) -> None:
    config = {
        'qdrant_db_path': str(tmp_path / 'new-state'),
        'cloud_drive_db_path': str(tmp_path / 'new-state' / 'cloud_drive.db'),
        'cloud_drive_storage': 'local',
        'cloud_drive_storage_root': str(tmp_path / 'new-state' / 'storage'),
    }
    monkeypatch.setattr(cloud_drive, 'load_config', lambda: dict(config))

    rc = cloud_drive.main(['preflight', '--mode', 'fresh-install', '--min-free-gb', '0'])

    assert rc == 0
    assert not Path(config['cloud_drive_db_path']).exists()


def test_cloud_drive_cli_s3_provider_backup_verify_and_restore_drill(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / 'state'
    db_path = state_dir / 'cloud_drive.db'
    registry = CloudDriveRegistryDB(str(db_path))
    root = registry.ensure_root_folder(root_name='root')
    storage_key = 'objects/sha256/aa/bb/hello.txt'
    empty_key = 'objects/sha256/e3/b0/empty'
    payload = b'hello-provider'
    registry.upsert_file(
        folder_id=root.id,
        path='hello.txt',
        name='hello.txt',
        storage_key=storage_key,
        mime_type='text/plain',
        size_bytes=len(payload),
        checksum='',
    )
    config = {
        'qdrant_db_path': str(state_dir),
        'cloud_drive_db_path': str(db_path),
        'cloud_drive_storage': 's3',
        'cloud_drive_bucket': 'source',
        'cloud_drive_s3_endpoint': 'http://minio:9000',
    }

    class FakeS3Storage:
        buckets: dict[str, dict[str, bytes]] = {'source': {storage_key: payload, empty_key: b''}}

        def __init__(self, bucket: str) -> None:
            self.bucket = bucket
            self._client = self

        def list_keys(self) -> set[str]:
            return set(self.buckets.get(self.bucket, {}))

        def download_file(self, key: str, target: Path) -> None:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(self.buckets[self.bucket][key])

        def put_file(self, source: Path, key: str) -> None:
            self.buckets.setdefault(self.bucket, {})[key] = source.read_bytes()

        def delete(self, key: str) -> None:
            self.buckets[self.bucket].pop(key, None)

        def ensure_container(self) -> dict:
            self.buckets.setdefault(self.bucket, {})
            return {'ok': True}

        def delete_bucket(self, *, Bucket: str) -> None:
            assert not self.buckets.get(Bucket)
            self.buckets.pop(Bucket, None)

    monkeypatch.setattr(cloud_drive, 'load_config', lambda: dict(config))
    monkeypatch.setattr(cloud_drive, 'S3StorageAdapter', FakeS3Storage)
    monkeypatch.setattr(
        cloud_drive,
        'resolve_storage_adapter',
        lambda cfg: FakeS3Storage(str(cfg.get('cloud_drive_bucket') or '')),
    )
    snapshot = tmp_path / 'provider-snapshot'

    assert cloud_drive.main(['provider-backup', '--output-dir', str(snapshot), '--workers', '2']) == 0
    assert cloud_drive.main(['provider-verify', str(snapshot)]) == 0
    assert (snapshot / 'provider-verify.json').is_file()
    monkeypatch.setattr(
        cloud_drive,
        '_verify_provider_snapshot',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError('fresh verification artifact was ignored')),
    )
    assert cloud_drive.main(['provider-restore-drill', str(snapshot), '--sample-size', '1']) == 0

    manifest = json.loads((snapshot / 'manifest.json').read_text(encoding='utf-8'))
    assert manifest['object_count'] == 2
    assert {item['storage_key'] for item in manifest['objects']} == {storage_key, empty_key}
    assert (snapshot / 'objects' / storage_key).read_bytes() == payload
    artifact = json.loads((snapshot / 'restore-drill.json').read_text(encoding='utf-8'))
    assert artifact['round_trip_objects_checked'] == 1
    assert set(FakeS3Storage.buckets) == {'source'}
    freshness = cloud_drive_backup_freshness({**config, 'cloud_drive_backup_dir': str(tmp_path)})
    assert freshness['status'] == 'healthy'
    assert freshness['provider'] == 's3'


def test_cloud_drive_cli_provider_reconcile_repairs_keys_and_soft_deletes_stale_rows(
    tmp_path: Path, monkeypatch
) -> None:
    state_dir = tmp_path / 'state'
    db_path = state_dir / 'cloud_drive.db'
    registry = CloudDriveRegistryDB(str(db_path))
    root = registry.ensure_root_folder(root_name='root')
    payload = b'recoverable-provider-object'
    checksum = hashlib.sha256(payload).hexdigest()
    canonical_key = f'objects/sha256/{checksum[:2]}/{checksum[2:4]}/{checksum}.txt'
    recoverable = registry.upsert_file(
        folder_id=root.id,
        path='recoverable.txt',
        name='recoverable.txt',
        storage_key='legacy/recoverable.txt',
        mime_type='text/plain',
        size_bytes=len(payload),
        checksum=checksum,
        source_path=str(tmp_path / 'removed-recoverable.txt'),
    )
    stale = registry.upsert_file(
        folder_id=root.id,
        path='stale.txt',
        name='stale.txt',
        storage_key='objects/sha256/ff/ff/missing.txt',
        mime_type='text/plain',
        size_bytes=7,
        checksum='f' * 64,
        source_path=str(tmp_path / 'removed-stale.txt'),
    )
    config = {
        'qdrant_db_path': str(state_dir),
        'cloud_drive_db_path': str(db_path),
        'cloud_drive_storage': 's3',
        'cloud_drive_bucket': 'source',
        'cloud_drive_s3_endpoint': 'http://minio:9000',
    }

    class FakeS3Storage:
        buckets: dict[str, dict[str, bytes]] = {'source': {canonical_key: payload}}

        def __init__(self, bucket: str) -> None:
            self.bucket = bucket

        def list_keys(self) -> set[str]:
            return set(self.buckets[self.bucket])

        def download_file(self, key: str, target: Path) -> None:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(self.buckets[self.bucket][key])

    monkeypatch.setattr(cloud_drive, 'load_config', lambda: dict(config))
    monkeypatch.setattr(cloud_drive, 'S3StorageAdapter', FakeS3Storage)
    monkeypatch.setattr(
        cloud_drive,
        'resolve_storage_adapter',
        lambda cfg: FakeS3Storage(str(cfg.get('cloud_drive_bucket') or 'source')),
    )
    snapshot = tmp_path / 'provider-snapshot'

    assert cloud_drive.main(['provider-backup', '--output-dir', str(snapshot)]) == 0
    assert cloud_drive.main(['provider-reconcile', str(snapshot)]) == 0
    assert registry.get_file_by_id(recoverable.id).storage_key == 'legacy/recoverable.txt'
    assert registry.get_file_by_id(stale.id).deleted_at == ''

    assert cloud_drive.main(['provider-reconcile', str(snapshot), '--apply']) == 0
    assert registry.get_file_by_id(recoverable.id).storage_key == canonical_key
    assert registry.get_file_by_id(stale.id).deleted_at
    assert cloud_drive.main(['provider-verify', str(snapshot)]) == 0
