from __future__ import annotations

import json
import zipfile
from pathlib import Path

from rag_catalog.cli import cloud_drive
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
    registry.upsert_file(
        folder_id=root.id,
        path='hello.txt',
        name='hello.txt',
        storage_key='objects/sha256/aa/bb/aabb.txt',
        mime_type='text/plain',
        size_bytes=5,
        checksum='aabb',
    )
    config = {
        'qdrant_db_path': str(state_dir),
        'cloud_drive_db_path': str(db_path),
        'cloud_drive_storage': 'local',
        'cloud_drive_storage_root': str(state_dir / 'storage'),
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

    restore_dir = tmp_path / 'restore'
    rc = cloud_drive.main(['restore', str(backup_path), '--target-dir', str(restore_dir)])

    assert rc == 0
    restored = CloudDriveRegistryDB(str(restore_dir / 'cloud_drive.db'))
    assert restored.get_file_by_path('hello.txt') is not None
