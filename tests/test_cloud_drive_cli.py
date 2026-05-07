from __future__ import annotations

import json
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
