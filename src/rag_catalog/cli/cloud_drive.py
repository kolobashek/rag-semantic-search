from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.rag_core import load_config, save_config


def _default_cloud_paths(cfg: Dict[str, Any]) -> tuple[str, str]:
    base = Path(str(cfg.get('qdrant_db_path') or '.')).resolve()
    db_path = str(Path(str(cfg.get('cloud_drive_db_path') or '')).resolve()) if str(cfg.get('cloud_drive_db_path') or '').strip() else str(base / 'cloud_drive.db')
    storage_root = str(Path(str(cfg.get('cloud_drive_storage_root') or '')).resolve()) if str(cfg.get('cloud_drive_storage_root') or '').strip() else str(base / 'cloud_storage')
    return db_path, storage_root


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Cloud drive registry bootstrap and diagnostics')
    sub = parser.add_subparsers(dest='command', required=True)

    init_cmd = sub.add_parser('init', help='Create cloud drive registry/storage config')
    init_cmd.add_argument('--db-path', default='', help='SQLite registry path')
    init_cmd.add_argument('--storage', default='local', choices=['local', 's3'], help='Storage backend kind')
    init_cmd.add_argument('--storage-root', default='', help='Local storage root')
    init_cmd.add_argument('--enable', action='store_true', help='Enable cloud drive in config.json')

    bootstrap_cmd = sub.add_parser('bootstrap', help='Import current catalog into cloud drive registry')
    bootstrap_cmd.add_argument('--catalog', default='', help='Catalog path to import')
    bootstrap_cmd.add_argument('--max-files', type=int, default=0, help='Import only first N files')
    bootstrap_cmd.add_argument('--import-files', action='store_true', help='Copy files into storage backend')

    sub.add_parser('stats', help='Show current cloud drive registry stats')
    return parser


def _init_cloud(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    db_path, storage_root = _default_cloud_paths(cfg)
    if args.db_path:
        db_path = str(Path(args.db_path).resolve())
    if args.storage_root:
        storage_root = str(Path(args.storage_root).resolve())

    cfg['cloud_drive_db_path'] = db_path
    cfg['cloud_drive_storage'] = args.storage
    if args.storage == 'local':
        cfg['cloud_drive_storage_root'] = storage_root
    if args.enable:
        cfg['cloud_drive_enabled'] = True
    save_config(cfg)
    service = CloudDriveService.from_config(cfg)
    stats = service.registry.stats()
    print(json.dumps({
        'cloud_drive_enabled': bool(cfg.get('cloud_drive_enabled')),
        'cloud_drive_db_path': db_path,
        'cloud_drive_storage': args.storage,
        'cloud_drive_storage_root': cfg.get('cloud_drive_storage_root', ''),
        'stats': asdict(stats),
    }, ensure_ascii=False, indent=2))
    return 0


def _bootstrap_cloud(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    db_path, storage_root = _default_cloud_paths(cfg)
    cfg.setdefault('cloud_drive_storage', 'local')
    cfg['cloud_drive_db_path'] = str(cfg.get('cloud_drive_db_path') or db_path)
    if str(cfg.get('cloud_drive_storage') or 'local') == 'local':
        cfg['cloud_drive_storage_root'] = str(cfg.get('cloud_drive_storage_root') or storage_root)
    catalog = str(args.catalog or cfg.get('catalog_path') or '').strip()
    if not catalog:
        raise SystemExit('Не задан каталог для bootstrap.')
    service = CloudDriveService.from_config(cfg)
    stats = service.bootstrap_from_catalog(
        catalog,
        max_files=(args.max_files or None),
        import_files=bool(args.import_files),
    )
    print(json.dumps({'catalog': catalog, 'stats': asdict(stats)}, ensure_ascii=False, indent=2))
    return 0


def _stats_cloud(cfg: Dict[str, Any]) -> int:
    db_path, storage_root = _default_cloud_paths(cfg)
    cfg['cloud_drive_db_path'] = str(cfg.get('cloud_drive_db_path') or db_path)
    cfg.setdefault('cloud_drive_storage', 'local')
    if str(cfg.get('cloud_drive_storage') or 'local') == 'local':
        cfg['cloud_drive_storage_root'] = str(cfg.get('cloud_drive_storage_root') or storage_root)
    service = CloudDriveService.from_config(cfg)
    stats = service.registry.stats()
    print(json.dumps(asdict(stats), ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    cfg = load_config()
    if args.command == 'init':
        return _init_cloud(cfg, args)
    if args.command == 'bootstrap':
        return _bootstrap_cloud(cfg, args)
    if args.command == 'stats':
        return _stats_cloud(cfg)
    parser.error(f'Unknown command: {args.command}')
    return 2


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
