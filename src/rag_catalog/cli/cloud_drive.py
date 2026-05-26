from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import zipfile
from dataclasses import asdict
from datetime import datetime
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
    sub.add_parser('compact-versions', help='Remove duplicate unchanged Cloud Drive version rows')
    backup_cmd = sub.add_parser('backup', help='Create a zip backup of config and local SQLite state')
    backup_cmd.add_argument('--output', default='', help='Backup zip path. Defaults to runtime/backups/cloud-drive-<timestamp>.zip')
    restore_cmd = sub.add_parser('restore', help='Restore files from a backup zip')
    restore_cmd.add_argument('backup', help='Backup zip path')
    restore_cmd.add_argument('--target-dir', default='', help='Restore all files into this directory instead of configured paths')
    restore_cmd.add_argument('--force', action='store_true', help='Allow overwriting existing files')
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


def _compact_versions(cfg: Dict[str, Any]) -> int:
    db_path, storage_root = _default_cloud_paths(cfg)
    cfg['cloud_drive_db_path'] = str(cfg.get('cloud_drive_db_path') or db_path)
    cfg.setdefault('cloud_drive_storage', 'local')
    if str(cfg.get('cloud_drive_storage') or 'local') == 'local':
        cfg['cloud_drive_storage_root'] = str(cfg.get('cloud_drive_storage_root') or storage_root)
    service = CloudDriveService.from_config(cfg)
    before = service.registry.stats()
    deleted = service.registry.compact_duplicate_versions()
    after = service.registry.stats()
    print(json.dumps({
        'deleted_versions': deleted,
        'before': asdict(before),
        'after': asdict(after),
    }, ensure_ascii=False, indent=2))
    return 0


def _configured_state_files(cfg: Dict[str, Any]) -> Dict[str, Path]:
    base = Path(str(cfg.get('qdrant_db_path') or '.')).resolve()
    candidates = {
        'cloud_drive_db': Path(str(cfg.get('cloud_drive_db_path') or base / 'cloud_drive.db')).resolve(),
        'users_db': Path(str(cfg.get('users_db_path') or cfg.get('user_db_path') or base / 'rag_users.db')).resolve(),
        'telemetry_db': Path(str(cfg.get('telemetry_db_path') or base / 'rag_telemetry.db')).resolve(),
        'index_state_db': (base / 'index_state.db').resolve(),
    }
    return {name: path for name, path in candidates.items() if path.exists() and path.is_file()}


def _checkpoint_sqlite(path: Path) -> None:
    if path.suffix.lower() not in {'.db', '.sqlite', '.sqlite3'}:
        return
    try:
        with sqlite3.connect(str(path), timeout=5) as conn:
            conn.execute('PRAGMA wal_checkpoint(FULL)')
    except sqlite3.Error:
        pass


def _backup_cloud(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    output_arg = str(args.output or '').strip()
    output = Path(output_arg).expanduser() if output_arg else Path('runtime') / 'backups' / f'cloud-drive-{timestamp}.zip'
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    files = _configured_state_files(cfg)
    for path in files.values():
        _checkpoint_sqlite(path)
    manifest = {
        'created_at': datetime.now().isoformat(timespec='seconds'),
        'kind': 'rag-catalog-cloud-drive-backup',
        'version': 1,
        'files': [
            {
                'name': name,
                'archive_path': f'files/{name}{path.suffix}',
                'original_path': str(path),
                'size_bytes': path.stat().st_size,
            }
            for name, path in files.items()
        ],
    }
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('manifest.json', json.dumps(manifest, ensure_ascii=False, indent=2))
        zf.writestr('config.snapshot.json', json.dumps(cfg, ensure_ascii=False, indent=2))
        for entry in manifest['files']:
            name = str(entry['name'])
            zf.write(files[name], str(entry['archive_path']))
    print(json.dumps({'backup_path': str(output), 'files': manifest['files']}, ensure_ascii=False, indent=2))
    return 0


def _restore_cloud(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    backup_path = Path(str(args.backup or '')).expanduser().resolve()
    if not backup_path.exists():
        raise SystemExit(f'Backup не найден: {backup_path}')
    target_dir = Path(str(args.target_dir or '')).expanduser().resolve() if str(args.target_dir or '').strip() else None
    restored: list[dict[str, str]] = []
    configured = _configured_state_files(cfg)
    with zipfile.ZipFile(backup_path, 'r') as zf:
        manifest = json.loads(zf.read('manifest.json').decode('utf-8'))
        for entry in manifest.get('files', []):
            name = str(entry.get('name') or '')
            archive_path = str(entry.get('archive_path') or '')
            if not name or not archive_path:
                continue
            if target_dir is not None:
                target = target_dir / Path(str(entry.get('original_path') or f'{name}.db')).name
            else:
                target = configured.get(name) or Path(str(entry.get('original_path') or '')).expanduser().resolve()
            if not str(target):
                continue
            if target.exists() and not args.force:
                raise SystemExit(f'Файл уже существует, используйте --force для перезаписи: {target}')
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(archive_path) as src, target.open('wb') as dst:
                shutil.copyfileobj(src, dst)
            restored.append({'name': name, 'path': str(target)})
    print(json.dumps({'backup_path': str(backup_path), 'restored': restored}, ensure_ascii=False, indent=2))
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
    if args.command == 'compact-versions':
        return _compact_versions(cfg)
    if args.command == 'backup':
        return _backup_cloud(cfg, args)
    if args.command == 'restore':
        return _restore_cloud(cfg, args)
    parser.error(f'Unknown command: {args.command}')
    return 2


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
