from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.cloud_drive.storage import S3StorageAdapter, resolve_storage_adapter
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

    import_add_cmd = sub.add_parser('import-source-add', help='Register a scanner/import folder')
    import_add_cmd.add_argument('--name', default='', help='Human-readable import source name')
    import_add_cmd.add_argument('--source-path', required=True, help='Local/network folder to import from')
    import_add_cmd.add_argument('--target-path', default='', help='Cloud Drive folder to import into')
    import_add_cmd.add_argument('--reference-only', action='store_true', help='Keep files by source_path instead of copying into storage')
    import_add_cmd.add_argument('--disabled', action='store_true', help='Register the source but keep it disabled')

    import_list_cmd = sub.add_parser('import-source-list', help='List registered scanner/import folders')
    import_list_cmd.add_argument('--enabled-only', action='store_true', help='Show only enabled sources')

    import_run_cmd = sub.add_parser('import-source-run', help='Queue or run a scanner/import folder')
    import_run_cmd.add_argument('source_id', help='Import source id')
    import_run_cmd.add_argument('--max-files', type=int, default=0, help='Import only first N changed/new files')
    import_run_cmd.add_argument('--run-now', action='store_true', help='Run synchronously instead of only queuing a job')

    sub.add_parser('stats', help='Show current cloud drive registry stats')
    sub.add_parser('compact-versions', help='Remove duplicate unchanged Cloud Drive version rows')
    backup_cmd = sub.add_parser('backup', help='Create a zip backup of config and local SQLite state')
    backup_cmd.add_argument('--output', default='', help='Backup zip path. Defaults to runtime/backups/cloud-drive-<timestamp>.zip')
    restore_cmd = sub.add_parser('restore', help='Restore files from a backup zip')
    restore_cmd.add_argument('backup', help='Backup zip path')
    restore_cmd.add_argument('--target-dir', default='', help='Restore all files into this directory instead of configured paths')
    restore_cmd.add_argument('--force', action='store_true', help='Allow overwriting existing files')
    verify_cmd = sub.add_parser('verify-backup', help='Verify archive checksums and SQLite integrity')
    verify_cmd.add_argument('backup', help='Backup zip path')
    drill_cmd = sub.add_parser('restore-drill', help='Restore into an empty target and verify registry, ACL and objects')
    drill_cmd.add_argument('backup', help='Backup zip path')
    drill_cmd.add_argument('--target-dir', default='', help='Keep the drill restore in this empty directory')
    preflight_cmd = sub.add_parser('preflight', help='Validate a fresh install or upgrade before changing runtime state')
    preflight_cmd.add_argument('--mode', choices=['fresh-install', 'upgrade'], default='upgrade')
    preflight_cmd.add_argument('--backup-dir', default='runtime/backups', help='Directory containing upgrade backups')
    preflight_cmd.add_argument('--max-backup-age-hours', type=float, default=24.0)
    preflight_cmd.add_argument('--min-free-gb', type=float, default=1.0)
    provider_backup_cmd = sub.add_parser('provider-backup', help='Create a full S3/MinIO object and SQLite snapshot')
    provider_backup_cmd.add_argument('--output-dir', default='', help='Snapshot directory')
    provider_backup_cmd.add_argument('--workers', type=int, default=8, help='Parallel object downloads')
    provider_verify_cmd = sub.add_parser('provider-verify', help='Verify a provider snapshot and all object hashes')
    provider_verify_cmd.add_argument('snapshot', help='Provider snapshot directory')
    provider_verify_cmd.add_argument('--workers', type=int, default=8, help='Parallel hash workers')
    provider_reconcile_cmd = sub.add_parser(
        'provider-reconcile',
        help='Reconcile active registry rows against a verified provider snapshot',
    )
    provider_reconcile_cmd.add_argument('snapshot', help='Provider snapshot directory')
    provider_reconcile_cmd.add_argument(
        '--apply',
        action='store_true',
        help='Apply safe key repairs and soft-delete source files that no longer exist',
    )
    provider_drill_cmd = sub.add_parser('provider-restore-drill', help='Verify snapshot and round-trip a sample via a temporary bucket')
    provider_drill_cmd.add_argument('snapshot', help='Provider snapshot directory')
    provider_drill_cmd.add_argument('--sample-size', type=int, default=25)
    provider_drill_cmd.add_argument('--workers', type=int, default=8, help='Parallel snapshot verification workers')
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


def _cloud_service_from_cfg(cfg: Dict[str, Any]) -> CloudDriveService:
    db_path, storage_root = _default_cloud_paths(cfg)
    cfg['cloud_drive_db_path'] = str(cfg.get('cloud_drive_db_path') or db_path)
    cfg.setdefault('cloud_drive_storage', 'local')
    if str(cfg.get('cloud_drive_storage') or 'local') == 'local':
        cfg['cloud_drive_storage_root'] = str(cfg.get('cloud_drive_storage_root') or storage_root)
    return CloudDriveService.from_config(cfg)


def _import_source_add(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    service = _cloud_service_from_cfg(cfg)
    source = service.upsert_import_source(
        name=str(args.name or ''),
        source_path=str(args.source_path or ''),
        target_path=str(args.target_path or ''),
        import_files=not bool(args.reference_only),
        enabled=not bool(args.disabled),
        created_by='cli',
    )
    print(json.dumps(source, ensure_ascii=False, indent=2))
    return 0


def _import_source_list(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    service = _cloud_service_from_cfg(cfg)
    print(json.dumps(service.list_import_sources(enabled_only=bool(args.enabled_only)), ensure_ascii=False, indent=2))
    return 0


def _import_source_run(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    service = _cloud_service_from_cfg(cfg)
    job = service.create_import_job(
        source_id=str(args.source_id or ''),
        max_files=(int(args.max_files or 0) or None),
    )
    result: Dict[str, Any] = {'job': asdict(job)}
    if args.run_now:
        result['stats'] = service.run_import_job(job.id)
        latest = service.get_job(job.id)
        if latest is not None:
            result['job'] = asdict(latest)
    print(json.dumps(result, ensure_ascii=False, indent=2))
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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _snapshot_sqlite(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(str(source), timeout=30)) as source_conn, closing(sqlite3.connect(str(target))) as target_conn:
        source_conn.backup(target_conn)
    with closing(sqlite3.connect(str(target))) as conn:
        result = str(conn.execute('PRAGMA integrity_check').fetchone()[0])
    if result.lower() != 'ok':
        raise RuntimeError(f'SQLite snapshot integrity check failed for {source}: {result}')


def _redact_config(value: Any, *, key: str = '') -> Any:
    sensitive = ('password', 'token', 'secret', 'api_key', 'access_key', 'private_key')
    if any(part in key.lower() for part in sensitive):
        return '[REDACTED]'
    if isinstance(value, dict):
        return {str(item_key): _redact_config(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, list):
        return [_redact_config(item, key=key) for item in value]
    return value


def _manifest_entry(path: Path, *, name: str, archive_path: str) -> Dict[str, Any]:
    return {
        'name': name,
        'archive_path': archive_path,
        'original_path': str(path),
        'size_bytes': path.stat().st_size,
        'sha256': _sha256_file(path),
    }


def _backup_cloud(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    output_arg = str(args.output or '').strip()
    output = Path(output_arg).expanduser() if output_arg else Path('runtime') / 'backups' / f'cloud-drive-{timestamp}.zip'
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    files = _configured_state_files(cfg)
    storage_backend = str(cfg.get('cloud_drive_storage') or 'local').strip().lower()
    _, default_storage_root = _default_cloud_paths(cfg)
    storage_root = Path(str(cfg.get('cloud_drive_storage_root') or default_storage_root)).expanduser().resolve()
    if storage_backend != 'local':
        raise SystemExit('Backup object storage is implemented only for the local backend; use provider-native backup for S3.')

    with tempfile.TemporaryDirectory(prefix='rag-backup-snapshot-') as temp_value:
        snapshot_dir = Path(temp_value)
        snapshots: Dict[str, Path] = {}
        for name, path in files.items():
            snapshot = snapshot_dir / f'{name}{path.suffix}'
            _snapshot_sqlite(path, snapshot)
            snapshots[name] = snapshot

        state_entries = []
        for name, path in snapshots.items():
            entry = _manifest_entry(path, name=name, archive_path=f'files/{name}{path.suffix}')
            entry['original_path'] = str(files[name])
            state_entries.append(entry)
        storage_entries: list[Dict[str, Any]] = []
        if storage_root.exists():
            for path in sorted(item for item in storage_root.rglob('*') if item.is_file()):
                relative = path.relative_to(storage_root).as_posix()
                if relative == '.healthcheck' or relative.startswith('.healthcheck/'):
                    continue
                storage_entries.append(
                    _manifest_entry(path, name=relative, archive_path=f'storage/{relative}')
                )
        manifest = {
            'created_at': datetime.now().astimezone().isoformat(timespec='seconds'),
            'kind': 'rag-catalog-cloud-drive-backup',
            'version': 2,
            'storage_backend': storage_backend,
            'files': state_entries,
            'storage_files': storage_entries,
        }
        with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            zf.writestr('manifest.json', json.dumps(manifest, ensure_ascii=False, indent=2))
            zf.writestr('config.snapshot.json', json.dumps(_redact_config(cfg), ensure_ascii=False, indent=2))
            for entry in state_entries:
                zf.write(snapshots[str(entry['name'])], str(entry['archive_path']))
            for entry in storage_entries:
                zf.write(storage_root / str(entry['name']), str(entry['archive_path']))
    print(json.dumps({'backup_path': str(output), 'files': manifest['files']}, ensure_ascii=False, indent=2))
    return 0


def _backup_path(value: str) -> Path:
    path = Path(str(value or '')).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise SystemExit(f'Backup не найден: {path}')
    return path


def _sha256_zip_entry(zf: zipfile.ZipFile, archive_path: str) -> str:
    digest = hashlib.sha256()
    with zf.open(archive_path) as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _verify_archive(backup_path: Path) -> Dict[str, Any]:
    with zipfile.ZipFile(backup_path, 'r') as zf:
        corrupt = zf.testzip()
        if corrupt:
            raise SystemExit(f'Повреждён ZIP entry: {corrupt}')
        try:
            manifest = json.loads(zf.read('manifest.json').decode('utf-8'))
        except (KeyError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise SystemExit(f'Некорректный backup manifest: {exc}') from exc
        if manifest.get('kind') != 'rag-catalog-cloud-drive-backup':
            raise SystemExit('Это не Cloud Drive backup.')
        names = set(zf.namelist())
        checked = 0
        for entry in [*manifest.get('files', []), *manifest.get('storage_files', [])]:
            archive_path = str(entry.get('archive_path') or '')
            if not archive_path or archive_path not in names:
                raise SystemExit(f'Backup entry отсутствует: {archive_path}')
            expected_size = int(entry.get('size_bytes') or 0)
            if zf.getinfo(archive_path).file_size != expected_size:
                raise SystemExit(f'Размер backup entry не совпадает: {archive_path}')
            expected_hash = str(entry.get('sha256') or '')
            if expected_hash and _sha256_zip_entry(zf, archive_path) != expected_hash:
                raise SystemExit(f'SHA-256 backup entry не совпадает: {archive_path}')
            checked += 1
    return {
        'backup_path': str(backup_path),
        'version': int(manifest.get('version') or 1),
        'created_at': str(manifest.get('created_at') or ''),
        'state_files': len(manifest.get('files', [])),
        'storage_files': len(manifest.get('storage_files', [])),
        'entries_checked': checked,
        'complete_local_backup': int(manifest.get('version') or 1) >= 2 and manifest.get('storage_backend') == 'local',
        'manifest': manifest,
    }


def _restore_archive(
    cfg: Dict[str, Any], backup_path: Path, *, target_dir: Path | None, force: bool
) -> list[dict[str, str]]:
    verification = _verify_archive(backup_path)
    manifest = verification['manifest']
    configured = _configured_state_files(cfg)
    _, default_storage_root = _default_cloud_paths(cfg)
    storage_root = Path(str(cfg.get('cloud_drive_storage_root') or default_storage_root)).expanduser().resolve()
    restored: list[dict[str, str]] = []
    with zipfile.ZipFile(backup_path, 'r') as zf:
        for entry in manifest.get('files', []):
            name = str(entry.get('name') or '')
            archive_path = str(entry.get('archive_path') or '')
            if target_dir is not None:
                target = target_dir / Path(str(entry.get('original_path') or f'{name}.db')).name
            else:
                target = configured.get(name) or Path(str(entry.get('original_path') or '')).expanduser().resolve()
            if target.exists() and not force:
                raise SystemExit(f'Файл уже существует, используйте --force для перезаписи: {target}')
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(archive_path) as src, target.open('wb') as dst:
                shutil.copyfileobj(src, dst)
            restored.append({'name': name, 'path': str(target)})
        for entry in manifest.get('storage_files', []):
            relative = Path(str(entry.get('name') or ''))
            if relative.is_absolute() or '..' in relative.parts:
                raise SystemExit(f'Небезопасный storage path в backup: {relative}')
            target_root = target_dir / 'cloud_storage' if target_dir is not None else storage_root
            target = target_root / relative
            if target.exists() and not force:
                raise SystemExit(f'Файл уже существует, используйте --force для перезаписи: {target}')
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(str(entry['archive_path'])) as src, target.open('wb') as dst:
                shutil.copyfileobj(src, dst)
            restored.append({'name': f'storage:{relative.as_posix()}', 'path': str(target)})
    return restored


def _restore_cloud(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    backup_path = _backup_path(args.backup)
    target_dir = Path(str(args.target_dir)).expanduser().resolve() if str(args.target_dir or '').strip() else None
    restored = _restore_archive(cfg, backup_path, target_dir=target_dir, force=bool(args.force))
    print(json.dumps({'backup_path': str(backup_path), 'restored': restored}, ensure_ascii=False, indent=2))
    return 0


def _verify_backup(args: argparse.Namespace) -> int:
    report = _verify_archive(_backup_path(args.backup))
    report.pop('manifest', None)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def _validate_drill_target(target_dir: Path) -> Dict[str, Any]:
    db_path = target_dir / 'cloud_drive.db'
    if not db_path.exists():
        raise SystemExit('Restore drill failed: cloud_drive.db отсутствует.')
    with closing(sqlite3.connect(str(db_path))) as conn:
        integrity = str(conn.execute('PRAGMA integrity_check').fetchone()[0])
        files = int(conn.execute('SELECT COUNT(*) FROM cloud_files').fetchone()[0])
        permissions = int(conn.execute('SELECT COUNT(*) FROM cloud_permissions').fetchone()[0])
        rows = conn.execute(
            "SELECT path, storage_key, checksum FROM cloud_files WHERE deleted_at='' AND storage_key<>'' ORDER BY path LIMIT 25"
        ).fetchall()
    if integrity.lower() != 'ok':
        raise SystemExit(f'Restore drill failed: SQLite integrity={integrity}')
    checked_objects = 0
    for path, storage_key, checksum in rows:
        object_path = target_dir / 'cloud_storage' / str(storage_key)
        if not object_path.is_file():
            raise SystemExit(f'Restore drill failed: object отсутствует для {path}: {storage_key}')
        expected = str(checksum or '').lower()
        if len(expected) == 64 and _sha256_file(object_path) != expected:
            raise SystemExit(f'Restore drill failed: checksum не совпадает для {path}')
        checked_objects += 1
    return {
        'ok': True,
        'sqlite_integrity': integrity,
        'registry_files': files,
        'acl_entries': permissions,
        'sample_objects_checked': checked_objects,
    }


def _restore_drill(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    backup_path = _backup_path(args.backup)
    explicit_target = str(args.target_dir or '').strip()
    if explicit_target:
        target_dir = Path(explicit_target).expanduser().resolve()
        if target_dir.exists() and any(target_dir.iterdir()):
            raise SystemExit(f'Restore drill target должен быть пустым: {target_dir}')
        target_dir.mkdir(parents=True, exist_ok=True)
        _restore_archive(cfg, backup_path, target_dir=target_dir, force=False)
        report = _validate_drill_target(target_dir)
        report.update({'backup_path': str(backup_path), 'target_dir': str(target_dir), 'kept': True})
    else:
        with tempfile.TemporaryDirectory(prefix='rag-restore-drill-') as temp_value:
            target_dir = Path(temp_value)
            _restore_archive(cfg, backup_path, target_dir=target_dir, force=False)
            report = _validate_drill_target(target_dir)
            report.update({'backup_path': str(backup_path), 'target_dir': '', 'kept': False})
    artifact = {
        **report,
        'completed_at': datetime.now().astimezone().isoformat(timespec='seconds'),
        'backup_size_bytes': backup_path.stat().st_size,
        'backup_mtime_ns': backup_path.stat().st_mtime_ns,
    }
    artifact_path = Path(f'{backup_path}.restore-drill.json')
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding='utf-8')
    report['artifact_path'] = str(artifact_path)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def _preflight_cloud(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    db_path, default_storage_root = _default_cloud_paths(cfg)
    registry_path = Path(str(cfg.get('cloud_drive_db_path') or db_path)).expanduser().resolve()
    storage_root = Path(str(cfg.get('cloud_drive_storage_root') or default_storage_root)).expanduser().resolve()
    checks: list[Dict[str, Any]] = []

    def record(name: str, ok: bool, detail: str) -> None:
        checks.append({'name': name, 'ok': bool(ok), 'detail': detail})

    for name, target in [('registry_parent', registry_path.parent), ('storage_root', storage_root)]:
        try:
            target.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(prefix='.rag-preflight-', dir=target):
                pass
            record(name, True, str(target))
        except OSError as exc:
            record(name, False, f'{target}: {exc}')

    free_bytes = shutil.disk_usage(storage_root).free if storage_root.exists() else 0
    required_bytes = max(0, int(float(args.min_free_gb) * 1024 ** 3))
    record('free_space', free_bytes >= required_bytes, f'free={free_bytes}; required={required_bytes}')

    if registry_path.exists():
        try:
            uri = f'file:{registry_path.as_posix()}?mode=ro'
            with closing(sqlite3.connect(uri, uri=True, timeout=5)) as conn:
                integrity = str(conn.execute('PRAGMA integrity_check').fetchone()[0])
            record('registry_integrity', integrity.lower() == 'ok', integrity)
        except sqlite3.Error as exc:
            record('registry_integrity', False, str(exc))
    else:
        record('registry_integrity', args.mode == 'fresh-install', 'new registry' if args.mode == 'fresh-install' else 'missing')

    if args.mode == 'upgrade':
        backup_dir = Path(str(args.backup_dir or 'runtime/backups')).expanduser().resolve()
        candidates = sorted(backup_dir.glob('*.zip'), key=lambda item: item.stat().st_mtime, reverse=True) if backup_dir.exists() else []
        if not candidates:
            record('fresh_verified_backup', False, f'no backup in {backup_dir}')
        else:
            latest = candidates[0]
            age_hours = max(0.0, (datetime.now().timestamp() - latest.stat().st_mtime) / 3600.0)
            try:
                verification = _verify_archive(latest)
                complete = bool(verification.get('complete_local_backup'))
                fresh = age_hours <= max(0.0, float(args.max_backup_age_hours))
                record('fresh_verified_backup', complete and fresh, f'{latest}; age_hours={age_hours:.2f}; complete={complete}')
            except SystemExit as exc:
                record('fresh_verified_backup', False, f'{latest}: {exc}')

    ok = all(bool(check['ok']) for check in checks)
    print(json.dumps({'ok': ok, 'mode': args.mode, 'checks': checks}, ensure_ascii=False, indent=2))
    return 0 if ok else 2


def _provider_snapshot_path(value: str) -> Path:
    path = Path(str(value or '')).expanduser().resolve()
    if not path.exists() or not path.is_dir():
        raise SystemExit(f'Provider snapshot не найден: {path}')
    if not (path / 'manifest.json').is_file():
        raise SystemExit(f'Provider snapshot manifest не найден: {path / "manifest.json"}')
    return path


def _safe_object_relative_path(storage_key: str) -> Path:
    relative = Path(str(storage_key or '').replace('\\', '/'))
    if not str(relative) or relative.is_absolute() or '..' in relative.parts:
        raise RuntimeError(f'Небезопасный storage key: {storage_key}')
    return relative


def _provider_backup(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    storage = resolve_storage_adapter(cfg)
    if not isinstance(storage, S3StorageAdapter):
        raise SystemExit('provider-backup предназначен для S3/MinIO storage.')
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    output_arg = str(args.output_dir or '').strip()
    output = (
        Path(output_arg).expanduser().resolve()
        if output_arg
        else (Path('runtime') / 'backups' / f's3-provider-{timestamp}').resolve()
    )
    if output.exists():
        raise SystemExit(f'Provider snapshot target уже существует: {output}')
    output.parent.mkdir(parents=True, exist_ok=True)
    workers = max(1, min(int(args.workers or 8), 32))
    state_files = _configured_state_files(cfg)

    with tempfile.TemporaryDirectory(prefix=f'{output.name}.partial-', dir=output.parent) as temp_value:
        snapshot = Path(temp_value)
        state_dir = snapshot / 'state'
        object_dir = snapshot / 'objects'
        state_entries: list[Dict[str, Any]] = []
        skipped_state_files: list[Dict[str, Any]] = []
        for name, source in state_files.items():
            target = state_dir / f'{name}{source.suffix}'
            snapshot_error = ''
            for attempt in range(3):
                try:
                    _snapshot_sqlite(source, target)
                    snapshot_error = ''
                    break
                except sqlite3.Error as exc:
                    snapshot_error = str(exc)
                    try:
                        target.unlink(missing_ok=True)
                    except OSError:
                        pass
                    time.sleep(0.5 * (attempt + 1))
            if snapshot_error:
                if name == 'index_state_db':
                    skipped_state_files.append(
                        {
                            'name': name,
                            'original_path': str(source),
                            'reason': 'snapshot_failed_rebuild_from_objects',
                            'error': snapshot_error,
                        }
                    )
                    continue
                raise RuntimeError(f'Provider backup state snapshot failed for {name}: {snapshot_error}')
            entry = _manifest_entry(target, name=name, archive_path=f'state/{target.name}')
            entry['original_path'] = str(source)
            state_entries.append(entry)

        keys = sorted(storage.list_keys())

        def download_object(storage_key: str) -> Dict[str, Any]:
            relative = _safe_object_relative_path(storage_key)
            target = object_dir / relative
            storage.download_file(storage_key, target)
            return {
                'storage_key': storage_key,
                'relative_path': relative.as_posix(),
                'size_bytes': target.stat().st_size,
                'sha256': _sha256_file(target),
            }

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='provider-backup') as executor:
            object_entries = []
            for index, entry in enumerate(executor.map(download_object, keys), start=1):
                object_entries.append(entry)
                if index % 1000 == 0 or index == len(keys):
                    print(
                        f'provider-backup progress objects={index}/{len(keys)} '
                        f'bytes={sum(int(item["size_bytes"]) for item in object_entries)}',
                        file=sys.stderr,
                        flush=True,
                    )

        manifest = {
            'kind': 'rag-catalog-s3-provider-backup',
            'version': 1,
            'created_at': datetime.now().astimezone().isoformat(timespec='seconds'),
            'backend': 's3',
            'endpoint': str(cfg.get('cloud_drive_s3_endpoint') or ''),
            'bucket': str(cfg.get('cloud_drive_bucket') or ''),
            'state_files': state_entries,
            'skipped_state_files': skipped_state_files,
            'objects': object_entries,
            'object_count': len(object_entries),
            'total_object_bytes': sum(int(entry['size_bytes']) for entry in object_entries),
        }
        (snapshot / 'config.snapshot.json').write_text(
            json.dumps(_redact_config(cfg), ensure_ascii=False, indent=2), encoding='utf-8'
        )
        (snapshot / 'manifest.json').write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8'
        )
        snapshot.rename(output)
    print(
        json.dumps(
            {
                'snapshot_path': str(output),
                'objects': len(object_entries),
                'total_object_bytes': manifest['total_object_bytes'],
                'state_files': len(state_entries),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _verify_provider_snapshot(snapshot: Path, *, workers: int = 8) -> Dict[str, Any]:
    try:
        manifest = json.loads((snapshot / 'manifest.json').read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f'Некорректный provider snapshot manifest: {exc}') from exc
    if manifest.get('kind') != 'rag-catalog-s3-provider-backup':
        raise SystemExit('Это не S3 provider snapshot.')
    checked_state = 0
    restored_registry: Path | None = None
    for entry in manifest.get('state_files', []):
        target = snapshot / str(entry.get('archive_path') or '')
        expected_size = int(entry['size_bytes']) if entry.get('size_bytes') is not None else -1
        if not target.is_file() or target.stat().st_size != expected_size:
            raise SystemExit(f'Provider state file отсутствует или имеет неверный размер: {target}')
        if _sha256_file(target) != str(entry.get('sha256') or ''):
            raise SystemExit(f'Provider state SHA-256 не совпадает: {target}')
        with closing(sqlite3.connect(str(target))) as conn:
            integrity = str(conn.execute('PRAGMA integrity_check').fetchone()[0])
        if integrity.lower() != 'ok':
            raise SystemExit(f'Provider state SQLite integrity failed: {target}: {integrity}')
        if str(entry.get('name') or '') == 'cloud_drive_db':
            restored_registry = target
        checked_state += 1
        print(
            f'provider-verify state={checked_state}/{len(manifest.get("state_files", []))} name={entry.get("name")}',
            file=sys.stderr,
            flush=True,
        )

    object_entries = list(manifest.get('objects', []))

    def verify_object(entry: Dict[str, Any]) -> tuple[str, int]:
        storage_key = str(entry.get('storage_key') or '')
        target = snapshot / 'objects' / _safe_object_relative_path(storage_key)
        expected_size = int(entry['size_bytes']) if entry.get('size_bytes') is not None else -1
        if not target.is_file() or target.stat().st_size != expected_size:
            raise SystemExit(f'Provider object отсутствует или имеет неверный размер: {storage_key}')
        if _sha256_file(target) != str(entry.get('sha256') or ''):
            raise SystemExit(f'Provider object SHA-256 не совпадает: {storage_key}')
        return storage_key, target.stat().st_size

    object_keys: set[str] = set()
    checked_objects = 0
    checked_bytes = 0
    worker_count = max(1, min(int(workers or 8), 32))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix='provider-verify') as executor:
        for storage_key, size_bytes in executor.map(verify_object, object_entries):
            object_keys.add(storage_key)
            checked_objects += 1
            checked_bytes += size_bytes
            if checked_objects % 2000 == 0 or checked_objects == len(object_entries):
                print(
                    f'provider-verify objects={checked_objects}/{len(object_entries)} bytes={checked_bytes}',
                    file=sys.stderr,
                    flush=True,
                )

    registry_keys: set[str] = set()
    if restored_registry is not None:
        with closing(sqlite3.connect(str(restored_registry))) as conn:
            rows = conn.execute(
                "SELECT DISTINCT storage_key FROM cloud_files WHERE deleted_at='' AND storage_key<>''"
            ).fetchall()
        registry_keys = {str(row[0]) for row in rows if str(row[0] or '').strip()}
        missing = sorted(registry_keys - object_keys)
        if missing:
            raise SystemExit(f'Provider snapshot не содержит {len(missing)} registry objects; пример: {missing[0]}')
    return {
        'ok': True,
        'snapshot_path': str(snapshot),
        'manifest_sha256': _sha256_file(snapshot / 'manifest.json'),
        'state_files_checked': checked_state,
        'objects_checked': checked_objects,
        'object_bytes_checked': checked_bytes,
        'registry_keys_checked': len(registry_keys),
        'manifest': manifest,
    }


def _provider_verify(args: argparse.Namespace) -> int:
    snapshot = _provider_snapshot_path(args.snapshot)
    report = _verify_provider_snapshot(snapshot, workers=int(args.workers or 8))
    report.pop('manifest', None)
    artifact = {
        **report,
        'completed_at': datetime.now().astimezone().isoformat(timespec='seconds'),
    }
    (snapshot / 'provider-verify.json').write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def _provider_registry_path(snapshot: Path, manifest: Dict[str, Any]) -> Path:
    for entry in manifest.get('state_files', []):
        if str(entry.get('name') or '') == 'cloud_drive_db':
            return snapshot / str(entry.get('archive_path') or '')
    raise SystemExit('Provider snapshot не содержит cloud_drive_db.')


def _provider_reconcile(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    snapshot = _provider_snapshot_path(args.snapshot)
    manifest_path = snapshot / 'manifest.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    if manifest.get('kind') != 'rag-catalog-s3-provider-backup':
        raise SystemExit('Это не S3 provider snapshot.')

    object_keys = {str(entry.get('storage_key') or '') for entry in manifest.get('objects', [])}
    objects_by_sha: dict[str, list[Dict[str, Any]]] = {}
    for entry in manifest.get('objects', []):
        objects_by_sha.setdefault(str(entry.get('sha256') or ''), []).append(entry)

    snapshot_registry = _provider_registry_path(snapshot, manifest)
    live_registry = Path(_default_cloud_paths(cfg)[0]).expanduser().resolve()
    if not live_registry.is_file():
        raise SystemExit(f'Рабочий Cloud Drive registry не найден: {live_registry}')

    def plan_registry(db_path: Path) -> Dict[str, Any]:
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, current_version_id, storage_key, checksum, source_path
                FROM cloud_files
                WHERE deleted_at='' AND storage_key<>''
                ORDER BY id
                """
            ).fetchall()
        repairs: list[Dict[str, str]] = []
        stale: list[Dict[str, str]] = []
        unresolved: list[Dict[str, str]] = []
        for row in rows:
            storage_key = str(row['storage_key'] or '')
            if storage_key in object_keys:
                continue
            checksum = str(row['checksum'] or '')
            candidates = sorted(
                objects_by_sha.get(checksum, []),
                key=lambda entry: str(entry.get('storage_key') or ''),
            )
            if candidates:
                candidate = candidates[0]
                candidate_key = str(candidate.get('storage_key') or '')
                candidate_path = snapshot / 'objects' / _safe_object_relative_path(candidate_key)
                if _sha256_file(candidate_path) != checksum:
                    raise SystemExit(f'Нельзя перепривязать повреждённый snapshot object: {candidate_key}')
                repairs.append(
                    {
                        'id': str(row['id']),
                        'current_version_id': str(row['current_version_id'] or ''),
                        'old_storage_key': storage_key,
                        'new_storage_key': candidate_key,
                    }
                )
                continue
            source_path = str(row['source_path'] or '').strip()
            source = Path(source_path) if source_path else None
            source_anchor = Path(source.anchor) if source is not None and source.anchor else None
            if source is not None and source_anchor is not None and source_anchor.exists() and not source.exists():
                stale.append({'id': str(row['id']), 'storage_key': storage_key})
            else:
                unresolved.append({'id': str(row['id']), 'storage_key': storage_key})
        return {'repairs': repairs, 'stale': stale, 'unresolved': unresolved}

    live_plan = plan_registry(live_registry)
    snapshot_plan = plan_registry(snapshot_registry)
    summary = {
        'apply': bool(args.apply),
        'live_registry': str(live_registry),
        'snapshot_registry': str(snapshot_registry),
        'rebound_rows': len(live_plan['repairs']),
        'soft_deleted_rows': len(live_plan['stale']),
        'unresolved_rows': len(live_plan['unresolved']),
    }
    if live_plan != snapshot_plan:
        raise SystemExit('Рабочий registry и snapshot registry требуют разного reconciliation plan.')
    if live_plan['unresolved']:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 2
    if not args.apply:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    changed_at = datetime.now().astimezone().isoformat(timespec='seconds')

    def apply_plan(db_path: Path) -> None:
        with closing(sqlite3.connect(str(db_path))) as conn, conn:
            for repair in live_plan['repairs']:
                conn.execute(
                    "UPDATE cloud_files SET storage_key=?, updated_at=? WHERE id=? AND deleted_at=''",
                    (repair['new_storage_key'], changed_at, repair['id']),
                )
                if repair['current_version_id']:
                    conn.execute(
                        'UPDATE cloud_file_versions SET storage_key=? WHERE id=?',
                        (repair['new_storage_key'], repair['current_version_id']),
                    )
            conn.executemany(
                "UPDATE cloud_files SET deleted_at=?, updated_at=? WHERE id=? AND deleted_at=''",
                [(changed_at, changed_at, item['id']) for item in live_plan['stale']],
            )
            integrity = str(conn.execute('PRAGMA integrity_check').fetchone()[0])
            if integrity.lower() != 'ok':
                raise RuntimeError(f'Registry integrity failed after reconciliation: {db_path}: {integrity}')

    apply_plan(live_registry)
    apply_plan(snapshot_registry)
    for entry in manifest.get('state_files', []):
        if str(entry.get('name') or '') == 'cloud_drive_db':
            entry['size_bytes'] = snapshot_registry.stat().st_size
            entry['sha256'] = _sha256_file(snapshot_registry)
            break
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    (snapshot / 'provider-verify.json').unlink(missing_ok=True)
    (snapshot / 'restore-drill.json').unlink(missing_ok=True)
    artifact = {
        **summary,
        'applied_at': changed_at,
        'manifest_sha256': _sha256_file(manifest_path),
    }
    (snapshot / 'provider-reconcile.json').write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    print(json.dumps(artifact, ensure_ascii=False, indent=2))
    return 0


def _provider_restore_drill(cfg: Dict[str, Any], args: argparse.Namespace) -> int:
    snapshot = _provider_snapshot_path(args.snapshot)
    manifest = json.loads((snapshot / 'manifest.json').read_text(encoding='utf-8'))
    manifest_sha256 = _sha256_file(snapshot / 'manifest.json')
    verification_artifact = snapshot / 'provider-verify.json'
    verification: Dict[str, Any] | None = None
    if verification_artifact.is_file():
        candidate = json.loads(verification_artifact.read_text(encoding='utf-8'))
        if bool(candidate.get('ok')) and str(candidate.get('manifest_sha256') or '') == manifest_sha256:
            verification = {**candidate, 'manifest': manifest}
    if verification is None:
        verification = _verify_provider_snapshot(snapshot, workers=int(args.workers or 8))
    entries = list(verification['manifest'].get('objects', []))
    sample_size = max(1, min(int(args.sample_size or 25), len(entries) or 1))
    sample = sorted(
        entries,
        key=lambda entry: hashlib.sha256(str(entry.get('storage_key') or '').encode('utf-8')).hexdigest(),
    )[:sample_size]
    base_bucket = str(cfg.get('cloud_drive_bucket') or 'rag').lower()
    drill_bucket = f'{base_bucket[:40]}-restore-drill-{uuid.uuid4().hex[:8]}'
    drill_cfg = dict(cfg)
    drill_cfg['cloud_drive_bucket'] = drill_bucket
    storage = resolve_storage_adapter(drill_cfg)
    if not isinstance(storage, S3StorageAdapter):
        raise SystemExit('provider-restore-drill предназначен для S3/MinIO storage.')
    storage.ensure_container()
    checked = 0
    try:
        with tempfile.TemporaryDirectory(prefix='rag-provider-drill-') as temp_value:
            temp_dir = Path(temp_value)
            for entry in sample:
                storage_key = str(entry.get('storage_key') or '')
                source = snapshot / 'objects' / _safe_object_relative_path(storage_key)
                storage.put_file(source, storage_key)
                restored = temp_dir / _safe_object_relative_path(storage_key)
                storage.download_file(storage_key, restored)
                if _sha256_file(restored) != str(entry.get('sha256') or ''):
                    raise RuntimeError(f'Restore round-trip SHA-256 не совпадает: {storage_key}')
                checked += 1
    finally:
        for entry in sample:
            storage.delete(str(entry.get('storage_key') or ''))
        storage._client.delete_bucket(Bucket=drill_bucket)
    artifact = {
        'ok': True,
        'completed_at': datetime.now().astimezone().isoformat(timespec='seconds'),
        'snapshot_path': str(snapshot),
        'manifest_sha256': verification['manifest_sha256'],
        'objects_verified': verification['objects_checked'],
        'object_bytes_verified': verification['object_bytes_checked'],
        'registry_keys_checked': verification['registry_keys_checked'],
        'round_trip_objects_checked': checked,
        'temporary_bucket_removed': True,
    }
    artifact_path = snapshot / 'restore-drill.json'
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({**artifact, 'artifact_path': str(artifact_path)}, ensure_ascii=False, indent=2))
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
    if args.command == 'import-source-add':
        return _import_source_add(cfg, args)
    if args.command == 'import-source-list':
        return _import_source_list(cfg, args)
    if args.command == 'import-source-run':
        return _import_source_run(cfg, args)
    if args.command == 'backup':
        return _backup_cloud(cfg, args)
    if args.command == 'restore':
        return _restore_cloud(cfg, args)
    if args.command == 'verify-backup':
        return _verify_backup(args)
    if args.command == 'restore-drill':
        return _restore_drill(cfg, args)
    if args.command == 'preflight':
        return _preflight_cloud(cfg, args)
    if args.command == 'provider-backup':
        return _provider_backup(cfg, args)
    if args.command == 'provider-verify':
        return _provider_verify(args)
    if args.command == 'provider-reconcile':
        return _provider_reconcile(cfg, args)
    if args.command == 'provider-restore-drill':
        return _provider_restore_drill(cfg, args)
    parser.error(f'Unknown command: {args.command}')
    return 2


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
