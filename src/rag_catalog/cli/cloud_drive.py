from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import tempfile
import zipfile
from contextlib import closing
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
    parser.error(f'Unknown command: {args.command}')
    return 2


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
