"""Bounded, version-preserving exchange with an explicitly mapped Windows disk.

This is not a storage mount: Cloud Drive remains independently usable when the
other disk is offline. Missing entries never imply deletion or resurrection.
"""
from __future__ import annotations

import ctypes
import json
import math
import os
import shutil
import sqlite3
import stat
import tempfile
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from filelock import FileLock

from .shared_folders import clean_path
from .storage import compute_file_checksum

PRIVATE = '.rag-disk-bridge'
OWNER = 'RAG disk bridge staging. Do not edit pending operations.'


def signature(path: Path) -> str:
    info = path.stat()
    return f'{info.st_size}:{info.st_mtime_ns}'


def safe_child(root: Path, relative: str) -> Path:
    parts = clean_path(relative).split('/') if relative else []
    result = root
    for part in [None, *parts]:
        if part is not None:
            result = result / part
        try:
            info = result.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise RuntimeError(f'Redirected path is excluded: {result}')
    result.absolute().relative_to(root.absolute())
    return result


@contextmanager
def prevent_writes(path: Path):
    """Deny Windows writers while the old name is archived and replaced."""
    if os.name != 'nt':
        raise RuntimeError('Disk publication requires Windows sharing semantics')
    from ctypes import wintypes

    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                  wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    handle = kernel.CreateFileW(str(path), 0x80000000, 5, None, 3, 0, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        yield
    finally:
        kernel.CloseHandle(handle)


class DiskBridge:
    def __init__(self, service, *, source_root: str, mappings: list[dict],
                 state_path: str, settle_seconds: float = 60):
        self.service = service
        self.registry = service.registry
        self.root = Path(source_root)
        if not self.root.is_absolute():
            raise ValueError('Bridge source must be absolute')
        self.mappings = mappings
        if not mappings:
            raise ValueError('Explicit folder mappings are required')
        sources, ids = set(), set()
        for item in mappings:
            source = clean_path(item['source']) if item['source'] else ''
            if '/' in source or source.casefold() in sources or item['folder_id'] in ids:
                raise ValueError('Bridge mappings must be distinct top-level folders')
            sources.add(source.casefold())
            ids.add(item['folder_id'])
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.settle_seconds = max(0, settle_seconds)
        self.completed_version = ''
        self.completed_signature = ''
        self.lock = FileLock(str(self.state_path) + '.lock', timeout=0)
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS entries (
                    binding TEXT NOT NULL, relative TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    since REAL NOT NULL, outcome TEXT NOT NULL DEFAULT '',
                    attempted REAL NOT NULL DEFAULT 0,
                    PRIMARY KEY(binding,relative));
                CREATE TABLE IF NOT EXISTS folders (
                    binding TEXT NOT NULL, relative TEXT NOT NULL,
                    PRIMARY KEY(binding,relative));
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY, binding TEXT NOT NULL, relative TEXT NOT NULL,
                    stage TEXT NOT NULL, status TEXT NOT NULL, created REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            ''')
            settings = json.dumps({'source_root': str(self.root.absolute()),
                                   'mappings': mappings}, sort_keys=True)
            prior = db.execute("SELECT value FROM settings WHERE key='binding'").fetchone()
            if prior and prior[0] != settings:
                raise ValueError('Mappings changed: use a new state database after reviewing the scope')
            db.execute("INSERT OR IGNORE INTO settings VALUES ('binding',?)", (settings,))
            db.execute("INSERT OR IGNORE INTO settings VALUES ('started',?)", (str(time.time()),))
            self.started = float(db.execute("SELECT value FROM settings WHERE key='started'").fetchone()[0])

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.state_path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def binding(self, item):
        with self.registry._connect() as db:
            folder = db.execute('SELECT * FROM cloud_folders WHERE id=?', (item['folder_id'],)).fetchone()
            homes = [row[0] for row in db.execute('SELECT folder_path FROM cloud_user_folders')]
        if folder is None or folder['deleted_at']:
            raise RuntimeError('Mapped cloud folder was deleted; mapping is fenced')
        path = folder['path']
        if bool(path) != bool(item['source']) or '/' in path:
            raise RuntimeError('Mapped folder moved out of its approved root')
        if any(path.casefold() == home.casefold() or
               path.casefold().startswith(home.casefold() + '/') or
               (path and home.casefold().startswith(path.casefold() + '/')) for home in homes):
            raise RuntimeError('Personal folders are excluded from disk exchange')
        if item['source'].casefold() in {home.casefold() for home in homes}:
            raise RuntimeError('Personal source folder is excluded from disk exchange')
        if self.service.shared_folders and self.service.shared_folders.mount(path):
            raise RuntimeError('Live shared folders use their own worker')
        source = safe_child(self.root, item['source'])
        source.stat()
        if not source.is_dir():
            raise RuntimeError('Mapped source folder is unavailable')
        return path, source

    @staticmethod
    def ignored(name):
        return name.casefold().startswith(('.rag-', '~$')) or name.casefold() in {
            'desktop.ini', 'thumbs.db', '$recycle.bin', 'system volume information'}

    def inventory(self, source: Path, *, recursive: bool):
        files, folders = {}, set()
        pending = [source]
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    if self.ignored(entry.name):
                        continue
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
                        raise RuntimeError(f'Redirected source entry: {entry.name}')
                    relative = Path(entry.path).relative_to(source).as_posix()
                    clean_path(relative)
                    if entry.is_dir(follow_symlinks=False):
                        if recursive:
                            folders.add(relative)
                            pending.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        key = relative.casefold()
                        if key in files:
                            raise RuntimeError('Case-colliding source filenames')
                        files[key] = (relative, f'{info.st_size}:{info.st_mtime_ns}')
        return files, folders

    def cloud_rows(self, path):
        with self.registry._connect() as db:
            if path:
                rows = db.execute("SELECT * FROM cloud_files WHERE path LIKE ? ESCAPE '\\'",
                                  (self.registry._like_subtree(path),)).fetchall()
                folders = db.execute("SELECT * FROM cloud_folders WHERE path LIKE ? ESCAPE '\\'",
                                     (self.registry._like_subtree(path),)).fetchall()
            else:
                rows = db.execute("SELECT * FROM cloud_files WHERE instr(path,'/')=0").fetchall()
                folders = []
        result = {}
        for row in rows:
            relative = row['path'][len(path) + 1:] if path else row['path']
            if any(self.ignored(part) for part in relative.split('/')):
                continue
            key = relative.casefold()
            if key in result:
                raise RuntimeError('Case-colliding cloud filenames; manual reconciliation required')
            result[key] = dict(row)
        directory_rows = {}
        for row in folders:
            relative = row['path'][len(path) + 1:]
            if any(self.ignored(part) for part in relative.split('/')):
                continue
            if relative.casefold() in directory_rows:
                raise RuntimeError('Case-colliding cloud folders; manual reconciliation required')
            directory_rows[relative.casefold()] = dict(row)
        return result, directory_rows

    @staticmethod
    def cloud_time(service, row):
        if row['source_mtime'] and math.isfinite(row['source_mtime']):
            return row['source_mtime']
        versions = service.registry.list_file_versions(path=row['path'])
        version = next(v for v in versions if v['id'] == row['current_version_id'])
        value = datetime.fromisoformat(version['created_at'])
        if value.tzinfo is None:
            raise RuntimeError('Cloud version date is unknown')
        return value.timestamp()

    def assert_current(self, item, cloud_path, row):
        current_root, _ = self.binding(item)
        if current_root and not cloud_path.startswith(current_root + '/'):
            raise RuntimeError('Mapped root changed during exchange')
        current = self.registry.get_file_by_path(cloud_path)
        if current is None or current.deleted_at or current.current_version_id != row['current_version_id']:
            raise RuntimeError('Cloud version changed during disk publication')

    def publish(self, item, relative, row, expected_source, *, archived_checksum=''):
        """Journal a non-overwriting swap; an interruption fences this one path."""
        cloud_path = row['path']
        _, source = self.binding(item)
        target = safe_child(source, relative)
        stamp = self.cloud_time(self.service, row)
        if not math.isfinite(stamp) or not 0 < stamp <= time.time() + 300:
            raise RuntimeError('Cloud modification date is unknown or in the future')
        with tempfile.TemporaryDirectory(prefix='rag-disk-bridge-') as local:
            download = Path(local) / 'download'
            self.service.storage.download_file(row['storage_key'], download)
            if download.stat().st_size != row['size_bytes'] or compute_file_checksum(download) != row['checksum']:
                raise RuntimeError('Cloud object failed checksum verification')
            private = safe_child(self.root, PRIVATE)
            marker = safe_child(private, 'owner.txt')
            if private.exists() and (not marker.is_file() or marker.read_text(encoding='ascii') != OWNER):
                raise RuntimeError('Bridge staging folder is not owned by this worker')
            private.mkdir(exist_ok=True)
            if not marker.exists():
                marker.write_text(OWNER, encoding='ascii')
            operation = uuid.uuid4().hex
            stage = private / operation
            stage.mkdir()
            with self.connect() as db:
                db.execute('INSERT INTO operations VALUES (?,?,?,?,?,?)',
                           (operation, item['folder_id'], relative.casefold(), str(stage), 'preparing', time.time()))
            incoming, old = stage / 'new', stage / 'old'
            shutil.copyfile(download, incoming)
            os.utime(incoming, (stamp, stamp))
            if compute_file_checksum(incoming) != row['checksum']:
                raise RuntimeError('Disk staging failed checksum verification')
            self.assert_current(item, cloud_path, row)
            safe_child(source, relative)
            target.parent.mkdir(parents=True, exist_ok=True)
            if expected_source:
                with prevent_writes(target):
                    if signature(target) != expected_source or compute_file_checksum(target) != archived_checksum:
                        raise RuntimeError('Source changed after archival; original left in place')
                    with self.connect() as db:
                        db.execute("UPDATE operations SET status='pending' WHERE id=?", (operation,))
                    # Windows rename fails if the destination exists. Neither a
                    # concurrent new name nor a previous operation is overwritten.
                    target.rename(old)
                    if signature(old) != expected_source or compute_file_checksum(old) != archived_checksum:
                        raise RuntimeError('Archived source changed; both staged copies preserved')
                    incoming.rename(target)
            else:
                if os.name != 'nt':
                    raise RuntimeError('Disk publication requires non-overwriting Windows rename')
                with self.connect() as db:
                    db.execute("UPDATE operations SET status='pending' WHERE id=?", (operation,))
                incoming.rename(target)
            published_signature = signature(target)
            if compute_file_checksum(target) != row['checksum'] or signature(target) != published_signature:
                raise RuntimeError('Published disk copy failed checksum verification')
            with self.connect() as db:
                db.execute("UPDATE operations SET status='done' WHERE id=?", (operation,))
            # Only our own staging files are removed; the old bytes were verified
            # in immutable Cloud Drive history before a replacement was allowed.
            if old.exists():
                old.unlink()
            stage.rmdir()
        return published_signature

    def sync_folders(self, item, root_path, source, folders, cloud_folders, known, *,
                     apply, budget, report):
        """Only create directories. Remember disappearances without undoing them."""
        used = 0
        source_folders = {name.casefold(): name for name in folders}
        deleted = {key for key, row in cloud_folders.items() if row['deleted_at']}
        missing = {key for key, row in cloud_folders.items() if key not in source_folders and (
            key in known or datetime.fromisoformat(row['created_at']).timestamp() < self.started)}
        for key in sorted(source_folders.keys() | cloud_folders.keys(),
                          key=lambda name: (name.count('/'), name)):
            row = cloud_folders.get(key)
            if key in source_folders and row and not row['deleted_at']:
                continue
            if any(key == name or key.startswith(name + '/') for name in deleted):
                continue
            if any(key == name or key.startswith(name + '/') for name in missing):
                report['counts']['folder_missing_review'] += 1
                continue
            if key not in source_folders:
                if key in known or any(key.startswith(name + '/') and name not in source_folders for name in known):
                    continue
            if not apply or used >= budget:
                report['counts']['folder_pending'] += 1
                continue
            fresh_root, _ = self.binding(item)
            if fresh_root != root_path:
                raise RuntimeError('Mapped folder changed during directory exchange')
            if key in source_folders:
                relative = source_folders[key]
                target_path = f'{root_path}/{relative}'
                origin = safe_child(source, relative)
                if not origin.is_dir():
                    continue
                self.registry.ensure_import_folder(path=target_path, source_path=str(origin))
            else:
                relative = row['path'][len(root_path) + 1:]
                current = self.registry.get_folder_by_path(row['path'])
                if current is None or current.id != row['id']:
                    continue
                safe_child(source, relative).mkdir(parents=True, exist_ok=True)
                with self.connect() as db:
                    db.execute('INSERT OR IGNORE INTO folders VALUES (?,?)', (item['folder_id'], key))
            report['counts']['folders_created'] += 1
            used += 1
        return used

    def exchange(self, item, root_path, relative, source_sig, row, known_folders):
        self.completed_version = ''
        self.completed_signature = source_sig
        source = safe_child(safe_child(self.root, item['source']), relative)
        path = row['path'] if row else '/'.join(v for v in (root_path, relative) if v)
        fresh_root, _ = self.binding(item)
        if fresh_root != root_path:
            raise RuntimeError('Mapped cloud folder changed during scan')
        if row and row['deleted_at']:
            return 'cloud_deleted'
        source_size = int(source_sig.split(':')[0]) if source_sig else 0
        required_space = source_size * 2 + (row['size_bytes'] if row else 0) + 512 * 1024 * 1024
        if shutil.disk_usage(tempfile.gettempdir()).free < required_space:
            raise RuntimeError('Insufficient local staging space; exchange deferred')
        if source_sig:
            if signature(source) != source_sig:
                return 'source_busy'
            if row is None:
                result = self.service.import_missing_file_from_source(
                    path=path, source_path=str(source), apply=True, actor='disk-bridge')
                self.completed_version = result['current_version_id']
                return 'imported'
            result = self.service.reconcile_file_from_source(path=path, source_path=str(source),
                                                             apply=True, actor='disk-bridge')
            if result['action'] in {'updated', 'unchanged'}:
                self.completed_version = result['current_version_id']
                return 'imported' if result['action'] == 'updated' else 'verified'
            if result['action'] != 'archived_older':
                return 'conflict'
            archived = next(v for v in self.registry.list_file_versions(path=path)
                            if v['id'] == result['version_id'])
            self.completed_signature = self.publish(item, relative, row, source_sig, archived_checksum=archived['checksum'])
            self.completed_version = row['current_version_id']
            return 'exported'
        if row is None:
            return 'missing'
        parents = [p.as_posix().casefold() for p in Path(relative).parents if p.as_posix() != '.']
        source_base = safe_child(self.root, item['source'])
        if any(parent in known_folders and not safe_child(source_base, parent).is_dir()
               for parent in parents):
            return 'source_deleted'
        versions = self.registry.list_file_versions(path=path)
        if any(v.get('source_mtime', 0) > 0 for v in versions):
            return 'source_missing_review'
        self.completed_signature = self.publish(item, relative, row, '')
        self.completed_version = row['current_version_id']
        return 'exported'

    def run(self, *, apply=False, max_files=50):
        with self.lock:
            if apply:
                self.clean_preparations()
            return self._run(apply=apply, max_files=max(1, max_files))

    def clean_preparations(self):
        """Discard only uncommitted staging copies; pending swaps require review."""
        with self.connect() as db:
            rows = db.execute("SELECT * FROM operations WHERE status='preparing'").fetchall()
        for row in rows:
            operation = row['id']
            if len(operation) != 32 or any(c not in '0123456789abcdef' for c in operation):
                raise RuntimeError('Invalid staging operation identity')
            private = safe_child(self.root, PRIVATE)
            stage = safe_child(private, operation)
            if Path(row['stage']).absolute() != stage.absolute():
                raise RuntimeError('Staging path does not belong to this bridge')
            marker = safe_child(private, 'owner.txt')
            if marker.read_text(encoding='ascii') != OWNER:
                raise RuntimeError('Staging owner does not match')
            if stage.exists():
                names = {child.name for child in stage.iterdir()}
                if names - {'new'}:
                    raise RuntimeError('Unexpected staged files; manual recovery required')
                if 'new' in names:
                    safe_child(stage, 'new').unlink()
                stage.rmdir()
            with self.connect() as db:
                db.execute("UPDATE operations SET status='aborted' WHERE id=?", (operation,))

    def _run(self, *, apply, max_files):
        report = {'started_at': time.time(), 'applied': apply, 'counts': Counter(),
                  'errors': [], 'changes': [], 'scanned_files': 0}
        self.root.stat()
        safe_child(self.root, '')
        actions = 0
        for item in self.mappings:
            try:
                root_path, source = self.binding(item)
                disk, folders = self.inventory(source, recursive=bool(root_path))
                cloud, cloud_folders = self.cloud_rows(root_path)
                deleted_folders = {row['path'].casefold() for row in cloud_folders.values() if row['deleted_at']}
                with self.connect() as db:
                    state = {row['relative']: dict(row) for row in db.execute(
                        'SELECT * FROM entries WHERE binding=?', (item['folder_id'],))}
                    known_folders = {row[0] for row in db.execute(
                        'SELECT relative FROM folders WHERE binding=?', (item['folder_id'],))}
                    blocked = {row[0] for row in db.execute(
                        "SELECT relative FROM operations WHERE binding=? AND status='pending'", (item['folder_id'],))}
                    db.executemany('INSERT OR IGNORE INTO folders VALUES (?,?)',
                                   ((item['folder_id'], folder.casefold()) for folder in folders))
                if root_path:
                    actions += self.sync_folders(item, root_path, source, folders, cloud_folders,
                                                 known_folders, apply=apply, budget=max_files-actions,
                                                 report=report)
                present_folders = {name.casefold() for name in folders}
                missing_folders = {
                    key for key, row in cloud_folders.items()
                    if key not in present_folders and (key in known_folders or
                        datetime.fromisoformat(row['created_at']).timestamp() < self.started)
                }
                keys = sorted(disk.keys() | cloud.keys(), key=lambda key: state.get(key, {}).get('attempted', 0))
                updates = []
                for key in keys:
                    row = cloud.get(key)
                    if key in disk:
                        relative, source_sig = disk[key]
                    else:
                        relative = row['path'][len(root_path) + 1:] if root_path else row['path']
                        source_sig = ''
                    fingerprint = json.dumps([source_sig, row['current_version_id'] if row else '',
                                              row['deleted_at'] if row else ''])
                    prior = state.get(key)
                    now = time.time()
                    since = prior['since'] if prior and prior['fingerprint'] == fingerprint else now
                    outcome = ''
                    report['scanned_files'] += 1
                    path = row['path'] if row else '/'.join(v for v in (root_path, relative) if v)
                    deleted_parent = any(path.casefold().startswith(folder + '/') for folder in deleted_folders)
                    if key in blocked:
                        outcome = 'recovery_required'
                    elif deleted_parent or (row and row['deleted_at']):
                        outcome = 'cloud_deleted'
                    elif prior and not source_sig and (json.loads(prior['fingerprint'])[0]
                                                       or prior['outcome'] == 'source_deleted'):
                        outcome = 'source_deleted'
                    elif not source_sig and any(key.startswith(folder + '/') for folder in missing_folders):
                        outcome = 'source_missing_review'
                    elif prior and prior['fingerprint'] == fingerprint and prior['outcome'] in {
                        'matched_metadata', 'verified', 'conflict', 'source_deleted',
                        'source_missing_review', 'cloud_deleted',
                    }:
                        outcome = prior['outcome']
                    elif row and source_sig and int(source_sig.split(':')[0]) == row['size_bytes'] and abs(
                            int(source_sig.split(':')[1]) / 1e9 - row['source_mtime']) <= 0.001:
                        outcome = 'matched_metadata'
                    elif now - since < self.settle_seconds:
                        outcome = 'settling'
                    elif not apply:
                        outcome = 'planned'
                    elif actions >= max_files:
                        outcome = 'pending'
                    else:
                        actions += 1
                        try:
                            outcome = self.exchange(item, root_path, relative, source_sig, row, known_folders)
                            report['changes'].append({'path': path, 'outcome': outcome})
                            if outcome in {'imported', 'exported'}:
                                fingerprint = json.dumps([self.completed_signature, self.completed_version, ''])
                        except Exception as exc:
                            outcome = 'error'
                            report['errors'].append({'path': path, 'error': str(exc)[:1000]})
                    report['counts'][outcome] += 1
                    updates.append((item['folder_id'], key, fingerprint, since,
                                    'verified' if outcome in {'imported', 'exported'} else outcome,
                                    now if outcome in {'imported', 'exported', 'error'} else
                                    prior['attempted'] if prior else 0))
                with self.connect() as db:
                    db.executemany('INSERT OR REPLACE INTO entries VALUES (?,?,?,?,?,?)', updates)
            except Exception as exc:
                report['errors'].append({'source': item['source'], 'error': str(exc)[:1000]})
        report['finished_at'] = time.time()
        report['complete'] = not report['errors'] and not any(
            report['counts'][key] for key in ('pending', 'settling', 'planned', 'conflict',
                                             'recovery_required', 'source_missing_review', 'source_busy',
                                             'folder_missing_review', 'folder_pending'))
        return report
