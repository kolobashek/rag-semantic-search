"""Opt-in server-side writable shares. SMB is never mounted on a client PC.

Cloud writes are serialized with polling and journalled before touching SMB.
An interrupted mutation fences the share for recovery, rather than guessing
which side won. Unreachable/incompletely enumerated shares never imply deletes.
"""
from __future__ import annotations

import inspect
import os
import shutil
import sqlite3
import stat
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path

from filelock import FileLock

from .storage import compute_file_checksum

_LOCKS: dict[str, FileLock] = {}
_LOCKS_GUARD = threading.Lock()
PRIVATE = '.rag-cloud'


def shared_write(method):
    signature = inspect.signature(method)

    @wraps(method)
    def wrapped(self, *args, **kwargs):
        bridge = getattr(self, 'shared_folders', None)
        if bridge is None:
            return method(self, *args, **kwargs)
        bound = signature.bind(self, *args, **kwargs)
        bound.apply_defaults()
        params = {k: v for k, v in bound.arguments.items() if k != 'self'}
        return bridge.mutate(self, method, params)

    return wrapped


def clean_path(value: str) -> str:
    value = str(value).replace('\\', '/').strip('/')
    for part in value.split('/'):
        if (not part or part in {'.', '..'} or part.lower() == PRIVATE
                or any(c in part for c in ':<>"|?*\x00') or part.endswith((' ', '.'))
                or any(ord(c) < 32 for c in part)
                or part.split('.')[0].upper() in {'CON', 'PRN', 'AUX', 'NUL',
                    *(f'COM{i}' for i in range(1, 10)), *(f'LPT{i}' for i in range(1, 10))}):
            raise RuntimeError('Invalid shared folder path')
    return value


def _is_link(path: Path) -> bool:
    info = path.lstat()
    return stat.S_ISLNK(info.st_mode) or bool(getattr(info, 'st_file_attributes', 0) & 0x400)


@dataclass(frozen=True)
class Share:
    path: str
    root: Path
    retention_days: int = 14
    settle_seconds: int = 60
    exclude_names: tuple[str, ...] = ()

    def physical(self, path: str) -> Path:
        path = clean_path(path)
        if path != self.path and not path.startswith(self.path + '/'):
            raise RuntimeError('Path is outside the share')
        self.root.stat()  # Permission/network errors must not become missing files.
        if not self.root.is_dir() or _is_link(self.root):
            raise RuntimeError('Shared root unavailable or redirected')
        target = self.root
        for part in path[len(self.path):].strip('/').split('/'):
            if not part:
                continue
            if part in self.exclude_names:
                raise RuntimeError('This entry is excluded from the shared folder')
            target = target / part
            try:
                if _is_link(target):
                    raise RuntimeError('Links/reparse points are not allowed in shared folders')
            except FileNotFoundError:
                pass
        return target


class SharedFolders:
    def __init__(self, registry, specifications: list[dict]):
        self.registry = registry
        self.shares = [Share(clean_path(s['path']), Path(s['source_path']),
                             max(1, int(s.get('retention_days', 14))),
                             max(1, int(s.get('settle_seconds', 60))),
                             tuple(s.get('exclude_names', []))) for s in specifications]
        for index, share in enumerate(self.shares):
            if '/' in share.path:
                raise RuntimeError('Shared roots must be top-level folders')
            if not share.root.is_absolute():
                raise RuntimeError('Shared folder source must be absolute')
            for other in self.shares[:index]:
                if (share.path.casefold() == other.path.casefold()
                        or share.path.casefold().startswith(other.path.casefold() + '/')
                        or other.path.casefold().startswith(share.path.casefold() + '/')):
                    raise RuntimeError('Overlapping shared folders')
        self.db_path = str(registry.db_path) + '.shared.sqlite3'
        with _LOCKS_GUARD:
            self.lock = _LOCKS.setdefault(self.db_path, FileLock(self.db_path + '.lock', timeout=5))
        with self.connect() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY, mount TEXT NOT NULL, action TEXT NOT NULL,
                    path TEXT NOT NULL, target TEXT NOT NULL, status TEXT NOT NULL,
                    created REAL NOT NULL, expires REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS observations (
                    path TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, since REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS expired (
                    kind TEXT NOT NULL, id TEXT NOT NULL, deleted_at TEXT NOT NULL,
                    PRIMARY KEY(kind,id,deleted_at));
                CREATE TABLE IF NOT EXISTS garbage (storage_key TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS status (
                    mount TEXT PRIMARY KEY, checked REAL NOT NULL, error TEXT NOT NULL,
                    imported INTEGER NOT NULL, pending INTEGER NOT NULL);
            ''')

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def mount(self, path: str) -> Share | None:
        path = self.registry._normalize_path(path)
        for share in self.shares:
            if path.casefold() == share.path.casefold() or path.casefold().startswith(share.path.casefold() + '/'):
                # Reject alternate spellings instead of creating duplicate Windows paths.
                if not (path == share.path or path.startswith(share.path + '/')):
                    raise RuntimeError('Use the registered spelling of the shared folder')
                return share
        return None

    def _ready(self, share):
        share.physical(share.path)
        with self.connect() as conn:
            pending = conn.execute("SELECT id FROM operations WHERE mount=? AND status='pending' LIMIT 1",
                                   (share.path,)).fetchone()
        if pending:
            raise RuntimeError(f'Shared folder recovery required; operation {pending[0]}')

    def rows(self, share, *, deleted=False):
        result = []
        with self.registry._connect() as conn:
            for kind, table in [('file', 'cloud_files'), ('folder', 'cloud_folders')]:
                result.extend(dict(row, kind=kind) for row in conn.execute(
                    f"SELECT * FROM {table} WHERE (path=? OR path LIKE ? ESCAPE '\\') "
                    + ("AND deleted_at!=''" if deleted else "AND deleted_at=''"),
                    (share.path, self.registry._like_subtree(share.path))))
        return result

    def is_expired(self, kind, node_id, deleted_at):
        with self.connect() as conn:
            return conn.execute('SELECT 1 FROM expired WHERE kind=? AND id=? AND deleted_at=?',
                                (kind, node_id, deleted_at)).fetchone() is not None

    def _inventory(self, share, base=None):
        root = base or share.physical(share.path)
        found = {}

        def fail(error):
            raise error

        for directory, dirs, files in os.walk(root, onerror=fail, followlinks=False):
            parent = Path(directory)
            dirs[:] = [name for name in dirs if name != PRIVATE and name not in share.exclude_names]
            files = [name for name in files if name not in share.exclude_names]
            for name in dirs + files:
                child = parent / name
                info = child.lstat()
                if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
                    raise RuntimeError(f'Redirected entry in shared folder: {child.name}')
                relative = child.relative_to(share.root).as_posix()
                logical = clean_path(share.path + '/' + relative)
                found[logical] = (child, info, 'folder' if stat.S_ISDIR(info.st_mode) else 'file')
        return found

    def _check_unchanged(self, service, share, path, physical):
        node = self.registry.get_node_by_path(path)
        if not physical.exists():
            if node is not None and not node.deleted_at:
                raise RuntimeError('SMB file changed; wait for synchronization and retry')
            return
        if node is None or node.deleted_at:
            raise RuntimeError('New SMB content exists here; wait for synchronization')
        if physical.is_file():
            before = physical.stat()
            if before.st_mtime != node.source_mtime and time.time() - before.st_mtime < share.settle_seconds:
                raise RuntimeError('SMB file is still settling; retry after synchronization')
            if compute_file_checksum(physical) != node.checksum:
                raise RuntimeError('SMB file changed; wait for synchronization and retry')
            after = physical.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise RuntimeError('SMB file is being written')
        else:
            actual = self._inventory(share, physical)
            known = {r['path']: r for r in self.rows(share)
                     if r['path'].startswith(path + '/')}
            if set(actual) != set(known):
                raise RuntimeError('SMB folder changed; wait for synchronization and retry')
            for logical, (file, _, kind) in actual.items():
                if kind == 'file':
                    before = file.stat()
                    if (before.st_mtime != known[logical]['source_mtime']
                            and time.time() - before.st_mtime < share.settle_seconds):
                        raise RuntimeError('SMB folder contains a file still being written')
                    if compute_file_checksum(file) != known[logical]['checksum']:
                        raise RuntimeError('SMB folder contains modified files; retry after synchronization')
                    after = file.stat()
                    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                        raise RuntimeError('SMB folder changed during validation')

    def _private(self, share, op_id):
        if str(uuid.UUID(op_id)) != op_id:
            raise RuntimeError('Invalid shared operation id')
        private = share.root / PRIVATE
        private.mkdir(exist_ok=True)
        if _is_link(private):
            raise RuntimeError('Private shared directory is redirected')
        target = private / op_id
        if target.exists() and _is_link(target):
            raise RuntimeError('Shared operation directory is redirected')
        target.mkdir(exist_ok=True)
        return target

    def mutate(self, service, method, params):
        action = method.__name__
        if action in {'upload_file', 'create_folder'}:
            name = params.get('filename', params.get('name', ''))
            if '/' in name or '\\' in name:
                raise RuntimeError('Expected a filename, not a path')
            path = '/'.join(filter(None, [params.get('parent_path', '').strip('/'), name]))
        else:
            path = params.get('path', params.get('source_path', ''))
        share = self.mount(path)
        target = ''
        if action == 'move_node':
            node = self.registry.get_node_by_path(path)
            target = '/'.join(filter(None, [params.get('dest_parent_path', '').strip('/'),
                                            params.get('new_name') or (node.name if node else '')]))
            if self.mount(target) != share:
                raise RuntimeError('Move across a shared folder boundary is not supported; copy first')
        # The lock also protects content-addressed object GC from ordinary uploads.
        with self.lock:
            if share is None:
                return method(service, **params)
            self._ready(share)
            path = clean_path(path)
            if path == share.path:
                raise RuntimeError('The shared root cannot be renamed or deleted')
            physical = share.physical(path)
            if action in {'delete_node', 'move_node'}:
                existing = self.registry.get_node_by_path(path)
                if existing is None or existing.deleted_at:
                    raise RuntimeError('Active shared node not found')
            if action == 'create_folder' and physical.exists():
                raise RuntimeError('Folder already exists on SMB')
            if action == 'upload_file' and physical.exists() and not physical.is_file():
                raise RuntimeError('Upload destination is a folder')
            if action == 'move_node' and target.startswith(path + '/'):
                raise RuntimeError('Cannot move a folder into itself')
            if action == 'restore_node':
                self._validate_restore(share, path)
                if physical.exists():
                    raise RuntimeError('Restore destination already exists on SMB')
            else:
                self._check_unchanged(service, share, path, physical)
            destination = share.physical(target) if target else physical
            if target and destination.exists():
                raise RuntimeError('Move destination already exists on SMB')
            if not destination.parent.is_dir():
                raise RuntimeError('SMB parent is unavailable')
            op_id = str(uuid.uuid4())
            stage = self._private(share, op_id)
            now = time.time()
            with self.connect() as conn:
                conn.execute('INSERT INTO operations VALUES (?,?,?,?,?,?,?,?)',
                             (op_id, share.path, action, path, target, 'staged', now,
                              now + share.retention_days * 86400))
            if action == 'upload_file':
                snapshot = stage / 'snapshot'
                shutil.copyfile(params['source_path'], snapshot)
                key = service._immutable_storage_key(checksum=compute_file_checksum(snapshot), filename=physical.name)
                if not service.storage.exists(key):
                    service.storage.put_file(snapshot, key)
                shutil.copyfile(snapshot, stage / 'new')
                params = dict(params, source_path=str(snapshot))
            elif action == 'restore_node':
                self._stage_restore(service, share, path, stage / 'new')
            with self.connect() as conn:
                conn.execute("UPDATE operations SET status='pending' WHERE id=?", (op_id,))
            # Keep old content on the same share. Any failure after this point is
            # recoverable and fences subsequent writes (including the scanner).
            if action in {'upload_file', 'delete_node'} and physical.exists():
                physical.rename(stage / 'old')
            if action in {'upload_file', 'restore_node'}:
                (stage / 'new').rename(physical)
            elif action == 'create_folder':
                physical.mkdir()
            elif action == 'move_node':
                physical.rename(destination)
            result = method(service, **params)
            if action in {'upload_file', 'restore_node', 'move_node'} and destination.is_file():
                with self.registry._connect() as conn:
                    conn.execute('UPDATE cloud_files SET source_mtime=? WHERE path=?',
                                 (destination.stat().st_mtime, target or path))
            with self.connect() as conn:
                conn.execute("UPDATE operations SET status='done' WHERE id=?", (op_id,))
            if action == 'upload_file':
                (stage / 'snapshot').unlink()
            return result

    def _validate_restore(self, share, path):
        rows = [r for r in self.rows(share, deleted=True) if r['path'] == path]
        if not rows:
            raise RuntimeError('Deleted node not found')
        row = rows[0]
        if (self.is_expired(row['kind'], row['id'], row['deleted_at'])
                or datetime.fromisoformat(row['deleted_at']) + timedelta(days=share.retention_days)
                <= datetime.now(timezone.utc)):
            raise RuntimeError('Trash retention expired')

    def _stage_restore(self, service, share, path, destination):
        rows = self.rows(share, deleted=True)
        root = next(r for r in rows if r['path'] == path)
        if root['kind'] == 'folder':
            destination.mkdir()
        for row in sorted(rows, key=lambda r: (r['path'].count('/'), r['kind'] != 'folder')):
            if row['deleted_at'] != root['deleted_at']:
                continue
            if row['path'] != path and not row['path'].startswith(path + '/'):
                continue
            local = destination if row['path'] == path else destination / row['path'][len(path) + 1:]
            if row['kind'] == 'folder':
                local.mkdir(parents=True, exist_ok=True)
            else:
                service.storage.download_file(row['storage_key'], local)
                if compute_file_checksum(local) != row['checksum']:
                    raise RuntimeError('Restore checksum mismatch')

    def scan(self, service, share, *, limit=100):
        self._ready(share)
        inventory = self._inventory(share)  # Complete or fail; never partial deletes.
        imported = pending = 0
        with self.lock:
            self._ready(share)
            service._ensure_folder_path(share.path)
            known = {r['path']: r for r in self.rows(share)}
            ready = set()
            now = time.time()
            # One transaction per inventory, not one SMB/database round trip for
            # each unchanged file in a large archive.
            with self.connect() as conn:
                observations = {r['path']: r for r in conn.execute('SELECT * FROM observations')}
                for path, (_, info, kind) in inventory.items():
                    if kind != 'file':
                        continue
                    fingerprint = f'{info.st_size}:{info.st_mtime_ns}'
                    seen = observations.get(path)
                    if not seen or seen['fingerprint'] != fingerprint:
                        conn.execute('INSERT OR REPLACE INTO observations VALUES (?,?,?)', (path, fingerprint, now))
                    elif now - seen['since'] >= share.settle_seconds and now - info.st_mtime >= share.settle_seconds:
                        ready.add(path)
        for path, (physical, info, kind) in sorted(inventory.items(), key=lambda p: (p[0].count('/'), p[0])):
            previous = known.get(path)
            if previous and previous['kind'] == kind:
                if kind == 'folder' or (previous['size_bytes'] == info.st_size and previous['source_mtime'] == info.st_mtime):
                    continue
            if kind == 'file' and (path not in ready or imported >= limit):
                pending += 1
                continue
            with self.lock:
                self._ready(share)
                physical = share.physical(path)
                try:
                    current = physical.stat()
                except FileNotFoundError:
                    continue
                if (current.st_size, current.st_mtime_ns) != (info.st_size, info.st_mtime_ns):
                    pending += 1
                    continue
                node = self.registry.get_node_by_path(path)
                if node and node.deleted_at:
                    # A genuinely new file at an old name is imported after settling.
                    node = None
                if kind == 'folder':
                    service._ensure_folder_path(path)
                    continue
                if node and node.size_bytes == info.st_size and node.source_mtime == info.st_mtime:
                    continue
                # Snapshot before publishing; never hash one revision and upload another.
                with tempfile.TemporaryDirectory(prefix='rag-share-') as tmp:
                    snapshot = Path(tmp) / physical.name
                    shutil.copyfile(physical, snapshot)
                    after = physical.stat()
                    if (after.st_size, after.st_mtime_ns) != (info.st_size, info.st_mtime_ns):
                        pending += 1
                        continue
                    checksum = compute_file_checksum(snapshot)
                    if not node or node.checksum != checksum:
                        service.upload_file.__wrapped__(service, parent_path=path.rsplit('/', 1)[0],
                                                        filename=physical.name, source_path=str(snapshot))
                    with self.registry._connect() as conn:
                        conn.execute('UPDATE cloud_files SET source_mtime=? WHERE path=?', (info.st_mtime, path))
                imported += 1
        with self.lock:
            self._ready(share)
            deleted_folders = []
            for row in sorted(self.rows(share), key=lambda r: (r['path'].count('/'), r['kind'] != 'folder')):
                path = row['path']
                if path == share.path or path in inventory:
                    continue
                if any(path.startswith(folder + '/') for folder in deleted_folders):
                    continue
                # Recheck absence under the lock: a concurrent cloud write may
                # have created it after the inventory was taken.
                try:
                    share.physical(path).stat()
                    continue
                except FileNotFoundError:
                    pass
                service.delete_node.__wrapped__(service, path)
                if row['kind'] == 'folder':
                    deleted_folders.append(path)
            self.purge(service, share)
            with self.connect() as conn:
                conn.execute('INSERT OR REPLACE INTO status VALUES (?,?,?,?,?)',
                             (share.path, time.time(), '', imported, pending))
        return {'imported': imported, 'pending': pending, 'entries': len(inventory)}

    def tick(self, service):
        for share in self.shares:
            try:
                self.scan(service, share)
            except Exception as exc:
                with self.connect() as conn:
                    conn.execute('INSERT OR REPLACE INTO status VALUES (?,?,?,?,?)',
                                 (share.path, time.time(), str(exc), 0, 0))
                import logging
                logging.getLogger(__name__).warning('Shared folder %s: %s', share.path, exc)

    def purge(self, service, share):
        cutoff = datetime.now(timezone.utc) - timedelta(days=share.retention_days)
        for row in self.rows(share, deleted=True):
            if datetime.fromisoformat(row['deleted_at']) > cutoff:
                continue
            with self.connect() as state:
                state.execute('INSERT OR IGNORE INTO expired VALUES (?,?,?)',
                              (row['kind'], row['id'], row['deleted_at']))
                if row['kind'] == 'file':
                    with self.registry._connect() as conn:
                        keys = [r[0] for r in conn.execute('SELECT storage_key FROM cloud_file_versions WHERE file_id=?',
                                                         (row['id'],))]
                        state.executemany('INSERT OR IGNORE INTO garbage VALUES (?)', [(key,) for key in keys if key])
                    state.commit()  # Persist GC intent before dropping references.
                    with self.registry._connect() as conn:
                        conn.execute('DELETE FROM cloud_file_versions WHERE file_id=?', (row['id'],))
                        conn.execute("UPDATE cloud_files SET storage_key='',current_version_id='' WHERE id=? AND deleted_at=?",
                                     (row['id'], row['deleted_at']))
        with self.connect() as conn:
            keys = [r[0] for r in conn.execute('SELECT storage_key FROM garbage LIMIT 100')]
        for key in keys:
            with self.registry._connect() as conn:
                used = conn.execute('SELECT 1 FROM cloud_file_versions WHERE storage_key=? UNION ALL '
                                    'SELECT 1 FROM cloud_files WHERE storage_key=? LIMIT 1', (key, key)).fetchone()
            if not used:
                service.storage.delete(key)
            with self.connect() as conn:
                conn.execute('DELETE FROM garbage WHERE storage_key=?', (key,))
        with self.connect() as conn:
            expired = conn.execute("SELECT id FROM operations WHERE mount=? AND status IN ('done','staged') AND expires<?",
                                   (share.path, time.time())).fetchall()
        for row in expired:
            stage = self._private(share, row['id'])
            # Validate every descendant before recursive deletion; never follow links.
            for parent, dirs, files in os.walk(stage):
                if any(_is_link(Path(parent) / name) for name in dirs + files):
                    raise RuntimeError('Trash contains a redirected entry')
            shutil.rmtree(stage)
            with self.connect() as conn:
                conn.execute('DELETE FROM operations WHERE id=?', (row['id'],))
