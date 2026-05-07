from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from rag_catalog.core.db_contract import ensure_schema_version

from .models import CloudDriveFile, CloudDriveFolder, CloudDriveJob, CloudDriveStats

CLOUD_DRIVE_SCHEMA_VERSION = 2


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class CloudDriveRegistryDB:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _prepare_connection(self, conn: sqlite3.Connection) -> None:
        last_error: Optional[Exception] = None
        for _ in range(3):
            try:
                conn.execute('PRAGMA busy_timeout=30000;')
                conn.execute('PRAGMA journal_mode=WAL;')
                conn.execute('PRAGMA synchronous=NORMAL;')
                return
            except sqlite3.OperationalError as exc:
                last_error = exc
                time.sleep(0.25)
        if last_error is not None:
            raise last_error

    def _init_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                self._prepare_connection(conn)
                current_version = self._read_schema_version(conn)
                conn.executescript(
                    '''
                    CREATE TABLE IF NOT EXISTS cloud_folders (
                        id TEXT PRIMARY KEY,
                        parent_id TEXT,
                        name TEXT NOT NULL,
                        path TEXT NOT NULL UNIQUE,
                        depth INTEGER NOT NULL DEFAULT 0,
                        source_path TEXT NOT NULL DEFAULT '',
                        is_root INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY(parent_id) REFERENCES cloud_folders(id)
                    );

                    CREATE TABLE IF NOT EXISTS cloud_files (
                        id TEXT PRIMARY KEY,
                        folder_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        path TEXT NOT NULL UNIQUE,
                        storage_key TEXT NOT NULL,
                        mime_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                        size_bytes INTEGER NOT NULL DEFAULT 0,
                        checksum TEXT NOT NULL DEFAULT '',
                        source_path TEXT NOT NULL DEFAULT '',
                        current_version_id TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        deleted_at TEXT NOT NULL DEFAULT '',
                        FOREIGN KEY(folder_id) REFERENCES cloud_folders(id)
                    );

                    CREATE TABLE IF NOT EXISTS cloud_file_versions (
                        id TEXT PRIMARY KEY,
                        file_id TEXT NOT NULL,
                        storage_key TEXT NOT NULL,
                        checksum TEXT NOT NULL DEFAULT '',
                        size_bytes INTEGER NOT NULL DEFAULT 0,
                        source_path TEXT NOT NULL DEFAULT '',
                        created_by TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        FOREIGN KEY(file_id) REFERENCES cloud_files(id)
                    );

                    CREATE TABLE IF NOT EXISTS cloud_permissions (
                        id TEXT PRIMARY KEY,
                        subject_type TEXT NOT NULL,
                        subject_id TEXT NOT NULL,
                        resource_type TEXT NOT NULL,
                        resource_id TEXT NOT NULL,
                        access_level TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS cloud_jobs (
                        id TEXT PRIMARY KEY,
                        job_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        file_id TEXT NOT NULL DEFAULT '',
                        version_id TEXT NOT NULL DEFAULT '',
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        attempts INTEGER NOT NULL DEFAULT 0,
                        last_error TEXT NOT NULL DEFAULT '',
                        started_at TEXT NOT NULL DEFAULT '',
                        finished_at TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    '''
                )
                self._apply_migrations(conn, current_version=current_version)
                ensure_schema_version(
                    conn,
                    db_kind='cloud_drive',
                    db_path=self.db_path,
                    expected_version=CLOUD_DRIVE_SCHEMA_VERSION,
                    code_root=Path(__file__).resolve().parents[3],
                )
                conn.executescript(
                    '''
                    CREATE INDEX IF NOT EXISTS idx_cloud_folders_parent ON cloud_folders(parent_id, name);
                    CREATE INDEX IF NOT EXISTS idx_cloud_files_folder ON cloud_files(folder_id, name);
                    CREATE INDEX IF NOT EXISTS idx_cloud_files_storage_key ON cloud_files(storage_key);
                    CREATE INDEX IF NOT EXISTS idx_cloud_versions_file ON cloud_file_versions(file_id, created_at);
                    CREATE INDEX IF NOT EXISTS idx_cloud_jobs_status ON cloud_jobs(status, job_type, created_at);
                    '''
                )

    def _read_schema_version(self, conn: sqlite3.Connection) -> int:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                db_kind TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                code_root TEXT NOT NULL DEFAULT ''
            )
            """
        )
        row = conn.execute(
            "SELECT schema_version FROM schema_meta WHERE db_kind='cloud_drive'",
        ).fetchone()
        return int(row["schema_version"]) if row is not None else 0

    def _has_column(self, conn: sqlite3.Connection, table: str, column: str) -> bool:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(str(row["name"]) == str(column) for row in rows)

    def _apply_migrations(self, conn: sqlite3.Connection, *, current_version: int) -> None:
        if current_version <= 1 and not self._has_column(conn, "cloud_jobs", "started_at"):
            conn.execute("ALTER TABLE cloud_jobs ADD COLUMN started_at TEXT NOT NULL DEFAULT ''")
        if current_version <= 1 and not self._has_column(conn, "cloud_jobs", "finished_at"):
            conn.execute("ALTER TABLE cloud_jobs ADD COLUMN finished_at TEXT NOT NULL DEFAULT ''")

    def ensure_root_folder(self, *, root_name: str, source_path: str = '') -> CloudDriveFolder:
        clean_name = str(root_name or '').strip() or 'root'
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute('SELECT * FROM cloud_folders WHERE is_root=1 LIMIT 1').fetchone()
                if row is not None:
                    return self._folder_from_row(row)
                folder_id = str(uuid.uuid4())
                conn.execute(
                    '''
                    INSERT INTO cloud_folders (id, parent_id, name, path, depth, source_path, is_root, created_at, updated_at)
                    VALUES (?, NULL, ?, '', 0, ?, 1, ?, ?)
                    ''',
                    (folder_id, clean_name, source_path, now, now),
                )
                row = conn.execute('SELECT * FROM cloud_folders WHERE id=?', (folder_id,)).fetchone()
                assert row is not None
                return self._folder_from_row(row)

    def get_root_folder(self) -> Optional[CloudDriveFolder]:
        with self._connect() as conn:
            row = conn.execute('SELECT * FROM cloud_folders WHERE is_root=1 LIMIT 1').fetchone()
            return self._folder_from_row(row) if row else None

    def create_folder(self, *, parent_path: str = '', name: str) -> CloudDriveFolder:
        clean_name = str(name or '').strip().strip('/\\')
        if not clean_name:
            raise RuntimeError('Не задано имя каталога.')
        if '/' in clean_name or '\\' in clean_name:
            raise RuntimeError('Имя каталога не должно содержать разделители пути.')
        clean_parent = self._normalize_path(parent_path)
        parent = self.get_root_folder() if not clean_parent else self.get_folder_by_path(clean_parent)
        if parent is None:
            raise RuntimeError(f'Родительский каталог не найден: {clean_parent or "/"}')
        folder_path = self._normalize_path(f'{clean_parent}/{clean_name}' if clean_parent else clean_name)
        if self.get_folder_by_path(folder_path) is not None:
            raise RuntimeError(f'Каталог уже существует: {folder_path}')
        if self.get_file_by_path(folder_path) is not None:
            raise RuntimeError(f'Файл с таким именем уже существует: {folder_path}')
        source_path = str(Path(parent.source_path) / clean_name) if parent.source_path else ''
        return self.upsert_folder(
            path=folder_path,
            name=clean_name,
            parent_id=parent.id,
            depth=int(parent.depth) + 1,
            source_path=source_path,
            is_root=False,
        )

    def upsert_folder(self, *, path: str, name: str, parent_id: Optional[str], depth: int, source_path: str = '', is_root: bool = False) -> CloudDriveFolder:
        clean_path = self._normalize_path(path)
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute('SELECT id FROM cloud_folders WHERE path=?', (clean_path,)).fetchone()
                folder_id = str(row['id']) if row else str(uuid.uuid4())
                conn.execute(
                    '''
                    INSERT INTO cloud_folders (id, parent_id, name, path, depth, source_path, is_root, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(path) DO UPDATE SET
                        parent_id=excluded.parent_id,
                        name=excluded.name,
                        depth=excluded.depth,
                        source_path=excluded.source_path,
                        is_root=excluded.is_root,
                        updated_at=excluded.updated_at
                    ''',
                    (folder_id, parent_id, name, clean_path, int(depth), source_path, 1 if is_root else 0, now, now),
                )
                saved = conn.execute('SELECT * FROM cloud_folders WHERE path=?', (clean_path,)).fetchone()
                assert saved is not None
                return self._folder_from_row(saved)

    def get_folder_by_path(self, path: str) -> Optional[CloudDriveFolder]:
        with self._connect() as conn:
            row = conn.execute('SELECT * FROM cloud_folders WHERE path=?', (self._normalize_path(path),)).fetchone()
            return self._folder_from_row(row) if row else None

    def list_child_folders(self, parent_id: Optional[str]) -> List[CloudDriveFolder]:
        with self._connect() as conn:
            if parent_id is None:
                rows = conn.execute('SELECT * FROM cloud_folders WHERE parent_id IS NULL ORDER BY name').fetchall()
            else:
                rows = conn.execute('SELECT * FROM cloud_folders WHERE parent_id=? ORDER BY name', (parent_id,)).fetchall()
            return [self._folder_from_row(row) for row in rows]

    def upsert_file(self, *, folder_id: str, path: str, name: str, storage_key: str, mime_type: str, size_bytes: int, checksum: str = '', source_path: str = '') -> CloudDriveFile:
        clean_path = self._normalize_path(path)
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute('SELECT id, current_version_id FROM cloud_files WHERE path=?', (clean_path,)).fetchone()
                file_id = str(row['id']) if row else str(uuid.uuid4())
                version_id = str(uuid.uuid4())
                conn.execute(
                    '''
                    INSERT INTO cloud_file_versions (id, file_id, storage_key, checksum, size_bytes, source_path, created_by, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, '', ?)
                    ''',
                    (version_id, file_id, storage_key, checksum, int(size_bytes), source_path, now),
                )
                conn.execute(
                    '''
                    INSERT INTO cloud_files (id, folder_id, name, path, storage_key, mime_type, size_bytes, checksum, source_path, current_version_id, created_at, updated_at, deleted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '')
                    ON CONFLICT(path) DO UPDATE SET
                        folder_id=excluded.folder_id,
                        name=excluded.name,
                        storage_key=excluded.storage_key,
                        mime_type=excluded.mime_type,
                        size_bytes=excluded.size_bytes,
                        checksum=excluded.checksum,
                        source_path=excluded.source_path,
                        current_version_id=excluded.current_version_id,
                        updated_at=excluded.updated_at,
                        deleted_at=''
                    ''',
                    (file_id, folder_id, name, clean_path, storage_key, mime_type, int(size_bytes), checksum, source_path, version_id, now, now),
                )
                saved = conn.execute('SELECT * FROM cloud_files WHERE path=?', (clean_path,)).fetchone()
                assert saved is not None
                return self._file_from_row(saved)

    def get_file_by_path(self, path: str) -> Optional[CloudDriveFile]:
        with self._connect() as conn:
            row = conn.execute('SELECT * FROM cloud_files WHERE path=?', (self._normalize_path(path),)).fetchone()
            return self._file_from_row(row) if row else None

    def get_file_by_id(self, file_id: str) -> Optional[CloudDriveFile]:
        with self._connect() as conn:
            row = conn.execute('SELECT * FROM cloud_files WHERE id=?', (str(file_id),)).fetchone()
            return self._file_from_row(row) if row else None

    def get_node_by_path(self, path: str) -> CloudDriveFolder | CloudDriveFile | None:
        clean_path = self._normalize_path(path)
        folder = self.get_folder_by_path(clean_path)
        if folder is not None:
            return folder
        return self.get_file_by_path(clean_path)

    def list_files_in_folder(self, folder_id: str) -> List[CloudDriveFile]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM cloud_files WHERE folder_id=? AND deleted_at='' ORDER BY name",
                (folder_id,),
            ).fetchall()
            return [self._file_from_row(row) for row in rows]

    def list_file_versions(self, *, path: str) -> List[Dict[str, Any]]:
        file_row = self.get_file_by_path(path)
        if file_row is None:
            raise RuntimeError(f'Файл не найден: {path}')
        with self._connect() as conn:
            rows = conn.execute(
                '''
                SELECT id, file_id, storage_key, checksum, size_bytes, source_path, created_by, created_at
                FROM cloud_file_versions
                WHERE file_id=?
                ORDER BY created_at DESC
                ''',
                (file_row.id,),
            ).fetchall()
        return [
            {
                'id': str(row['id']),
                'file_id': str(row['file_id']),
                'storage_key': str(row['storage_key'] or ''),
                'checksum': str(row['checksum'] or ''),
                'size_bytes': int(row['size_bytes'] or 0),
                'source_path': str(row['source_path'] or ''),
                'created_by': str(row['created_by'] or ''),
                'created_at': str(row['created_at'] or ''),
                'is_current': str(row['id']) == str(file_row.current_version_id),
            }
            for row in rows
        ]

    def list_files_under_path(self, path: str) -> List[CloudDriveFile]:
        clean_path = self._normalize_path(path)
        if not clean_path:
            raise RuntimeError('Для корневого каталога используйте list_files_in_folder/root traversal.')
        like_value = f"{clean_path}/%"
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM cloud_files
                WHERE deleted_at='' AND (path=? OR path LIKE ?)
                ORDER BY path
                """,
                (clean_path, like_value),
            ).fetchall()
            return [self._file_from_row(row) for row in rows]

    def rename_move_file(self, *, source_path: str, dest_parent_path: str = '', new_name: str = '') -> CloudDriveFile:
        clean_source = self._normalize_path(source_path)
        file_row = self.get_file_by_path(clean_source)
        if file_row is None:
            raise RuntimeError(f'Файл не найден: {clean_source}')
        parent = self.get_root_folder() if not self._normalize_path(dest_parent_path) else self.get_folder_by_path(dest_parent_path)
        if parent is None:
            raise RuntimeError(f'Родительский каталог не найден: {dest_parent_path or "/"}')
        target_name = str(new_name or file_row.name).strip().strip('/\\')
        if not target_name:
            raise RuntimeError('Не задано новое имя файла.')
        if '/' in target_name or '\\' in target_name:
            raise RuntimeError('Имя файла не должно содержать разделители пути.')
        clean_parent = self._normalize_path(dest_parent_path)
        target_path = self._normalize_path(f'{clean_parent}/{target_name}' if clean_parent else target_name)
        if target_path != clean_source:
            if self.get_folder_by_path(target_path) is not None:
                raise RuntimeError(f'Каталог с таким именем уже существует: {target_path}')
            existing_file = self.get_file_by_path(target_path)
            if existing_file is not None and existing_file.id != file_row.id:
                raise RuntimeError(f'Файл с таким именем уже существует: {target_path}')
        now = _utc_now()
        source_path_value = str(Path(parent.source_path) / target_name) if parent.source_path else file_row.source_path
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    '''
                    UPDATE cloud_files
                    SET folder_id=?, name=?, path=?, storage_key=?, source_path=?, updated_at=?
                    WHERE id=?
                    ''',
                    (parent.id, target_name, target_path, target_path, source_path_value, now, file_row.id),
                )
                conn.execute(
                    '''
                    UPDATE cloud_file_versions
                    SET storage_key=?, source_path=?
                    WHERE file_id=?
                    ''',
                    (target_path, source_path_value, file_row.id),
                )
                saved = conn.execute('SELECT * FROM cloud_files WHERE id=?', (file_row.id,)).fetchone()
        assert saved is not None
        return self._file_from_row(saved)

    def rename_move_folder(self, *, source_path: str, dest_parent_path: str = '', new_name: str = '') -> CloudDriveFolder:
        clean_source = self._normalize_path(source_path)
        folder = self.get_folder_by_path(clean_source)
        if folder is None:
            raise RuntimeError(f'Каталог не найден: {clean_source}')
        if folder.is_root:
            raise RuntimeError('Корневой каталог нельзя перемещать или переименовывать.')
        clean_parent = self._normalize_path(dest_parent_path)
        parent = self.get_root_folder() if not clean_parent else self.get_folder_by_path(clean_parent)
        if parent is None:
            raise RuntimeError(f'Родительский каталог не найден: {dest_parent_path or "/"}')
        target_name = str(new_name or folder.name).strip().strip('/\\')
        if not target_name:
            raise RuntimeError('Не задано новое имя каталога.')
        if '/' in target_name or '\\' in target_name:
            raise RuntimeError('Имя каталога не должно содержать разделители пути.')
        target_path = self._normalize_path(f'{clean_parent}/{target_name}' if clean_parent else target_name)
        if clean_parent == clean_source or clean_parent.startswith(f'{clean_source}/'):
            raise RuntimeError('Нельзя переместить каталог внутрь самого себя.')
        if target_path != clean_source:
            if self.get_folder_by_path(target_path) is not None:
                raise RuntimeError(f'Каталог уже существует: {target_path}')
            if self.get_file_by_path(target_path) is not None:
                raise RuntimeError(f'Файл с таким именем уже существует: {target_path}')
        now = _utc_now()
        old_prefix = clean_source
        new_prefix = target_path
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    '''
                    UPDATE cloud_folders
                    SET parent_id=?, name=?, path=?, depth=?, source_path=?, updated_at=?
                    WHERE id=?
                    ''',
                    (
                        parent.id,
                        target_name,
                        new_prefix,
                        int(parent.depth) + 1,
                        str(Path(parent.source_path) / target_name) if parent.source_path else folder.source_path,
                        now,
                        folder.id,
                    ),
                )
                folder_rows = conn.execute(
                    "SELECT * FROM cloud_folders WHERE path LIKE ? ORDER BY depth ASC",
                    (f"{old_prefix}/%",),
                ).fetchall()
                for row in folder_rows:
                    row_path = str(row['path'])
                    suffix = row_path[len(old_prefix):].lstrip('/')
                    next_path = self._normalize_path(f'{new_prefix}/{suffix}' if suffix else new_prefix)
                    next_depth = len([part for part in next_path.split('/') if part])
                    parent_path = next_path.rsplit('/', 1)[0] if '/' in next_path else ''
                    parent_row = conn.execute(
                        "SELECT id, source_path FROM cloud_folders WHERE path=?",
                        (parent_path,),
                    ).fetchone()
                    next_source = str(Path(str(parent_row['source_path'] or '')) / str(row['name'])) if parent_row and str(parent_row['source_path'] or '') else str(row['source_path'] or '')
                    conn.execute(
                        '''
                        UPDATE cloud_folders
                        SET parent_id=?, path=?, depth=?, source_path=?, updated_at=?
                        WHERE id=?
                        ''',
                        (
                            str(parent_row['id']) if parent_row else None,
                            next_path,
                            next_depth,
                            next_source,
                            now,
                            str(row['id']),
                        ),
                    )
                file_rows = conn.execute(
                    "SELECT * FROM cloud_files WHERE deleted_at='' AND path LIKE ?",
                    (f"{old_prefix}/%",),
                ).fetchall()
                for row in file_rows:
                    row_path = str(row['path'])
                    suffix = row_path[len(old_prefix):].lstrip('/')
                    next_path = self._normalize_path(f'{new_prefix}/{suffix}' if suffix else new_prefix)
                    parent_path = next_path.rsplit('/', 1)[0] if '/' in next_path else ''
                    parent_row = conn.execute(
                        "SELECT id, source_path FROM cloud_folders WHERE path=?",
                        (parent_path,),
                    ).fetchone()
                    filename = str(row['name'])
                    next_source = str(Path(str(parent_row['source_path'] or '')) / filename) if parent_row and str(parent_row['source_path'] or '') else str(row['source_path'] or '')
                    conn.execute(
                        '''
                        UPDATE cloud_files
                        SET folder_id=?, path=?, storage_key=?, source_path=?, updated_at=?
                        WHERE id=?
                        ''',
                        (
                            str(parent_row['id']) if parent_row else row['folder_id'],
                            next_path,
                            next_path,
                            next_source,
                            now,
                            str(row['id']),
                        ),
                    )
                    conn.execute(
                        '''
                        UPDATE cloud_file_versions
                        SET storage_key=?, source_path=?
                        WHERE file_id=?
                        ''',
                        (next_path, next_source, str(row['id'])),
                    )
                saved = conn.execute('SELECT * FROM cloud_folders WHERE id=?', (folder.id,)).fetchone()
        assert saved is not None
        return self._folder_from_row(saved)

    def delete_file(self, path: str) -> CloudDriveFile:
        clean_path = self._normalize_path(path)
        file_row = self.get_file_by_path(clean_path)
        if file_row is None:
            raise RuntimeError(f'Файл не найден: {clean_path}')
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE cloud_files SET deleted_at=?, updated_at=? WHERE id=?",
                    (now, now, file_row.id),
                )
                saved = conn.execute('SELECT * FROM cloud_files WHERE id=?', (file_row.id,)).fetchone()
        assert saved is not None
        return self._file_from_row(saved)

    def delete_folder(self, path: str) -> CloudDriveFolder:
        clean_path = self._normalize_path(path)
        folder = self.get_folder_by_path(clean_path)
        if folder is None:
            raise RuntimeError(f'Каталог не найден: {clean_path}')
        if folder.is_root:
            raise RuntimeError('Корневой каталог нельзя удалить.')
        with self._lock:
            with self._connect() as conn:
                file_ids = [
                    str(row['id'])
                    for row in conn.execute(
                        "SELECT id FROM cloud_files WHERE path LIKE ? OR path=?",
                        (f"{clean_path}/%", clean_path),
                    ).fetchall()
                ]
                if file_ids:
                    placeholders = ",".join("?" for _ in file_ids)
                    conn.execute(
                        f"DELETE FROM cloud_file_versions WHERE file_id IN ({placeholders})",
                        file_ids,
                    )
                conn.execute("DELETE FROM cloud_files WHERE path LIKE ? OR path=?", (f"{clean_path}/%", clean_path))
                conn.execute("DELETE FROM cloud_folders WHERE path LIKE ? OR path=?", (f"{clean_path}/%", clean_path))
        return folder

    def queue_job(self, *, job_type: str, status: str = 'pending', file_id: str = '', version_id: str = '', payload: Optional[Dict[str, Any]] = None) -> CloudDriveJob:
        now = _utc_now()
        job_id = str(uuid.uuid4())
        payload_json = json.dumps(payload or {}, ensure_ascii=False)
        started_at = now if str(status) == 'running' else ''
        finished_at = now if str(status) in {'completed', 'failed', 'cancelled'} else ''
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    '''
                    INSERT INTO cloud_jobs (id, job_type, status, file_id, version_id, payload_json, attempts, last_error, started_at, finished_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 0, '', ?, ?, ?, ?)
                    ''',
                    (job_id, job_type, status, file_id, version_id, payload_json, started_at, finished_at, now, now),
                )
        return CloudDriveJob(
            id=job_id,
            job_type=job_type,
            status=status,
            file_id=file_id,
            version_id=version_id,
            payload=payload or {},
            progress=dict((payload or {}).get('progress') or {}),
            started_at=started_at,
            finished_at=finished_at,
            created_at=now,
            updated_at=now,
        )

    def get_job(self, job_id: str) -> Optional[CloudDriveJob]:
        with self._connect() as conn:
            row = conn.execute('SELECT * FROM cloud_jobs WHERE id=?', (str(job_id),)).fetchone()
            return self._job_from_row(row) if row else None

    def get_latest_job(self, *, job_type: str) -> Optional[CloudDriveJob]:
        with self._connect() as conn:
            row = conn.execute(
                'SELECT * FROM cloud_jobs WHERE job_type=? ORDER BY created_at DESC LIMIT 1',
                (str(job_type),),
            ).fetchone()
            return self._job_from_row(row) if row else None

    def list_jobs(self, *, job_type: Optional[str] = None, limit: int = 20) -> List[CloudDriveJob]:
        clean_limit = max(1, int(limit))
        with self._connect() as conn:
            if job_type:
                rows = conn.execute(
                    'SELECT * FROM cloud_jobs WHERE job_type=? ORDER BY created_at DESC LIMIT ?',
                    (str(job_type), clean_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    'SELECT * FROM cloud_jobs ORDER BY created_at DESC LIMIT ?',
                    (clean_limit,),
                ).fetchall()
            return [self._job_from_row(row) for row in rows]

    def update_job(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        last_error: Optional[str] = None,
        attempts: Optional[int] = None,
    ) -> CloudDriveJob:
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute('SELECT * FROM cloud_jobs WHERE id=?', (str(job_id),)).fetchone()
                if row is None:
                    raise RuntimeError(f'Job не найден: {job_id}')
                next_status = str(status or row['status'])
                current_payload = json.loads(str(row['payload_json'] or '{}'))
                if payload:
                    current_payload.update(payload)
                started_at = str(row['started_at'] or '')
                finished_at = str(row['finished_at'] or '')
                if next_status == 'running' and not started_at:
                    started_at = now
                if next_status in {'completed', 'failed', 'cancelled'}:
                    finished_at = now
                conn.execute(
                    '''
                    UPDATE cloud_jobs
                    SET status=?,
                        payload_json=?,
                        last_error=?,
                        attempts=?,
                        started_at=?,
                        finished_at=?,
                        updated_at=?
                    WHERE id=?
                    ''',
                    (
                        next_status,
                        json.dumps(current_payload, ensure_ascii=False),
                        str(last_error if last_error is not None else row['last_error'] or ''),
                        int(attempts if attempts is not None else row['attempts'] or 0),
                        started_at,
                        finished_at,
                        now,
                        str(job_id),
                    ),
                )
                saved = conn.execute('SELECT * FROM cloud_jobs WHERE id=?', (str(job_id),)).fetchone()
                assert saved is not None
                return self._job_from_row(saved)

    def stats(self) -> CloudDriveStats:
        with self._connect() as conn:
            folders = int(conn.execute('SELECT COUNT(*) FROM cloud_folders').fetchone()[0])
            files = int(conn.execute("SELECT COUNT(*) FROM cloud_files WHERE deleted_at=''").fetchone()[0])
            versions = int(conn.execute('SELECT COUNT(*) FROM cloud_file_versions').fetchone()[0])
            pending_jobs = int(conn.execute("SELECT COUNT(*) FROM cloud_jobs WHERE status IN ('pending','running')").fetchone()[0])
            root_row = conn.execute('SELECT path FROM cloud_folders WHERE is_root=1 LIMIT 1').fetchone()
            root_path = str(root_row['path']) if root_row else ''
        return CloudDriveStats(folders=folders, files=files, versions=versions, pending_jobs=pending_jobs, root_path=root_path)

    def _normalize_path(self, path: str) -> str:
        value = str(path or '').strip().replace('\\', '/')
        value = '/'.join(part for part in value.split('/') if part not in {'', '.'})
        return value

    def _folder_from_row(self, row: sqlite3.Row) -> CloudDriveFolder:
        return CloudDriveFolder(
            id=str(row['id']),
            parent_id=str(row['parent_id']) if row['parent_id'] is not None else None,
            name=str(row['name']),
            path=str(row['path']),
            depth=int(row['depth'] or 0),
            source_path=str(row['source_path'] or ''),
            is_root=bool(int(row['is_root'] or 0)),
            created_at=str(row['created_at'] or ''),
            updated_at=str(row['updated_at'] or ''),
        )

    def _file_from_row(self, row: sqlite3.Row) -> CloudDriveFile:
        return CloudDriveFile(
            id=str(row['id']),
            folder_id=str(row['folder_id']),
            name=str(row['name']),
            path=str(row['path']),
            storage_key=str(row['storage_key']),
            mime_type=str(row['mime_type']),
            size_bytes=int(row['size_bytes'] or 0),
            checksum=str(row['checksum'] or ''),
            source_path=str(row['source_path'] or ''),
            current_version_id=str(row['current_version_id'] or ''),
            created_at=str(row['created_at'] or ''),
            updated_at=str(row['updated_at'] or ''),
            deleted_at=str(row['deleted_at'] or ''),
        )

    def _job_from_row(self, row: sqlite3.Row) -> CloudDriveJob:
        payload = json.loads(str(row['payload_json'] or '{}'))
        return CloudDriveJob(
            id=str(row['id']),
            job_type=str(row['job_type']),
            status=str(row['status']),
            file_id=str(row['file_id'] or ''),
            version_id=str(row['version_id'] or ''),
            payload=payload,
            attempts=int(row['attempts'] or 0),
            last_error=str(row['last_error'] or ''),
            created_at=str(row['created_at'] or ''),
            updated_at=str(row['updated_at'] or ''),
            started_at=str(row['started_at'] or ''),
            finished_at=str(row['finished_at'] or ''),
            progress=dict(payload.get('progress') or {}),
        )
