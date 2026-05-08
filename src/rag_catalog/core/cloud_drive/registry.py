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

from .models import (
    CloudDriveFile,
    CloudDriveFolder,
    CloudDriveJob,
    CloudDriveStats,
    CloudDriveSyncClient,
    CloudDriveSyncConflict,
    CloudDriveSyncPair,
)

CLOUD_DRIVE_SCHEMA_VERSION = 4


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
                        source_mtime REAL NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        deleted_at TEXT NOT NULL DEFAULT '',
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
                        source_mtime REAL NOT NULL DEFAULT 0,
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

                    CREATE TABLE IF NOT EXISTS cloud_sync_clients (
                        id TEXT PRIMARY KEY,
                        username TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        display_name TEXT NOT NULL DEFAULT '',
                        platform TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL DEFAULT 'offline',
                        last_seen_at TEXT NOT NULL DEFAULT '',
                        metadata_json TEXT NOT NULL DEFAULT '{}',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(username, device_id)
                    );

                    CREATE TABLE IF NOT EXISTS cloud_sync_pairs (
                        id TEXT PRIMARY KEY,
                        client_id TEXT NOT NULL,
                        username TEXT NOT NULL,
                        local_path TEXT NOT NULL,
                        cloud_path TEXT NOT NULL DEFAULT '',
                        conflict_policy TEXT NOT NULL DEFAULT 'ask',
                        enabled INTEGER NOT NULL DEFAULT 1,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY(client_id) REFERENCES cloud_sync_clients(id),
                        UNIQUE(client_id, local_path, cloud_path)
                    );

                    CREATE TABLE IF NOT EXISTS cloud_sync_selective_paths (
                        id TEXT PRIMARY KEY,
                        client_id TEXT NOT NULL,
                        username TEXT NOT NULL,
                        cloud_path TEXT NOT NULL,
                        mode TEXT NOT NULL DEFAULT 'exclude',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY(client_id) REFERENCES cloud_sync_clients(id),
                        UNIQUE(client_id, cloud_path)
                    );

                    CREATE TABLE IF NOT EXISTS cloud_sync_conflicts (
                        id TEXT PRIMARY KEY,
                        client_id TEXT NOT NULL,
                        pair_id TEXT NOT NULL DEFAULT '',
                        username TEXT NOT NULL,
                        path TEXT NOT NULL,
                        local_path TEXT NOT NULL DEFAULT '',
                        cloud_path TEXT NOT NULL DEFAULT '',
                        conflict_type TEXT NOT NULL,
                        local_version TEXT NOT NULL DEFAULT '',
                        cloud_version TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL DEFAULT 'open',
                        resolution TEXT NOT NULL DEFAULT '',
                        details_json TEXT NOT NULL DEFAULT '{}',
                        resolved_by TEXT NOT NULL DEFAULT '',
                        resolved_at TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY(client_id) REFERENCES cloud_sync_clients(id)
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
                    CREATE INDEX IF NOT EXISTS idx_cloud_sync_clients_username ON cloud_sync_clients(username, status, updated_at);
                    CREATE INDEX IF NOT EXISTS idx_cloud_sync_pairs_client ON cloud_sync_pairs(client_id, enabled, cloud_path);
                    CREATE INDEX IF NOT EXISTS idx_cloud_sync_selective_client ON cloud_sync_selective_paths(client_id, mode, cloud_path);
                    CREATE INDEX IF NOT EXISTS idx_cloud_sync_conflicts_status ON cloud_sync_conflicts(status, username, updated_at);
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
        if current_version <= 2 and not self._has_column(conn, "cloud_folders", "deleted_at"):
            conn.execute("ALTER TABLE cloud_folders ADD COLUMN deleted_at TEXT NOT NULL DEFAULT ''")
        if current_version <= 3:
            if not self._has_column(conn, "cloud_folders", "source_mtime"):
                conn.execute("ALTER TABLE cloud_folders ADD COLUMN source_mtime REAL NOT NULL DEFAULT 0")
            if not self._has_column(conn, "cloud_files", "source_mtime"):
                conn.execute("ALTER TABLE cloud_files ADD COLUMN source_mtime REAL NOT NULL DEFAULT 0")

    def ensure_root_folder(self, *, root_name: str, source_path: str = '', source_mtime: float = 0.0) -> CloudDriveFolder:
        clean_name = str(root_name or '').strip() or 'root'
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute("SELECT * FROM cloud_folders WHERE is_root=1 AND deleted_at='' LIMIT 1").fetchone()
                if row is not None:
                    return self._folder_from_row(row)
                folder_id = str(uuid.uuid4())
                conn.execute(
                    '''
                    INSERT INTO cloud_folders (id, parent_id, name, path, depth, source_path, is_root, source_mtime, created_at, updated_at)
                    VALUES (?, NULL, ?, '', 0, ?, 1, ?, ?, ?)
                    ''',
                    (folder_id, clean_name, source_path, float(source_mtime), now, now),
                )
                row = conn.execute('SELECT * FROM cloud_folders WHERE id=?', (folder_id,)).fetchone()
                assert row is not None
                return self._folder_from_row(row)

    def get_root_folder(self) -> Optional[CloudDriveFolder]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM cloud_folders WHERE is_root=1 AND deleted_at='' LIMIT 1").fetchone()
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

    def upsert_folder(self, *, path: str, name: str, parent_id: Optional[str], depth: int, source_path: str = '', is_root: bool = False, source_mtime: float = 0.0) -> CloudDriveFolder:
        clean_path = self._normalize_path(path)
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute('SELECT id FROM cloud_folders WHERE path=?', (clean_path,)).fetchone()
                folder_id = str(row['id']) if row else str(uuid.uuid4())
                conn.execute(
                    '''
                    INSERT INTO cloud_folders (id, parent_id, name, path, depth, source_path, is_root, source_mtime, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(path) DO UPDATE SET
                        parent_id=excluded.parent_id,
                        name=excluded.name,
                        depth=excluded.depth,
                        source_path=excluded.source_path,
                        is_root=excluded.is_root,
                        source_mtime=excluded.source_mtime,
                        updated_at=excluded.updated_at,
                        deleted_at=''
                    ''',
                    (folder_id, parent_id, name, clean_path, int(depth), source_path, 1 if is_root else 0, float(source_mtime), now, now),
                )
                saved = conn.execute('SELECT * FROM cloud_folders WHERE path=?', (clean_path,)).fetchone()
                assert saved is not None
                return self._folder_from_row(saved)

    def get_folder_by_path(self, path: str, *, include_deleted: bool = False) -> Optional[CloudDriveFolder]:
        with self._connect() as conn:
            if include_deleted:
                row = conn.execute('SELECT * FROM cloud_folders WHERE path=?', (self._normalize_path(path),)).fetchone()
            else:
                row = conn.execute("SELECT * FROM cloud_folders WHERE path=? AND deleted_at=''", (self._normalize_path(path),)).fetchone()
            return self._folder_from_row(row) if row else None

    def list_child_folders(self, parent_id: Optional[str]) -> List[CloudDriveFolder]:
        with self._connect() as conn:
            if parent_id is None:
                rows = conn.execute("SELECT * FROM cloud_folders WHERE parent_id IS NULL AND deleted_at='' ORDER BY name").fetchall()
            else:
                rows = conn.execute("SELECT * FROM cloud_folders WHERE parent_id=? AND deleted_at='' ORDER BY name", (parent_id,)).fetchall()
            return [self._folder_from_row(row) for row in rows]

    def upsert_file(self, *, folder_id: str, path: str, name: str, storage_key: str, mime_type: str, size_bytes: int, checksum: str = '', source_path: str = '', source_mtime: float = 0.0) -> CloudDriveFile:
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
                    INSERT INTO cloud_files (id, folder_id, name, path, storage_key, mime_type, size_bytes, checksum, source_path, source_mtime, current_version_id, created_at, updated_at, deleted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '')
                    ON CONFLICT(path) DO UPDATE SET
                        folder_id=excluded.folder_id,
                        name=excluded.name,
                        storage_key=excluded.storage_key,
                        mime_type=excluded.mime_type,
                        size_bytes=excluded.size_bytes,
                        checksum=excluded.checksum,
                        source_path=excluded.source_path,
                        source_mtime=excluded.source_mtime,
                        current_version_id=excluded.current_version_id,
                        updated_at=excluded.updated_at,
                        deleted_at=''
                    ''',
                    (file_id, folder_id, name, clean_path, storage_key, mime_type, int(size_bytes), checksum, source_path, float(source_mtime), version_id, now, now),
                )
                saved = conn.execute('SELECT * FROM cloud_files WHERE path=?', (clean_path,)).fetchone()
                assert saved is not None
                return self._file_from_row(saved)

    def update_folder_mtime(self, folder_id: str, source_mtime: float) -> None:
        """Cheaply update only the source_mtime of a folder after a successful scan."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE cloud_folders SET source_mtime=? WHERE id=?",
                    (float(source_mtime), folder_id),
                )

    def get_folder_source_mtime(self, folder_id: str) -> float:
        """Return the stored source_mtime for a folder (0 if unknown)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT source_mtime FROM cloud_folders WHERE id=?", (folder_id,)
            ).fetchone()
            return float(row["source_mtime"] or 0) if row else 0.0

    def get_file_mtimes_in_folder(self, folder_id: str) -> Dict[str, tuple]:
        """Return {name: (source_mtime, size_bytes)} for all non-deleted files in a folder."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT name, source_mtime, size_bytes FROM cloud_files WHERE folder_id=? AND deleted_at=''",
                (folder_id,),
            ).fetchall()
        return {str(r["name"]): (float(r["source_mtime"] or 0), int(r["size_bytes"] or 0)) for r in rows}

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

    def list_changes(self, *, since: str = '', limit: int = 500) -> List[Dict[str, Any]]:
        """Return changed file/folder registry rows ordered by update time."""
        clean_since = str(since or '').strip()
        max_rows = max(1, min(int(limit or 500), 5000))
        where = "WHERE updated_at > ?" if clean_since else ""
        params: tuple[Any, ...] = (clean_since,) if clean_since else ()
        with self._connect() as conn:
            folder_rows = conn.execute(
                f"""
                SELECT 'folder' AS node_type, id, path, name, updated_at, deleted_at, '' AS current_version_id
                FROM cloud_folders
                {where}
                """,
                params,
            ).fetchall()
            file_rows = conn.execute(
                f"""
                SELECT 'file' AS node_type, id, path, name, updated_at, deleted_at, current_version_id
                FROM cloud_files
                {where}
                """,
                params,
            ).fetchall()
        rows = [*folder_rows, *file_rows]
        rows.sort(key=lambda row: (str(row['updated_at'] or ''), str(row['node_type']), str(row['path'] or '')))
        return [
            {
                'node_type': str(row['node_type']),
                'id': str(row['id']),
                'path': str(row['path'] or ''),
                'name': str(row['name'] or ''),
                'updated_at': str(row['updated_at'] or ''),
                'deleted_at': str(row['deleted_at'] or ''),
                'current_version_id': str(row['current_version_id'] or ''),
            }
            for row in rows[-max_rows:]
        ]

    def list_deleted_nodes(self, *, limit: int = 200) -> List[Dict[str, Any]]:
        """Return soft-deleted files and folders ordered by deletion time."""
        max_rows = max(1, min(int(limit or 200), 1000))
        with self._connect() as conn:
            folder_rows = conn.execute(
                """
                SELECT
                    'folder' AS node_type,
                    id,
                    path,
                    name,
                    source_path,
                    0 AS size_bytes,
                    updated_at,
                    deleted_at,
                    '' AS mime_type
                FROM cloud_folders
                WHERE deleted_at!='' AND is_root=0
                """
            ).fetchall()
            file_rows = conn.execute(
                """
                SELECT
                    'file' AS node_type,
                    id,
                    path,
                    name,
                    source_path,
                    size_bytes,
                    updated_at,
                    deleted_at,
                    mime_type
                FROM cloud_files
                WHERE deleted_at!=''
                """
            ).fetchall()
        rows = [*folder_rows, *file_rows]
        rows.sort(
            key=lambda row: (
                str(row['deleted_at'] or ''),
                str(row['node_type'] or ''),
                str(row['path'] or ''),
            ),
            reverse=True,
        )
        return [
            {
                'node_type': str(row['node_type']),
                'id': str(row['id']),
                'path': str(row['path'] or ''),
                'name': str(row['name'] or ''),
                'source_path': str(row['source_path'] or ''),
                'size_bytes': int(row['size_bytes'] or 0),
                'mime_type': str(row['mime_type'] or ''),
                'updated_at': str(row['updated_at'] or ''),
                'deleted_at': str(row['deleted_at'] or ''),
            }
            for row in rows[:max_rows]
        ]

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
                    SET folder_id=?, name=?, path=?, source_path=?, updated_at=?
                    WHERE id=?
                    ''',
                    (parent.id, target_name, target_path, source_path_value, now, file_row.id),
                )
                conn.execute(
                    '''
                    UPDATE cloud_file_versions
                    SET source_path=?
                    WHERE file_id=?
                    ''',
                    (source_path_value, file_row.id),
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
                        SET folder_id=?, path=?, source_path=?, updated_at=?
                        WHERE id=?
                        ''',
                        (
                            str(parent_row['id']) if parent_row else row['folder_id'],
                            next_path,
                            next_source,
                            now,
                            str(row['id']),
                        ),
                    )
                    conn.execute(
                        '''
                        UPDATE cloud_file_versions
                        SET source_path=?
                        WHERE file_id=?
                        ''',
                        (next_source, str(row['id'])),
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
                now = _utc_now()
                conn.execute(
                    "UPDATE cloud_files SET deleted_at=?, updated_at=? WHERE path LIKE ? OR path=?",
                    (now, now, f"{clean_path}/%", clean_path),
                )
                conn.execute(
                    "UPDATE cloud_folders SET deleted_at=?, updated_at=? WHERE path LIKE ? OR path=?",
                    (now, now, f"{clean_path}/%", clean_path),
                )
                saved = conn.execute('SELECT * FROM cloud_folders WHERE id=?', (folder.id,)).fetchone()
        assert saved is not None
        return self._folder_from_row(saved)

    def restore_file(self, path: str) -> CloudDriveFile:
        clean_path = self._normalize_path(path)
        file_row = self.get_file_by_path(clean_path)
        if file_row is None:
            raise RuntimeError(f'Файл не найден: {clean_path}')
        parent_path = clean_path.rsplit('/', 1)[0] if '/' in clean_path else ''
        parent = self.get_root_folder() if not parent_path else self.get_folder_by_path(parent_path)
        if parent is None:
            raise RuntimeError(f'Родительский каталог не найден или удалён: {parent_path or "/"}')
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE cloud_files SET deleted_at='', updated_at=? WHERE id=?",
                    (now, file_row.id),
                )
                saved = conn.execute('SELECT * FROM cloud_files WHERE id=?', (file_row.id,)).fetchone()
        assert saved is not None
        return self._file_from_row(saved)

    def restore_folder(self, path: str) -> CloudDriveFolder:
        clean_path = self._normalize_path(path)
        folder = self.get_folder_by_path(clean_path, include_deleted=True)
        if folder is None:
            raise RuntimeError(f'Каталог не найден: {clean_path}')
        if folder.is_root:
            raise RuntimeError('Корневой каталог нельзя восстанавливать.')
        parent_path = clean_path.rsplit('/', 1)[0] if '/' in clean_path else ''
        parent = self.get_root_folder() if not parent_path else self.get_folder_by_path(parent_path)
        if parent is None:
            raise RuntimeError(f'Родительский каталог не найден или удалён: {parent_path or "/"}')
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE cloud_folders SET deleted_at='', updated_at=? WHERE path LIKE ? OR path=?",
                    (now, f"{clean_path}/%", clean_path),
                )
                conn.execute(
                    "UPDATE cloud_files SET deleted_at='', updated_at=? WHERE path LIKE ? OR path=?",
                    (now, f"{clean_path}/%", clean_path),
                )
                saved = conn.execute('SELECT * FROM cloud_folders WHERE id=?', (folder.id,)).fetchone()
        assert saved is not None
        return self._folder_from_row(saved)

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

    def list_latest_jobs_for_files(
        self,
        file_ids: List[str],
        *,
        job_types: Optional[List[str]] = None,
    ) -> Dict[str, CloudDriveJob]:
        ids = [str(file_id or '').strip() for file_id in file_ids if str(file_id or '').strip()]
        if not ids:
            return {}
        types = [str(job_type or '').strip() for job_type in (job_types or ['reindex', 'cleanup', 'ocr', 'preview']) if str(job_type or '').strip()]
        id_placeholders = ','.join('?' for _ in ids)
        type_placeholders = ','.join('?' for _ in types)
        params = [*ids, *types]
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT j.*
                FROM cloud_jobs j
                INNER JOIN (
                    SELECT file_id, MAX(created_at) AS max_ts
                    FROM cloud_jobs
                    WHERE file_id IN ({id_placeholders})
                      AND job_type IN ({type_placeholders})
                    GROUP BY file_id
                ) latest ON j.file_id = latest.file_id AND j.created_at = latest.max_ts
                """,
                params,
            ).fetchall()
        return {str(row['file_id']): self._job_from_row(row) for row in rows}

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

    def register_sync_client(
        self,
        *,
        username: str,
        device_id: str,
        display_name: str = '',
        platform: str = '',
        status: str = 'online',
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CloudDriveSyncClient:
        clean_username = str(username or '').strip().lower()
        clean_device = str(device_id or '').strip()
        if not clean_username:
            raise RuntimeError('Не задан username sync-клиента.')
        if not clean_device:
            raise RuntimeError('Не задан device_id sync-клиента.')
        clean_status = str(status or 'online').strip().lower()
        if clean_status not in {'online', 'offline', 'paused', 'error'}:
            clean_status = 'online'
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    'SELECT id FROM cloud_sync_clients WHERE username=? AND device_id=?',
                    (clean_username, clean_device),
                ).fetchone()
                client_id = str(row['id']) if row else str(uuid.uuid4())
                conn.execute(
                    '''
                    INSERT INTO cloud_sync_clients (
                        id, username, device_id, display_name, platform, status,
                        last_seen_at, metadata_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(username, device_id) DO UPDATE SET
                        display_name=excluded.display_name,
                        platform=excluded.platform,
                        status=excluded.status,
                        last_seen_at=excluded.last_seen_at,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    ''',
                    (
                        client_id,
                        clean_username,
                        clean_device,
                        str(display_name or '').strip() or clean_device,
                        str(platform or '').strip(),
                        clean_status,
                        now,
                        json.dumps(metadata or {}, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                saved = conn.execute('SELECT * FROM cloud_sync_clients WHERE id=?', (client_id,)).fetchone()
        assert saved is not None
        return self._sync_client_from_row(saved)

    def get_sync_client(self, client_id: str) -> Optional[CloudDriveSyncClient]:
        with self._connect() as conn:
            row = conn.execute('SELECT * FROM cloud_sync_clients WHERE id=?', (str(client_id),)).fetchone()
            return self._sync_client_from_row(row) if row else None

    def list_sync_clients(self, *, username: str = '', include_offline: bool = True, limit: int = 100) -> List[CloudDriveSyncClient]:
        clean_username = str(username or '').strip().lower()
        max_rows = max(1, min(int(limit or 100), 1000))
        with self._connect() as conn:
            if clean_username and include_offline:
                rows = conn.execute(
                    'SELECT * FROM cloud_sync_clients WHERE username=? ORDER BY updated_at DESC LIMIT ?',
                    (clean_username, max_rows),
                ).fetchall()
            elif clean_username:
                rows = conn.execute(
                    "SELECT * FROM cloud_sync_clients WHERE username=? AND status!='offline' ORDER BY updated_at DESC LIMIT ?",
                    (clean_username, max_rows),
                ).fetchall()
            elif include_offline:
                rows = conn.execute(
                    'SELECT * FROM cloud_sync_clients ORDER BY updated_at DESC LIMIT ?',
                    (max_rows,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM cloud_sync_clients WHERE status!='offline' ORDER BY updated_at DESC LIMIT ?",
                    (max_rows,),
                ).fetchall()
        return [self._sync_client_from_row(row) for row in rows]

    def upsert_sync_pair(
        self,
        *,
        client_id: str,
        local_path: str,
        cloud_path: str = '',
        conflict_policy: str = 'ask',
        enabled: bool = True,
    ) -> CloudDriveSyncPair:
        client = self.get_sync_client(client_id)
        if client is None:
            raise RuntimeError(f'Sync-клиент не найден: {client_id}')
        clean_local = str(local_path or '').strip()
        if not clean_local:
            raise RuntimeError('Не задан локальный путь sync-пары.')
        clean_cloud = self._normalize_path(cloud_path)
        if clean_cloud and self.get_folder_by_path(clean_cloud) is None:
            raise RuntimeError(f'Cloud Drive каталог не найден: {clean_cloud}')
        policy = str(conflict_policy or 'ask').strip().lower()
        if policy not in {'ask', 'local_wins', 'cloud_wins', 'newest_wins'}:
            policy = 'ask'
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    'SELECT id FROM cloud_sync_pairs WHERE client_id=? AND local_path=? AND cloud_path=?',
                    (str(client_id), clean_local, clean_cloud),
                ).fetchone()
                pair_id = str(row['id']) if row else str(uuid.uuid4())
                conn.execute(
                    '''
                    INSERT INTO cloud_sync_pairs (
                        id, client_id, username, local_path, cloud_path, conflict_policy,
                        enabled, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(client_id, local_path, cloud_path) DO UPDATE SET
                        conflict_policy=excluded.conflict_policy,
                        enabled=excluded.enabled,
                        updated_at=excluded.updated_at
                    ''',
                    (
                        pair_id,
                        str(client_id),
                        client.username,
                        clean_local,
                        clean_cloud,
                        policy,
                        1 if enabled else 0,
                        now,
                        now,
                    ),
                )
                saved = conn.execute('SELECT * FROM cloud_sync_pairs WHERE id=?', (pair_id,)).fetchone()
        assert saved is not None
        return self._sync_pair_from_row(saved)

    def list_sync_pairs(self, *, username: str = '', client_id: str = '', enabled_only: bool = False) -> List[CloudDriveSyncPair]:
        clean_username = str(username or '').strip().lower()
        clean_client = str(client_id or '').strip()
        clauses: list[str] = []
        params: list[Any] = []
        if clean_username:
            clauses.append('username=?')
            params.append(clean_username)
        if clean_client:
            clauses.append('client_id=?')
            params.append(clean_client)
        if enabled_only:
            clauses.append('enabled=1')
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ''
        with self._connect() as conn:
            rows = conn.execute(
                f'SELECT * FROM cloud_sync_pairs {where} ORDER BY updated_at DESC',
                params,
            ).fetchall()
        return [self._sync_pair_from_row(row) for row in rows]

    def delete_sync_pair(self, pair_id: str, *, client_id: str = '') -> bool:
        clean_id = str(pair_id or '').strip()
        if not clean_id:
            return False
        clean_client = str(client_id or '').strip()
        with self._lock:
            with self._connect() as conn:
                if clean_client:
                    cur = conn.execute('DELETE FROM cloud_sync_pairs WHERE id=? AND client_id=?', (clean_id, clean_client))
                else:
                    cur = conn.execute('DELETE FROM cloud_sync_pairs WHERE id=?', (clean_id,))
                conn.execute('UPDATE cloud_sync_conflicts SET pair_id="" WHERE pair_id=?', (clean_id,))
                return int(cur.rowcount or 0) > 0

    def set_selective_sync_paths(
        self,
        *,
        client_id: str,
        paths: List[str],
        mode: str = 'exclude',
        replace: bool = True,
    ) -> List[Dict[str, str]]:
        client = self.get_sync_client(client_id)
        if client is None:
            raise RuntimeError(f'Sync-клиент не найден: {client_id}')
        clean_mode = str(mode or 'exclude').strip().lower()
        if clean_mode not in {'include', 'exclude'}:
            clean_mode = 'exclude'
        clean_paths = list(dict.fromkeys(self._normalize_path(path) for path in paths if self._normalize_path(path)))
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                if replace:
                    conn.execute('DELETE FROM cloud_sync_selective_paths WHERE client_id=?', (str(client_id),))
                for cloud_path in clean_paths:
                    row = conn.execute(
                        'SELECT id FROM cloud_sync_selective_paths WHERE client_id=? AND cloud_path=?',
                        (str(client_id), cloud_path),
                    ).fetchone()
                    path_id = str(row['id']) if row else str(uuid.uuid4())
                    conn.execute(
                        '''
                        INSERT INTO cloud_sync_selective_paths (
                            id, client_id, username, cloud_path, mode, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(client_id, cloud_path) DO UPDATE SET
                            mode=excluded.mode,
                            updated_at=excluded.updated_at
                        ''',
                        (path_id, str(client_id), client.username, cloud_path, clean_mode, now, now),
                    )
        return self.list_selective_sync_paths(client_id=client_id)

    def list_selective_sync_paths(self, *, username: str = '', client_id: str = '') -> List[Dict[str, str]]:
        clean_username = str(username or '').strip().lower()
        clean_client = str(client_id or '').strip()
        clauses: list[str] = []
        params: list[Any] = []
        if clean_username:
            clauses.append('username=?')
            params.append(clean_username)
        if clean_client:
            clauses.append('client_id=?')
            params.append(clean_client)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ''
        with self._connect() as conn:
            rows = conn.execute(
                f'SELECT id, client_id, username, cloud_path, mode, created_at, updated_at FROM cloud_sync_selective_paths {where} ORDER BY cloud_path',
                params,
            ).fetchall()
        return [
            {
                'id': str(row['id']),
                'client_id': str(row['client_id']),
                'username': str(row['username']),
                'cloud_path': str(row['cloud_path']),
                'mode': str(row['mode']),
                'created_at': str(row['created_at']),
                'updated_at': str(row['updated_at']),
            }
            for row in rows
        ]

    def record_sync_conflict(
        self,
        *,
        client_id: str,
        path: str,
        conflict_type: str,
        pair_id: str = '',
        local_path: str = '',
        cloud_path: str = '',
        local_version: str = '',
        cloud_version: str = '',
        details: Optional[Dict[str, Any]] = None,
    ) -> CloudDriveSyncConflict:
        client = self.get_sync_client(client_id)
        if client is None:
            raise RuntimeError(f'Sync-клиент не найден: {client_id}')
        clean_path = self._normalize_path(path) or str(local_path or cloud_path or '').strip()
        if not clean_path:
            raise RuntimeError('Не задан путь sync-конфликта.')
        clean_type = str(conflict_type or '').strip().lower() or 'unknown'
        now = _utc_now()
        conflict_id = str(uuid.uuid4())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    '''
                    INSERT INTO cloud_sync_conflicts (
                        id, client_id, pair_id, username, path, local_path, cloud_path,
                        conflict_type, local_version, cloud_version, status, resolution,
                        details_json, resolved_by, resolved_at, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', '', ?, '', '', ?, ?)
                    ''',
                    (
                        conflict_id,
                        str(client_id),
                        str(pair_id or ''),
                        client.username,
                        clean_path,
                        str(local_path or '').strip(),
                        self._normalize_path(cloud_path),
                        clean_type,
                        str(local_version or ''),
                        str(cloud_version or ''),
                        json.dumps(details or {}, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                saved = conn.execute('SELECT * FROM cloud_sync_conflicts WHERE id=?', (conflict_id,)).fetchone()
        assert saved is not None
        return self._sync_conflict_from_row(saved)

    def list_sync_conflicts(
        self,
        *,
        username: str = '',
        client_id: str = '',
        status: str = 'open',
        limit: int = 100,
    ) -> List[CloudDriveSyncConflict]:
        clean_username = str(username or '').strip().lower()
        clean_client = str(client_id or '').strip()
        clean_status = str(status or '').strip().lower()
        clauses: list[str] = []
        params: list[Any] = []
        if clean_username:
            clauses.append('username=?')
            params.append(clean_username)
        if clean_client:
            clauses.append('client_id=?')
            params.append(clean_client)
        if clean_status and clean_status != 'all':
            clauses.append('status=?')
            params.append(clean_status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ''
        max_rows = max(1, min(int(limit or 100), 1000))
        params.append(max_rows)
        with self._connect() as conn:
            rows = conn.execute(
                f'SELECT * FROM cloud_sync_conflicts {where} ORDER BY updated_at DESC LIMIT ?',
                params,
            ).fetchall()
        return [self._sync_conflict_from_row(row) for row in rows]

    def resolve_sync_conflict(
        self,
        conflict_id: str,
        *,
        resolution: str,
        resolved_by: str = '',
    ) -> CloudDriveSyncConflict:
        clean_id = str(conflict_id or '').strip()
        clean_resolution = str(resolution or '').strip().lower()
        if clean_resolution not in {'local_wins', 'cloud_wins', 'newest_wins', 'manual', 'ignored'}:
            raise RuntimeError('Недопустимое решение sync-конфликта.')
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute('SELECT * FROM cloud_sync_conflicts WHERE id=?', (clean_id,)).fetchone()
                if row is None:
                    raise RuntimeError(f'Sync-конфликт не найден: {clean_id}')
                conn.execute(
                    '''
                    UPDATE cloud_sync_conflicts
                    SET status='resolved',
                        resolution=?,
                        resolved_by=?,
                        resolved_at=?,
                        updated_at=?
                    WHERE id=?
                    ''',
                    (clean_resolution, str(resolved_by or '').strip().lower(), now, now, clean_id),
                )
                saved = conn.execute('SELECT * FROM cloud_sync_conflicts WHERE id=?', (clean_id,)).fetchone()
        assert saved is not None
        return self._sync_conflict_from_row(saved)

    def stats(self) -> CloudDriveStats:
        with self._connect() as conn:
            folders = int(conn.execute('SELECT COUNT(*) FROM cloud_folders').fetchone()[0])
            files = int(conn.execute("SELECT COUNT(*) FROM cloud_files WHERE deleted_at=''").fetchone()[0])
            versions = int(conn.execute('SELECT COUNT(*) FROM cloud_file_versions').fetchone()[0])
            pending_jobs = int(conn.execute("SELECT COUNT(*) FROM cloud_jobs WHERE status IN ('pending','running')").fetchone()[0])
            root_row = conn.execute('SELECT path FROM cloud_folders WHERE is_root=1 LIMIT 1').fetchone()
            root_path = str(root_row['path']) if root_row else ''
        return CloudDriveStats(folders=folders, files=files, versions=versions, pending_jobs=pending_jobs, root_path=root_path)

    # ── Analytics queries ────────────────────────────────────────────────────

    def get_top_changed_files(self, *, limit: int = 20, since: str = '') -> List[Dict[str, Any]]:
        """Return top N files by version count, optionally filtered by created_at >= since."""
        params: tuple = (since,) if since else ()
        where = "WHERE v.created_at >= ?" if since else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT f.path, f.name, f.size_bytes, f.mime_type,
                       COUNT(v.id) AS version_count,
                       MIN(v.created_at) AS first_change,
                       MAX(v.created_at) AS last_change,
                       GROUP_CONCAT(DISTINCT v.created_by) AS changed_by
                FROM cloud_file_versions v
                JOIN cloud_files f ON f.id = v.file_id
                {where}
                GROUP BY v.file_id
                ORDER BY version_count DESC
                LIMIT ?
                """,
                (*params, int(limit)),
            ).fetchall()
        return [
            {
                "path": str(r["path"] or ""),
                "name": str(r["name"] or ""),
                "size_bytes": int(r["size_bytes"] or 0),
                "mime_type": str(r["mime_type"] or ""),
                "version_count": int(r["version_count"] or 0),
                "first_change": str(r["first_change"] or ""),
                "last_change": str(r["last_change"] or ""),
                "changed_by": [u for u in str(r["changed_by"] or "").split(",") if u.strip()],
            }
            for r in rows
        ]

    def get_change_timeline(self, *, bucket: str = "day", since: str = '', limit: int = 90) -> List[Dict[str, Any]]:
        """Return version counts grouped by time bucket.

        bucket: 'minute' | 'hour' | 'day' | 'week' | 'month'
        Returns list of {bucket, count, unique_files, changed_by_count}.
        """
        _BUCKET_EXPR = {
            "minute": "substr(v.created_at, 1, 16)",
            "hour":   "substr(v.created_at, 1, 13)",
            "day":    "substr(v.created_at, 1, 10)",
            "week":   "strftime('%Y-W%W', v.created_at)",
            "month":  "substr(v.created_at, 1, 7)",
        }
        bucket_expr = _BUCKET_EXPR.get(bucket, _BUCKET_EXPR["day"])
        where = "WHERE v.created_at >= ?" if since else ""
        params = (since,) if since else ()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {bucket_expr} AS bucket,
                       COUNT(v.id) AS count,
                       COUNT(DISTINCT v.file_id) AS unique_files,
                       COUNT(DISTINCT v.created_by) AS changed_by_count
                FROM cloud_file_versions v
                {where}
                GROUP BY bucket
                ORDER BY bucket DESC
                LIMIT ?
                """,
                (*params, int(limit)),
            ).fetchall()
        return [
            {
                "bucket": str(r["bucket"] or ""),
                "count": int(r["count"] or 0),
                "unique_files": int(r["unique_files"] or 0),
                "changed_by_count": int(r["changed_by_count"] or 0),
            }
            for r in reversed(rows)
        ]

    def find_duplicates(self, *, min_size_bytes: int = 0) -> List[Dict[str, Any]]:
        """Return groups of files sharing the same checksum (content duplicates).

        Each group: {checksum, file_count, size_bytes, wasted_bytes, files: [{path, name}]}
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT checksum, COUNT(*) AS file_count, MAX(size_bytes) AS size_bytes
                FROM cloud_files
                WHERE deleted_at='' AND checksum != '' AND size_bytes >= ?
                GROUP BY checksum
                HAVING COUNT(*) > 1
                ORDER BY (COUNT(*) - 1) * MAX(size_bytes) DESC
                LIMIT 500
                """,
                (int(min_size_bytes),),
            ).fetchall()
            groups = []
            for r in rows:
                checksum = str(r["checksum"])
                size = int(r["size_bytes"] or 0)
                count = int(r["file_count"] or 0)
                file_rows = conn.execute(
                    "SELECT path, name FROM cloud_files WHERE checksum=? AND deleted_at='' ORDER BY path",
                    (checksum,),
                ).fetchall()
                groups.append({
                    "checksum": checksum[:16] + "…",
                    "file_count": count,
                    "size_bytes": size,
                    "wasted_bytes": size * (count - 1),
                    "files": [{"path": str(fr["path"]), "name": str(fr["name"])} for fr in file_rows],
                })
        return groups

    def get_storage_savings(self) -> Dict[str, Any]:
        """Return storage deduplication stats."""
        with self._connect() as conn:
            total = conn.execute(
                "SELECT COUNT(*) AS c, SUM(size_bytes) AS s FROM cloud_files WHERE deleted_at=''"
            ).fetchone()
            unique = conn.execute(
                "SELECT COUNT(DISTINCT storage_key) AS c, SUM(DISTINCT size_bytes) AS s FROM cloud_files WHERE deleted_at=''"
            ).fetchone()
        total_files = int((total or {})["c"] or 0)
        total_bytes = int((total or {})["s"] or 0)
        unique_keys = int((unique or {})["c"] or 0)
        return {
            "total_files": total_files,
            "total_logical_bytes": total_bytes,
            "unique_storage_keys": unique_keys,
            "saved_bytes": max(0, total_bytes - int((unique or {})["s"] or 0)),
        }

    def _normalize_path(self, path: str) -> str:
        value = str(path or '').strip().replace('\\', '/')
        value = '/'.join(part for part in value.split('/') if part not in {'', '.'})
        return value

    def _folder_from_row(self, row: sqlite3.Row) -> CloudDriveFolder:
        keys = row.keys()
        return CloudDriveFolder(
            id=str(row['id']),
            parent_id=str(row['parent_id']) if row['parent_id'] is not None else None,
            name=str(row['name']),
            path=str(row['path']),
            depth=int(row['depth'] or 0),
            source_path=str(row['source_path'] or ''),
            is_root=bool(int(row['is_root'] or 0)),
            source_mtime=float(row['source_mtime'] or 0) if 'source_mtime' in keys else 0.0,
            created_at=str(row['created_at'] or ''),
            updated_at=str(row['updated_at'] or ''),
            deleted_at=str(row['deleted_at'] or '') if 'deleted_at' in keys else '',
        )

    def _file_from_row(self, row: sqlite3.Row) -> CloudDriveFile:
        keys = row.keys()
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
            source_mtime=float(row['source_mtime'] or 0) if 'source_mtime' in keys else 0.0,
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

    def _sync_client_from_row(self, row: sqlite3.Row) -> CloudDriveSyncClient:
        try:
            metadata = json.loads(str(row['metadata_json'] or '{}'))
        except json.JSONDecodeError:
            metadata = {}
        return CloudDriveSyncClient(
            id=str(row['id']),
            username=str(row['username']),
            device_id=str(row['device_id']),
            display_name=str(row['display_name'] or ''),
            platform=str(row['platform'] or ''),
            status=str(row['status'] or 'offline'),
            last_seen_at=str(row['last_seen_at'] or ''),
            metadata=dict(metadata or {}),
            created_at=str(row['created_at'] or ''),
            updated_at=str(row['updated_at'] or ''),
        )

    def _sync_pair_from_row(self, row: sqlite3.Row) -> CloudDriveSyncPair:
        return CloudDriveSyncPair(
            id=str(row['id']),
            client_id=str(row['client_id']),
            username=str(row['username']),
            local_path=str(row['local_path']),
            cloud_path=str(row['cloud_path'] or ''),
            conflict_policy=str(row['conflict_policy'] or 'ask'),
            enabled=bool(int(row['enabled'] or 0)),
            created_at=str(row['created_at'] or ''),
            updated_at=str(row['updated_at'] or ''),
        )

    def _sync_conflict_from_row(self, row: sqlite3.Row) -> CloudDriveSyncConflict:
        try:
            details = json.loads(str(row['details_json'] or '{}'))
        except json.JSONDecodeError:
            details = {}
        return CloudDriveSyncConflict(
            id=str(row['id']),
            client_id=str(row['client_id']),
            pair_id=str(row['pair_id'] or ''),
            username=str(row['username']),
            path=str(row['path']),
            local_path=str(row['local_path'] or ''),
            cloud_path=str(row['cloud_path'] or ''),
            conflict_type=str(row['conflict_type'] or 'unknown'),
            local_version=str(row['local_version'] or ''),
            cloud_version=str(row['cloud_version'] or ''),
            status=str(row['status'] or 'open'),
            resolution=str(row['resolution'] or ''),
            details=dict(details or {}),
            resolved_by=str(row['resolved_by'] or ''),
            resolved_at=str(row['resolved_at'] or ''),
            created_at=str(row['created_at'] or ''),
            updated_at=str(row['updated_at'] or ''),
        )
