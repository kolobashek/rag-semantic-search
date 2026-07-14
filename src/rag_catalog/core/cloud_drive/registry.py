from __future__ import annotations

import json
import secrets
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from rag_catalog.core.db_contract import ensure_schema_version
from rag_catalog.core.sqlite_runtime import prepare_sqlite_connection

from .models import (
    CloudDriveFile,
    CloudDriveFolder,
    CloudDriveImportSource,
    CloudDriveJob,
    CloudDriveStats,
    CloudDriveSyncClient,
    CloudDriveSyncConflict,
    CloudDriveSyncPair,
)

CLOUD_DRIVE_SCHEMA_VERSION = 8


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_future_timestamp(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError("Срок действия ссылки должен быть датой и временем в ISO-формате.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if parsed <= datetime.now(timezone.utc):
        raise RuntimeError("Срок действия публичной ссылки должен быть в будущем.")
    return parsed.isoformat()


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
        prepare_sqlite_connection(conn)

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

                    CREATE TABLE IF NOT EXISTS cloud_user_folders (
                        username TEXT PRIMARY KEY,
                        folder_id TEXT NOT NULL,
                        folder_path TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY(folder_id) REFERENCES cloud_folders(id)
                    );

                    CREATE TABLE IF NOT EXISTS cloud_share_links (
                        token TEXT PRIMARY KEY,
                        resource_type TEXT NOT NULL,
                        resource_id TEXT NOT NULL,
                        path TEXT NOT NULL DEFAULT '',
                        access_level TEXT NOT NULL DEFAULT 'viewer',
                        created_by TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL DEFAULT '',
                        revoked_at TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS cloud_import_sources (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        source_path TEXT NOT NULL,
                        target_path TEXT NOT NULL DEFAULT '',
                        import_files INTEGER NOT NULL DEFAULT 1,
                        enabled INTEGER NOT NULL DEFAULT 1,
                        created_by TEXT NOT NULL DEFAULT '',
                        last_job_id TEXT NOT NULL DEFAULT '',
                        last_status TEXT NOT NULL DEFAULT '',
                        last_error TEXT NOT NULL DEFAULT '',
                        last_scan_at TEXT NOT NULL DEFAULT '',
                        stats_json TEXT NOT NULL DEFAULT '{}',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(source_path, target_path)
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
                        lease_owner TEXT NOT NULL DEFAULT '',
                        lease_until TEXT NOT NULL DEFAULT '',
                        next_run_at TEXT NOT NULL DEFAULT '',
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
                    CREATE INDEX IF NOT EXISTS idx_cloud_folders_parent_active_cover
                        ON cloud_folders(parent_id, deleted_at, id);
                    CREATE INDEX IF NOT EXISTS idx_cloud_folders_source_path_active
                        ON cloud_folders(source_path) WHERE deleted_at='';
                    CREATE INDEX IF NOT EXISTS idx_cloud_files_folder ON cloud_files(folder_id, name);
                    CREATE INDEX IF NOT EXISTS idx_cloud_files_folder_active_size
                        ON cloud_files(folder_id, deleted_at, size_bytes);
                    CREATE INDEX IF NOT EXISTS idx_cloud_files_source_path_active
                        ON cloud_files(source_path) WHERE deleted_at='';
                    CREATE INDEX IF NOT EXISTS idx_cloud_files_storage_key ON cloud_files(storage_key);
                    CREATE INDEX IF NOT EXISTS idx_cloud_versions_file ON cloud_file_versions(file_id, created_at);
                    CREATE INDEX IF NOT EXISTS idx_cloud_permissions_subject ON cloud_permissions(subject_type, subject_id, access_level);
                    CREATE INDEX IF NOT EXISTS idx_cloud_permissions_resource ON cloud_permissions(resource_type, resource_id);
                    CREATE INDEX IF NOT EXISTS idx_cloud_user_folders_folder ON cloud_user_folders(folder_id);
                    CREATE INDEX IF NOT EXISTS idx_cloud_share_links_resource ON cloud_share_links(resource_type, resource_id);
                    CREATE INDEX IF NOT EXISTS idx_cloud_import_sources_enabled ON cloud_import_sources(enabled, updated_at);
                    CREATE INDEX IF NOT EXISTS idx_cloud_jobs_status ON cloud_jobs(status, job_type, created_at);
                    CREATE INDEX IF NOT EXISTS idx_cloud_jobs_lease ON cloud_jobs(status, lease_until, next_run_at);
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
        if not self._has_column(conn, "cloud_jobs", "lease_owner"):
            conn.execute("ALTER TABLE cloud_jobs ADD COLUMN lease_owner TEXT NOT NULL DEFAULT ''")
        if not self._has_column(conn, "cloud_jobs", "lease_until"):
            conn.execute("ALTER TABLE cloud_jobs ADD COLUMN lease_until TEXT NOT NULL DEFAULT ''")
        if not self._has_column(conn, "cloud_jobs", "next_run_at"):
            conn.execute("ALTER TABLE cloud_jobs ADD COLUMN next_run_at TEXT NOT NULL DEFAULT ''")
        if not self._has_column(conn, "cloud_folders", "deleted_at"):
            conn.execute("ALTER TABLE cloud_folders ADD COLUMN deleted_at TEXT NOT NULL DEFAULT ''")
        if not self._has_column(conn, "cloud_folders", "source_mtime"):
            conn.execute("ALTER TABLE cloud_folders ADD COLUMN source_mtime REAL NOT NULL DEFAULT 0")
        if not self._has_column(conn, "cloud_files", "source_mtime"):
            conn.execute("ALTER TABLE cloud_files ADD COLUMN source_mtime REAL NOT NULL DEFAULT 0")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS cloud_user_folders (
                username TEXT PRIMARY KEY,
                folder_id TEXT NOT NULL,
                folder_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(folder_id) REFERENCES cloud_folders(id)
            );
            CREATE TABLE IF NOT EXISTS cloud_share_links (
                token TEXT PRIMARY KEY,
                resource_type TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                path TEXT NOT NULL DEFAULT '',
                access_level TEXT NOT NULL DEFAULT 'viewer',
                created_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL DEFAULT '',
                revoked_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS cloud_import_sources (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                source_path TEXT NOT NULL,
                target_path TEXT NOT NULL DEFAULT '',
                import_files INTEGER NOT NULL DEFAULT 1,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL DEFAULT '',
                last_job_id TEXT NOT NULL DEFAULT '',
                last_status TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                last_scan_at TEXT NOT NULL DEFAULT '',
                stats_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(source_path, target_path)
            );
            """
        )

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

    @staticmethod
    def _clean_username(value: str) -> str:
        return str(value or '').strip().lower()

    def ensure_user_home_folder(self, *, username: str) -> CloudDriveFolder:
        clean_username = self._clean_username(username)
        if not clean_username:
            raise RuntimeError('Не задан username для личной папки Cloud Drive.')
        folder_name = clean_username.replace('/', '_').replace('\\', '_').strip() or clean_username
        expected_path = self._normalize_path(folder_name)
        self.ensure_root_folder(root_name='Cloud Drive')

        mapped_folder: Optional[CloudDriveFolder] = None
        with self._lock:
            with self._connect() as conn:
                mapping = conn.execute(
                    "SELECT folder_id FROM cloud_user_folders WHERE username=?",
                    (clean_username,),
                ).fetchone()
                if mapping is not None:
                    folder_row = conn.execute(
                        "SELECT * FROM cloud_folders WHERE id=? AND deleted_at=''",
                        (str(mapping["folder_id"]),),
                    ).fetchone()
                    if folder_row is not None:
                        mapped_folder = self._folder_from_row(folder_row)

        target = self.get_folder_by_path(expected_path)
        if target is not None:
            folder = target
        elif mapped_folder is not None:
            folder = self.rename_move_folder(
                source_path=mapped_folder.path,
                dest_parent_path='',
                new_name=folder_name,
            )
        else:
            folder = self.create_folder(parent_path='', name=folder_name)

        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO cloud_user_folders (username, folder_id, folder_path, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(username) DO UPDATE SET
                        folder_id=excluded.folder_id,
                        folder_path=excluded.folder_path,
                        updated_at=excluded.updated_at
                    """,
                    (clean_username, folder.id, folder.path, now, now),
                )
        self.grant_permission(
            subject_type='user',
            subject_id=clean_username,
            resource_type='folder',
            resource_id=folder.id,
            access_level='owner',
        )
        return folder

    def is_user_home_folder_path(self, path: str) -> bool:
        clean_path = self._normalize_path(path)
        if not clean_path or '/' in clean_path:
            return False
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM cloud_user_folders WHERE folder_path=? LIMIT 1",
                (clean_path,),
            ).fetchone()
            return row is not None

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
                row = conn.execute(
                    'SELECT id, current_version_id, storage_key, checksum, size_bytes FROM cloud_files WHERE path=?',
                    (clean_path,),
                ).fetchone()
                file_id = str(row['id']) if row else str(uuid.uuid4())
                same_content = (
                    row is not None
                    and str(row['current_version_id'] or '')
                    and str(row['storage_key'] or '') == storage_key
                    and str(row['checksum'] or '') == checksum
                    and int(row['size_bytes'] or 0) == int(size_bytes)
                )
                version_id = str(row['current_version_id']) if same_content else str(uuid.uuid4())
                if not same_content:
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

    def get_node_by_source_path(self, source_path: str) -> CloudDriveFolder | CloudDriveFile | None:
        raw = str(source_path or '').strip()
        if not raw:
            return None
        variants = {
            raw,
            raw.replace('\\', '/'),
            raw.replace('/', '\\'),
        }
        with self._connect() as conn:
            for value in variants:
                folder = conn.execute(
                    "SELECT * FROM cloud_folders WHERE source_path=? AND deleted_at='' LIMIT 1",
                    (value,),
                ).fetchone()
                if folder is not None:
                    return self._folder_from_row(folder)
                file_row = conn.execute(
                    "SELECT * FROM cloud_files WHERE source_path=? AND deleted_at='' LIMIT 1",
                    (value,),
                ).fetchone()
                if file_row is not None:
                    return self._file_from_row(file_row)
        return None

    def list_files_in_folder(self, folder_id: str) -> List[CloudDriveFile]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM cloud_files WHERE folder_id=? AND deleted_at='' ORDER BY name",
                (folder_id,),
            ).fetchall()
            return [self._file_from_row(row) for row in rows]

    def folder_size_bytes_map(self, folder_ids: List[str]) -> Dict[str, int]:
        """Return recursive logical file sizes for the requested folders."""
        clean_ids = [str(folder_id) for folder_id in folder_ids if str(folder_id or "").strip()]
        if not clean_ids:
            return {}
        placeholders = ",".join("?" for _ in clean_ids)
        query = f"""
            WITH RECURSIVE descendants(root_id, folder_id) AS (
                SELECT id, id
                FROM cloud_folders
                WHERE id IN ({placeholders}) AND deleted_at=''
                UNION ALL
                SELECT descendants.root_id, child.id
                FROM cloud_folders AS child
                JOIN descendants ON child.parent_id = descendants.folder_id
                WHERE child.deleted_at=''
            )
            SELECT descendants.root_id AS folder_id,
                   COALESCE(SUM(cloud_files.size_bytes), 0) AS size_bytes
            FROM descendants
            LEFT JOIN cloud_files
              ON cloud_files.folder_id = descendants.folder_id
             AND cloud_files.deleted_at=''
            GROUP BY descendants.root_id
        """
        sizes = {folder_id: 0 for folder_id in clean_ids}
        with self._connect() as conn:
            rows = conn.execute(query, clean_ids).fetchall()
        for row in rows:
            sizes[str(row["folder_id"])] = int(row["size_bytes"] or 0)
        return sizes

    @staticmethod
    def _escape_like(value: str) -> str:
        return (
            str(value or '')
            .replace('\\', '\\\\')
            .replace('%', '\\%')
            .replace('_', '\\_')
        )

    @classmethod
    def _like_contains(cls, value: str) -> str:
        escaped = cls._escape_like(value)
        return f'%{escaped}%'

    def search_nodes_page(
        self,
        *,
        query: str,
        path: str = '',
        limit: int = 50,
        offset: int = 0,
        node_type: str = '',
        extension: str = '',
        mime_type: str = '',
    ) -> Dict[str, Any]:
        needle = str(query or '').strip()
        if not needle:
            clean_path = self._normalize_path(path)
            clean_limit = max(1, min(int(limit or 50), 500))
            clean_offset = max(0, int(offset or 0))
            return {
                'query': '',
                'path': clean_path,
                'items': [],
                'count': 0,
                'total': 0,
                'limit': clean_limit,
                'offset': clean_offset,
                'next_offset': None,
            }
        clean_path = self._normalize_path(path)
        clean_limit = max(1, min(int(limit or 50), 500))
        clean_offset = max(0, int(offset or 0))
        clean_type = str(node_type or '').strip().lower()
        if clean_type not in {'', 'file', 'folder'}:
            clean_type = ''
        clean_ext = str(extension or '').strip().lower().lstrip('.')
        clean_mime = str(mime_type or '').strip().lower()
        pattern = self._like_contains(needle)
        folder_where = ["deleted_at=''", "(name LIKE ? ESCAPE '\\' OR path LIKE ? ESCAPE '\\')"]
        file_where = ["deleted_at=''", "(name LIKE ? ESCAPE '\\' OR path LIKE ? ESCAPE '\\')"]
        folder_params: list[Any] = [pattern, pattern]
        file_params: list[Any] = [pattern, pattern]
        if clean_path:
            folder_where.append("(path=? OR path LIKE ? ESCAPE '\\')")
            file_where.append("(path=? OR path LIKE ? ESCAPE '\\')")
            path_like = f"{self._escape_like(clean_path)}/%"
            folder_params.extend([clean_path, path_like])
            file_params.extend([clean_path, path_like])
        if clean_ext:
            file_where.append("lower(name) LIKE ? ESCAPE '\\'")
            file_params.append(f"%.{self._escape_like(clean_ext)}")
        if clean_mime:
            file_where.append("lower(mime_type) LIKE ? ESCAPE '\\'")
            file_params.append(self._like_contains(clean_mime))

        selects: list[str] = []
        params: list[Any] = []
        count_selects: list[str] = []
        count_params: list[Any] = []
        if clean_type in {'', 'folder'} and not clean_ext and not clean_mime:
            folder_filter = ' AND '.join(folder_where)
            selects.append(
                f"""
                SELECT 'folder' AS node_type, id, name, path, source_path, 0 AS size_bytes,
                       '' AS mime_type, created_at, updated_at
                FROM cloud_folders
                WHERE {folder_filter}
                """
            )
            params.extend(folder_params)
            count_selects.append(f"SELECT id FROM cloud_folders WHERE {folder_filter}")
            count_params.extend(folder_params)
        if clean_type in {'', 'file'}:
            file_filter = ' AND '.join(file_where)
            selects.append(
                f"""
                SELECT 'file' AS node_type, id, name, path, source_path, size_bytes,
                       mime_type, created_at, updated_at
                FROM cloud_files
                WHERE {file_filter}
                """
            )
            params.extend(file_params)
            count_selects.append(f"SELECT id FROM cloud_files WHERE {file_filter}")
            count_params.extend(file_params)
        if not selects:
            return {
                'query': needle,
                'path': clean_path,
                'items': [],
                'count': 0,
                'total': 0,
                'limit': clean_limit,
                'offset': clean_offset,
                'next_offset': None,
            }

        union_sql = "\nUNION ALL\n".join(selects)
        count_sql = "\nUNION ALL\n".join(count_selects)
        with self._connect() as conn:
            total_row = conn.execute(f"SELECT COUNT(*) AS total FROM ({count_sql})", tuple(count_params)).fetchone()
            rows = conn.execute(
                f"""
                SELECT *
                FROM ({union_sql})
                ORDER BY updated_at DESC, name
                LIMIT ?
                OFFSET ?
                """,
                (*params, clean_limit, clean_offset),
            ).fetchall()
        total = int(total_row['total'] if total_row else 0)
        items = [
            {
                'node_type': str(row['node_type']),
                'id': str(row['id']),
                'name': str(row['name']),
                'path': str(row['path']),
                'source_path': str(row['source_path'] or ''),
                'size_bytes': int(row['size_bytes'] or 0),
                'mime_type': str(row['mime_type'] or ''),
                'created_at': str(row['created_at'] or ''),
                'updated_at': str(row['updated_at'] or ''),
            }
            for row in rows
        ]
        next_offset = clean_offset + len(items) if clean_offset + len(items) < total else None
        return {
            'query': needle,
            'path': clean_path,
            'items': items,
            'count': len(items),
            'total': total,
            'limit': clean_limit,
            'offset': clean_offset,
            'next_offset': next_offset,
        }

    def search_nodes(self, *, query: str, path: str = '', limit: int = 50) -> List[Dict[str, Any]]:
        return list(self.search_nodes_page(query=query, path=path, limit=limit).get('items') or [])

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

    def has_any_permissions(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM cloud_permissions LIMIT 1").fetchone()
            return row is not None

    @staticmethod
    def _access_rank(access_level: str) -> int:
        return {
            "viewer": 1,
            "read": 1,
            "editor": 2,
            "write": 2,
            "admin": 3,
            "owner": 3,
        }.get(str(access_level or "").strip().lower(), 0)

    def grant_permission(
        self,
        *,
        subject_type: str,
        subject_id: str,
        resource_type: str,
        resource_id: str,
        access_level: str = "viewer",
    ) -> Dict[str, str]:
        clean_subject_type = str(subject_type or "").strip().lower()
        clean_resource_type = str(resource_type or "").strip().lower()
        clean_access = str(access_level or "viewer").strip().lower()
        if clean_subject_type not in {"user", "role", "group", "*"}:
            raise RuntimeError("Недопустимый subject_type для Cloud Drive permission.")
        if clean_resource_type not in {"file", "folder", "path", "global"}:
            raise RuntimeError("Недопустимый resource_type для Cloud Drive permission.")
        if self._access_rank(clean_access) <= 0:
            raise RuntimeError("Недопустимый access_level для Cloud Drive permission.")
        clean_subject_id = str(subject_id or "*").strip().lower() or "*"
        clean_resource_id = str(resource_id or "*").strip()
        if clean_resource_type == "path":
            clean_resource_id = self._normalize_path(clean_resource_id) or "*"
        if clean_resource_type == "global":
            clean_resource_id = "*"
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                existing = conn.execute(
                    """
                    SELECT * FROM cloud_permissions
                    WHERE subject_type=? AND subject_id=? AND resource_type=? AND resource_id=? AND access_level=?
                    LIMIT 1
                    """,
                    (clean_subject_type, clean_subject_id, clean_resource_type, clean_resource_id, clean_access),
                ).fetchone()
                if existing is not None:
                    return {
                        "id": str(existing["id"]),
                        "subject_type": clean_subject_type,
                        "subject_id": clean_subject_id,
                        "resource_type": clean_resource_type,
                        "resource_id": clean_resource_id,
                        "access_level": clean_access,
                        "created_at": str(existing["created_at"] or ""),
                    }
                permission_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO cloud_permissions (
                        id, subject_type, subject_id, resource_type, resource_id, access_level, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        permission_id,
                        clean_subject_type,
                        clean_subject_id,
                        clean_resource_type,
                        clean_resource_id,
                        clean_access,
                        now,
                    ),
                )
        return {
            "id": permission_id,
            "subject_type": clean_subject_type,
            "subject_id": clean_subject_id,
            "resource_type": clean_resource_type,
            "resource_id": clean_resource_id,
            "access_level": clean_access,
            "created_at": now,
        }

    def list_permissions(self, *, path: str = "") -> List[Dict[str, str]]:
        clean_path = self._normalize_path(path)
        with self._connect() as conn:
            if not clean_path:
                rows = conn.execute(
                    """
                    SELECT id, subject_type, subject_id, resource_type, resource_id, access_level, created_at
                    FROM cloud_permissions
                    ORDER BY created_at DESC
                    """
                ).fetchall()
            else:
                node = self.get_node_by_path(clean_path)
                resource_ids = {clean_path}
                if node is not None:
                    resource_ids.add(str(node.id))
                placeholders = ",".join("?" for _ in resource_ids)
                rows = conn.execute(
                    f"""
                    SELECT id, subject_type, subject_id, resource_type, resource_id, access_level, created_at
                    FROM cloud_permissions
                    WHERE resource_id IN ({placeholders})
                       OR resource_type='global'
                       OR (resource_type='path' AND (?=resource_id OR ? LIKE resource_id || '/%'))
                    ORDER BY created_at DESC
                    """,
                    (*resource_ids, clean_path, clean_path),
                ).fetchall()
        return [dict(row) for row in rows]

    def revoke_permission(self, permission_id: str) -> bool:
        clean_id = str(permission_id or "").strip()
        if not clean_id:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM cloud_permissions WHERE id=?", (clean_id,))
                return cur.rowcount > 0

    def _folder_ancestor_ids_for_path(self, conn: sqlite3.Connection, path: str, *, file_folder_id: str = "") -> set[str]:
        clean_path = self._normalize_path(path)
        candidate_paths = {""}
        parts = [part for part in clean_path.split("/") if part]
        for idx in range(1, len(parts) + 1):
            candidate_paths.add("/".join(parts[:idx]))
        ids: set[str] = set()
        if file_folder_id:
            ids.add(str(file_folder_id))
        if candidate_paths:
            placeholders = ",".join("?" for _ in candidate_paths)
            rows = conn.execute(
                f"SELECT id FROM cloud_folders WHERE path IN ({placeholders})",
                tuple(candidate_paths),
            ).fetchall()
            ids.update(str(row["id"]) for row in rows)
        return ids

    def create_share_link(
        self,
        *,
        path: str,
        created_by: str = "",
        expires_at: str = "",
    ) -> Dict[str, str]:
        clean_path = self._normalize_path(path)
        clean_expires_at = _normalize_future_timestamp(expires_at)
        node = self.get_node_by_path(clean_path)
        if node is None:
            raise RuntimeError(f'Узел не найден: {clean_path}')
        resource_type = 'folder' if hasattr(node, 'is_root') else 'file'
        token = secrets.token_urlsafe(32)
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO cloud_share_links (
                        token, resource_type, resource_id, path, access_level,
                        created_by, created_at, expires_at, revoked_at
                    )
                    VALUES (?, ?, ?, ?, 'viewer', ?, ?, ?, '')
                    """,
                    (
                        token,
                        resource_type,
                        str(node.id),
                        clean_path,
                        self._clean_username(created_by),
                        now,
                        clean_expires_at,
                    ),
                )
        return {
            'token': token,
            'resource_type': resource_type,
            'resource_id': str(node.id),
            'path': clean_path,
            'access_level': 'viewer',
            'created_by': self._clean_username(created_by),
            'created_at': now,
            'expires_at': clean_expires_at,
            'url_path': f'/api/cloud-drive/public/download?token={token}',
        }

    def list_share_links(self, *, path: str = "", include_inactive: bool = False) -> List[Dict[str, str]]:
        clean_path = self._normalize_path(path)
        now = _utc_now()
        clauses: list[str] = []
        params: list[str] = []
        if clean_path:
            clauses.append("path=?")
            params.append(clean_path)
        if not include_inactive:
            clauses.append("revoked_at=''")
            clauses.append("(expires_at='' OR expires_at>?)")
            params.append(now)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT token, resource_type, resource_id, path, access_level,
                       created_by, created_at, expires_at, revoked_at
                FROM cloud_share_links
                {where}
                ORDER BY created_at DESC
                """,
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_share_link(self, token: str) -> Optional[Dict[str, str]]:
        clean_token = str(token or '').strip()
        if not clean_token:
            return None
        now = _utc_now()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM cloud_share_links
                WHERE token=? AND revoked_at=''
                  AND (expires_at='' OR expires_at > ?)
                LIMIT 1
                """,
                (clean_token, now),
            ).fetchone()
        return dict(row) if row is not None else None

    def revoke_share_link(self, token: str) -> bool:
        clean_token = str(token or '').strip()
        if not clean_token:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "UPDATE cloud_share_links SET revoked_at=? WHERE token=? AND revoked_at=''",
                    (_utc_now(), clean_token),
                )
                return cur.rowcount > 0

    def share_link_can_access(self, *, token: str, path: str, required_level: str = 'viewer') -> bool:
        if self._access_rank(required_level) > self._access_rank('viewer'):
            return False
        link = self.get_share_link(token)
        if link is None:
            return False
        clean_path = self._normalize_path(path) or self._normalize_path(str(link.get('path') or ''))
        with self._connect() as conn:
            file_row = conn.execute("SELECT * FROM cloud_files WHERE path=? AND deleted_at=''", (clean_path,)).fetchone()
            folder_row = conn.execute("SELECT * FROM cloud_folders WHERE path=? AND deleted_at=''", (clean_path,)).fetchone()
            folder_ids = self._folder_ancestor_ids_for_path(
                conn,
                clean_path,
                file_folder_id=str(file_row["folder_id"] or "") if file_row is not None else "",
            )
            if folder_row is not None:
                folder_ids.add(str(folder_row["id"]))
            file_ids = {str(file_row["id"])} if file_row is not None else set()
        resource_type = str(link.get('resource_type') or '').lower()
        resource_id = str(link.get('resource_id') or '').strip()
        if resource_type == 'folder':
            return resource_id in folder_ids
        if resource_type == 'file':
            return resource_id in file_ids
        if resource_type == 'path':
            clean_resource = self._normalize_path(resource_id)
            return clean_path == clean_resource or clean_path.startswith(f'{clean_resource}/')
        if resource_type == 'global':
            return True
        return False

    def upsert_import_source(
        self,
        *,
        name: str,
        source_path: str,
        target_path: str = "",
        import_files: bool = True,
        enabled: bool = True,
        created_by: str = "",
    ) -> CloudDriveImportSource:
        raw_source = str(source_path or "").strip()
        if not raw_source:
            raise RuntimeError("Не задан source_path import source.")
        clean_source = str(Path(raw_source).expanduser())
        clean_target = self._normalize_path(target_path)
        clean_name = str(name or "").strip() or Path(clean_source).name or clean_source
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT id, created_at FROM cloud_import_sources WHERE source_path=? AND target_path=?",
                    (clean_source, clean_target),
                ).fetchone()
                source_id = str(row["id"]) if row else str(uuid.uuid4())
                created_at = str(row["created_at"]) if row else now
                conn.execute(
                    """
                    INSERT INTO cloud_import_sources (
                        id, name, source_path, target_path, import_files, enabled,
                        created_by, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_path, target_path) DO UPDATE SET
                        name=excluded.name,
                        import_files=excluded.import_files,
                        enabled=excluded.enabled,
                        created_by=CASE
                            WHEN cloud_import_sources.created_by='' THEN excluded.created_by
                            ELSE cloud_import_sources.created_by
                        END,
                        updated_at=excluded.updated_at
                    """,
                    (
                        source_id,
                        clean_name,
                        clean_source,
                        clean_target,
                        1 if import_files else 0,
                        1 if enabled else 0,
                        self._clean_username(created_by),
                        created_at,
                        now,
                    ),
                )
                saved = conn.execute("SELECT * FROM cloud_import_sources WHERE id=?", (source_id,)).fetchone()
        assert saved is not None
        return self._import_source_from_row(saved)

    def get_import_source(self, source_id: str) -> Optional[CloudDriveImportSource]:
        clean_id = str(source_id or "").strip()
        if not clean_id:
            return None
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM cloud_import_sources WHERE id=?", (clean_id,)).fetchone()
            return self._import_source_from_row(row) if row else None

    def list_import_sources(self, *, enabled_only: bool = False, limit: int = 200) -> List[CloudDriveImportSource]:
        clean_limit = max(1, min(int(limit or 200), 1000))
        with self._connect() as conn:
            if enabled_only:
                rows = conn.execute(
                    "SELECT * FROM cloud_import_sources WHERE enabled=1 ORDER BY updated_at DESC LIMIT ?",
                    (clean_limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM cloud_import_sources ORDER BY updated_at DESC LIMIT ?",
                    (clean_limit,),
                ).fetchall()
        return [self._import_source_from_row(row) for row in rows]

    def set_import_source_enabled(self, source_id: str, enabled: bool) -> CloudDriveImportSource:
        clean_id = str(source_id or "").strip()
        if not clean_id:
            raise RuntimeError("Не задан import source id.")
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE cloud_import_sources SET enabled=?, updated_at=? WHERE id=?",
                    (1 if enabled else 0, now, clean_id),
                )
                saved = conn.execute("SELECT * FROM cloud_import_sources WHERE id=?", (clean_id,)).fetchone()
        if saved is None:
            raise RuntimeError(f"Import source не найден: {clean_id}")
        return self._import_source_from_row(saved)

    def update_import_source_run(
        self,
        source_id: str,
        *,
        job_id: str = "",
        status: str = "",
        error: str = "",
        stats: Optional[Dict[str, Any]] = None,
        scanned: bool = False,
    ) -> Optional[CloudDriveImportSource]:
        clean_id = str(source_id or "").strip()
        if not clean_id:
            return None
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute("SELECT * FROM cloud_import_sources WHERE id=?", (clean_id,)).fetchone()
                if row is None:
                    return None
                next_job_id = str(job_id or row["last_job_id"] or "")
                next_status = str(status or row["last_status"] or "")
                next_error = str(error if error is not None else row["last_error"] or "")
                next_stats = stats if stats is not None else json.loads(str(row["stats_json"] or "{}"))
                next_scan = now if scanned else str(row["last_scan_at"] or "")
                conn.execute(
                    """
                    UPDATE cloud_import_sources
                    SET last_job_id=?,
                        last_status=?,
                        last_error=?,
                        last_scan_at=?,
                        stats_json=?,
                        updated_at=?
                    WHERE id=?
                    """,
                    (
                        next_job_id,
                        next_status,
                        next_error,
                        next_scan,
                        json.dumps(next_stats or {}, ensure_ascii=False),
                        now,
                        clean_id,
                    ),
                )
                saved = conn.execute("SELECT * FROM cloud_import_sources WHERE id=?", (clean_id,)).fetchone()
        return self._import_source_from_row(saved) if saved else None

    def user_can_access(
        self,
        *,
        username: str,
        role: str = "",
        groups: Iterable[str] | None = None,
        path: str = "",
        file_id: str = "",
        required_level: str = "viewer",
    ) -> bool:
        clean_path = self._normalize_path(path)
        clean_file_id = str(file_id or "").strip()
        decisions = self.user_access_map(
            username=username,
            role=role,
            groups=groups,
            nodes=[(clean_path, clean_file_id)],
            required_level=required_level,
        )
        return bool(decisions.get((clean_path, clean_file_id), False))

    def user_access_map(
        self,
        *,
        username: str,
        role: str = "",
        groups: Iterable[str] | None = None,
        nodes: Iterable[tuple[str, str]] = (),
        required_level: str = "viewer",
    ) -> Dict[tuple[str, str], bool]:
        """Resolve ACL decisions for multiple nodes with one registry snapshot."""
        clean_username = str(username or "").strip().lower()
        clean_role = str(role or "").strip().lower()
        clean_groups = {str(group or "").strip().lower() for group in (groups or []) if str(group or "").strip()}
        clean_nodes = list(dict.fromkeys(
            (self._normalize_path(path), str(file_id or "").strip())
            for path, file_id in nodes
        ))
        if not clean_nodes:
            return {}
        required_rank = self._access_rank(required_level)
        if required_rank <= 0:
            required_rank = self._access_rank("viewer")
        if clean_role == "admin":
            return {node: True for node in clean_nodes}

        path_prefixes: set[str] = {""}

        def _add_path_prefixes(clean_path: str) -> None:
            built = ""
            for segment in clean_path.split("/"):
                if not segment:
                    continue
                built = f"{built}/{segment}".strip("/")
                path_prefixes.add(built)

        for clean_path, _file_id in clean_nodes:
            _add_path_prefixes(clean_path)
        clean_file_ids = {file_id for _path, file_id in clean_nodes if file_id}

        def _chunks(values: Iterable[str], size: int = 500) -> Iterable[list[str]]:
            batch = list(values)
            for offset in range(0, len(batch), size):
                yield batch[offset : offset + size]

        with self._connect() as conn:
            if conn.execute("SELECT 1 FROM cloud_permissions LIMIT 1").fetchone() is None:
                return {node: True for node in clean_nodes}

            permission_rows = conn.execute(
                """
                SELECT subject_type, subject_id, resource_type, resource_id, access_level
                FROM cloud_permissions
                """
            ).fetchall()

            folder_ids_by_path: Dict[str, str] = {}
            for batch in _chunks(sorted(path_prefixes)):
                placeholders = ",".join("?" for _ in batch)
                rows = conn.execute(
                    f"SELECT id, path FROM cloud_folders WHERE deleted_at='' AND path IN ({placeholders})",
                    batch,
                ).fetchall()
                folder_ids_by_path.update({self._normalize_path(str(row["path"] or "")): str(row["id"]) for row in rows})

            file_rows_by_id: Dict[str, sqlite3.Row] = {}
            for batch in _chunks(sorted(clean_file_ids)):
                placeholders = ",".join("?" for _ in batch)
                rows = conn.execute(
                    f"SELECT id, folder_id, path FROM cloud_files WHERE id IN ({placeholders})",
                    batch,
                ).fetchall()
                file_rows_by_id.update({str(row["id"]): row for row in rows})

            node_paths = sorted({path for path, _file_id in clean_nodes if path})
            file_rows_by_path: Dict[str, sqlite3.Row] = {}
            for batch in _chunks(node_paths):
                placeholders = ",".join("?" for _ in batch)
                rows = conn.execute(
                    f"SELECT id, folder_id, path FROM cloud_files WHERE path IN ({placeholders})",
                    batch,
                ).fetchall()
                file_rows_by_path.update({self._normalize_path(str(row["path"] or "")): row for row in rows})

            known_folder_paths = set(folder_ids_by_path)
            for row in [*file_rows_by_id.values(), *file_rows_by_path.values()]:
                _add_path_prefixes(self._normalize_path(str(row["path"] or "")))
            for batch in _chunks(sorted(path_prefixes - known_folder_paths)):
                placeholders = ",".join("?" for _ in batch)
                rows = conn.execute(
                    f"SELECT id, path FROM cloud_folders WHERE deleted_at='' AND path IN ({placeholders})",
                    batch,
                ).fetchall()
                folder_ids_by_path.update({self._normalize_path(str(row["path"] or "")): str(row["id"]) for row in rows})

            home_paths = {
                self._normalize_path(str(row["folder_path"] or ""))
                for row in conn.execute("SELECT folder_path FROM cloud_user_folders").fetchall()
            }

        def subject_matches(row: sqlite3.Row) -> bool:
            subject_type = str(row["subject_type"] or "").lower()
            subject_id = str(row["subject_id"] or "").lower()
            if subject_type == "*" and subject_id == "*":
                return True
            if subject_type == "user" and subject_id in {clean_username, "*"}:
                return bool(clean_username or subject_id == "*")
            if subject_type == "role" and subject_id in {clean_role, "*"}:
                return bool(clean_role or subject_id == "*")
            if subject_type == "group" and subject_id in clean_groups:
                return True
            return False

        def path_matches(resource_id: str, node_path: str) -> bool:
            clean_resource = self._normalize_path(resource_id)
            if clean_resource in {"", "*"}:
                return True
            return node_path == clean_resource or node_path.startswith(f"{clean_resource}/")

        applicable_rows = [
            row for row in permission_rows
            if self._access_rank(str(row["access_level"] or "")) >= required_rank and subject_matches(row)
        ]
        decisions: Dict[tuple[str, str], bool] = {}
        for node in clean_nodes:
            clean_path, clean_file_id = node
            file_row = file_rows_by_id.get(clean_file_id) or file_rows_by_path.get(clean_path)
            effective_path = clean_path or (
                self._normalize_path(str(file_row["path"] or "")) if file_row is not None else ""
            )
            if not effective_path and required_rank <= self._access_rank("viewer"):
                decisions[node] = True
                continue
            if required_rank <= self._access_rank("viewer") and effective_path in home_paths:
                decisions[node] = True
                continue

            file_ids = {clean_file_id} if clean_file_id else set()
            if file_row is not None:
                file_ids.add(str(file_row["id"]))
            folder_ids = {
                folder_id
                for prefix, folder_id in folder_ids_by_path.items()
                if not prefix or effective_path == prefix or effective_path.startswith(f"{prefix}/")
            }
            if file_row is not None and str(file_row["folder_id"] or ""):
                folder_ids.add(str(file_row["folder_id"]))

            allowed = False
            for row in applicable_rows:
                resource_type = str(row["resource_type"] or "").lower()
                resource_id = str(row["resource_id"] or "").strip()
                if resource_type == "global":
                    allowed = True
                elif resource_type == "path" and path_matches(resource_id, effective_path):
                    allowed = True
                elif resource_type == "folder" and resource_id in folder_ids:
                    allowed = True
                elif resource_type == "file" and resource_id in file_ids:
                    allowed = True
                if allowed:
                    break
            decisions[node] = allowed
        return decisions

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

    def list_pending_jobs(self, *, job_types: Optional[List[str]] = None, limit: int = 20) -> List[CloudDriveJob]:
        clean_limit = max(1, int(limit))
        clean_types = [str(item or '').strip() for item in (job_types or []) if str(item or '').strip()]
        with self._connect() as conn:
            if clean_types:
                placeholders = ','.join('?' for _ in clean_types)
                rows = conn.execute(
                    f"""
                    SELECT * FROM cloud_jobs
                    WHERE status='pending' AND job_type IN ({placeholders})
                    ORDER BY created_at ASC
                    LIMIT ?
                    """,
                    (*clean_types, clean_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM cloud_jobs
                    WHERE status='pending'
                    ORDER BY created_at ASC
                    LIMIT ?
                    """,
                    (clean_limit,),
                ).fetchall()
            return [self._job_from_row(row) for row in rows]

    def claim_pending_job(
        self,
        *,
        job_types: Optional[List[str]] = None,
        worker_id: str = "",
        lease_seconds: int = 900,
    ) -> Optional[CloudDriveJob]:
        clean_types = [str(item or '').strip() for item in (job_types or []) if str(item or '').strip()]
        owner = str(worker_id or "cloud-drive-worker").strip()
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        lease_until = (now_dt + timedelta(seconds=max(1, int(lease_seconds or 900)))).isoformat()
        with self._lock:
            with self._connect() as conn:
                params: list[Any] = [now]
                type_clause = ""
                if clean_types:
                    placeholders = ",".join("?" for _ in clean_types)
                    type_clause = f"AND job_type IN ({placeholders})"
                    params.extend(clean_types)
                row = conn.execute(
                    f"""
                    SELECT * FROM cloud_jobs
                    WHERE status='pending'
                      AND (next_run_at='' OR next_run_at<=?)
                      {type_clause}
                    ORDER BY created_at ASC
                    LIMIT 1
                    """,
                    params,
                ).fetchone()
                if row is None:
                    return None
                started_at = str(row["started_at"] or "") or now
                conn.execute(
                    """
                    UPDATE cloud_jobs
                    SET status='running',
                        attempts=?,
                        lease_owner=?,
                        lease_until=?,
                        started_at=?,
                        updated_at=?
                    WHERE id=? AND status='pending'
                    """,
                    (
                        int(row["attempts"] or 0) + 1,
                        owner,
                        lease_until,
                        started_at,
                        now,
                        str(row["id"]),
                    ),
                )
                saved = conn.execute("SELECT * FROM cloud_jobs WHERE id=?", (str(row["id"]),)).fetchone()
                assert saved is not None
                return self._job_from_row(saved)

    def recover_stale_jobs(
        self,
        *,
        job_types: Optional[List[str]] = None,
        lease_timeout_seconds: int = 3600,
        limit: int = 100,
    ) -> int:
        clean_types = [str(item or '').strip() for item in (job_types or []) if str(item or '').strip()]
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        stale_before = (now_dt - timedelta(seconds=max(1, int(lease_timeout_seconds or 3600)))).isoformat()
        recovered = 0
        with self._lock:
            with self._connect() as conn:
                params: list[Any] = [now, stale_before]
                type_clause = ""
                if clean_types:
                    placeholders = ",".join("?" for _ in clean_types)
                    type_clause = f"AND job_type IN ({placeholders})"
                    params.extend(clean_types)
                params.append(max(1, min(int(limit or 100), 1000)))
                rows = conn.execute(
                    f"""
                    SELECT * FROM cloud_jobs
                    WHERE status='running'
                      AND (
                        (lease_until!='' AND lease_until<?)
                        OR (lease_until='' AND started_at!='' AND started_at<?)
                      )
                      {type_clause}
                    ORDER BY updated_at ASC
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
                for row in rows:
                    payload = json.loads(str(row["payload_json"] or "{}"))
                    progress = dict(payload.get("progress") or {})
                    progress.update(
                        {
                            "status": "pending",
                            "recovered_at": now,
                            "recovered_reason": "lease_expired",
                        }
                    )
                    payload["progress"] = progress
                    conn.execute(
                        """
                        UPDATE cloud_jobs
                        SET status='pending',
                            payload_json=?,
                            last_error='lease_expired',
                            lease_owner='',
                            lease_until='',
                            next_run_at='',
                            updated_at=?
                        WHERE id=?
                        """,
                        (json.dumps(payload, ensure_ascii=False), now, str(row["id"])),
                    )
                    recovered += 1
        return recovered

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
                lease_owner = str(row["lease_owner"] or "")
                lease_until = str(row["lease_until"] or "")
                next_run_at = str(row["next_run_at"] or "")
                if next_status in {'completed', 'failed', 'cancelled'}:
                    finished_at = now
                    lease_owner = ''
                    lease_until = ''
                    next_run_at = ''
                if next_status == 'pending':
                    lease_owner = ''
                    lease_until = ''
                conn.execute(
                    '''
                    UPDATE cloud_jobs
                    SET status=?,
                        payload_json=?,
                        last_error=?,
                        attempts=?,
                        started_at=?,
                        finished_at=?,
                        lease_owner=?,
                        lease_until=?,
                        next_run_at=?,
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
                        lease_owner,
                        lease_until,
                        next_run_at,
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

    def update_sync_client_status(self, client_id: str, status: str = 'online') -> bool:
        valid = {'online', 'offline', 'paused', 'error'}
        clean_status = str(status or 'online').strip().lower()
        if clean_status not in valid:
            clean_status = 'online'
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    'UPDATE cloud_sync_clients SET status=?, last_seen_at=?, updated_at=? WHERE id=?',
                    (clean_status, now, now, str(client_id or '').strip()),
                )
                return cur.rowcount > 0

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

    def list_active_file_index_records(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, path, name, current_version_id, storage_key, checksum, size_bytes, source_path, updated_at
                FROM cloud_files
                WHERE deleted_at=''
                ORDER BY path
                """
            ).fetchall()
        return [
            {
                "id": str(row["id"]),
                "path": str(row["path"] or ""),
                "name": str(row["name"] or ""),
                "current_version_id": str(row["current_version_id"] or ""),
                "storage_key": str(row["storage_key"] or ""),
                "checksum": str(row["checksum"] or ""),
                "size_bytes": int(row["size_bytes"] or 0),
                "source_path": str(row["source_path"] or ""),
                "updated_at": str(row["updated_at"] or ""),
            }
            for row in rows
        ]

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

    def compact_duplicate_versions(self) -> int:
        """Remove duplicate version rows for unchanged file content.

        Keeps the current version row for every file/content tuple when present;
        otherwise keeps the newest row. Distinct historical contents remain.
        """
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT v.id, v.file_id, v.storage_key, v.checksum, v.size_bytes, v.created_at, f.current_version_id
                    FROM cloud_file_versions v
                    LEFT JOIN cloud_files f ON f.id = v.file_id
                    ORDER BY v.file_id, v.storage_key, v.checksum, v.size_bytes, v.created_at DESC
                    """
                ).fetchall()
                grouped: dict[tuple[str, str, str, int], list[sqlite3.Row]] = {}
                for row in rows:
                    key = (
                        str(row["file_id"] or ""),
                        str(row["storage_key"] or ""),
                        str(row["checksum"] or ""),
                        int(row["size_bytes"] or 0),
                    )
                    grouped.setdefault(key, []).append(row)
                delete_ids: list[str] = []
                for items in grouped.values():
                    if len(items) <= 1:
                        continue
                    current_id = str(items[0]["current_version_id"] or "")
                    keep_id = current_id if any(str(item["id"]) == current_id for item in items) else str(items[0]["id"])
                    delete_ids.extend(str(item["id"]) for item in items if str(item["id"]) != keep_id)
                if delete_ids:
                    conn.executemany("DELETE FROM cloud_file_versions WHERE id=?", [(item_id,) for item_id in delete_ids])
                return len(delete_ids)

    def all_storage_keys(self) -> set[str]:
        """Return the set of unique storage keys currently registered (non-deleted files)."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT storage_key FROM cloud_files "
                "WHERE deleted_at='' AND storage_key<>'' AND storage_key IS NOT NULL"
            ).fetchall()
        return {str(row["storage_key"]) for row in rows}

    def sample_storage_objects(self, *, limit: int = 25) -> list[Dict[str, str]]:
        """Return a small deterministic sample of registry objects for storage checks."""
        safe_limit = max(1, min(int(limit or 25), 500))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT path, storage_key
                FROM cloud_files
                WHERE deleted_at='' AND storage_key<>''
                ORDER BY updated_at DESC, path ASC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [
            {
                "path": str(row["path"] or ""),
                "storage_key": str(row["storage_key"] or ""),
            }
            for row in rows
        ]

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
        keys = row.keys()
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
            lease_owner=str(row['lease_owner'] or '') if 'lease_owner' in keys else '',
            lease_until=str(row['lease_until'] or '') if 'lease_until' in keys else '',
            next_run_at=str(row['next_run_at'] or '') if 'next_run_at' in keys else '',
            progress=dict(payload.get('progress') or {}),
        )

    def _import_source_from_row(self, row: sqlite3.Row) -> CloudDriveImportSource:
        try:
            stats = json.loads(str(row["stats_json"] or "{}"))
        except json.JSONDecodeError:
            stats = {}
        return CloudDriveImportSource(
            id=str(row["id"]),
            name=str(row["name"] or ""),
            source_path=str(row["source_path"] or ""),
            target_path=str(row["target_path"] or ""),
            import_files=bool(int(row["import_files"] or 0)),
            enabled=bool(int(row["enabled"] or 0)),
            created_by=str(row["created_by"] or ""),
            last_job_id=str(row["last_job_id"] or ""),
            last_status=str(row["last_status"] or ""),
            last_error=str(row["last_error"] or ""),
            last_scan_at=str(row["last_scan_at"] or ""),
            stats=dict(stats or {}),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
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
