from __future__ import annotations

import logging
import mimetypes
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

from ..embedding_collections import resolve_collection_name_from_config
from .models import CloudDriveJob, CloudDriveStats, CloudDriveStorageHealth
from .registry import CloudDriveRegistryDB
from .storage import StorageAdapter, compute_file_checksum, guess_mime_type, resolve_storage_adapter

INDEXABLE_EXTENSIONS = {
    ".doc",
    ".docx",
    ".xlsx",
    ".xls",
    ".pdf",
    ".pptx",
    ".rtf",
    ".txt",
    ".csv",
    ".zip",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".tif",
    ".tiff",
    ".bmp",
    ".webp",
}


class CloudDriveJobCancelled(RuntimeError):
    pass


class CloudDriveService:
    def __init__(self, *, registry: CloudDriveRegistryDB, storage: StorageAdapter) -> None:
        self.registry = registry
        self.storage = storage

    @classmethod
    def from_config(cls, config: Dict[str, object]) -> 'CloudDriveService':
        db_path = str(config.get('cloud_drive_db_path') or '').strip()
        if not db_path:
            raise RuntimeError('Не задан cloud_drive_db_path.')
        return cls(
            registry=CloudDriveRegistryDB(db_path),
            storage=resolve_storage_adapter(config),
        )

    def create_bootstrap_job(self, *, catalog_root: str, max_files: Optional[int] = None, import_files: bool = False) -> CloudDriveJob:
        return self.registry.queue_job(
            job_type='bootstrap',
            status='pending',
            payload={
                'catalog_root': str(catalog_root),
                'max_files': max_files,
                'import_files': bool(import_files),
                'progress': {
                    'status': 'pending',
                    'catalog': str(catalog_root),
                    'import_files': bool(import_files),
                    'limit_value': int(max_files or 0),
                    'total_files': 0,
                    'imported_files': 0,
                    'imported_folders': 0,
                    'current_path': '',
                },
            },
        )

    def get_latest_bootstrap_job(self) -> Optional[CloudDriveJob]:
        return self.registry.get_latest_job(job_type='bootstrap')

    def list_bootstrap_jobs(self, *, limit: int = 20) -> list[CloudDriveJob]:
        return self.registry.list_jobs(job_type='bootstrap', limit=limit)

    def get_job(self, job_id: str) -> Optional[CloudDriveJob]:
        return self.registry.get_job(job_id)

    def get_latest_job(self, *, job_type: str) -> Optional[CloudDriveJob]:
        return self.registry.get_latest_job(job_type=job_type)

    def list_jobs(self, *, job_type: str = '', limit: int = 20) -> list[CloudDriveJob]:
        return self.registry.list_jobs(job_type=(job_type or None), limit=limit)

    def recover_stale_jobs(
        self,
        *,
        job_types: Optional[list[str]] = None,
        lease_timeout_seconds: int = 3600,
        limit: int = 100,
    ) -> int:
        return self.registry.recover_stale_jobs(
            job_types=job_types,
            lease_timeout_seconds=lease_timeout_seconds,
            limit=limit,
        )

    def grant_permission(
        self,
        *,
        subject_type: str,
        subject_id: str,
        resource_type: str,
        resource_id: str,
        access_level: str = "viewer",
    ) -> Dict[str, str]:
        return self.registry.grant_permission(
            subject_type=subject_type,
            subject_id=subject_id,
            resource_type=resource_type,
            resource_id=resource_id,
            access_level=access_level,
        )

    def grant_path_permission(
        self,
        *,
        subject_type: str,
        subject_id: str,
        path: str,
        access_level: str = "viewer",
    ) -> Dict[str, str]:
        clean_path = str(path or "").strip().replace("\\", "/").strip("/")
        node = self.registry.get_node_by_path(clean_path)
        if node is not None:
            resource_type = "folder" if hasattr(node, "is_root") else "file"
            resource_id = str(node.id)
        else:
            resource_type = "path"
            resource_id = clean_path or "*"
        return self.grant_permission(
            subject_type=subject_type,
            subject_id=subject_id,
            resource_type=resource_type,
            resource_id=resource_id,
            access_level=access_level,
        )

    def user_can_access(
        self,
        *,
        username: str,
        role: str = "",
        path: str = "",
        file_id: str = "",
        required_level: str = "viewer",
    ) -> bool:
        return self.registry.user_can_access(
            username=username,
            role=role,
            path=path,
            file_id=file_id,
            required_level=required_level,
        )

    def get_storage_health(self) -> CloudDriveStorageHealth:
        result = dict(self.storage.healthcheck())
        return CloudDriveStorageHealth(
            backend=str(result.get('backend') or ''),
            ok=bool(result.get('ok')),
            writable=bool(result.get('writable')),
            target=str(result.get('target') or ''),
            error=str(result.get('error') or ''),
        )

    def ensure_storage_container(self) -> dict:
        ensure = getattr(self.storage, 'ensure_container', None)
        if callable(ensure):
            return dict(ensure())
        return dict(self.storage.healthcheck())

    def get_exact_storage_coverage(self) -> dict:
        """Exact coverage: compare all registry keys against all storage keys.

        For S3/MinIO uses list_objects_v2 (one paginated call, much faster than
        per-file HEAD requests). For local storage walks the directory once.
        Returns exact counts — no sampling.
        """
        db_keys = self.registry.all_storage_keys()
        storage_keys = self.storage.list_keys()
        missing = db_keys - storage_keys
        present = db_keys & storage_keys
        return {
            "registry_keys": len(db_keys),
            "storage_keys": len(storage_keys),
            "present": len(present),
            "missing": len(missing),
            "missing_examples": list(missing)[:5],
            "ok": not missing,
        }

    def get_storage_coverage(self, *, sample_limit: int = 25) -> dict:
        """Check whether registry objects are present in the configured storage backend.

        This is intentionally sample-based: S3/MinIO HEAD requests across the full
        registry can be slow and expensive, while a small recent sample is enough
        to detect the common misconfiguration after switching storage backends.
        """
        sample = self.registry.sample_storage_objects(limit=sample_limit)
        missing: list[dict] = []
        checked = 0
        for item in sample:
            storage_key = str(item.get("storage_key") or "")
            if not storage_key:
                continue
            checked += 1
            if not self.storage.exists(storage_key):
                missing.append(item)
        return {
            "checked": checked,
            "missing": len(missing),
            "sample_limit": max(1, min(int(sample_limit or 25), 500)),
            "missing_examples": missing[:5],
            "ok": checked == 0 or not missing,
            "needs_backfill": bool(missing),
        }

    def get_index_coverage(self, *, index_state_db_path: str, sample_limit: int = 25) -> dict:
        files = self.registry.list_active_file_index_records()
        sample_size = max(1, min(int(sample_limit or 25), 500))
        index_path = Path(str(index_state_db_path or "")).expanduser()
        index_available, indexed_by_file, indexed_by_path = self._load_index_coverage_state(index_path)

        missing: list[dict[str, Any]] = []
        stale: list[dict[str, Any]] = []
        errored: list[dict[str, Any]] = []
        unsupported_missing: list[dict[str, Any]] = []
        indexable_missing: list[dict[str, Any]] = []
        indexable_stale: list[dict[str, Any]] = []
        indexable_errored: list[dict[str, Any]] = []
        indexed_current = 0
        indexable_total = 0
        indexable_indexed_current = 0
        for file_row in files:
            is_indexable = self._is_indexable_registry_file(file_row)
            if is_indexable:
                indexable_total += 1
            reason, best, legacy_path_match = self._classify_index_coverage_file(
                file_row,
                indexed_by_file=indexed_by_file,
                indexed_by_path=indexed_by_path,
            )
            if reason == "missing":
                missing.append(file_row)
                if is_indexable:
                    indexable_missing.append(file_row)
                else:
                    unsupported_missing.append(file_row)
                continue
            if reason == "error":
                error_row = {**file_row, "last_error": str(best.get("last_error") or "")}
                errored.append(error_row)
                if is_indexable:
                    indexable_errored.append(error_row)
                continue
            if reason == "stale":
                stale_row = {
                    **file_row,
                    "indexed_version_id": str(best.get("cloud_version_id") or ""),
                    "indexed_path": str(best.get("full_path") or ""),
                }
                stale.append(stale_row)
                if is_indexable:
                    indexable_stale.append(stale_row)
                continue
            indexed_current += 1
            if is_indexable:
                indexable_indexed_current += 1

        total = len(files)
        coverage_pct = round((indexed_current / total) * 100, 2) if total else 100.0
        indexable_coverage_pct = (
            round((indexable_indexed_current / indexable_total) * 100, 2)
            if indexable_total
            else 100.0
        )
        return {
            "index_state_db_path": str(index_path),
            "index_available": index_available,
            "registry_files": total,
            "indexed_current": indexed_current,
            "indexable_registry_files": indexable_total,
            "indexable_indexed_current": indexable_indexed_current,
            "indexable_missing": len(indexable_missing),
            "indexable_stale": len(indexable_stale),
            "indexable_errored": len(indexable_errored),
            "unsupported_missing": len(unsupported_missing),
            "missing": len(missing),
            "stale": len(stale),
            "errored": len(errored),
            "coverage_pct": coverage_pct,
            "indexable_coverage_pct": indexable_coverage_pct,
            "ok": (
                index_available
                and not indexable_missing
                and not indexable_stale
                and not indexable_errored
            ),
            "sample_limit": sample_size,
            "missing_examples": missing[:sample_size],
            "indexable_missing_examples": indexable_missing[:sample_size],
            "unsupported_missing_examples": unsupported_missing[:sample_size],
            "stale_examples": stale[:sample_size],
            "error_examples": errored[:sample_size],
        }

    def enqueue_index_coverage_repair(
        self,
        *,
        index_state_db_path: str,
        scopes: str | list[str] = "missing,stale,error",
        limit: int = 100,
    ) -> dict[str, Any]:
        requested = {
            str(item or "").strip().lower()
            for item in (scopes.split(",") if isinstance(scopes, str) else scopes)
            if str(item or "").strip()
        }
        allowed = {"missing", "stale", "error"}
        clean_scopes = requested & allowed
        if not clean_scopes:
            raise RuntimeError(f"Не задан scope repair. Допустимо: {', '.join(sorted(allowed))}")
        clean_limit = max(1, min(int(limit or 100), 1000))
        index_path = Path(str(index_state_db_path or "")).expanduser()
        index_available, indexed_by_file, indexed_by_path = self._load_index_coverage_state(index_path)
        if not index_available:
            raise RuntimeError(f"Index state DB недоступна: {index_path}")

        candidates: list[tuple[dict[str, Any], str]] = []
        for file_row in self.registry.list_active_file_index_records():
            if not self._is_indexable_registry_file(file_row):
                continue
            reason, _best, _legacy_path_match = self._classify_index_coverage_file(
                file_row,
                indexed_by_file=indexed_by_file,
                indexed_by_path=indexed_by_path,
            )
            if reason in clean_scopes:
                candidates.append((file_row, reason))

        latest_jobs: dict[str, CloudDriveJob] = {}
        candidate_ids = [str(row.get("id") or "") for row, _reason in candidates]
        for offset in range(0, len(candidate_ids), 500):
            latest_jobs.update(
                self.registry.list_latest_jobs_for_files(
                    candidate_ids[offset:offset + 500],
                    job_types=["reindex"],
                )
            )
        queued: list[dict[str, str]] = []
        skipped_existing = 0
        skipped_missing_file = 0
        for file_row, reason in candidates:
            if len(queued) >= clean_limit:
                break
            file_id = str(file_row.get("id") or "")
            latest = latest_jobs.get(file_id)
            if latest is not None and latest.status in {"pending", "running"}:
                skipped_existing += 1
                continue
            registry_file = self.registry.get_file_by_id(file_id)
            if registry_file is None:
                skipped_missing_file += 1
                continue
            job = self._queue_reindex_file(registry_file, reason=f"coverage_{reason}")
            queued.append({"job_id": job.id, "file_id": file_id, "path": registry_file.path, "reason": reason})

        return {
            "ok": True,
            "index_state_db_path": str(index_path),
            "scopes": sorted(clean_scopes),
            "limit": clean_limit,
            "candidates": len(candidates),
            "queued": len(queued),
            "skipped_existing": skipped_existing,
            "skipped_missing_file": skipped_missing_file,
            "jobs": queued,
        }

    @staticmethod
    def _classify_index_coverage_file(
        file_row: dict[str, Any],
        *,
        indexed_by_file: dict[str, list[dict[str, Any]]],
        indexed_by_path: dict[str, dict[str, Any]],
    ) -> tuple[str, dict[str, Any], bool]:
        file_id = str(file_row.get("id") or "")
        current_version = str(file_row.get("current_version_id") or "")
        rows = indexed_by_file.get(file_id) or []
        current_rows = [
            row for row in rows
            if str(row.get("cloud_version_id") or "") == current_version
        ]
        best = current_rows[0] if current_rows else (rows[0] if rows else None)
        legacy_path_match = False
        if best is None:
            source_key = CloudDriveService._index_coverage_path_key(str(file_row.get("source_path") or ""))
            best = indexed_by_path.get(source_key) if source_key else None
            legacy_path_match = best is not None
        if best is None:
            return "missing", {}, False
        if str(best.get("status") or "") == "error":
            return "error", best, legacy_path_match
        if not current_rows and not legacy_path_match:
            return "stale", best, legacy_path_match
        return "current", best, legacy_path_match

    @staticmethod
    def _load_index_coverage_state(index_path: Path) -> tuple[bool, dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
        indexed_by_file: dict[str, list[dict[str, Any]]] = {}
        indexed_by_path: dict[str, dict[str, Any]] = {}
        index_available = index_path.is_file()
        if index_available:
            try:
                with sqlite3.connect(str(index_path)) as conn:
                    conn.row_factory = sqlite3.Row
                    rows = conn.execute(
                        """
                        SELECT full_path, cloud_file_id, cloud_version_id, cloud_path,
                               indexed_stage, status, last_error, updated_at
                        FROM state_entries
                        """
                    ).fetchall()
                for row in rows:
                    row_dict = dict(row)
                    full_path_key = CloudDriveService._index_coverage_path_key(str(row["full_path"] or ""))
                    if full_path_key and full_path_key not in indexed_by_path:
                        indexed_by_path[full_path_key] = row_dict
                    file_id = str(row["cloud_file_id"] or "")
                    if not file_id:
                        continue
                    indexed_by_file.setdefault(file_id, []).append(row_dict)
            except sqlite3.Error:
                index_available = False
        return index_available, indexed_by_file, indexed_by_path

    @staticmethod
    def _is_indexable_registry_file(file_row: dict[str, Any]) -> bool:
        path = Path(str(file_row.get("path") or file_row.get("name") or ""))
        if path.name.startswith("~$"):
            return False
        ext = path.suffix.lower()
        return ext in INDEXABLE_EXTENSIONS

    @staticmethod
    def _index_coverage_path_key(path: str) -> str:
        clean = str(path or "").strip().replace("\\", "/")
        while "//" in clean:
            clean = clean.replace("//", "/")
        return clean.lower()

    @staticmethod
    def _immutable_storage_key(*, checksum: str, filename: str) -> str:
        clean_checksum = str(checksum or '').strip().lower()
        if not clean_checksum:
            raise RuntimeError('Не удалось вычислить checksum файла.')
        suffix = Path(str(filename or '')).suffix.lower()
        return f'objects/sha256/{clean_checksum[:2]}/{clean_checksum[2:4]}/{clean_checksum}{suffix}'

    def get_node(self, path: str = '') -> dict:
        clean_path = str(path or '').strip().replace('\\', '/').strip('/')
        if not clean_path:
            folder = self.registry.get_root_folder()
            if folder is None:
                raise RuntimeError('Cloud Drive registry ещё не инициализирован.')
            return {
                'node_type': 'folder',
                'id': folder.id,
                'name': folder.name,
                'path': folder.path,
                'source_path': folder.source_path,
                'depth': folder.depth,
                'is_root': folder.is_root,
                'created_at': folder.created_at,
                'updated_at': folder.updated_at,
            }
        node = self.registry.get_node_by_path(clean_path)
        if node is None:
            raise RuntimeError(f'Узел не найден: {clean_path}')
        if hasattr(node, 'folder_id'):
            return {
                'node_type': 'file',
                'id': node.id,
                'folder_id': node.folder_id,
                'name': node.name,
                'path': node.path,
                'storage_key': node.storage_key,
                'mime_type': node.mime_type,
                'size_bytes': node.size_bytes,
                'checksum': node.checksum,
                'source_path': node.source_path,
                'current_version_id': node.current_version_id,
                'created_at': node.created_at,
                'updated_at': node.updated_at,
                'deleted_at': node.deleted_at,
            }
        return {
            'node_type': 'folder',
            'id': node.id,
            'name': node.name,
            'path': node.path,
            'source_path': node.source_path,
            'depth': node.depth,
            'is_root': node.is_root,
            'created_at': node.created_at,
            'updated_at': node.updated_at,
        }

    def list_directory(self, path: str = '') -> dict:
        folder_data = self.get_node(path)
        if folder_data['node_type'] != 'folder':
            raise RuntimeError(f'Путь не является каталогом: {path}')
        folder = self.registry.get_root_folder() if not folder_data['path'] else self.registry.get_folder_by_path(folder_data['path'])
        if folder is None:
            raise RuntimeError(f'Каталог не найден: {path}')
        folders = self.registry.list_child_folders(folder.id)
        files = self.registry.list_files_in_folder(folder.id)
        return {
            'folder': folder_data,
            'folders': [
                {
                    'node_type': 'folder',
                    'id': item.id,
                    'name': item.name,
                    'path': item.path,
                    'source_path': item.source_path,
                    'depth': item.depth,
                    'is_root': item.is_root,
                    'created_at': item.created_at,
                    'updated_at': item.updated_at,
                }
                for item in folders
            ],
            'files': [
                {
                    'node_type': 'file',
                    'id': item.id,
                    'folder_id': item.folder_id,
                    'name': item.name,
                    'path': item.path,
                    'storage_key': item.storage_key,
                    'mime_type': item.mime_type,
                    'size_bytes': item.size_bytes,
                    'checksum': item.checksum,
                    'source_path': item.source_path,
                    'current_version_id': item.current_version_id,
                    'created_at': item.created_at,
                    'updated_at': item.updated_at,
                    'deleted_at': item.deleted_at,
                }
                for item in files
            ],
        }

    def search_nodes(self, *, query: str, path: str = '', limit: int = 50) -> dict:
        clean_query = str(query or '').strip()
        if not clean_query:
            return {'query': '', 'path': str(path or ''), 'items': [], 'count': 0}
        clean_path = str(path or '').strip().replace('\\', '/').strip('/')
        if clean_path and self.registry.get_folder_by_path(clean_path) is None:
            raise RuntimeError(f'Каталог не найден: {path}')
        items = self.registry.search_nodes(query=clean_query, path=clean_path, limit=limit)
        return {
            'query': clean_query,
            'path': clean_path,
            'items': items,
            'count': len(items),
        }

    def list_changes(self, *, since: str = '', limit: int = 500) -> dict:
        changes = self.registry.list_changes(since=since, limit=limit)
        next_cursor = changes[-1]['updated_at'] if changes else str(since or '')
        return {
            'since': str(since or ''),
            'next_cursor': next_cursor,
            'changes': changes,
        }

    def register_sync_client(
        self,
        *,
        username: str,
        device_id: str,
        display_name: str = '',
        platform: str = '',
        status: str = 'online',
        metadata: Optional[Dict[str, Any]] = None,
    ) -> dict:
        return self._sync_client_to_dict(
            self.registry.register_sync_client(
                username=username,
                device_id=device_id,
                display_name=display_name,
                platform=platform,
                status=status,
                metadata=metadata or {},
            )
        )

    def list_sync_clients(self, *, username: str = '', include_offline: bool = True, limit: int = 100) -> list[dict]:
        return [
            self._sync_client_to_dict(client)
            for client in self.registry.list_sync_clients(
                username=username,
                include_offline=include_offline,
                limit=limit,
            )
        ]

    def upsert_sync_pair(
        self,
        *,
        client_id: str,
        local_path: str,
        cloud_path: str = '',
        conflict_policy: str = 'ask',
        enabled: bool = True,
    ) -> dict:
        return self._sync_pair_to_dict(
            self.registry.upsert_sync_pair(
                client_id=client_id,
                local_path=local_path,
                cloud_path=cloud_path,
                conflict_policy=conflict_policy,
                enabled=enabled,
            )
        )

    def list_sync_pairs(self, *, username: str = '', client_id: str = '', enabled_only: bool = False) -> list[dict]:
        return [
            self._sync_pair_to_dict(pair)
            for pair in self.registry.list_sync_pairs(
                username=username,
                client_id=client_id,
                enabled_only=enabled_only,
            )
        ]

    def delete_sync_pair(self, pair_id: str, *, client_id: str = '') -> dict:
        return {'ok': self.registry.delete_sync_pair(pair_id, client_id=client_id)}

    def set_selective_sync_paths(
        self,
        *,
        client_id: str,
        paths: list[str],
        mode: str = 'exclude',
        replace: bool = True,
    ) -> dict:
        items = self.registry.set_selective_sync_paths(
            client_id=client_id,
            paths=paths,
            mode=mode,
            replace=replace,
        )
        return {'client_id': str(client_id), 'mode': str(mode or 'exclude'), 'paths': items, 'count': len(items)}

    def list_selective_sync_paths(self, *, username: str = '', client_id: str = '') -> dict:
        items = self.registry.list_selective_sync_paths(username=username, client_id=client_id)
        return {'client_id': str(client_id or ''), 'paths': items, 'count': len(items)}

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
    ) -> dict:
        return self._sync_conflict_to_dict(
            self.registry.record_sync_conflict(
                client_id=client_id,
                pair_id=pair_id,
                path=path,
                local_path=local_path,
                cloud_path=cloud_path,
                conflict_type=conflict_type,
                local_version=local_version,
                cloud_version=cloud_version,
                details=details or {},
            )
        )

    def list_sync_conflicts(
        self,
        *,
        username: str = '',
        client_id: str = '',
        status: str = 'open',
        limit: int = 100,
    ) -> list[dict]:
        return [
            self._sync_conflict_to_dict(conflict)
            for conflict in self.registry.list_sync_conflicts(
                username=username,
                client_id=client_id,
                status=status,
                limit=limit,
            )
        ]

    def resolve_sync_conflict(self, conflict_id: str, *, resolution: str, resolved_by: str = '') -> dict:
        return self._sync_conflict_to_dict(
            self.registry.resolve_sync_conflict(
                conflict_id,
                resolution=resolution,
                resolved_by=resolved_by,
            )
        )

    def create_folder(self, *, parent_path: str = '', name: str) -> dict:
        folder = self.registry.create_folder(parent_path=parent_path, name=name)
        return {
            'node_type': 'folder',
            'id': folder.id,
            'name': folder.name,
            'path': folder.path,
            'source_path': folder.source_path,
            'depth': folder.depth,
            'is_root': folder.is_root,
            'created_at': folder.created_at,
            'updated_at': folder.updated_at,
        }

    @staticmethod
    def _sync_client_to_dict(client: Any) -> dict:
        return {
            'id': client.id,
            'username': client.username,
            'device_id': client.device_id,
            'display_name': client.display_name,
            'platform': client.platform,
            'status': client.status,
            'last_seen_at': client.last_seen_at,
            'metadata': dict(client.metadata or {}),
            'created_at': client.created_at,
            'updated_at': client.updated_at,
        }

    @staticmethod
    def _sync_pair_to_dict(pair: Any) -> dict:
        return {
            'id': pair.id,
            'client_id': pair.client_id,
            'username': pair.username,
            'local_path': pair.local_path,
            'cloud_path': pair.cloud_path,
            'conflict_policy': pair.conflict_policy,
            'enabled': bool(pair.enabled),
            'created_at': pair.created_at,
            'updated_at': pair.updated_at,
        }

    @staticmethod
    def _sync_conflict_to_dict(conflict: Any) -> dict:
        return {
            'id': conflict.id,
            'client_id': conflict.client_id,
            'pair_id': conflict.pair_id,
            'username': conflict.username,
            'path': conflict.path,
            'local_path': conflict.local_path,
            'cloud_path': conflict.cloud_path,
            'conflict_type': conflict.conflict_type,
            'local_version': conflict.local_version,
            'cloud_version': conflict.cloud_version,
            'status': conflict.status,
            'resolution': conflict.resolution,
            'details': dict(conflict.details or {}),
            'resolved_by': conflict.resolved_by,
            'resolved_at': conflict.resolved_at,
            'created_at': conflict.created_at,
            'updated_at': conflict.updated_at,
        }

    def get_download_descriptor(self, path: str) -> dict:
        node = self.registry.get_file_by_path(str(path or '').strip().replace('\\', '/').strip('/'))
        if node is None:
            raise RuntimeError(f'Файл не найден: {path}')
        presign = getattr(self.storage, 'presigned_download_url', None)
        if callable(presign):
            if not self.storage.exists(node.storage_key):
                raise RuntimeError(f'Файл отсутствует в storage backend: {node.storage_key}')
            return {
                'mode': 'redirect_url',
                'url': str(presign(node.storage_key, expires_in=3600)),
                'filename': node.name,
                'mime_type': node.mime_type or mimetypes.guess_type(node.name)[0] or 'application/octet-stream',
                'size_bytes': node.size_bytes,
                'storage_key': node.storage_key,
                'path': node.path,
            }
        storage_path = Path(self.storage.resolve_path(node.storage_key))
        if not storage_path.exists() or not storage_path.is_file():
            raise RuntimeError(f'Файл отсутствует в storage backend: {node.storage_key}')
        mime_type = node.mime_type or mimetypes.guess_type(node.name)[0] or 'application/octet-stream'
        return {
            'mode': 'local_file',
            'file_path': str(storage_path),
            'filename': node.name,
            'mime_type': mime_type,
            'size_bytes': node.size_bytes,
            'storage_key': node.storage_key,
            'path': node.path,
        }

    def upload_file(
        self,
        *,
        parent_path: str = '',
        filename: str,
        source_path: str,
        mime_type: str = '',
    ) -> dict:
        clean_name = str(filename or '').strip().strip('/\\')
        if not clean_name:
            raise RuntimeError('Не задано имя файла.')
        clean_parent = str(parent_path or '').strip().replace('\\', '/').strip('/')
        parent = self.registry.get_root_folder() if not clean_parent else self.registry.get_folder_by_path(clean_parent)
        if parent is None:
            raise RuntimeError(f'Родительский каталог не найден: {clean_parent or "/"}')
        source = Path(source_path)
        if not source.exists() or not source.is_file():
            raise RuntimeError(f'Временный файл не найден: {source}')
        target_path = f'{clean_parent}/{clean_name}' if clean_parent else clean_name
        actual_mime = str(mime_type or '').strip() or guess_mime_type(source)
        checksum = compute_file_checksum(source)
        storage_key = self._immutable_storage_key(checksum=checksum, filename=clean_name)
        if not self.storage.exists(storage_key):
            self.storage.put_file(source, storage_key)
        file_row = self.registry.upsert_file(
            folder_id=parent.id,
            path=target_path,
            name=clean_name,
            storage_key=storage_key,
            mime_type=actual_mime,
            size_bytes=int(source.stat().st_size),
            checksum=checksum,
            source_path=str(Path(parent.source_path) / clean_name) if parent.source_path else '',
        )
        self._queue_reindex_file(file_row, reason='upload')
        return {
            'node_type': 'file',
            'id': file_row.id,
            'folder_id': file_row.folder_id,
            'name': file_row.name,
            'path': file_row.path,
            'storage_key': file_row.storage_key,
            'mime_type': file_row.mime_type,
            'size_bytes': file_row.size_bytes,
            'checksum': file_row.checksum,
            'source_path': file_row.source_path,
            'current_version_id': file_row.current_version_id,
            'created_at': file_row.created_at,
            'updated_at': file_row.updated_at,
            'deleted_at': file_row.deleted_at,
        }

    def list_versions(self, path: str) -> dict:
        versions = self.registry.list_file_versions(path=path)
        file_row = self.registry.get_file_by_path(path)
        if file_row is None:
            raise RuntimeError(f'Файл не найден: {path}')
        return {
            'file': {
                'id': file_row.id,
                'name': file_row.name,
                'path': file_row.path,
                'current_version_id': file_row.current_version_id,
                'size_bytes': file_row.size_bytes,
                'mime_type': file_row.mime_type,
                'updated_at': file_row.updated_at,
            },
            'versions': versions,
        }

    def move_node(self, *, source_path: str, dest_parent_path: str = '', new_name: str = '') -> dict:
        source_node = self.registry.get_node_by_path(source_path)
        if source_node is None:
            raise RuntimeError(f'Узел не найден: {source_path}')
        if hasattr(source_node, 'folder_id'):
            file_row = self.registry.rename_move_file(
                source_path=source_path,
                dest_parent_path=dest_parent_path,
                new_name=new_name,
            )
            if file_row.path != source_node.path:
                self._queue_cleanup_file(source_node, reason='move', path=source_node.path)
            self._queue_reindex_file(file_row, reason='move')
            return {
                'node_type': 'file',
                'id': file_row.id,
                'folder_id': file_row.folder_id,
                'name': file_row.name,
                'path': file_row.path,
                'storage_key': file_row.storage_key,
                'mime_type': file_row.mime_type,
                'size_bytes': file_row.size_bytes,
                'checksum': file_row.checksum,
                'source_path': file_row.source_path,
                'current_version_id': file_row.current_version_id,
                'created_at': file_row.created_at,
                'updated_at': file_row.updated_at,
                'deleted_at': file_row.deleted_at,
            }
        old_prefix = source_node.path
        target_name = str(new_name or source_node.name).strip()
        clean_parent = str(dest_parent_path or '').strip().replace('\\', '/').strip('/')
        new_prefix = f'{clean_parent}/{target_name}' if clean_parent else target_name
        folder = self.registry.rename_move_folder(
            source_path=source_path,
            dest_parent_path=dest_parent_path,
            new_name=new_name,
        )
        if new_prefix != old_prefix:
            for child in self.registry.list_files_under_path(folder.path):
                suffix = child.path[len(folder.path):].lstrip('/')
                old_path = f'{old_prefix}/{suffix}' if suffix else old_prefix
                self._queue_cleanup_file(child, reason='move_folder', path=old_path)
                self._queue_reindex_file(child, reason='move_folder')
        return {
            'node_type': 'folder',
            'id': folder.id,
            'name': folder.name,
            'path': folder.path,
            'source_path': folder.source_path,
            'depth': folder.depth,
            'is_root': folder.is_root,
            'created_at': folder.created_at,
            'updated_at': folder.updated_at,
            'deleted_at': folder.deleted_at,
        }

    def delete_node(self, path: str) -> dict:
        source_node = self.registry.get_node_by_path(path)
        if source_node is None:
            raise RuntimeError(f'Узел не найден: {path}')
        if hasattr(source_node, 'folder_id'):
            self._queue_cleanup_file(source_node, reason='delete', path=source_node.path)
            file_row = self.registry.delete_file(path)
            return {
                'node_type': 'file',
                'id': file_row.id,
                'path': file_row.path,
                'deleted_at': file_row.deleted_at,
            }
        for child in self.registry.list_files_under_path(source_node.path):
            self._queue_cleanup_file(child, reason='delete_folder', path=child.path)
        folder = self.registry.delete_folder(path)
        return {
            'node_type': 'folder',
            'id': folder.id,
            'path': folder.path,
            'deleted': True,
            'deleted_at': folder.deleted_at,
        }

    def restore_node(self, path: str) -> dict:
        clean_path = str(path or '').strip().replace('\\', '/').strip('/')
        file_row = self.registry.get_file_by_path(clean_path)
        if file_row is not None and file_row.deleted_at:
            restored = self.registry.restore_file(clean_path)
            self._queue_reindex_file(restored, reason='restore')
            return {
                'node_type': 'file',
                'id': restored.id,
                'path': restored.path,
                'deleted_at': restored.deleted_at,
            }
        folder = self.registry.get_folder_by_path(clean_path, include_deleted=True)
        if folder is not None and folder.deleted_at:
            restored_folder = self.registry.restore_folder(clean_path)
            for child in self.registry.list_files_under_path(restored_folder.path):
                self._queue_reindex_file(child, reason='restore_folder')
            return {
                'node_type': 'folder',
                'id': restored_folder.id,
                'path': restored_folder.path,
                'deleted_at': restored_folder.deleted_at,
            }
        raise RuntimeError(f'Удалённый узел не найден: {path}')

    def list_trash(self, *, limit: int = 200) -> dict:
        items = self.registry.list_deleted_nodes(limit=limit)
        return {
            'items': items,
            'count': len(items),
        }

    def cancel_job(self, job_id: str) -> CloudDriveJob:
        job = self.registry.get_job(job_id)
        if job is None:
            raise RuntimeError(f'Job не найден: {job_id}')
        if job.job_type != 'bootstrap':
            raise RuntimeError(f'Cancel пока поддержан только для bootstrap jobs: {job.job_type}')
        progress = dict(job.progress or {})
        progress['cancel_requested'] = True
        if job.status == 'pending':
            progress['status'] = 'cancelled'
            progress['finished_at'] = datetime.now(timezone.utc).isoformat()
            return self.registry.update_job(job_id, status='cancelled', payload={'progress': progress}, last_error='cancelled_by_user')
        return self.registry.update_job(job_id, payload={'progress': progress})

    def retry_bootstrap_job(self, job_id: str) -> CloudDriveJob:
        job = self.registry.get_job(job_id)
        if job is None:
            raise RuntimeError(f'Job не найден: {job_id}')
        if job.job_type != 'bootstrap':
            raise RuntimeError(f'Retry пока поддержан только для bootstrap jobs: {job.job_type}')
        payload = dict(job.payload or {})
        return self.create_bootstrap_job(
            catalog_root=str(payload.get('catalog_root') or ''),
            max_files=payload.get('max_files'),
            import_files=bool(payload.get('import_files')),
        )

    def retry_job(self, job_id: str) -> CloudDriveJob:
        job = self.registry.get_job(job_id)
        if job is None:
            raise RuntimeError(f'Job не найден: {job_id}')
        if job.job_type == 'bootstrap':
            return self.retry_bootstrap_job(job_id)
        if job.job_type not in {'reindex', 'cleanup'}:
            raise RuntimeError(f'Retry не поддержан для job_type={job.job_type}')
        payload = dict(job.payload or {})
        progress = dict(payload.get('progress') or {})
        progress.update(
            {
                'status': 'pending',
                'retried_from_job_id': job.id,
                'queued_at': datetime.now(timezone.utc).isoformat(),
            }
        )
        payload['progress'] = progress
        payload['retried_from_job_id'] = job.id
        return self.registry.queue_job(
            job_type=job.job_type,
            file_id=job.file_id,
            version_id=job.version_id,
            payload=payload,
        )

    def recover_bootstrap_jobs(self) -> int:
        recovered = 0
        for job in self.list_bootstrap_jobs(limit=100):
            if job.status not in {'running', 'pending'}:
                continue
            progress = dict(job.progress or {})
            progress['status'] = 'stale'
            progress['error'] = 'server_restart_recovery'
            progress['finished_at'] = datetime.now(timezone.utc).isoformat()
            self.registry.update_job(job.id, status='failed', payload={'progress': progress}, last_error='server_restart_recovery')
            recovered += 1
        return recovered

    def run_reindex_job(self, job_id: str, *, index_config: Optional[Dict[str, object]] = None) -> CloudDriveJob:
        job = self.registry.get_job(job_id)
        if job is None:
            raise RuntimeError(f'Job не найден: {job_id}')
        if job.job_type not in {'reindex', 'cleanup'}:
            raise RuntimeError(f'run_reindex_job поддерживает только reindex/cleanup jobs: {job.job_type}')

        progress = dict(job.progress or {})
        progress.update(
            {
                'status': 'running',
                'started_at': datetime.now(timezone.utc).isoformat(),
            }
        )
        next_attempts = int(job.attempts or 0) if job.status == 'running' else int(job.attempts or 0) + 1
        self.registry.update_job(job.id, status='running', payload={'progress': progress}, attempts=next_attempts)

        try:
            if job.job_type == 'cleanup':
                deleted_points = 0
                if index_config:
                    deleted_points = self._delete_index_vectors(
                        index_config=index_config,
                        payload_match=self._cleanup_payload_match(job),
                    )
                progress.update(
                    {
                        'status': 'done',
                        'action': 'cleanup',
                        'path': str((job.payload or {}).get('path') or ''),
                        'deleted_points': int(deleted_points),
                        'finished_at': datetime.now(timezone.utc).isoformat(),
                    }
                )
                return self.registry.update_job(job.id, status='completed', payload={'progress': progress})

            file_row = self._resolve_job_file(job)
            if file_row.deleted_at:
                raise RuntimeError(f'Файл удалён и не может быть переиндексирован: {file_row.path}')
            source_path = Path(file_row.source_path) if file_row.source_path else Path()
            storage_path = Path(self.storage.resolve_path(file_row.storage_key))
            source_exists = bool(source_path and source_path.exists() and source_path.is_file())
            storage_exists = storage_path.exists() and storage_path.is_file()
            target_path = source_path if source_exists else storage_path if storage_exists else Path()
            if not target_path:
                raise RuntimeError(f'Файл отсутствует в source_path и storage: {file_row.path}')

            indexed = False
            points_added = 0
            if index_config:
                indexed, points_added = self._run_indexer_for_file(
                    target_path=target_path,
                    file_row=file_row,
                    index_config=index_config,
                )

            progress.update(
                {
                    'status': 'done',
                    'action': 'reindex',
                    'path': file_row.path,
                    'file_id': file_row.id,
                    'version_id': file_row.current_version_id,
                    'source_path': str(source_path) if source_path else '',
                    'storage_key': file_row.storage_key,
                    'storage_path': str(storage_path),
                    'indexed': bool(indexed),
                    'points_added': int(points_added),
                    'finished_at': datetime.now(timezone.utc).isoformat(),
                }
            )
            return self.registry.update_job(job.id, status='completed', payload={'progress': progress})
        except Exception as exc:
            progress.update(
                {
                    'status': 'failed',
                    'error': str(exc),
                    'finished_at': datetime.now(timezone.utc).isoformat(),
                }
            )
            self.registry.update_job(job.id, status='failed', payload={'progress': progress}, last_error=str(exc))
            raise

    def run_pending_reindex_jobs(self, *, index_config: Optional[Dict[str, object]] = None, limit: int = 5) -> list[CloudDriveJob]:
        completed: list[CloudDriveJob] = []
        previous_shared = getattr(self, "_shared_reindex_indexer", None)
        self._shared_reindex_indexer = self._build_shared_reindex_indexer(index_config or {})
        try:
            for _ in range(max(1, int(limit or 1))):
                job = self.registry.claim_pending_job(
                    job_types=['reindex', 'cleanup'],
                    worker_id='nicegui-cloud-worker',
                    lease_seconds=900,
                )
                if job is None:
                    break
                completed.append(self.run_reindex_job(job.id, index_config=index_config))
        finally:
            self._shared_reindex_indexer = previous_shared
        return completed

    def run_bootstrap_job(self, job_id: str) -> CloudDriveStats:
        job = self.registry.get_job(job_id)
        if job is None:
            raise RuntimeError(f'Job не найден: {job_id}')
        payload = dict(job.payload)
        catalog_root = str(payload.get('catalog_root') or '').strip()
        if not catalog_root:
            raise RuntimeError('В bootstrap job не задан catalog_root.')
        max_files_raw = payload.get('max_files')
        max_files = int(max_files_raw) if max_files_raw not in (None, '') else None
        import_files = bool(payload.get('import_files'))
        total_files = self._count_catalog_files(Path(catalog_root), int(max_files or 0))
        self.registry.update_job(
            job_id,
            status='running',
            payload={
                'progress': {
                    'status': 'running',
                    'catalog': catalog_root,
                    'import_files': import_files,
                    'limit_value': int(max_files or 0),
                    'total_files': int(total_files),
                    'imported_files': 0,
                    'imported_folders': 0,
                    'current_path': catalog_root,
                    'started_at': datetime.now(timezone.utc).isoformat(),
                },
            },
        )

        def on_progress(progress_payload: Dict[str, object]) -> None:
            current_job = self.registry.get_job(job_id)
            progress = dict(current_job.progress if current_job else {})
            if progress.get('cancel_requested'):
                raise CloudDriveJobCancelled('cancelled_by_user')
            progress.update(progress_payload)
            progress['status'] = 'running'
            self.registry.update_job(job_id, status='running', payload={'progress': progress})

        def should_continue() -> bool:
            current_job = self.registry.get_job(job_id)
            if current_job is None:
                return False
            return not bool(dict(current_job.progress or {}).get('cancel_requested'))

        _CATALOG_WAIT_SEC = 60
        try:
            while not Path(catalog_root).is_dir():
                current_job = self.registry.get_job(job_id)
                if current_job is None or dict(current_job.progress or {}).get('cancel_requested'):
                    raise CloudDriveJobCancelled('cancelled_by_user')
                logger.warning(
                    "bootstrap: каталог недоступен %s — жду %ds...",
                    catalog_root, _CATALOG_WAIT_SEC,
                )
                self.registry.update_job(
                    job_id, status='running',
                    payload={'progress': {
                        'status': 'waiting_catalog',
                        'catalog': catalog_root,
                        'started_at': datetime.now(timezone.utc).isoformat(),
                    }},
                )
                time.sleep(_CATALOG_WAIT_SEC)

            stats = self.bootstrap_from_catalog(
                catalog_root,
                max_files=max_files,
                import_files=import_files,
                progress_callback=on_progress,
                should_continue=should_continue,
            )
            current_job = self.registry.get_job(job_id)
            progress = dict(current_job.progress if current_job else {})
            progress.update(
                {
                    'status': 'done',
                    'finished_at': datetime.now(timezone.utc).isoformat(),
                }
            )
            self.registry.update_job(job_id, status='completed', payload={'progress': progress})
            return stats
        except CloudDriveJobCancelled as exc:
            current_job = self.registry.get_job(job_id)
            progress = dict(current_job.progress if current_job else {})
            progress.update(
                {
                    'status': 'cancelled',
                    'error': str(exc),
                    'finished_at': datetime.now(timezone.utc).isoformat(),
                }
            )
            self.registry.update_job(job_id, status='cancelled', payload={'progress': progress}, last_error=str(exc))
            raise
        except Exception as exc:
            current_job = self.registry.get_job(job_id)
            progress = dict(current_job.progress if current_job else {})
            progress.update(
                {
                    'status': 'error',
                    'error': str(exc),
                    'finished_at': datetime.now(timezone.utc).isoformat(),
                }
            )
            self.registry.update_job(job_id, status='failed', payload={'progress': progress}, last_error=str(exc))
            raise

    def _count_catalog_files(self, catalog_root: Path, limit_value: int) -> int:
        total = 0
        for _dirpath, _dirnames, filenames in __import__('os').walk(catalog_root):
            total += len(filenames)
            if limit_value > 0 and total >= limit_value:
                return limit_value
        return total

    def bootstrap_from_catalog(
        self,
        catalog_root: str,
        *,
        max_files: Optional[int] = None,
        import_files: bool = False,
        progress_callback: Optional[Callable[[Dict[str, object]], None]] = None,
        should_continue: Optional[Callable[[], bool]] = None,
    ) -> CloudDriveStats:
        root = Path(catalog_root)
        if not root.exists() or not root.is_dir():
            raise RuntimeError(f'Каталог не найден: {root}')
        root_folder = self.registry.ensure_root_folder(root_name=root.name or 'root', source_path=str(root))
        imported = 0
        imported_folders = 1
        folder_cache: Dict[Path, str] = {root: root_folder.id}
        progress_seq = 0

        def emit_progress(kind: str, *, current_path: str = '', done: bool = False) -> None:
            nonlocal progress_seq
            if progress_callback is None:
                return
            progress_seq += 1
            progress_callback(
                {
                    'kind': kind,
                    'done': done,
                    'imported_files': imported,
                    'imported_folders': imported_folders,
                    'current_path': current_path,
                    'import_files': import_files,
                    'max_files': max_files,
                    'sequence': progress_seq,
                }
            )

        emit_progress('start', current_path=str(root))
        skipped_files = 0

        import os as _os  # noqa: PLC0415

        for dirpath, dirnames, filenames in _os.walk(root):
            if should_continue is not None and not should_continue():
                raise CloudDriveJobCancelled('cancelled_by_user')
            base = Path(dirpath)

            # ── Level 1: directory mtime check ──────────────────────────────
            # If the directory's mtime hasn't changed since last scan, no file
            # inside it was added, deleted, or renamed → skip the whole subtree.
            try:
                dir_mtime = base.stat().st_mtime
            except OSError:
                dir_mtime = 0.0

            base_id = folder_cache.get(base)
            if base_id is None:
                rel_base = base.relative_to(root)
                parent = base.parent if base.parent in folder_cache else root
                parent_id = folder_cache[parent]
                base_folder = self.registry.upsert_folder(
                    path=str(rel_base).replace('\\', '/'),
                    name=base.name,
                    parent_id=parent_id,
                    depth=len(rel_base.parts),
                    source_path=str(base),
                )
                base_id = base_folder.id
                folder_cache[base] = base_id
                imported_folders += 1
                if imported_folders % 25 == 0:
                    emit_progress('folder', current_path=str(base))
            else:
                stored_dir_mtime = self.registry.get_folder_source_mtime(base_id)
                if not import_files and stored_dir_mtime != 0.0 and abs(dir_mtime - stored_dir_mtime) < 1e-3:
                    # Directory unchanged — skip all files, but still recurse
                    # into subdirs (os.walk continues naturally; subdir mtimes
                    # are checked individually when we reach them).
                    skipped_files += len(filenames)
                    continue
                # Directory changed — update stored mtime after processing files
                # (done below after the file loop)

            for dirname in sorted(dirnames):
                child = base / dirname
                rel_child = child.relative_to(root)
                folder = self.registry.upsert_folder(
                    path=str(rel_child).replace('\\', '/'),
                    name=dirname,
                    parent_id=base_id,
                    depth=len(rel_child.parts),
                    source_path=str(child),
                )
                folder_cache[child] = folder.id
                imported_folders += 1
                if imported_folders % 25 == 0:
                    emit_progress('folder', current_path=str(child))

            # ── Level 2: file mtime + size check ────────────────────────────
            # Fetch all known mtime/size for files in this folder in one query.
            known_mtimes = self.registry.get_file_mtimes_in_folder(base_id)

            for filename in sorted(filenames):
                if should_continue is not None and not should_continue():
                    raise CloudDriveJobCancelled('cancelled_by_user')
                file_path = base / filename
                rel_file = file_path.relative_to(root)

                try:
                    st = file_path.stat()
                    file_mtime = st.st_mtime
                    file_size = st.st_size
                except OSError:
                    file_mtime, file_size = 0.0, 0

                known = known_mtimes.get(filename)
                if known is not None and not import_files:
                    stored_mtime, stored_size = known
                    if (
                        stored_mtime != 0.0
                        and abs(file_mtime - stored_mtime) < 1e-3
                        and file_size == stored_size
                    ):
                        # File unchanged — skip content read and upsert entirely
                        skipped_files += 1
                        continue

                # File is new or modified — compute checksum and upsert
                try:
                    checksum = compute_file_checksum(file_path)
                    storage_key = self._immutable_storage_key(checksum=checksum, filename=filename)
                    if import_files and not self.storage.exists(storage_key):
                        self.storage.put_file(file_path, storage_key)
                    self.registry.upsert_file(
                        folder_id=base_id,
                        path=str(rel_file).replace('\\', '/'),
                        name=filename,
                        storage_key=storage_key,
                        mime_type=guess_mime_type(file_path),
                        size_bytes=file_size,
                        checksum=checksum,
                        source_path=str(file_path),
                        source_mtime=file_mtime,
                    )
                    imported += 1
                    if imported == 1 or imported % 25 == 0:
                        emit_progress('file', current_path=str(file_path))
                    if max_files is not None and imported >= max_files:
                        emit_progress('done', current_path=str(file_path), done=True)
                        return self.registry.stats()
                except CloudDriveJobCancelled:
                    raise
                except Exception as _file_exc:
                    logger.warning("bootstrap: пропускаю %s — %s", file_path, _file_exc)

            # Update stored dir mtime so next scan can skip this directory if unchanged
            if base_id is not None:
                self.registry.update_folder_mtime(base_id, dir_mtime)
        emit_progress('done', current_path=str(root), done=True)
        return self.registry.stats()

    def enqueue_reindex(self, path: str) -> CloudDriveJob:
        file_row = self.registry.get_file_by_path(path)
        if file_row is None:
            raise RuntimeError(f'Файл не найден в registry: {path}')
        return self._queue_reindex_file(file_row, reason='manual')

    def _queue_reindex_file(self, file_row, *, reason: str) -> CloudDriveJob:
        return self.registry.queue_job(
            job_type='reindex',
            file_id=file_row.id,
            version_id=file_row.current_version_id,
            payload={
                'path': file_row.path,
                'storage_key': file_row.storage_key,
                'source_path': file_row.source_path,
                'reason': reason,
                'progress': {
                    'status': 'pending',
                    'action': 'reindex',
                    'path': file_row.path,
                    'reason': reason,
                },
            },
        )

    def _queue_cleanup_file(self, file_row, *, reason: str, path: str) -> CloudDriveJob:
        return self.registry.queue_job(
            job_type='cleanup',
            file_id=file_row.id,
            version_id=file_row.current_version_id,
            payload={
                'path': path,
                'storage_key': file_row.storage_key,
                'source_path': file_row.source_path,
                'reason': reason,
                'progress': {
                    'status': 'pending',
                    'action': 'cleanup',
                    'path': path,
                    'reason': reason,
                },
            },
        )

    def _resolve_job_file(self, job: CloudDriveJob):
        file_row = self.registry.get_file_by_id(job.file_id) if job.file_id else None
        if file_row is None:
            file_row = self.registry.get_file_by_path(str((job.payload or {}).get('path') or ''))
        if file_row is None:
            raise RuntimeError(f'Файл job не найден: {job.file_id or (job.payload or {}).get("path") or ""}')
        return file_row

    def _cloud_state_key(self, file_row) -> str:
        return f"cloud:{file_row.id}"

    def _cloud_payload(self, file_row) -> Dict[str, Any]:
        return {
            'cloud_file_id': file_row.id,
            'cloud_version_id': file_row.current_version_id,
            'cloud_path': file_row.path,
            'storage_key': file_row.storage_key,
            'source_path': file_row.source_path,
            'source': 'cloud_drive',
        }

    def _cleanup_payload_match(self, job: CloudDriveJob) -> Dict[str, Any]:
        if job.file_id:
            return {'cloud_file_id': job.file_id}
        path = str((job.payload or {}).get('path') or '').strip()
        return {'cloud_path': path} if path else {}

    def _run_indexer_for_file(self, *, target_path: Path, file_row, index_config: Dict[str, object]) -> tuple[bool, int]:
        if not str(index_config.get('qdrant_db_path') or '').strip() and not str(index_config.get('qdrant_url') or '').strip():
            return False, 0
        catalog_root = Path(str(index_config.get('catalog_path') or ''))
        logical_path = file_row.path
        index_root = target_path.parent
        if str(catalog_root).strip() and catalog_root.exists():
            try:
                target_path.resolve().relative_to(catalog_root.resolve())
                index_root = catalog_root
                logical_path = None
            except Exception:
                index_root = target_path.parent
        shared_indexer = getattr(self, "_shared_reindex_indexer", None)
        indexer = (
            shared_indexer
            if shared_indexer is not None and self._indexer_root_matches(shared_indexer, index_root)
            else self._build_reindex_indexer(index_config, index_root)
        )
        before = int(indexer.point_count)
        payload_extra = self._cloud_payload(file_row)
        indexer._delete_file_vectors(target_path, payload_match={'cloud_file_id': file_row.id})
        indexer.process_file(
            target_path,
            logical_path=logical_path,
            state_key=self._cloud_state_key(file_row),
            payload_extra=payload_extra,
            fingerprint_override=str(file_row.checksum or ''),
            delete_payload_match={'cloud_file_id': file_row.id},
        )
        return True, max(0, int(indexer.point_count) - before)

    def _build_shared_reindex_indexer(self, index_config: Dict[str, object]):
        catalog_root = Path(str(index_config.get('catalog_path') or ''))
        if not catalog_root.exists():
            return None
        if not str(index_config.get('qdrant_db_path') or '').strip() and not str(index_config.get('qdrant_url') or '').strip():
            return None
        return self._build_reindex_indexer(index_config, catalog_root)

    @staticmethod
    def _indexer_root_matches(indexer, index_root: Path) -> bool:  # noqa: ANN001
        try:
            return Path(indexer.catalog_path).resolve() == index_root.resolve()
        except Exception:
            return False

    @staticmethod
    def _build_reindex_indexer(index_config: Dict[str, object], index_root: Path):
        from rag_catalog.core.index_rag import RAGIndexer

        return RAGIndexer(
            catalog_path=str(index_root),
            qdrant_db_path=str(index_config.get('qdrant_db_path') or ''),
            embedding_model=str(index_config.get('embedding_model') or 'sentence-transformers/all-MiniLM-L6-v2'),
            collection_name=resolve_collection_name_from_config(index_config),
            vector_size=int(index_config.get('vector_size') or 384),
            chunk_size=int(index_config.get('chunk_size') or 500),
            chunk_overlap=int(index_config.get('chunk_overlap') or 100),
            chunk_group_size=int(index_config.get('chunk_group_size') or 4),
            batch_size=int(index_config.get('batch_size') or index_config.get('index_batch_size') or 64),
            recreate_collection=False,
            skip_ocr=bool(index_config.get('skip_ocr_in_index') or index_config.get('index_skip_ocr')),
            max_chunks_per_file=int(index_config.get('index_max_chunks') or 0),
            read_workers=1,
            use_onnx=bool(index_config.get('use_onnx') or False),
            qdrant_url=str(index_config.get('qdrant_url') or ''),
            telemetry_db_path=str(index_config.get('telemetry_db_path') or ''),
            small_office_mb=float(index_config.get('small_office_mb') or 20.0),
            small_pdf_mb=float(index_config.get('small_pdf_mb') or 50.0),
            synonym_map=dict(index_config.get('synonym_map') or {}),
            ollama_url=str(index_config.get('ollama_url') or 'http://localhost:11434'),
            ocr_tesseract_cmd=str(index_config.get('ocr_tesseract_cmd') or ''),
            ocr_poppler_bin=str(index_config.get('ocr_poppler_bin') or ''),
            qdrant_timeout_sec=int(index_config.get('qdrant_timeout_sec') or 60),
            ocr_max_image_pages=int(index_config.get('ocr_max_image_pages') or 50),
        )

    def _delete_index_vectors(self, *, index_config: Dict[str, object], payload_match: Dict[str, Any]) -> int:
        if not payload_match:
            return 0
        qdrant_db_path = str(index_config.get('qdrant_db_path') or '').strip()
        qdrant_url = str(index_config.get('qdrant_url') or '').strip()
        collection_name = resolve_collection_name_from_config(index_config)
        if not qdrant_db_path and not qdrant_url:
            return 0
        try:
            from qdrant_client import QdrantClient
            from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchValue

            client = QdrantClient(
                url=qdrant_url,
                timeout=int(index_config.get('qdrant_timeout_sec') or 60),
            ) if qdrant_url else QdrantClient(
                path=qdrant_db_path,
                timeout=int(index_config.get('qdrant_timeout_sec') or 60),
            )
            conditions = [
                FieldCondition(key=str(key), match=MatchValue(value=value))
                for key, value in payload_match.items()
                if value not in (None, '')
            ]
            if not conditions:
                return 0
            client.delete(
                collection_name=collection_name,
                wait=False,
                timeout=int(index_config.get('qdrant_timeout_sec') or 60),
                points_selector=FilterSelector(filter=Filter(must=conditions)),
            )
            return -1
        except Exception:
            return 0
