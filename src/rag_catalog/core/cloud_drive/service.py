from __future__ import annotations

import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from ..embedding_collections import resolve_collection_name_from_config
from .models import CloudDriveJob, CloudDriveStats, CloudDriveStorageHealth
from .registry import CloudDriveRegistryDB
from .storage import StorageAdapter, compute_file_checksum, guess_mime_type, resolve_storage_adapter


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

    def get_storage_health(self) -> CloudDriveStorageHealth:
        result = dict(self.storage.healthcheck())
        return CloudDriveStorageHealth(
            backend=str(result.get('backend') or ''),
            ok=bool(result.get('ok')),
            writable=bool(result.get('writable')),
            target=str(result.get('target') or ''),
            error=str(result.get('error') or ''),
        )

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

    def get_download_descriptor(self, path: str) -> dict:
        node = self.registry.get_file_by_path(str(path or '').strip().replace('\\', '/').strip('/'))
        if node is None:
            raise RuntimeError(f'Файл не найден: {path}')
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
        storage_key = target_path
        actual_mime = str(mime_type or '').strip() or guess_mime_type(source)
        checksum = compute_file_checksum(source)
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
            old_storage_key = source_node.storage_key
            target_name = str(new_name or source_node.name).strip()
            clean_parent = str(dest_parent_path or '').strip().replace('\\', '/').strip('/')
            new_storage_key = f'{clean_parent}/{target_name}' if clean_parent else target_name
            if new_storage_key != old_storage_key:
                self.storage.move(old_storage_key, new_storage_key)
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
        for child in self.registry.list_files_under_path(old_prefix):
            suffix = child.path[len(old_prefix):].lstrip('/')
            next_key = f'{new_prefix}/{suffix}' if suffix else new_prefix
            if next_key != child.storage_key:
                self.storage.move(child.storage_key, next_key)
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
        }

    def delete_node(self, path: str) -> dict:
        source_node = self.registry.get_node_by_path(path)
        if source_node is None:
            raise RuntimeError(f'Узел не найден: {path}')
        if hasattr(source_node, 'folder_id'):
            if self.storage.exists(source_node.storage_key):
                self.storage.delete(source_node.storage_key)
            self._queue_cleanup_file(source_node, reason='delete', path=source_node.path)
            file_row = self.registry.delete_file(path)
            return {
                'node_type': 'file',
                'id': file_row.id,
                'path': file_row.path,
                'deleted_at': file_row.deleted_at,
            }
        for child in self.registry.list_files_under_path(source_node.path):
            if self.storage.exists(child.storage_key):
                self.storage.delete(child.storage_key)
            self._queue_cleanup_file(child, reason='delete_folder', path=child.path)
        folder = self.registry.delete_folder(path)
        return {
            'node_type': 'folder',
            'id': folder.id,
            'path': folder.path,
            'deleted': True,
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
        self.registry.update_job(job.id, status='running', payload={'progress': progress})

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

        try:
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

        for dirpath, dirnames, filenames in __import__('os').walk(root):
            if should_continue is not None and not should_continue():
                raise CloudDriveJobCancelled('cancelled_by_user')
            base = Path(dirpath)
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

            for filename in sorted(filenames):
                if should_continue is not None and not should_continue():
                    raise CloudDriveJobCancelled('cancelled_by_user')
                file_path = base / filename
                rel_file = file_path.relative_to(root)
                storage_key = str(rel_file).replace('\\', '/')
                if import_files and not self.storage.exists(storage_key):
                    self.storage.put_file(file_path, storage_key)
                checksum = compute_file_checksum(file_path)
                self.registry.upsert_file(
                    folder_id=base_id,
                    path=storage_key,
                    name=filename,
                    storage_key=storage_key,
                    mime_type=guess_mime_type(file_path),
                    size_bytes=file_path.stat().st_size,
                    checksum=checksum,
                    source_path=str(file_path),
                )
                imported += 1
                if imported == 1 or imported % 25 == 0:
                    emit_progress('file', current_path=str(file_path))
                if max_files is not None and imported >= max_files:
                    emit_progress('done', current_path=str(file_path), done=True)
                    return self.registry.stats()
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
        from rag_catalog.core.index_rag import RAGIndexer

        indexer = RAGIndexer(
            catalog_path=str(index_root),
            qdrant_db_path=str(index_config.get('qdrant_db_path') or ''),
            embedding_model=str(index_config.get('embedding_model') or 'sentence-transformers/all-MiniLM-L6-v2'),
            collection_name=resolve_collection_name_from_config(index_config),
            vector_size=int(index_config.get('vector_size') or 384),
            chunk_size=int(index_config.get('chunk_size') or 500),
            chunk_overlap=int(index_config.get('chunk_overlap') or 100),
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
        indexer._flush_buffer()
        return True, max(0, int(indexer.point_count) - before)

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
