from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Optional

from .models import CloudDriveJob, CloudDriveStats
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

    def enqueue_reindex(self, path: str) -> None:
        file_row = self.registry.get_file_by_path(path)
        if file_row is None:
            raise RuntimeError(f'Файл не найден в registry: {path}')
        self.registry.queue_job(
            job_type='reindex',
            file_id=file_row.id,
            version_id=file_row.current_version_id,
            payload={'path': file_row.path},
        )
