from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(slots=True)
class CloudDriveFolder:
    id: str
    parent_id: Optional[str]
    name: str
    path: str
    depth: int
    source_path: str = ""
    is_root: bool = False
    source_mtime: float = 0.0
    created_at: str = ""
    updated_at: str = ""
    deleted_at: str = ""


@dataclass(slots=True)
class CloudDriveFile:
    id: str
    folder_id: str
    name: str
    path: str
    storage_key: str
    mime_type: str
    size_bytes: int
    checksum: str = ""
    source_path: str = ""
    source_mtime: float = 0.0
    current_version_id: str = ""
    created_at: str = ""
    updated_at: str = ""
    deleted_at: str = ""


@dataclass(slots=True)
class CloudDriveJob:
    id: str
    job_type: str
    status: str
    file_id: str = ""
    version_id: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    last_error: str = ""
    created_at: str = ""
    updated_at: str = ""
    started_at: str = ""
    finished_at: str = ""
    lease_owner: str = ""
    lease_until: str = ""
    next_run_at: str = ""
    progress: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CloudDriveImportSource:
    id: str
    name: str
    source_path: str
    target_path: str = ""
    import_files: bool = True
    enabled: bool = True
    created_by: str = ""
    last_job_id: str = ""
    last_status: str = ""
    last_error: str = ""
    last_scan_at: str = ""
    stats: Dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class CloudDriveStats:
    folders: int
    files: int
    versions: int
    pending_jobs: int
    root_path: str = ""


@dataclass(slots=True)
class CloudDriveStorageHealth:
    backend: str
    ok: bool
    writable: bool
    target: str = ""
    error: str = ""


@dataclass(slots=True)
class CloudDriveSyncClient:
    id: str
    username: str
    device_id: str
    display_name: str
    platform: str = ""
    status: str = "offline"
    last_seen_at: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class CloudDriveSyncPair:
    id: str
    client_id: str
    username: str
    local_path: str
    cloud_path: str
    conflict_policy: str = "ask"
    enabled: bool = True
    created_at: str = ""
    updated_at: str = ""


@dataclass(slots=True)
class CloudDriveSyncConflict:
    id: str
    client_id: str
    pair_id: str
    username: str
    path: str
    local_path: str
    cloud_path: str
    conflict_type: str
    local_version: str = ""
    cloud_version: str = ""
    status: str = "open"
    resolution: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    resolved_by: str = ""
    resolved_at: str = ""
    created_at: str = ""
    updated_at: str = ""
