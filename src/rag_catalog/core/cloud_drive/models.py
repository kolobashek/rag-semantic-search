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
    created_at: str = ""
    updated_at: str = ""


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
    progress: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CloudDriveStats:
    folders: int
    files: int
    versions: int
    pending_jobs: int
    root_path: str = ""
