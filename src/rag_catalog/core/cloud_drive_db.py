"""Cloud Drive state persistence layer (SQLite).

Minimal backend for Cloud Drive admin UX:
- Configuration (source_path, storage_path, enabled)
- Bootstrap jobs (status, progress, phases, history)
- Drive stats (file counts, sizes, last scan)

Designed to be replaced by Codex registry/service layer once Sprint 2 merges.
Until then, this module acts as the local source of truth for bootstrap state.
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional


# ── Schema ───────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS cloud_drive_config (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS cloud_bootstrap_jobs (
    job_id       TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'pending',
    phase        TEXT,
    progress_pct INTEGER DEFAULT 0,
    files_total  INTEGER DEFAULT 0,
    files_done   INTEGER DEFAULT 0,
    error        TEXT,
    detail       TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL,
    finished_at  REAL
);

CREATE TABLE IF NOT EXISTS cloud_drive_stats (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

# Job status constants
STATUS_PENDING   = "pending"
STATUS_RUNNING   = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED    = "failed"
STATUS_CANCELLED = "cancelled"

ALL_STATUSES = [STATUS_PENDING, STATUS_RUNNING, STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED]

PHASE_LABELS: Dict[str, str] = {
    "scan":      "Сканирование",
    "import":    "Импорт",
    "hash":      "Хеширование",
    "thumbnail": "Превью",
    "done":      "Завершено",
}


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class CloudDriveConfig:
    enabled: bool = False
    source_path: str = ""
    storage_path: str = ""
    auto_bootstrap: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "source_path": self.source_path,
            "storage_path": self.storage_path,
            "auto_bootstrap": self.auto_bootstrap,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CloudDriveConfig":
        return cls(
            enabled=bool(d.get("enabled")),
            source_path=str(d.get("source_path") or ""),
            storage_path=str(d.get("storage_path") or ""),
            auto_bootstrap=bool(d.get("auto_bootstrap")),
        )


@dataclass
class BootstrapJob:
    job_id: str
    status: str = STATUS_PENDING
    phase: Optional[str] = None
    progress_pct: int = 0
    files_total: int = 0
    files_done: int = 0
    error: Optional[str] = None
    detail: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None

    @property
    def is_active(self) -> bool:
        return self.status in (STATUS_PENDING, STATUS_RUNNING)

    @property
    def is_terminal(self) -> bool:
        return self.status in (STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED)

    @property
    def phase_label(self) -> str:
        return PHASE_LABELS.get(self.phase or "", self.phase or "")

    @property
    def created_dt(self) -> datetime:
        return datetime.fromtimestamp(self.created_at, tz=timezone.utc).astimezone()

    @property
    def finished_dt(self) -> Optional[datetime]:
        if self.finished_at is None:
            return None
        return datetime.fromtimestamp(self.finished_at, tz=timezone.utc).astimezone()

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.finished_at:
            return self.finished_at - self.created_at
        if self.status == STATUS_RUNNING:
            return time.time() - self.created_at
        return None


@dataclass
class CloudDriveStats:
    total_files: int = 0
    total_folders: int = 0
    total_size_bytes: int = 0
    last_scanned_at: Optional[float] = None

    @property
    def last_scanned_dt(self) -> Optional[datetime]:
        if self.last_scanned_at is None:
            return None
        return datetime.fromtimestamp(self.last_scanned_at, tz=timezone.utc).astimezone()


# ── DB class ─────────────────────────────────────────────────────────────────

class CloudDriveDB:
    """Thread-safe SQLite wrapper for Cloud Drive admin state."""

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        con = sqlite3.connect(str(self._path), timeout=10, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        try:
            yield con
            con.commit()
        finally:
            con.close()

    def _init_schema(self) -> None:
        with self._conn() as con:
            con.executescript(_DDL)

    # ── Config ───────────────────────────────────────────────────────────────

    def get_config(self) -> CloudDriveConfig:
        with self._conn() as con:
            rows = con.execute("SELECT key, value FROM cloud_drive_config").fetchall()
            data = {r["key"]: r["value"] for r in rows}
        raw: Dict[str, Any] = json.loads(data.get("config", "{}"))
        return CloudDriveConfig.from_dict(raw)

    def save_config(self, cfg: CloudDriveConfig) -> None:
        val = json.dumps(cfg.to_dict())
        with self._conn() as con:
            con.execute(
                "INSERT OR REPLACE INTO cloud_drive_config(key, value) VALUES(?, ?)",
                ("config", val),
            )

    # ── Bootstrap jobs ────────────────────────────────────────────────────────

    def create_job(self, job_id: str) -> BootstrapJob:
        now = time.time()
        with self._conn() as con:
            con.execute(
                """INSERT OR REPLACE INTO cloud_bootstrap_jobs
                   (job_id, status, phase, progress_pct, files_total, files_done,
                    error, detail, created_at, updated_at, finished_at)
                   VALUES (?, ?, NULL, 0, 0, 0, NULL, NULL, ?, ?, NULL)""",
                (job_id, STATUS_PENDING, now, now),
            )
        return BootstrapJob(job_id=job_id, created_at=now, updated_at=now)

    def update_job(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        phase: Optional[str] = None,
        progress_pct: Optional[int] = None,
        files_total: Optional[int] = None,
        files_done: Optional[int] = None,
        error: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        now = time.time()
        finished_at = now if status in (STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED) else None
        with self._conn() as con:
            job = self._get_job_con(con, job_id)
            if job is None:
                return
            con.execute(
                """UPDATE cloud_bootstrap_jobs SET
                   status=?, phase=?, progress_pct=?, files_total=?,
                   files_done=?, error=?, detail=?, updated_at=?, finished_at=COALESCE(?, finished_at)
                   WHERE job_id=?""",
                (
                    status if status is not None else job.status,
                    phase if phase is not None else job.phase,
                    progress_pct if progress_pct is not None else job.progress_pct,
                    files_total if files_total is not None else job.files_total,
                    files_done if files_done is not None else job.files_done,
                    error if error is not None else job.error,
                    detail if detail is not None else job.detail,
                    now,
                    finished_at,
                    job_id,
                ),
            )

    def get_job(self, job_id: str) -> Optional[BootstrapJob]:
        with self._conn() as con:
            return self._get_job_con(con, job_id)

    def _get_job_con(self, con: sqlite3.Connection, job_id: str) -> Optional[BootstrapJob]:
        row = con.execute(
            "SELECT * FROM cloud_bootstrap_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    def list_jobs(self, limit: int = 10) -> List[BootstrapJob]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM cloud_bootstrap_jobs ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def get_active_job(self) -> Optional[BootstrapJob]:
        with self._conn() as con:
            row = con.execute(
                "SELECT * FROM cloud_bootstrap_jobs WHERE status IN (?, ?) ORDER BY created_at DESC LIMIT 1",
                (STATUS_PENDING, STATUS_RUNNING),
            ).fetchone()
        return self._row_to_job(row) if row else None

    def cancel_active_jobs(self) -> int:
        """Cancel all pending/running jobs. Returns count cancelled."""
        now = time.time()
        with self._conn() as con:
            cur = con.execute(
                """UPDATE cloud_bootstrap_jobs
                   SET status=?, updated_at=?, finished_at=?
                   WHERE status IN (?, ?)""",
                (STATUS_CANCELLED, now, now, STATUS_PENDING, STATUS_RUNNING),
            )
        return cur.rowcount

    def recover_stale_jobs(self) -> int:
        """Mark running jobs as failed on startup (stale recovery). Returns count fixed."""
        now = time.time()
        with self._conn() as con:
            cur = con.execute(
                """UPDATE cloud_bootstrap_jobs
                   SET status=?, error=?, updated_at=?, finished_at=?
                   WHERE status IN (?, ?)""",
                (STATUS_FAILED, "Стале: восстановлено после рестарта", now, now,
                 STATUS_RUNNING, STATUS_PENDING),
            )
        return cur.rowcount

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> BootstrapJob:
        return BootstrapJob(
            job_id=str(row["job_id"]),
            status=str(row["status"]),
            phase=row["phase"],
            progress_pct=int(row["progress_pct"] or 0),
            files_total=int(row["files_total"] or 0),
            files_done=int(row["files_done"] or 0),
            error=row["error"],
            detail=row["detail"],
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            finished_at=float(row["finished_at"]) if row["finished_at"] is not None else None,
        )

    # ── Stats ─────────────────────────────────────────────────────────────────

    def get_stats(self) -> CloudDriveStats:
        with self._conn() as con:
            rows = con.execute("SELECT key, value FROM cloud_drive_stats").fetchall()
        data = {r["key"]: r["value"] for r in rows}
        return CloudDriveStats(
            total_files=int(data.get("total_files", 0) or 0),
            total_folders=int(data.get("total_folders", 0) or 0),
            total_size_bytes=int(data.get("total_size_bytes", 0) or 0),
            last_scanned_at=float(data["last_scanned_at"]) if data.get("last_scanned_at") else None,
        )

    def save_stats(self, stats: CloudDriveStats) -> None:
        rows = [
            ("total_files", str(stats.total_files)),
            ("total_folders", str(stats.total_folders)),
            ("total_size_bytes", str(stats.total_size_bytes)),
            ("last_scanned_at", str(stats.last_scanned_at) if stats.last_scanned_at else ""),
        ]
        with self._conn() as con:
            con.executemany(
                "INSERT OR REPLACE INTO cloud_drive_stats(key, value) VALUES(?, ?)", rows
            )


# ── Singleton helper ──────────────────────────────────────────────────────────

_db_instance: Optional[CloudDriveDB] = None


def get_cloud_drive_db(cfg: Dict[str, Any]) -> CloudDriveDB:
    """Return module-level CloudDriveDB singleton (one per process)."""
    global _db_instance
    if _db_instance is None:
        db_path_str = str(cfg.get("cloud_drive_db_path") or "")
        if db_path_str:
            db_path = Path(db_path_str)
        else:
            # default: alongside telemetry db or in project root
            tel_path = str(cfg.get("telemetry_db_path") or "")
            if tel_path:
                db_path = Path(tel_path).parent / "cloud_drive.db"
            else:
                db_path = Path("cloud_drive.db")
        _db_instance = CloudDriveDB(db_path)
    return _db_instance
