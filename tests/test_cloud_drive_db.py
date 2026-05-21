"""Tests for cloud_drive_db module."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from rag_catalog.core.cloud_drive_db import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    CloudDriveConfig,
    CloudDriveDB,
    CloudDriveStats,
)


@pytest.fixture()
def db(tmp_path: Path) -> CloudDriveDB:
    return CloudDriveDB(tmp_path / "cloud_drive.db")


# ── Config tests ──────────────────────────────────────────────────────────────

def test_default_config_is_disabled(db: CloudDriveDB) -> None:
    cfg = db.get_config()
    assert cfg.enabled is False
    assert cfg.source_path == ""
    assert cfg.storage_path == ""


def test_save_and_reload_config(db: CloudDriveDB) -> None:
    cfg = CloudDriveConfig(
        enabled=True,
        source_path=r"O:\Обмен",
        storage_path=r"D:\Storage",
        auto_bootstrap=True,
    )
    db.save_config(cfg)
    loaded = db.get_config()
    assert loaded.enabled is True
    assert loaded.source_path == r"O:\Обмен"
    assert loaded.storage_path == r"D:\Storage"
    assert loaded.auto_bootstrap is True


def test_overwrite_config(db: CloudDriveDB) -> None:
    db.save_config(CloudDriveConfig(enabled=True, source_path="A"))
    db.save_config(CloudDriveConfig(enabled=False, source_path="B"))
    cfg = db.get_config()
    assert cfg.enabled is False
    assert cfg.source_path == "B"


# ── Bootstrap job tests ───────────────────────────────────────────────────────

def test_create_job(db: CloudDriveDB) -> None:
    job = db.create_job("job-1")
    assert job.job_id == "job-1"
    assert job.status == STATUS_PENDING
    assert job.progress_pct == 0
    assert job.is_active


def test_get_nonexistent_job(db: CloudDriveDB) -> None:
    assert db.get_job("does-not-exist") is None


def test_update_job_status(db: CloudDriveDB) -> None:
    db.create_job("j2")
    db.update_job("j2", status=STATUS_RUNNING, phase="scan", progress_pct=25, files_total=100)
    job = db.get_job("j2")
    assert job is not None
    assert job.status == STATUS_RUNNING
    assert job.phase == "scan"
    assert job.progress_pct == 25
    assert job.files_total == 100
    assert job.is_active


def test_job_terminal_statuses(db: CloudDriveDB) -> None:
    for status in (STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED):
        jid = f"j-{status}"
        db.create_job(jid)
        db.update_job(jid, status=status)
        job = db.get_job(jid)
        assert job is not None
        assert job.is_terminal
        assert job.finished_at is not None


def test_get_active_job(db: CloudDriveDB) -> None:
    assert db.get_active_job() is None
    db.create_job("active-1")
    db.update_job("active-1", status=STATUS_RUNNING)
    active = db.get_active_job()
    assert active is not None
    assert active.job_id == "active-1"


def test_cancel_active_jobs(db: CloudDriveDB) -> None:
    db.create_job("c1")
    db.create_job("c2")
    db.update_job("c1", status=STATUS_RUNNING)
    count = db.cancel_active_jobs()
    assert count == 2
    assert db.get_active_job() is None
    for jid in ("c1", "c2"):
        job = db.get_job(jid)
        assert job is not None
        assert job.status == STATUS_CANCELLED


def test_recover_stale_jobs(db: CloudDriveDB) -> None:
    db.create_job("stale-1")
    db.update_job("stale-1", status=STATUS_RUNNING)
    db.create_job("stale-2")  # pending
    count = db.recover_stale_jobs()
    assert count == 2
    for jid in ("stale-1", "stale-2"):
        job = db.get_job(jid)
        assert job is not None
        assert job.status == STATUS_FAILED
        assert job.error is not None


def test_list_jobs_newest_first(db: CloudDriveDB) -> None:
    for i in range(5):
        db.create_job(f"job-{i}")
        time.sleep(0.01)
    jobs = db.list_jobs(limit=3)
    assert len(jobs) == 3
    assert jobs[0].job_id == "job-4"
    assert jobs[1].job_id == "job-3"


def test_job_duration(db: CloudDriveDB) -> None:
    db.create_job("dur-1")
    db.update_job("dur-1", status=STATUS_RUNNING)
    job = db.get_job("dur-1")
    assert job is not None
    dur = job.duration_seconds
    assert dur is not None
    assert dur >= 0

    db.update_job("dur-1", status=STATUS_COMPLETED)
    job2 = db.get_job("dur-1")
    assert job2 is not None
    assert job2.finished_at is not None
    assert job2.duration_seconds is not None


# ── Stats tests ───────────────────────────────────────────────────────────────

def test_default_stats(db: CloudDriveDB) -> None:
    stats = db.get_stats()
    assert stats.total_files == 0
    assert stats.total_folders == 0
    assert stats.last_scanned_at is None


def test_save_and_reload_stats(db: CloudDriveDB) -> None:
    now = time.time()
    stats = CloudDriveStats(
        total_files=1234,
        total_folders=56,
        total_size_bytes=987654321,
        last_scanned_at=now,
    )
    db.save_stats(stats)
    loaded = db.get_stats()
    assert loaded.total_files == 1234
    assert loaded.total_folders == 56
    assert loaded.total_size_bytes == 987654321
    assert loaded.last_scanned_at is not None
    assert abs(loaded.last_scanned_at - now) < 1.0


# ── Config dataclass ──────────────────────────────────────────────────────────

def test_config_roundtrip_dict() -> None:
    cfg = CloudDriveConfig(enabled=True, source_path="X", storage_path="Y", auto_bootstrap=True)
    d = cfg.to_dict()
    loaded = CloudDriveConfig.from_dict(d)
    assert loaded.enabled is True
    assert loaded.source_path == "X"
    assert loaded.storage_path == "Y"
    assert loaded.auto_bootstrap is True
