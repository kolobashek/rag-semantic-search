from __future__ import annotations

from pathlib import Path

from rag_catalog.core.consistency_audit import (
    collect_state_consistency,
    collect_telemetry_consistency,
    evaluate_consistency,
    snapshot_sha256,
)
from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.telemetry_db import TelemetryDB


def test_state_consistency_exposes_errors_unreadable_and_stale_retries(tmp_path: Path) -> None:
    path = tmp_path / "index_state.db"
    db = IndexStateDB(str(path))
    db.upsert_many(
        [
            {
                "full_path": "failed.jpg",
                "fingerprint": "1",
                "mtime": 1.0,
                "stage": "error",
                "indexed_stage": "large",
                "status": "error",
                "extension": ".jpg",
            },
            {
                "full_path": "unreadable.jpg",
                "fingerprint": "2",
                "mtime": 1.0,
                "stage": "empty",
                "indexed_stage": "large",
                "status": "unreadable",
                "extension": ".jpg",
            },
            {
                "full_path": "ok.pdf",
                "fingerprint": "3",
                "mtime": 1.0,
                "stage": "content",
                "indexed_stage": "large",
                "status": "ok",
                "extension": ".pdf",
            },
        ]
    )
    db.record_failed_path("failed.jpg", fingerprint="1", error="broken")
    db.record_failed_path("ok.pdf", fingerprint="3", error="old timeout")

    state = collect_state_consistency(path)

    assert state["final_error_count"] == 1
    assert state["unreadable_count"] == 1
    assert state["stale_failed_path_count"] == 1


def test_consistency_go_allows_explicit_unreadable_sources(tmp_path: Path) -> None:
    state_path = tmp_path / "index_state.db"
    state_db = IndexStateDB(str(state_path))
    state_db.upsert_many(
        [
            {
                "full_path": "unreadable.jpg",
                "fingerprint": "1",
                "mtime": 1.0,
                "stage": "empty",
                "indexed_stage": "large",
                "status": "unreadable",
                "extension": ".jpg",
            }
        ]
    )
    telemetry_path = tmp_path / "telemetry.db"
    telemetry_db = TelemetryDB(str(telemetry_path))
    run_id = telemetry_db.start_ocr_run(worker_pid=0)
    telemetry_db.finish_ocr_run(ocr_run_id=run_id, status="completed")

    snapshot = evaluate_consistency(
        collection_name="catalog_v2_e5",
        readiness={"ready": True},
        spreadsheet_evidence={"ok": True},
        state=collect_state_consistency(state_path),
        telemetry=collect_telemetry_consistency(telemetry_path),
    )

    assert snapshot["verdict"] == "GO"
    assert snapshot["state"]["unreadable_count"] == 1
    assert len(snapshot_sha256(snapshot)) == 64
