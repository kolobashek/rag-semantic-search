from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Mapping


def _dict_rows(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _pid_alive(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def collect_state_consistency(path: str | Path, *, example_limit: int = 50) -> dict[str, Any]:
    db_path = Path(path)
    with sqlite3.connect(str(db_path), timeout=30.0) as conn:
        conn.row_factory = sqlite3.Row
        total = int(conn.execute("SELECT COUNT(*) FROM state_entries").fetchone()[0])
        buckets = _dict_rows(
            conn,
            """
            SELECT stage, status, indexed_stage, COUNT(*) AS count
            FROM state_entries
            GROUP BY stage, status, indexed_stage
            ORDER BY stage, status, indexed_stage
            """,
        )
        extension_buckets = _dict_rows(
            conn,
            """
            SELECT extension, stage, status, COUNT(*) AS count
            FROM state_entries
            GROUP BY extension, stage, status
            ORDER BY extension, stage, status
            """,
        )
        final_errors = _dict_rows(
            conn,
            """
            SELECT full_path, extension, last_error
            FROM state_entries
            WHERE status='error'
            ORDER BY full_path
            LIMIT ?
            """,
            (max(1, int(example_limit)),),
        )
        unreadable = _dict_rows(
            conn,
            """
            SELECT full_path, extension, size_bytes, last_error
            FROM state_entries
            WHERE status='unreadable'
            ORDER BY full_path
            LIMIT ?
            """,
            (max(1, int(example_limit)),),
        )
        error_count = int(conn.execute("SELECT COUNT(*) FROM state_entries WHERE status='error'").fetchone()[0])
        unreadable_count = int(
            conn.execute("SELECT COUNT(*) FROM state_entries WHERE status='unreadable'").fetchone()[0]
        )
        empty_count = int(conn.execute("SELECT COUNT(*) FROM state_entries WHERE stage='empty'").fetchone()[0])
        queue_rows = _dict_rows(
            conn,
            "SELECT status, COUNT(*) AS count FROM index_queue GROUP BY status ORDER BY status",
        )
        active_queue_entries = sum(
            int(row["count"] or 0) for row in queue_rows if str(row["status"] or "") in {"pending", "running"}
        )
        failed_rows = _dict_rows(
            conn,
            """
            SELECT
                failed_paths.full_path,
                failed_paths.fingerprint,
                failed_paths.retry_count,
                failed_paths.last_error,
                state_entries.fingerprint AS state_fingerprint,
                state_entries.status AS state_status
            FROM failed_paths
            LEFT JOIN state_entries ON state_entries.full_path=failed_paths.full_path
            ORDER BY failed_paths.full_path
            """,
        )
    stale_failed = [
        row
        for row in failed_rows
        if str(row.get("state_status") or "") != "error"
        or str(row.get("state_fingerprint") or "") != str(row.get("fingerprint") or "")
    ]
    return {
        "total_entries": total,
        "buckets": buckets,
        "extension_buckets": extension_buckets,
        "active_queue_entries": active_queue_entries,
        "queue": queue_rows,
        "failed_path_count": len(failed_rows),
        "stale_failed_path_count": len(stale_failed),
        "stale_failed_paths": stale_failed[: max(1, int(example_limit))],
        "final_empty_count": empty_count,
        "final_error_count": error_count,
        "final_errors": final_errors,
        "unreadable_count": unreadable_count,
        "unreadable_sources": unreadable,
    }


def collect_telemetry_consistency(path: str | Path) -> dict[str, Any]:
    db_path = Path(path)
    with sqlite3.connect(str(db_path), timeout=30.0) as conn:
        conn.row_factory = sqlite3.Row
        latest_rows = _dict_rows(
            conn,
            "SELECT * FROM ocr_runs ORDER BY ts_started DESC LIMIT 1",
        )
        running = _dict_rows(
            conn,
            "SELECT * FROM ocr_runs WHERE status='running' ORDER BY ts_started, ocr_run_id",
        )
        for row in running:
            row["worker_alive"] = _pid_alive(int(row.get("worker_pid") or 0))
        result_summary = _dict_rows(
            conn,
            """
            SELECT status, requested_engine, engine, fallback_used,
                   COUNT(*) AS events, COUNT(DISTINCT file_path) AS unique_files
            FROM ocr_file_results
            GROUP BY status, requested_engine, engine, fallback_used
            ORDER BY status, requested_engine, engine, fallback_used
            """,
        )
        fallback_events = int(
            conn.execute("SELECT COUNT(*) FROM ocr_file_results WHERE fallback_used != 0").fetchone()[0]
        )
    return {
        "latest_ocr_run": latest_rows[0] if latest_rows else None,
        "ocr_result_summary": result_summary,
        "fallback_events": fallback_events,
        "running_ocr_runs": running,
        "stale_running_ocr_runs": [row for row in running if not bool(row["worker_alive"])],
        "live_running_ocr_runs": [row for row in running if bool(row["worker_alive"])],
    }


def evaluate_consistency(
    *,
    collection_name: str,
    readiness: Mapping[str, Any],
    spreadsheet_evidence: Mapping[str, Any],
    state: Mapping[str, Any],
    telemetry: Mapping[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    if not bool(readiness.get("ready")):
        reasons.append("collection_not_ready")
    if not bool(spreadsheet_evidence.get("ok")):
        reasons.append("spreadsheet_evidence_not_ready")
    if int(state.get("final_error_count") or 0):
        reasons.append("state_error_files_present")
    if int(state.get("stale_failed_path_count") or 0):
        reasons.append("stale_failed_path_markers")
    if int(state.get("active_queue_entries") or 0):
        reasons.append("active_index_queue_entries")
    if telemetry.get("stale_running_ocr_runs"):
        reasons.append("stale_running_ocr_telemetry")
    if telemetry.get("live_running_ocr_runs"):
        reasons.append("active_ocr_run")
    if int(telemetry.get("fallback_events") or 0):
        reasons.append("ocr_fallback_events_present")
    latest = telemetry.get("latest_ocr_run") or {}
    if not latest or str(latest.get("status") or "") != "completed":
        reasons.append("latest_ocr_run_not_completed")
    return {
        "ok": not reasons,
        "verdict": "GO" if not reasons else "NO_GO",
        "reasons": reasons,
        "collection_name": str(collection_name),
        "readiness": dict(readiness),
        "spreadsheet_evidence": dict(spreadsheet_evidence),
        "state": dict(state),
        "telemetry": dict(telemetry),
    }


def snapshot_sha256(snapshot: Mapping[str, Any]) -> str:
    payload = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
