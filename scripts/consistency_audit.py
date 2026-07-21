from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rag_catalog.cli.finalize_search_index import collection_readiness
from rag_catalog.core.consistency_audit import (
    collect_state_consistency,
    collect_telemetry_consistency,
    evaluate_consistency,
    snapshot_sha256,
)
from rag_catalog.core.embedding_collections import resolve_embedding_collection_name
from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.qdrant_connection import create_qdrant_client
from rag_catalog.core.rag_core import load_config
from rag_catalog.core.telemetry_db import TelemetryDB


def _spreadsheet_evidence(path: Path, *, collection_name: str, points_count: int) -> dict[str, Any]:
    if not path.exists():
        return {"ok": False, "path": str(path), "reasons": ["artifact_missing"]}
    artifact = json.loads(path.read_text(encoding="utf-8"))
    spreadsheet = artifact.get("spreadsheet_integrity") or {}
    reasons: list[str] = []
    if not bool(artifact.get("ready")):
        reasons.append("index_readiness_no_go")
    if str(artifact.get("collection_name") or "") != collection_name:
        reasons.append("collection_mismatch")
    if int(artifact.get("points_count") or 0) != int(points_count):
        reasons.append("points_count_mismatch")
    if not bool(spreadsheet.get("ok")) or not bool(spreadsheet.get("scanned_all")):
        reasons.append("spreadsheet_full_audit_missing")
    return {
        "ok": not reasons,
        "path": str(path),
        "reasons": reasons,
        "collection_name": str(artifact.get("collection_name") or ""),
        "points_count": int(artifact.get("points_count") or 0),
        "scanned_all": bool(spreadsheet.get("scanned_all")),
        "sample_size": int(spreadsheet.get("sample_size") or 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic retrieval rollout consistency audit")
    parser.add_argument("--config", default="")
    parser.add_argument("--state-dir", default="")
    parser.add_argument("--index-readiness-evidence", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repair-stale", action="store_true")
    parser.add_argument("--max-unindexed-vectors", type=int, default=50_000)
    args = parser.parse_args()
    if str(args.config).strip():
        os.environ["RAG_CONFIG_PATH"] = str(Path(args.config).resolve())
    cfg = load_config()
    collection_name = resolve_embedding_collection_name(
        str(cfg.get("collection_name") or "catalog"),
        str(cfg.get("embedding_model") or ""),
        enabled=bool(cfg.get("embedding_collection_versioning", False)),
        suffix=str(cfg.get("embedding_collection_suffix") or ""),
    )
    state_dir = Path(str(args.state_dir or cfg.get("qdrant_db_path") or "")).resolve()
    state_path = state_dir / "index_state.db"
    telemetry_path = Path(str(cfg.get("telemetry_db_path") or state_dir / "rag_telemetry.db")).resolve()
    repairs = {"stale_failed_paths_removed": 0, "stale_ocr_runs_finalized": 0}
    if args.repair_stale:
        repairs["stale_failed_paths_removed"] = IndexStateDB(str(state_path)).reconcile_stale_failed_paths()
        repairs["stale_ocr_runs_finalized"] = TelemetryDB(str(telemetry_path)).finalize_running_ocr_runs(
            status="cancelled",
            note="recovered_by_consistency_audit_dead_worker",
            skip_alive_pids=True,
        )
    client = create_qdrant_client(
        url=str(cfg.get("qdrant_url") or ""),
        path=str(cfg.get("qdrant_db_path") or "") or None,
        timeout=int(cfg.get("qdrant_timeout_sec") or 60),
    )

    def collect() -> dict[str, Any]:
        info = client.get_collection(collection_name)
        readiness = collection_readiness(
            info,
            require_fulltext=True,
            max_unindexed_vectors=max(0, int(args.max_unindexed_vectors)),
        )
        spreadsheet = _spreadsheet_evidence(
            Path(args.index_readiness_evidence).resolve(),
            collection_name=collection_name,
            points_count=int(readiness.get("points_count") or 0),
        )
        return evaluate_consistency(
            collection_name=collection_name,
            readiness=readiness,
            spreadsheet_evidence=spreadsheet,
            state=collect_state_consistency(state_path),
            telemetry=collect_telemetry_consistency(telemetry_path),
        )

    first = collect()
    second = collect()
    deterministic_repeat_match = first == second
    snapshot = second
    if not deterministic_repeat_match:
        reasons = list(snapshot.get("reasons") or [])
        if "nondeterministic_repeat" not in reasons:
            reasons.append("nondeterministic_repeat")
        snapshot["reasons"] = reasons
        snapshot["ok"] = False
        snapshot["verdict"] = "NO_GO"
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "deterministic_repeat_match": deterministic_repeat_match,
        "snapshot_sha256": snapshot_sha256(snapshot),
        "repairs": repairs,
        "snapshot": snapshot,
    }
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if bool(snapshot.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
