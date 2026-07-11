from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from rag_catalog.core.cloud_drive.operations import cloud_drive_operations_health
from rag_catalog.core.rag_core import load_config

REQUIRED_UI_CHECKS = {
    "authenticated_login",
    "search_state_transition",
    "group_management",
    "acl_api_enforcement",
    "audit_correlation_evidence",
}
REQUIRED_ROUTES = {"/search", "/explorer", "/jobs", "/index", "/stats", "/settings"}
REQUIRED_WIDTHS = {480, 900, 1280}


def _load_json(path: Path | None) -> Dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _latest_json(root: Path, pattern: str, *, predicate: Any = None) -> tuple[Path | None, Dict[str, Any]]:
    candidates = sorted(root.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True) if root.exists() else []
    for path in candidates:
        value = _load_json(path)
        if value and (predicate is None or predicate(value)):
            return path.resolve(), value
    return None, {}


def _artifact_age_hours(path: Path | None) -> float | None:
    if path is None or not path.exists():
        return None
    return round(max(0.0, datetime.now().timestamp() - path.stat().st_mtime) / 3600.0, 2)


def _check(name: str, ok: bool, *, evidence: str = "", details: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "name": name,
        "ok": bool(ok),
        "evidence": evidence,
        "details": details or {},
    }


def evaluate_ui_smoke(path: Path | None, artifact: Dict[str, Any], *, max_age_hours: float) -> Dict[str, Any]:
    checks = list(artifact.get("checks") or [])
    names = {str(item.get("name") or "") for item in checks if item.get("ok")}
    responsive = {
        (
            str((item.get("details") or {}).get("route") or ""),
            int(((item.get("details") or {}).get("viewport") or {}).get("width") or 0),
        )
        for item in checks
        if item.get("name") == "responsive_screen" and item.get("ok")
    }
    expected_responsive = {(route, width) for route in REQUIRED_ROUTES for width in REQUIRED_WIDTHS}
    age = _artifact_age_hours(path)
    ok = bool(
        artifact.get("ok")
        and REQUIRED_UI_CHECKS.issubset(names)
        and expected_responsive.issubset(responsive)
        and not artifact.get("console_errors")
        and not artifact.get("page_errors")
        and age is not None
        and age <= max_age_hours
    )
    return _check(
        "authenticated_ui_acl_audit_smoke",
        ok,
        evidence=str(path or ""),
        details={
            "age_hours": age,
            "max_age_hours": max_age_hours,
            "checks_passed": int(artifact.get("checks_passed") or 0),
            "required_checks_present": sorted(REQUIRED_UI_CHECKS.intersection(names)),
            "responsive_cells": len(expected_responsive.intersection(responsive)),
            "responsive_cells_required": len(expected_responsive),
        },
    )


def evaluate_retrieval(path: Path | None, artifact: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    recall_floor = float(cfg.get("pilot_retrieval_recall_min") or 0.875)
    p95_budget = int(cfg.get("pilot_search_p95_ms") or 4000)
    ground_truth_floor = float(cfg.get("pilot_ground_truth_coverage_min") or 0.5)
    no_answer_floor = float(cfg.get("pilot_no_answer_accuracy_min") or 0.8)
    max_age_hours = float(cfg.get("pilot_retrieval_max_age_hours") or 168.0)
    recall = float(artifact.get("recall_at_k") or 0.0)
    p95 = int(artifact.get("latency_p95_ms") or 0)
    leakage = float(artifact.get("acl_leakage_rate") or 0.0)
    coverage = float(artifact.get("ground_truth_coverage") or 0.0)
    no_answer_raw = artifact.get("no_answer_accuracy")
    no_answer = float(no_answer_raw) if no_answer_raw is not None else None
    queries = int(artifact.get("queries") or 0)
    age = _artifact_age_hours(path)
    ok = bool(
        queries > 0
        and recall >= recall_floor
        and 0 < p95 <= p95_budget
        and leakage <= 0.0
        and coverage >= ground_truth_floor
        and no_answer is not None
        and no_answer >= no_answer_floor
        and age is not None
        and age <= max_age_hours
    )
    return _check(
        "retrieval_acceptance",
        ok,
        evidence=str(path or ""),
        details={
            "queries": queries,
            "recall_at_k": recall,
            "recall_floor": recall_floor,
            "latency_p95_ms": p95,
            "latency_p95_budget_ms": p95_budget,
            "acl_leakage_rate": leakage,
            "ground_truth_coverage": coverage,
            "ground_truth_coverage_floor": ground_truth_floor,
            "no_answer_accuracy": no_answer,
            "no_answer_accuracy_floor": no_answer_floor,
            "age_hours": age,
            "max_age_hours": max_age_hours,
        },
    )


def evaluate_test_evidence(path: Path | None, artifact: Dict[str, Any], *, max_age_hours: float) -> Dict[str, Any]:
    age = _artifact_age_hours(path)
    ok = bool(
        artifact.get("ok")
        and int(artifact.get("returncode") or 0) == 0
        and int(artifact.get("passed") or 0) > 0
        and age is not None
        and age <= max_age_hours
    )
    return _check(
        "full_regression_tests",
        ok,
        evidence=str(path or ""),
        details={
            "age_hours": age,
            "max_age_hours": max_age_hours,
            "passed": int(artifact.get("passed") or 0),
            "warnings": int(artifact.get("warnings") or 0),
            "duration_seconds": artifact.get("duration_seconds"),
        },
    )


def evaluate_signoff(path: Path | None, artifact: Dict[str, Any]) -> Dict[str, Any]:
    required = {
        "data_owner": bool(str(artifact.get("data_owner") or "").strip()),
        "pilot_admin": bool(str(artifact.get("pilot_admin") or "").strip()),
        "service_operator": bool(str(artifact.get("service_operator") or "").strip()),
        "product_owner": bool(str(artifact.get("product_owner") or "").strip()),
        "customer_accepted": artifact.get("customer_accepted") is True,
        "update_rehearsed": artifact.get("update_rehearsed") is True,
        "open_sev1_zero": "open_sev1" in artifact and int(artifact.get("open_sev1") or 0) == 0,
        "open_sev2_zero": "open_sev2" in artifact and int(artifact.get("open_sev2") or 0) == 0,
        "accepted_at": bool(str(artifact.get("accepted_at") or "").strip()),
    }
    return _check(
        "pilot_acceptance_signoff",
        bool(artifact) and all(required.values()),
        evidence=str(path or ""),
        details={"requirements": required},
    )


def evaluate_pilot_gate(
    *,
    health: Dict[str, Any],
    ui_path: Path | None,
    ui_artifact: Dict[str, Any],
    retrieval_path: Path | None,
    retrieval_artifact: Dict[str, Any],
    test_path: Path | None,
    test_artifact: Dict[str, Any],
    signoff_path: Path | None,
    signoff_artifact: Dict[str, Any],
    cfg: Dict[str, Any],
    max_age_hours: float,
) -> Dict[str, Any]:
    components = dict(health.get("components") or {})
    backup = dict(components.get("backup") or {})
    checks = [
        _check(
            "operations_health",
            bool(health.get("pilot_ready")),
            details={
                "status": health.get("status"),
                "pilot_ready": health.get("pilot_ready"),
                "jobs": (components.get("jobs") or {}).get("status"),
            },
        ),
        _check(
            "verified_recovery",
            bool(backup.get("ok") and backup.get("restore_drill_ok")),
            evidence=str(backup.get("latest_path") or ""),
            details={
                "status": backup.get("status"),
                "restore_drill_ok": backup.get("restore_drill_ok"),
                "age_hours": backup.get("age_hours"),
            },
        ),
        evaluate_ui_smoke(ui_path, ui_artifact, max_age_hours=max_age_hours),
        evaluate_test_evidence(test_path, test_artifact, max_age_hours=max_age_hours),
        evaluate_retrieval(retrieval_path, retrieval_artifact, cfg),
        evaluate_signoff(signoff_path, signoff_artifact),
    ]
    ready = all(check["ok"] for check in checks)
    return {
        "ready": ready,
        "decision": "GO" if ready else "NO_GO",
        "completed_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "checks": checks,
        "failed_checks": [check["name"] for check in checks if not check["ok"]],
    }


def _run_full_tests(output_dir: Path) -> tuple[Path, Dict[str, Any]]:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    command = [sys.executable, "-m", "pytest", "-q"]
    clock_started = datetime.now().timestamp()
    completed = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace")
    duration = round(datetime.now().timestamp() - clock_started, 2)
    output = f"{completed.stdout}\n{completed.stderr}"
    passed_match = re.search(r"(\d+) passed", output)
    warning_match = re.search(r"(\d+) warnings?", output)
    artifact = {
        "ok": completed.returncode == 0 and passed_match is not None,
        "completed_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "command": command,
        "returncode": completed.returncode,
        "passed": int(passed_match.group(1)) if passed_match else 0,
        "warnings": int(warning_match.group(1)) if warning_match else 0,
        "duration_seconds": duration,
        "summary_tail": output[-4000:],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"tests-{timestamp}.json"
    path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    return path, artifact


def _write_signoff_template(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise SystemExit(f"Sign-off template уже существует: {path}")
    template = {
        "data_owner": "",
        "pilot_admin": "",
        "service_operator": "",
        "product_owner": "",
        "customer_accepted": False,
        "update_rehearsed": False,
        "open_sev1": 0,
        "open_sev2": 0,
        "accepted_at": "",
        "notes": "",
    }
    path.write_text(json.dumps(template, ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate Paid Dedicated Pilot release evidence.")
    parser.add_argument("--run-tests", action="store_true", help="Run full pytest and store signed local evidence")
    parser.add_argument("--ui-artifact", default="")
    parser.add_argument("--retrieval-artifact", default="")
    parser.add_argument("--test-artifact", default="")
    parser.add_argument("--signoff", default="runtime/pilot-acceptance/signoff.json")
    parser.add_argument("--output-dir", default="runtime/pilot-gates")
    parser.add_argument("--max-age-hours", type=float, default=24.0)
    parser.add_argument("--write-signoff-template", action="store_true")
    args = parser.parse_args(argv)

    signoff_path = Path(args.signoff).expanduser().resolve()
    if args.write_signoff_template:
        _write_signoff_template(signoff_path)
        print(json.dumps({"signoff_template": str(signoff_path)}, ensure_ascii=True, indent=2))
        return 0

    output_dir = Path(args.output_dir).expanduser().resolve()
    cfg = load_config()
    health = cloud_drive_operations_health(cfg)
    if str(args.ui_artifact or "").strip():
        ui_path = Path(args.ui_artifact).expanduser().resolve()
        ui_artifact = _load_json(ui_path)
    else:
        ui_path, ui_artifact = _latest_json(
            Path("runtime/pilot-ui-smoke"), "*/pilot-ui-smoke.json", predicate=lambda value: bool(value.get("ok"))
        )
    if str(args.retrieval_artifact or "").strip():
        retrieval_path = Path(args.retrieval_artifact).expanduser().resolve()
        retrieval_artifact = _load_json(retrieval_path)
    else:
        retrieval_path, retrieval_artifact = _latest_json(
            Path("runtime/eval"), "*.json", predicate=lambda value: "recall_at_k" in value
        )
    if args.run_tests:
        test_path, test_artifact = _run_full_tests(output_dir / "tests")
    elif str(args.test_artifact or "").strip():
        test_path = Path(args.test_artifact).expanduser().resolve()
        test_artifact = _load_json(test_path)
    else:
        test_path, test_artifact = _latest_json(output_dir / "tests", "*.json")

    report = evaluate_pilot_gate(
        health=health,
        ui_path=ui_path,
        ui_artifact=ui_artifact,
        retrieval_path=retrieval_path,
        retrieval_artifact=retrieval_artifact,
        test_path=test_path,
        test_artifact=test_artifact,
        signoff_path=signoff_path,
        signoff_artifact=_load_json(signoff_path),
        cfg=cfg,
        max_age_hours=max(0.1, float(args.max_age_hours)),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    report_path = output_dir / f"pilot-gate-{timestamp}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = {
        "decision": report["decision"],
        "ready": report["ready"],
        "failed_checks": report["failed_checks"],
        "artifact_path": str(report_path),
    }
    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0 if report["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
