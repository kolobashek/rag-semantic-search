from __future__ import annotations

import json
from pathlib import Path

from rag_catalog.cli.pilot_gate import REQUIRED_ROUTES, REQUIRED_WIDTHS, evaluate_pilot_gate


def _write(path: Path, value: dict) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def _ui_artifact() -> dict:
    checks = [
        {"name": name, "ok": True, "details": {}}
        for name in (
            "authenticated_login",
            "search_state_transition",
            "group_management",
            "acl_api_enforcement",
            "audit_correlation_evidence",
        )
    ]
    checks.extend(
        {
            "name": "responsive_screen",
            "ok": True,
            "details": {"route": route, "viewport": {"width": width, "height": 900}},
        }
        for route in REQUIRED_ROUTES
        for width in REQUIRED_WIDTHS
    )
    return {
        "ok": True,
        "checks": checks,
        "checks_passed": len(checks),
        "console_errors": [],
        "page_errors": [],
    }


def _evaluate(tmp_path: Path, *, retrieval: dict, signoff: dict) -> dict:
    ui_path = _write(tmp_path / "ui.json", _ui_artifact())
    retrieval_path = _write(tmp_path / "retrieval.json", retrieval)
    test_path = _write(tmp_path / "tests.json", {"ok": True, "returncode": 0, "passed": 590, "warnings": 4})
    signoff_path = _write(tmp_path / "signoff.json", signoff) if signoff else tmp_path / "missing-signoff.json"
    health = {
        "pilot_ready": True,
        "status": "ready",
        "components": {
            "jobs": {"status": "idle"},
            "backup": {
                "ok": True,
                "status": "healthy",
                "restore_drill_ok": True,
                "latest_path": "backup",
            },
        },
    }
    return evaluate_pilot_gate(
        health=health,
        ui_path=ui_path,
        ui_artifact=_ui_artifact(),
        retrieval_path=retrieval_path,
        retrieval_artifact=retrieval,
        test_path=test_path,
        test_artifact={"ok": True, "returncode": 0, "passed": 590, "warnings": 4},
        signoff_path=signoff_path,
        signoff_artifact=signoff,
        cfg={},
        max_age_hours=24,
    )


def test_pilot_gate_is_go_only_with_complete_evidence(tmp_path: Path) -> None:
    report = _evaluate(
        tmp_path,
        retrieval={
            "queries": 32,
            "recall_at_k": 0.95,
            "latency_p95_ms": 3000,
            "acl_leakage_rate": 0.0,
            "ground_truth_coverage": 0.8,
            "no_answer_accuracy": 0.9,
        },
        signoff={
            "data_owner": "owner",
            "pilot_admin": "admin",
            "service_operator": "operator",
            "product_owner": "product",
            "customer_accepted": True,
            "update_rehearsed": True,
            "open_sev1": 0,
            "open_sev2": 0,
            "accepted_at": "2026-07-11T14:00:00+07:00",
        },
    )

    assert report["ready"] is True
    assert report["decision"] == "GO"
    assert report["failed_checks"] == []


def test_pilot_gate_reports_missing_retrieval_labels_and_signoff(tmp_path: Path) -> None:
    report = _evaluate(
        tmp_path,
        retrieval={
            "queries": 32,
            "recall_at_k": 0.96875,
            "latency_p95_ms": 3998,
            "acl_leakage_rate": 0.0,
            "ground_truth_coverage": 0.0,
            "no_answer_accuracy": None,
        },
        signoff={},
    )

    assert report["ready"] is False
    assert report["decision"] == "NO_GO"
    assert report["failed_checks"] == ["retrieval_acceptance", "pilot_acceptance_signoff"]
