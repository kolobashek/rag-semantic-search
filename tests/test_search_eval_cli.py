from __future__ import annotations

import pytest

from scripts.search_eval import (
    _apply_acl_evidence,
    _apply_config_overrides,
    _enforce_eval_runtime_contracts,
    _evaluation_fingerprints,
)


def test_config_override_applies_named_retrieval_preset() -> None:
    config = {
        "retrieval_preset": "legacy",
        "retrieval_pipeline": "legacy",
        "retrieval_bm25_enabled": False,
    }

    result = _apply_config_overrides(config, ["retrieval_preset=release_v2"])

    assert result["retrieval_pipeline"] == "v2"
    assert result["retrieval_bm25_enabled"] is True


def test_reranker_eval_is_always_fail_closed() -> None:
    result = _enforce_eval_runtime_contracts(
        {"retrieval_reranker_enabled": True, "retrieval_reranker_fail_open": True}
    )

    assert result["retrieval_reranker_fail_open"] is False


def test_fulltext_eval_is_always_fail_closed() -> None:
    result = _enforce_eval_runtime_contracts(
        {"retrieval_fulltext_enabled": True, "retrieval_fulltext_fail_open": True}
    )

    assert result["retrieval_fulltext_fail_open"] is False


def test_config_override_preserves_explicit_candidate_setting() -> None:
    config = {"retrieval_preset": "legacy", "retrieval_pipeline": "legacy"}

    result = _apply_config_overrides(
        config,
        ["retrieval_preset=release_v2", "retrieval_pipeline=experimental"],
    )

    assert result["retrieval_pipeline"] == "experimental"


def test_config_override_does_not_reapply_existing_preset() -> None:
    config = {
        "retrieval_preset": "release_v2",
        "retrieval_pipeline": "custom_pipeline",
        "retrieval_bm25_enabled": False,
    }

    result = _apply_config_overrides(config, ["retrieval_bm25_top_k=25"])

    assert result["retrieval_pipeline"] == "custom_pipeline"
    assert result["retrieval_bm25_enabled"] is False
    assert result["retrieval_bm25_top_k"] == 25


def test_acl_evidence_is_merged_with_retrieval_forbidden_checks() -> None:
    report = {"acl_results_checked": 3, "acl_leakage_rate": 1 / 3}
    evidence = {
        "ok": True,
        "source_fingerprint": "current",
        "acl_results_checked": 2,
        "acl_leakage_rate": 0.0,
    }

    merged = _apply_acl_evidence(
        report,
        evidence,
        evidence_path="acl.json",
        current_source_fingerprint="current",
    )

    assert merged["acl_results_checked"] == 5
    assert merged["acl_leakage_rate"] == pytest.approx(0.2)
    assert merged["acl_evidence"]["results_checked"] == 2


def test_acl_evidence_rejects_stale_source_fingerprint() -> None:
    with pytest.raises(ValueError, match="fingerprint"):
        _apply_acl_evidence(
            {},
            {
                "ok": True,
                "source_fingerprint": "stale",
                "acl_results_checked": 2,
                "acl_leakage_rate": 0.0,
            },
            evidence_path="acl.json",
            current_source_fingerprint="current",
        )


def test_evaluation_fingerprint_binds_sources_and_golden(tmp_path) -> None:
    golden = tmp_path / "golden.json"
    golden.write_text('[{"query":"alpha","expected":["alpha"]}]', encoding="utf-8")

    first = _evaluation_fingerprints(golden, source="source-a", limit=10)
    second = _evaluation_fingerprints(golden, source="source-a", limit=10)
    changed_source = _evaluation_fingerprints(golden, source="source-b", limit=10)
    changed_limit = _evaluation_fingerprints(golden, source="source-a", limit=5)
    golden.write_text('[{"query":"beta","expected":["beta"]}]', encoding="utf-8")
    changed_golden = _evaluation_fingerprints(golden, source="source-a", limit=10)

    assert first == second
    assert first["evaluation_protocol"] == {"version": "search-eval-v2", "limit": 10}
    assert first["evaluation_fingerprint"] != changed_source["evaluation_fingerprint"]
    assert first["evaluation_fingerprint"] != changed_limit["evaluation_fingerprint"]
    assert first["evaluation_fingerprint"] != changed_golden["evaluation_fingerprint"]
