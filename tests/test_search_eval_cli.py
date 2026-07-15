from __future__ import annotations

import io

import pytest

from scripts.search_eval import (
    _apply_acl_evidence,
    _apply_config_overrides,
    _build_evaluation_profile,
    _enforce_eval_runtime_contracts,
    _evaluation_fingerprints,
    _parse_named_values,
    _print_console_safe,
    _validate_index_readiness_evidence,
)


def test_evaluation_profile_records_query_and_index_embedding_runtime() -> None:
    profile = _build_evaluation_profile(
        {
            "embedding_model": "intfloat/multilingual-e5-small",
            "embedding_backend": "onnx",
            "embedding_onnx_provider": "DmlExecutionProvider",
            "embedding_onnx_file_name": "onnx/model.onnx",
            "index_embedding_backend": "onnx",
            "index_embedding_onnx_provider": "DmlExecutionProvider",
            "index_embedding_onnx_file_name": "onnx/model.onnx",
        },
        collection_name="catalog_v2_e5",
    )

    assert profile["embedding_onnx_provider"] == "DmlExecutionProvider"
    assert profile["embedding_onnx_file_name"] == "onnx/model.onnx"
    assert profile["index_embedding_backend"] == "onnx"
    assert profile["index_embedding_onnx_provider"] == "DmlExecutionProvider"
    assert profile["index_embedding_onnx_file_name"] == "onnx/model.onnx"
    assert profile["collection_name"] == "catalog_v2_e5"


def test_console_output_does_not_fail_on_legacy_windows_encoding() -> None:
    buffer = io.BytesIO()
    stream = io.TextIOWrapper(buffer, encoding="cp1251")

    _print_console_safe("Цена: £100", stream=stream)
    stream.flush()

    assert buffer.getvalue().decode("cp1251").splitlines() == [r"Цена: \xa3100"]


def test_evaluation_profile_inherits_index_runtime_from_query_runtime() -> None:
    profile = _build_evaluation_profile(
        {
            "embedding_backend": "onnx",
            "embedding_onnx_provider": "CPUExecutionProvider",
            "embedding_onnx_file_name": "onnx/model_qint8.onnx",
        },
        collection_name="catalog",
    )

    assert profile["index_embedding_backend"] == "onnx"
    assert profile["index_embedding_onnx_provider"] == "CPUExecutionProvider"
    assert profile["index_embedding_onnx_file_name"] == "onnx/model_qint8.onnx"


def test_config_override_applies_named_retrieval_preset() -> None:
    config = {
        "retrieval_preset": "legacy",
        "retrieval_pipeline": "legacy",
        "retrieval_bm25_enabled": False,
    }

    result = _apply_config_overrides(config, ["retrieval_preset=release_v2"])

    assert result["retrieval_pipeline"] == "v2"
    assert result["retrieval_bm25_enabled"] is True


def test_required_profile_values_are_typed() -> None:
    assert _parse_named_values(
        ["collection_name=catalog_v2_e5", "vector_size=384", "fulltext_enabled=true"],
        option_name="--require-profile",
    ) == {
        "collection_name": "catalog_v2_e5",
        "vector_size": 384,
        "fulltext_enabled": True,
    }

    with pytest.raises(ValueError, match="--require-profile"):
        _parse_named_values(["missing-separator"], option_name="--require-profile")


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


def test_index_readiness_evidence_binds_full_audit_to_live_collection() -> None:
    index_profile = {
        "embedding_model": "intfloat/multilingual-e5-small",
        "index_embedding_backend": "onnx",
    }
    evidence = {
        "ready": True,
        "collection_name": "catalog_v2_e5",
        "points_count": 500_000,
        "indexed_vectors_count": 495_000,
        "index_runtime_profile": index_profile,
        "payload_integrity": {"ok": True, "sample_size": 1_000},
        "spreadsheet_integrity": {
            "ok": True,
            "scanned_all": True,
            "sampling_strategy": "full_scroll",
            "sample_size": 250_000,
        },
    }
    live = {
        "ready": True,
        "collection_name": "catalog_v2_e5",
        "points_count": 500_000,
        "indexed_vectors_count": 500_000,
    }

    result = _validate_index_readiness_evidence(
        evidence,
        evidence_path="readiness.json",
        collection_name="catalog_v2_e5",
        live_readiness=live,
        expected_index_profile=index_profile,
    )

    assert result["ok"] is True
    assert result["live_points_count"] == 500_000
    assert result["spreadsheet_integrity"]["scanned_all"] is True


def test_index_readiness_evidence_rejects_stale_or_partial_audit() -> None:
    index_profile = {
        "embedding_model": "intfloat/multilingual-e5-small",
        "index_embedding_backend": "onnx",
    }
    evidence = {
        "ready": True,
        "collection_name": "catalog_v2_e5",
        "points_count": 499_999,
        "indexed_vectors_count": 499_000,
        "index_runtime_profile": index_profile,
        "payload_integrity": {"ok": True, "sample_size": 1_000},
        "spreadsheet_integrity": {
            "ok": True,
            "scanned_all": False,
            "sampling_strategy": "random",
            "sample_size": 500,
        },
    }
    live = {
        "ready": True,
        "collection_name": "catalog_v2_e5",
        "points_count": 500_000,
        "indexed_vectors_count": 500_000,
    }

    with pytest.raises(ValueError, match="points_count_mismatch.*spreadsheet_full_audit_missing"):
        _validate_index_readiness_evidence(
            evidence,
            evidence_path="readiness.json",
            collection_name="catalog_v2_e5",
            live_readiness=live,
            expected_index_profile=index_profile,
        )


def test_index_readiness_evidence_rejects_embedding_profile_mismatch() -> None:
    evidence = {
        "ready": True,
        "collection_name": "catalog_v2_e5",
        "points_count": 500_000,
        "indexed_vectors_count": 500_000,
        "payload_integrity": {"ok": True, "sample_size": 1_000},
        "spreadsheet_integrity": {
            "ok": True,
            "scanned_all": True,
            "sampling_strategy": "full_scroll",
            "sample_size": 250_000,
        },
        "index_runtime_profile": {
            "embedding_model": "intfloat/multilingual-e5-small",
            "index_embedding_onnx_provider": "CPUExecutionProvider",
        },
    }
    live = {
        "ready": True,
        "collection_name": "catalog_v2_e5",
        "points_count": 500_000,
        "indexed_vectors_count": 500_000,
    }

    with pytest.raises(ValueError, match="index_runtime_profile_mismatch:index_embedding_onnx_provider"):
        _validate_index_readiness_evidence(
            evidence,
            evidence_path="readiness.json",
            collection_name="catalog_v2_e5",
            live_readiness=live,
            expected_index_profile={
                "embedding_model": "intfloat/multilingual-e5-small",
                "index_embedding_onnx_provider": "DmlExecutionProvider",
            },
        )
