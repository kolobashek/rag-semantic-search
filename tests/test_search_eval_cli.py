from __future__ import annotations

from scripts.search_eval import _apply_config_overrides


def test_config_override_applies_named_retrieval_preset() -> None:
    config = {
        "retrieval_preset": "legacy",
        "retrieval_pipeline": "legacy",
        "retrieval_bm25_enabled": False,
    }

    result = _apply_config_overrides(config, ["retrieval_preset=release_v2"])

    assert result["retrieval_pipeline"] == "v2"
    assert result["retrieval_bm25_enabled"] is True


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
