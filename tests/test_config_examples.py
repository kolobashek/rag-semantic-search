from __future__ import annotations

import json
from pathlib import Path

from rag_catalog.core import rag_core
from rag_catalog.core.rag_core import DEFAULT_CONFIG


def test_config_examples_include_all_default_keys() -> None:
    for filename in ("config.example.json", "config.docker.example.json"):
        data = json.loads(Path(filename).read_text(encoding="utf-8"))
        missing = [key for key in DEFAULT_CONFIG if key not in data]

        assert missing == []


def test_unapproved_retrieval_candidate_remains_opt_in_by_default() -> None:
    assert DEFAULT_CONFIG["retrieval_preset"] == "legacy"
    assert DEFAULT_CONFIG["retrieval_pipeline"] == "legacy"
    assert DEFAULT_CONFIG["retrieval_reranker_enabled"] is False
    assert DEFAULT_CONFIG["embedding_model"] == "sentence-transformers/all-MiniLM-L6-v2"

    for filename in ("config.example.json", "config.docker.example.json"):
        data = json.loads(Path(filename).read_text(encoding="utf-8"))
        assert data["retrieval_preset"] == "legacy"
        assert data["retrieval_pipeline"] == "legacy"
        assert data["retrieval_reranker_enabled"] is False
        assert data["embedding_model"] == "sentence-transformers/all-MiniLM-L6-v2"


def test_explicit_config_path_overrides_project_config(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "isolated.json"
    config_path.write_text(json.dumps({"collection_name": "isolated"}), encoding="utf-8")
    monkeypatch.setenv("RAG_CONFIG_PATH", str(config_path))

    assert rag_core._resolve_config_file() == config_path.resolve()
    assert rag_core.load_config()["collection_name"] == "isolated"
