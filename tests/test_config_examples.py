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


def test_explicit_config_path_overrides_project_config(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "isolated.json"
    config_path.write_text(json.dumps({"collection_name": "isolated"}), encoding="utf-8")
    monkeypatch.setenv("RAG_CONFIG_PATH", str(config_path))

    assert rag_core._resolve_config_file() == config_path.resolve()
    assert rag_core.load_config()["collection_name"] == "isolated"
