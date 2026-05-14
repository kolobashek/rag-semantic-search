from __future__ import annotations

import json
from pathlib import Path

from rag_catalog.core.rag_core import DEFAULT_CONFIG


def test_config_examples_include_all_default_keys() -> None:
    for filename in ("config.example.json", "config.docker.example.json"):
        data = json.loads(Path(filename).read_text(encoding="utf-8"))
        missing = [key for key in DEFAULT_CONFIG if key not in data]

        assert missing == []
