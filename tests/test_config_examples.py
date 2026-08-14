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


def test_shipped_configs_contain_no_default_credentials() -> None:
    """Секреты не должны ехать в поставке: их задают через .env / переменные окружения."""
    forbidden = ("minioadmin", "minioadmin123", "rag-catalog-local-secret")
    for filename in ("config.example.json", "config.docker.example.json", "docker-compose.yml"):
        text = Path(filename).read_text(encoding="utf-8").lower()
        for needle in forbidden:
            assert needle not in text, f"{filename} содержит дефолтный секрет {needle!r}"

    for filename in ("config.example.json", "config.docker.example.json"):
        data = json.loads(Path(filename).read_text(encoding="utf-8"))
        assert str(data.get("cloud_drive_s3_access_key") or "") == ""
        assert str(data.get("cloud_drive_s3_secret_key") or "") == ""
        assert str(data.get("ui_storage_secret") or "") == ""


def test_https_deployment_marks_session_cookie_secure() -> None:
    docker_cfg = json.loads(Path("config.docker.example.json").read_text(encoding="utf-8"))

    assert docker_cfg["ui_session_https_only"] is True


def test_ui_storage_secret_has_no_shared_literal_fallback() -> None:
    source = Path("src/rag_catalog/ui/nice_app.py").read_text(encoding="utf-8")

    assert "rag-catalog-local-secret" not in source


def test_s3_credentials_can_come_from_environment(monkeypatch) -> None:
    from rag_catalog.core.cloud_drive.storage import resolve_s3_credential

    monkeypatch.setenv("RAG_CLOUD_DRIVE_S3_SECRET_KEY", "from-env")
    assert resolve_s3_credential({"cloud_drive_s3_secret_key": "from-config"},
                                 "cloud_drive_s3_secret_key",
                                 "RAG_CLOUD_DRIVE_S3_SECRET_KEY") == "from-env"

    monkeypatch.delenv("RAG_CLOUD_DRIVE_S3_SECRET_KEY", raising=False)
    assert resolve_s3_credential({"cloud_drive_s3_secret_key": "from-config"},
                                 "cloud_drive_s3_secret_key",
                                 "RAG_CLOUD_DRIVE_S3_SECRET_KEY") == "from-config"
