from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import httpx

from rag_catalog.core import qdrant_connection


def test_loopback_qdrant_client_bypasses_proxy_and_reuses_connections(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_client(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(qdrant_connection, "QdrantClient", fake_client)

    qdrant_connection.create_qdrant_client(url="http://localhost:6333", timeout=300)

    assert calls[0]["url"] == "http://localhost:6333"
    assert calls[0]["timeout"] == 300
    assert calls[0]["trust_env"] is False
    assert calls[0]["check_compatibility"] is False
    assert isinstance(calls[0]["limits"], httpx.Limits)
    assert calls[0]["limits"].max_connections == 32
    assert calls[0]["limits"].max_keepalive_connections == 8


def test_remote_qdrant_client_preserves_environment_proxy_behavior(monkeypatch) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(
        qdrant_connection,
        "QdrantClient",
        lambda **kwargs: calls.append(kwargs) or SimpleNamespace(),
    )

    qdrant_connection.create_qdrant_client(url="https://qdrant.example.test", timeout=60)

    assert calls == [{"url": "https://qdrant.example.test", "timeout": 60}]


def test_local_path_qdrant_client_does_not_receive_http_options(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(
        qdrant_connection,
        "QdrantClient",
        lambda **kwargs: calls.append(kwargs) or SimpleNamespace(),
    )

    qdrant_connection.create_qdrant_client(path=tmp_path, timeout=15)

    assert calls == [{"path": str(tmp_path), "timeout": 15}]


def test_loopback_detection_supports_ipv4_and_ipv6() -> None:
    assert qdrant_connection.is_loopback_qdrant_url("http://127.0.0.1:6333")
    assert qdrant_connection.is_loopback_qdrant_url("http://[::1]:6333")
    assert not qdrant_connection.is_loopback_qdrant_url("https://qdrant.example.test")
