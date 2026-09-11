"""POST /api/client-logs — приём журналов установленных клиентов."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient
from nicegui import app

import rag_catalog.ui.api as cloud_api
from rag_catalog.core.client_logs import MAX_EVENTS_PER_BATCH
from rag_catalog.core.user_auth_db import UserAuthDB

_PASSWORD = "Secret-123"


class _RecordingTelemetry:
    def __init__(self) -> None:
        self.events: List[Dict[str, Any]] = []

    def log_app_event(self, **kwargs: Any) -> None:
        self.events.append(kwargs)


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def env(tmp_path, monkeypatch):
    cfg = {
        "catalog_path": str(tmp_path / "catalog"),
        "users_db_path": str(tmp_path / "rag_users.db"),
        "telemetry_db_path": str(tmp_path / "telemetry.db"),
        "cloud_drive_enabled": False,
    }
    auth = UserAuthDB(cfg["users_db_path"])
    assert auth.admin_create_user(
        username="kris", password=_PASSWORD, role="user", must_change_password=False
    )
    token = auth.create_session(username="kris")

    telemetry = _RecordingTelemetry()
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "TelemetryDB", lambda _path: telemetry)
    return {"cfg": cfg, "token": token, "telemetry": telemetry}


def _auth(env) -> Dict[str, str]:
    return {"Authorization": f"Bearer {env['token']}"}


def test_requires_authorization(client, env) -> None:
    response = client.post("/api/client-logs", json={"client": "sync", "events": [{"message": "x"}]})
    assert response.status_code == 401
    assert env["telemetry"].events == []


def test_rejects_invalid_token(client, env) -> None:
    response = client.post(
        "/api/client-logs",
        json={"client": "sync", "events": [{"message": "x"}]},
        headers={"Authorization": "Bearer nope"},
    )
    assert response.status_code == 401


def test_stores_events_under_authenticated_user(client, env) -> None:
    response = client.post(
        "/api/client-logs",
        json={
            "client": "sync",
            "device_id": "win-ABC",
            "app_version": "1.1.0",
            # Клиент пытается подписаться чужим именем — должно быть проигнорировано.
            "username": "admin",
            "events": [
                {"level": "ERROR", "message": "Не удалось синхронизировать Demo/local.txt"},
                {"level": "WARNING", "message": "повтор через 30 с"},
            ],
        },
        headers=_auth(env),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True, "stored": 2, "received": 2}
    stored = env["telemetry"].events
    assert [e["username"] for e in stored] == ["kris", "kris"]
    assert [e["screen"] for e in stored] == ["sync", "sync"]
    assert [e["action"] for e in stored] == ["client_error", "client_warning"]
    assert stored[0]["details"]["device_id"] == "win-ABC"


def test_redacts_secrets_from_client_messages(client, env) -> None:
    client.post(
        "/api/client-logs",
        json={
            "client": "sync",
            "events": [{"message": "401 for Authorization: Bearer supersecrettoken"}],
        },
        headers=_auth(env),
    )
    assert "supersecrettoken" not in env["telemetry"].events[0]["details"]["message"]


def test_rejects_empty_and_malformed_payloads(client, env) -> None:
    assert client.post("/api/client-logs", json={"client": "sync"}, headers=_auth(env)).status_code == 400
    assert client.post("/api/client-logs", json={"events": []}, headers=_auth(env)).status_code == 400
    assert client.post("/api/client-logs", json=[1, 2], headers=_auth(env)).status_code == 400
    assert env["telemetry"].events == []


def test_rejects_oversized_batch(client, env) -> None:
    response = client.post(
        "/api/client-logs",
        json={
            "client": "sync",
            "events": [{"message": f"e{i}"} for i in range(MAX_EVENTS_PER_BATCH + 1)],
        },
        headers=_auth(env),
    )
    assert response.status_code == 413
    assert env["telemetry"].events == []
