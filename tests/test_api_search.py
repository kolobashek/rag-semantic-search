"""Публичный API поиска GET /api/search.

Проверяется через FastAPI TestClient поверх приложения NiceGUI с подменённым
RAGSearcher: авторизация как у остальных защищённых маршрутов, ACL-фильтрация
результатов тем же фильтром, что и веб-интерфейс.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient
from nicegui import app

import rag_catalog.ui.api as cloud_api
import rag_catalog.ui.helpers as ui_helpers
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.user_auth_db import UserAuthDB

_PASSWORD = "Secret-123"


class _FakeSearcher:
    connected = True

    def __init__(self, results: List[Dict[str, Any]], *, fail: bool = False) -> None:
        self._results = results
        self._fail = fail
        self.calls: List[Dict[str, Any]] = []
        self.config: Dict[str, Any] = {}

    def search(self, query: str, **kwargs: Any) -> List[Dict[str, Any]]:
        self.calls.append({"query": query, **kwargs})
        if self._fail:
            raise RuntimeError("qdrant exploded")
        return [dict(item) for item in self._results]

    def _lexical_catalog_search(self, **_kwargs: Any) -> List[Dict[str, Any]]:
        return []


@pytest.fixture
def client() -> TestClient:
    # Без контекстного менеджера: lifespan NiceGUI требует ui.run().
    return TestClient(app)


@pytest.fixture
def acl_env(tmp_path, monkeypatch):
    """Cloud Drive реестр с двумя папками, из которых пользователю открыта одна."""
    cfg = {
        "catalog_path": str(tmp_path / "catalog"),
        "users_db_path": str(tmp_path / "rag_users.db"),
        "cloud_drive_enabled": False,
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен")
    allowed = service.registry.upsert_folder(path="Allowed", name="Allowed", parent_id=root.id, depth=1)
    blocked = service.registry.upsert_folder(path="Blocked", name="Blocked", parent_id=root.id, depth=1)
    allowed_file = service.registry.upsert_file(
        folder_id=allowed.id,
        path="Allowed/report.txt",
        name="report.txt",
        storage_key="objects/sha256/aa/bb/aabb.txt",
        mime_type="text/plain",
        size_bytes=12,
        checksum="aabb",
        source_path="",
    )
    blocked_file = service.registry.upsert_file(
        folder_id=blocked.id,
        path="Blocked/secret.txt",
        name="secret.txt",
        storage_key="objects/sha256/cc/dd/ccdd.txt",
        mime_type="text/plain",
        size_bytes=12,
        checksum="ccdd",
        source_path="",
    )
    service.grant_path_permission(subject_type="user", subject_id="ivan", path="Allowed", access_level="viewer")

    auth = UserAuthDB(cfg["users_db_path"])
    assert auth.admin_create_user(username="ivan", password=_PASSWORD, role="user", must_change_password=False)
    token = auth.create_session(username="ivan")

    results = [
        {
            "score": 0.91,
            "type": "content",
            "text": "  Квартальный   отчёт  по продажам ",
            "filename": "report.txt",
            "path": "Allowed/report.txt",
            "cloud_file_id": allowed_file.id,
            "cloud_path": "Allowed/report.txt",
            "page": 2,
            "retrieval_source": "dense",
        },
        {
            "score": 0.88,
            "type": "content",
            "text": "Секретные зарплаты",
            "filename": "secret.txt",
            "path": "Blocked/secret.txt",
            "cloud_file_id": blocked_file.id,
            "cloud_path": "Blocked/secret.txt",
            "retrieval_source": "lexical",
        },
    ]
    searcher = _FakeSearcher(results)
    ui_helpers._CD_SERVICE_CACHE.clear()
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_get_api_searcher", lambda _cfg: searcher)
    monkeypatch.setattr(cloud_api, "_audit_cloud_drive_api_event", lambda *_a, **_k: None)
    yield {"cfg": cfg, "token": token, "searcher": searcher, "auth": auth}
    ui_helpers._CD_SERVICE_CACHE.clear()


def test_search_requires_authorization(client, acl_env) -> None:
    response = client.get("/api/search", params={"q": "отчёт"})

    assert response.status_code == 401
    assert "detail" in response.json()


def test_search_rejects_invalid_session(client, acl_env) -> None:
    response = client.get("/api/search", params={"q": "отчёт"}, headers={"Authorization": "Bearer nope"})

    assert response.status_code == 401


def test_search_returns_only_permitted_documents(client, acl_env) -> None:
    headers = {"Authorization": f"Bearer {acl_env['token']}"}

    response = client.get("/api/search", params={"q": "отчёт", "limit": 5, "type": "txt"}, headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["query"] == "отчёт"
    assert body["total"] == 1
    assert [item["filename"] for item in body["results"]] == ["report.txt"]
    assert "secret" not in response.text
    item = body["results"][0]
    assert item["path"] == "Allowed/report.txt"
    assert item["score"] == 0.91
    assert item["snippet"] == "Квартальный отчёт по продажам"
    assert item["page"] == 2
    assert item["sheet"] is None
    assert item["cloud_file_id"] == acl_env["searcher"]._results[0]["cloud_file_id"]
    diagnostics = body["diagnostics"]
    assert diagnostics["channels"] == {"dense": 1}
    assert diagnostics["retrieved"] == 2
    assert diagnostics["acl_filtered"] == 1

    call = acl_env["searcher"].calls[0]
    assert call["query"] == "отчёт"
    assert call["limit"] == 5
    assert call["file_type"] == ".txt"
    assert call["source"] == "api"
    assert call["username"] == "ivan"


def test_search_admin_sees_everything(client, acl_env) -> None:
    auth = acl_env["auth"]
    assert auth.admin_create_user(username="boss", password=_PASSWORD, role="admin", must_change_password=False)
    token = auth.create_session(username="boss")

    response = client.get("/api/search", params={"q": "отчёт"}, headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200
    assert sorted(item["filename"] for item in response.json()["results"]) == ["report.txt", "secret.txt"]


def test_search_validates_query_and_limit(client, acl_env) -> None:
    headers = {"Authorization": f"Bearer {acl_env['token']}"}

    assert client.get("/api/search", params={"q": "   "}, headers=headers).status_code == 400
    assert client.get("/api/search", params={"q": "x", "limit": 0}, headers=headers).status_code == 400
    assert client.get("/api/search", params={"q": "x", "limit": 1000}, headers=headers).status_code == 400
    response = client.get(
        "/api/search", params={"q": "x", "content_only": True, "title_only": True}, headers=headers
    )
    assert response.status_code == 400
    assert "detail" in response.json()


def test_search_reports_backend_failure_as_500(client, acl_env, monkeypatch) -> None:
    failing = _FakeSearcher([], fail=True)
    monkeypatch.setattr(cloud_api, "_get_api_searcher", lambda _cfg: failing)
    headers = {"Authorization": f"Bearer {acl_env['token']}"}

    response = client.get("/api/search", params={"q": "отчёт"}, headers=headers)

    assert response.status_code == 500
    assert "qdrant exploded" in response.json()["detail"]


def test_search_applies_query_operators_after_acl(client, acl_env) -> None:
    headers = {"Authorization": f"Bearer {acl_env['token']}"}

    response = client.get("/api/search", params={"q": "отчёт -продажам"}, headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 0
    assert body["diagnostics"]["operators"]["excluded_words"] == ["продажам"]
    assert acl_env["searcher"].calls[-1]["query"] == "отчёт"


def test_get_api_searcher_reuses_ui_cache(monkeypatch) -> None:
    cfg = {"qdrant_url": "http://localhost:6333", "collection_name": "catalog"}
    searcher = _FakeSearcher([])
    ui_helpers._SEARCHER_CACHE.clear()
    ui_helpers._SEARCHER_CACHE[ui_helpers._searcher_cache_key(cfg)] = searcher
    monkeypatch.setattr(ui_helpers, "_qdrant_http_ready", lambda _cfg: True)

    assert cloud_api._get_api_searcher(cfg) is searcher
    ui_helpers._SEARCHER_CACHE.clear()
