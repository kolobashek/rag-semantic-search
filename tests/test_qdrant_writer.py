from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from qdrant_client.models import PointStruct

from rag_catalog.core.indexing import delete_file_vectors, ensure_collection, upsert_points


class _FakeClient:
    def __init__(self, collections: list[str] | None = None) -> None:
        self.collections = list(collections or [])
        self.deleted_collections: list[str] = []
        self.created: list[tuple[str, object]] = []
        self.deleted_points: list[dict] = []
        self.upserted: list[tuple[str, int]] = []

    def get_collections(self):
        return SimpleNamespace(collections=[SimpleNamespace(name=name) for name in self.collections])

    def delete_collection(self, name: str) -> None:
        self.deleted_collections.append(name)
        if name in self.collections:
            self.collections.remove(name)

    def create_collection(self, *, collection_name: str, vectors_config) -> None:
        self.created.append((collection_name, vectors_config))
        self.collections.append(collection_name)

    def delete(self, **kwargs) -> None:
        self.deleted_points.append(kwargs)

    def upsert(self, collection_name: str, points) -> None:
        self.upserted.append((collection_name, len(points)))


def test_ensure_collection_creates_missing_collection() -> None:
    client = _FakeClient()

    recreated = ensure_collection(client, collection_name="catalog", vector_size=384)

    assert recreated is False
    assert client.created[0][0] == "catalog"


def test_ensure_collection_recreates_existing_collection() -> None:
    client = _FakeClient(["catalog"])

    recreated = ensure_collection(client, collection_name="catalog", vector_size=384, recreate=True)

    assert recreated is True
    assert client.deleted_collections == ["catalog"]
    assert client.created[0][0] == "catalog"


def test_delete_file_vectors_uses_payload_identity_when_present() -> None:
    client = _FakeClient(["catalog"])

    delete_file_vectors(
        client,
        collection_name="catalog",
        filepath=Path("ignored.docx"),
        timeout_sec=5,
        payload_match={"cloud_file_id": "file-1", "empty": ""},
    )

    call = client.deleted_points[0]
    assert call["collection_name"] == "catalog"
    assert call["timeout"] == 5
    conditions = call["points_selector"].filter.must
    assert [condition.key for condition in conditions] == ["cloud_file_id"]


def test_upsert_points_returns_written_count() -> None:
    client = _FakeClient(["catalog"])
    points = [PointStruct(id="p1", vector=[0.1, 0.2], payload={"x": 1})]

    assert upsert_points(client, collection_name="catalog", points=points) == 1
    assert client.upserted == [("catalog", 1)]
