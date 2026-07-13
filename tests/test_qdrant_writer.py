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
        self.payload_indexes: list[dict] = []
        self.delete_failures = 0

    def get_collections(self):
        return SimpleNamespace(collections=[SimpleNamespace(name=name) for name in self.collections])

    def delete_collection(self, name: str) -> None:
        self.deleted_collections.append(name)
        if name in self.collections:
            self.collections.remove(name)

    def create_collection(self, *, collection_name: str, vectors_config) -> None:
        self.created.append((collection_name, vectors_config))
        self.collections.append(collection_name)

    def create_payload_index(self, **kwargs) -> None:
        self.payload_indexes.append(kwargs)

    def delete(self, **kwargs) -> None:
        if self.delete_failures > 0:
            self.delete_failures -= 1
            raise TimeoutError("timed out")
        self.deleted_points.append(kwargs)

    def upsert(self, collection_name: str, points, **kwargs) -> None:
        self.upserted.append((collection_name, len(points), kwargs))


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


def test_ensure_collection_builds_russian_fulltext_index_when_enabled() -> None:
    client = _FakeClient()

    ensure_collection(
        client,
        collection_name="catalog_v2",
        vector_size=384,
        fulltext_enabled=True,
    )

    text_index = next(item for item in client.payload_indexes if item["field_name"] == "text")
    assert text_index["field_schema"].lowercase is True
    assert text_index["field_schema"].phrase_matching is True
    assert text_index["field_schema"].stemmer.language.value == "russian"


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


def test_delete_file_vectors_retries_transient_timeout(monkeypatch) -> None:
    client = _FakeClient(["catalog"])
    client.delete_failures = 1
    delays: list[float] = []
    monkeypatch.setattr("rag_catalog.core.indexing.qdrant_writer.time.sleep", lambda delay: delays.append(delay))

    delete_file_vectors(
        client,
        collection_name="catalog",
        filepath=Path("doc.pdf"),
        timeout_sec=5,
        retries=2,
    )

    assert len(client.deleted_points) == 1
    assert delays == [0.75]


def test_upsert_points_returns_written_count() -> None:
    client = _FakeClient(["catalog"])
    points = [PointStruct(id="p1", vector=[0.1, 0.2], payload={"x": 1})]

    assert upsert_points(client, collection_name="catalog", points=points) == 1
    assert client.upserted == [("catalog", 1, {"wait": False, "timeout": 60})]


def test_upsert_points_passes_timeout_and_retries() -> None:
    client = _FakeClient(["catalog"])
    points = [PointStruct(id="p1", vector=[0.1, 0.2], payload={"x": 1})]

    assert upsert_points(client, collection_name="catalog", points=points, timeout_sec=300, retries=1) == 1
    assert client.upserted == [("catalog", 1, {"wait": False, "timeout": 300})]
