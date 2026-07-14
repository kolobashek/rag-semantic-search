from __future__ import annotations

from types import SimpleNamespace

from rag_catalog.cli.finalize_search_index import (
    collection_readiness,
    finalize_collection,
    sample_payload_integrity,
)


def _info(*, points: int, indexed: int, schema: tuple[str, ...], status: str = "green", optimizer: str = "ok"):
    return SimpleNamespace(
        status=status,
        optimizer_status=optimizer,
        points_count=points,
        indexed_vectors_count=indexed,
        payload_schema={key: object() for key in schema},
    )


def test_collection_readiness_rejects_green_collection_without_hnsw_or_fulltext() -> None:
    result = collection_readiness(
        _info(points=500_000, indexed=0, schema=("type", "extension")),
        require_fulltext=True,
        max_unindexed_vectors=50_000,
    )

    assert result["ready"] is False
    assert "fulltext_index_missing" in result["reasons"]
    assert "unindexed_vectors=500000" in result["reasons"]


def test_collection_readiness_accepts_small_unindexed_tail() -> None:
    result = collection_readiness(
        _info(points=500_000, indexed=475_000, schema=("type", "extension", "text")),
        require_fulltext=True,
        max_unindexed_vectors=50_000,
    )

    assert result["ready"] is True
    assert result["unindexed_vectors"] == 25_000


class _Client:
    def __init__(self) -> None:
        self.payload_indexes: list[dict] = []
        self.optimizer_updates: list[dict] = []
        self.infos = [
            _info(points=100_000, indexed=0, schema=("type", "extension", "text"), optimizer="optimizing"),
            _info(points=100_000, indexed=100_000, schema=("type", "extension", "text")),
        ]

    def create_payload_index(self, **kwargs) -> None:
        self.payload_indexes.append(kwargs)

    def update_collection(self, **kwargs) -> None:
        self.optimizer_updates.append(kwargs)

    def get_collection(self, _collection_name: str):
        return self.infos.pop(0)

    def scroll(self, **_kwargs):
        return (
            [
                SimpleNamespace(
                    payload={
                        "type": "pdf_content",
                        "text": "условия договора",
                        "full_path": r"O:\Договор.pdf",
                        "doc_id": "doc-1",
                        "payload_schema_version": 3,
                        "chunk_index": 0,
                    }
                )
            ],
            None,
        )


def test_finalize_collection_waits_for_payload_and_vector_indexes(monkeypatch) -> None:
    client = _Client()
    monkeypatch.setattr("rag_catalog.cli.finalize_search_index.time.sleep", lambda _seconds: None)

    result = finalize_collection(
        client,
        collection_name="catalog_v2",
        indexing_threshold=20_000,
        require_fulltext=True,
        timeout_sec=60,
        poll_seconds=0.1,
        max_unindexed_vectors=10_000,
        payload_sample_size=100,
    )

    assert result["ready"] is True
    assert any(call["field_name"] == "text" and call["wait"] is True for call in client.payload_indexes)
    optimizer = client.optimizer_updates[0]["optimizers_config"]
    assert optimizer.indexing_threshold == 20_000
    assert result["payload_integrity"]["ok"] is True


def test_payload_integrity_rejects_missing_content_contract_fields() -> None:
    class _BrokenClient:
        def scroll(self, **_kwargs):
            return ([SimpleNamespace(payload={"type": "pdf_content", "text": "текст"})], None)

    result = sample_payload_integrity(
        _BrokenClient(),
        collection_name="catalog_v2",
        sample_size=1_000,
    )

    assert result["ok"] is False
    assert result["missing_fields"] == {
        "content.chunk_index": 1,
        "doc_id": 1,
        "full_path": 1,
        "payload_schema_version": 1,
    }
