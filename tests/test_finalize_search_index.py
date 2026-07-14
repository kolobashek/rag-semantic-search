from __future__ import annotations

from types import SimpleNamespace

from qdrant_client.models import Filter

from rag_catalog.cli.finalize_search_index import (
    collection_readiness,
    finalize_collection,
    sample_payload_integrity,
    scan_payload_integrity,
)


def _info(*, points: int, indexed: int, schema: tuple[str, ...], status: str = "green", optimizer: str = "ok"):
    return SimpleNamespace(
        status=status,
        optimizer_status=optimizer,
        points_count=points,
        indexed_vectors_count=indexed,
        payload_schema={
            key: SimpleNamespace(data_type="text" if key == "text" else "keyword")
            for key in schema
        },
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
    assert result["payload_index_types"]["text"] == "text"


def test_collection_readiness_rejects_text_field_with_keyword_index() -> None:
    info = _info(points=100, indexed=100, schema=("type", "text"))
    info.payload_schema["text"] = SimpleNamespace(data_type="keyword")

    result = collection_readiness(
        info,
        require_fulltext=True,
        max_unindexed_vectors=0,
    )

    assert result["ready"] is False
    assert result["fulltext_ready"] is False
    assert "fulltext_index_wrong_type=keyword" in result["reasons"]


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


def test_finalize_collection_can_require_full_spreadsheet_audit(monkeypatch) -> None:
    client = _Client()
    monkeypatch.setattr("rag_catalog.cli.finalize_search_index.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        "rag_catalog.cli.finalize_search_index.sample_payload_integrity",
        lambda *_args, **_kwargs: {"ok": True, "scanned_all": False},
    )
    full_audit_calls: list[dict] = []

    def _full_audit(*_args, **kwargs):
        full_audit_calls.append(kwargs)
        return {"ok": True, "scanned_all": True, "sample_size": 1234}

    monkeypatch.setattr(
        "rag_catalog.cli.finalize_search_index.scan_payload_integrity",
        _full_audit,
    )

    result = finalize_collection(
        client,
        collection_name="catalog_v2",
        indexing_threshold=20_000,
        require_fulltext=True,
        timeout_sec=60,
        poll_seconds=0.1,
        max_unindexed_vectors=10_000,
        payload_sample_size=100,
        spreadsheet_sample_size=2_000,
        spreadsheet_full_audit=True,
    )

    assert result["ready"] is True
    assert result["spreadsheet_integrity"]["scanned_all"] is True
    assert full_audit_calls[0]["batch_size"] == 2_000
    assert isinstance(full_audit_calls[0]["query_filter"], Filter)
    assert full_audit_calls[0]["required_content_fields"] == (
        "sheet",
        "row_start",
        "row_end",
        "spreadsheet_payload_schema_version",
    )
    assert full_audit_calls[0]["expected_content_values"] == {
        "spreadsheet_payload_schema_version": 2
    }


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
    assert result["sampling_strategy"] == "scroll_fallback"


def test_payload_integrity_uses_random_qdrant_sampling_when_available() -> None:
    class _RandomClient:
        query = None

        def query_points(self, **kwargs):
            self.query = kwargs["query"]
            return SimpleNamespace(
                points=[
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
                ]
            )

        def scroll(self, **_kwargs):
            raise AssertionError("scroll fallback must not be used")

    client = _RandomClient()
    result = sample_payload_integrity(client, collection_name="catalog_v2", sample_size=100)

    assert result["ok"] is True
    assert result["sampling_strategy"] == "random"
    assert client.query.sample.value == "random"


def test_payload_integrity_forwards_filter_and_requires_spreadsheet_provenance() -> None:
    class _RandomClient:
        kwargs = None

        def query_points(self, **kwargs):
            self.kwargs = kwargs
            return SimpleNamespace(
                points=[
                    SimpleNamespace(
                        payload={
                            "type": "xlsx_content",
                            "text": "Позиция 1 | Экскаватор | 1000 руб.",
                            "full_path": r"O:\\Прайс.xlsx",
                            "doc_id": "sheet-1",
                            "payload_schema_version": 3,
                            "chunk_index": 0,
                            "sheet": "Прайс",
                            "row_start": 1,
                            "row_end": 2,
                            "spreadsheet_payload_schema_version": 2,
                        }
                    )
                ]
            )

    query_filter = Filter()
    client = _RandomClient()
    result = sample_payload_integrity(
        client,
        collection_name="catalog_v2",
        sample_size=100,
        query_filter=query_filter,
        required_content_fields=(
            "sheet",
            "row_start",
            "row_end",
            "spreadsheet_payload_schema_version",
        ),
        expected_content_values={"spreadsheet_payload_schema_version": 2},
    )

    assert result["ok"] is True
    assert client.kwargs["query_filter"] is query_filter


def test_payload_integrity_rejects_old_spreadsheet_schema_version() -> None:
    class _LegacySpreadsheetClient:
        def scroll(self, **_kwargs):
            return (
                [
                    SimpleNamespace(
                        payload={
                            "type": "xlsx_content",
                            "text": "Позиция 1 | Экскаватор | 1000 руб.",
                            "full_path": r"O:\\Прайс.xlsx",
                            "doc_id": "sheet-1",
                            "payload_schema_version": 3,
                            "chunk_index": 0,
                            "sheet": "Прайс",
                            "row_start": 1,
                            "row_end": 2,
                            "spreadsheet_payload_schema_version": 1,
                        }
                    )
                ],
                None,
            )

    result = sample_payload_integrity(
        _LegacySpreadsheetClient(),
        collection_name="catalog_v2",
        sample_size=100,
        required_content_fields=(
            "sheet",
            "row_start",
            "row_end",
            "spreadsheet_payload_schema_version",
        ),
        expected_content_values={"spreadsheet_payload_schema_version": 2},
    )

    assert result["ok"] is False
    assert result["quality_violations"] == {
        "content.spreadsheet_payload_schema_version.unexpected_value": 1
    }


def test_full_payload_integrity_scrolls_every_page() -> None:
    class _PagedClient:
        offsets: list[object] = []

        def scroll(self, **kwargs):
            self.offsets.append(kwargs.get("offset"))
            base = {
                "type": "xlsx_content",
                "text": "Позиция 1 | Экскаватор | 1000 руб.",
                "full_path": r"O:\\Прайс.xlsx",
                "doc_id": "sheet-1",
                "payload_schema_version": 3,
                "sheet": "Прайс",
                "row_start": 1,
                "row_end": 2,
                "spreadsheet_payload_schema_version": 2,
            }
            if kwargs.get("offset") is None:
                return [SimpleNamespace(payload={**base, "chunk_index": 0})], "page-2"
            return [SimpleNamespace(payload={**base, "text": "обрывок", "chunk_index": 2})], None

    client = _PagedClient()
    result = scan_payload_integrity(
        client,
        collection_name="catalog_v2",
        batch_size=100,
        min_content_chars=120,
        query_filter=Filter(),
        required_content_fields=(
            "sheet",
            "row_start",
            "row_end",
            "spreadsheet_payload_schema_version",
        ),
        expected_content_values={"spreadsheet_payload_schema_version": 2},
    )

    assert client.offsets == [None, "page-2"]
    assert result["scanned_all"] is True
    assert result["sampling_strategy"] == "full_scroll"
    assert result["sample_size"] == 2
    assert result["quality_violations"] == {"content.short_noninitial": 1}
    assert result["ok"] is False


def test_finalize_collection_blocks_on_spreadsheet_integrity_failure() -> None:
    class _SpreadsheetBrokenClient:
        def create_payload_index(self, **_kwargs) -> None:
            pass

        def update_collection(self, **_kwargs) -> None:
            pass

        def get_collection(self, _collection_name: str):
            return _info(points=100, indexed=100, schema=("type", "extension", "text"))

        def query_points(self, **kwargs):
            if kwargs.get("query_filter") is None:
                payload = {
                    "type": "pdf_content",
                    "text": "условия договора",
                    "full_path": r"O:\\Договор.pdf",
                    "doc_id": "doc-1",
                    "payload_schema_version": 3,
                    "chunk_index": 0,
                }
            else:
                payload = {
                    "type": "xlsx_content",
                    "text": "обрывок",
                    "full_path": r"O:\\Прайс.xlsx",
                    "doc_id": "sheet-1",
                    "payload_schema_version": 3,
                    "chunk_index": 3,
                }
            return SimpleNamespace(points=[SimpleNamespace(payload=payload)])

    result = finalize_collection(
        _SpreadsheetBrokenClient(),
        collection_name="catalog_v2",
        indexing_threshold=20_000,
        require_fulltext=True,
        timeout_sec=60,
        poll_seconds=0.1,
        max_unindexed_vectors=0,
        payload_sample_size=10,
        min_content_chars=120,
        spreadsheet_sample_size=10,
    )

    assert result["ready"] is False
    assert result["spreadsheet_integrity"]["ok"] is False
    assert "spreadsheet_integrity_failed" in result["reasons"]
    assert result["spreadsheet_integrity"]["missing_fields"] == {
        "content.row_end": 1,
        "content.row_start": 1,
        "content.sheet": 1,
        "content.spreadsheet_payload_schema_version": 1,
    }
    assert result["spreadsheet_integrity"]["quality_violations"] == {
        "content.short_noninitial": 1,
    }


def test_payload_integrity_rejects_separator_only_content() -> None:
    class _BrokenClient:
        def scroll(self, **_kwargs):
            return (
                [
                    SimpleNamespace(
                        payload={
                            "type": "xlsx_content",
                            "text": "|  |  |  |",
                            "full_path": r"O:\noise.xlsx",
                            "doc_id": "doc-1",
                            "payload_schema_version": 3,
                            "chunk_index": 0,
                        }
                    )
                ],
                None,
            )

    result = sample_payload_integrity(_BrokenClient(), collection_name="catalog_v2", sample_size=100)

    assert result["ok"] is False
    assert result["quality_violations"] == {"content.separator_only": 1}


def test_payload_integrity_rejects_short_tail_but_allows_short_whole_document() -> None:
    class _ClientWithShortContent:
        def scroll(self, **_kwargs):
            base = {
                "type": "txt_content",
                "full_path": r"O:\note.txt",
                "doc_id": "doc-1",
                "payload_schema_version": 3,
            }
            return (
                [
                    SimpleNamespace(payload={**base, "text": "Краткая записка", "chunk_index": 0}),
                    SimpleNamespace(payload={**base, "text": "обрывок", "chunk_index": 2}),
                ],
                None,
            )

    result = sample_payload_integrity(
        _ClientWithShortContent(),
        collection_name="catalog_v2",
        sample_size=100,
        min_content_chars=120,
    )

    assert result["ok"] is False
    assert result["content_quality"]["short_under_min"] == 2
    assert result["quality_violations"] == {"content.short_noninitial": 1}
