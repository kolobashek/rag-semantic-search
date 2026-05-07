from __future__ import annotations

from rag_catalog.core.embedding_collections import (
    embedding_model_slug,
    resolve_collection_name_from_config,
    resolve_embedding_collection_name,
)


def test_embedding_model_slug_is_qdrant_collection_safe() -> None:
    assert embedding_model_slug("BAAI/bge-m3") == "baai_bge_m3"
    assert embedding_model_slug("intfloat/multilingual-e5-large") == "intfloat_multilingual_e5_large"


def test_resolve_embedding_collection_name_is_opt_in_and_idempotent() -> None:
    assert resolve_embedding_collection_name("catalog", "BAAI/bge-m3") == "catalog"
    assert resolve_embedding_collection_name("catalog", "BAAI/bge-m3", enabled=True) == "catalog__baai_bge_m3"
    assert (
        resolve_embedding_collection_name("catalog__baai_bge_m3", "BAAI/bge-m3", enabled=True)
        == "catalog__baai_bge_m3"
    )


def test_resolve_collection_name_from_config_supports_explicit_suffix() -> None:
    assert (
        resolve_collection_name_from_config(
            {
                "collection_name": "catalog",
                "embedding_model": "ignored",
                "embedding_collection_versioning": True,
                "embedding_collection_suffix": "v2_bge_m3",
            }
        )
        == "catalog__v2_bge_m3"
    )
