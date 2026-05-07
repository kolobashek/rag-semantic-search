"""Helpers for embedding-model aware Qdrant collection names."""

from __future__ import annotations

import re
from typing import Any, Mapping


def embedding_model_slug(model_name: str) -> str:
    """Return a stable collection-safe slug for an embedding model name."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", str(model_name or "").strip().lower()).strip("_")
    return slug or "embedding"


def resolve_embedding_collection_name(
    base_collection: str,
    embedding_model: str,
    *,
    enabled: bool = False,
    suffix: str = "",
) -> str:
    """Resolve collection name for side-by-side embedding model migrations.

    When enabled, `catalog` + `BAAI/bge-m3` becomes `catalog__baai_bge_m3`.
    This lets old and new embedding models coexist in Qdrant for A/B evaluation.
    """
    base = str(base_collection or "catalog").strip() or "catalog"
    if not enabled:
        return base
    explicit_suffix = str(suffix or "").strip()
    model_part = explicit_suffix if explicit_suffix else embedding_model_slug(embedding_model)
    if not model_part:
        return base
    marker = f"__{model_part}"
    if base.endswith(marker):
        return base
    return f"{base}{marker}"


def resolve_collection_name_from_config(config: Mapping[str, Any]) -> str:
    return resolve_embedding_collection_name(
        str(config.get("collection_name") or config.get("qdrant_collection") or "catalog"),
        str(config.get("embedding_model") or ""),
        enabled=bool(config.get("embedding_collection_versioning", False)),
        suffix=str(config.get("embedding_collection_suffix") or ""),
    )
