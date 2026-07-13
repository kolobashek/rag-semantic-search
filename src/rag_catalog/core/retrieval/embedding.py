"""Embedding input conventions shared by indexing and querying."""

from __future__ import annotations

from typing import Iterable


def uses_e5_prefixes(model_name: str) -> bool:
    """Return whether the model expects E5 query/passage prefixes."""
    clean = str(model_name or "").strip().lower().replace("\\", "/")
    return "multilingual-e5" in clean or clean.startswith("intfloat/e5-")


def prepare_query_text(model_name: str, text: str) -> str:
    clean = str(text or "").strip()
    if uses_e5_prefixes(model_name) and not clean.lower().startswith("query: "):
        return f"query: {clean}"
    return clean


def prepare_passage_text(model_name: str, text: str) -> str:
    clean = str(text or "").strip()
    if uses_e5_prefixes(model_name) and not clean.lower().startswith("passage: "):
        return f"passage: {clean}"
    return clean


def prepare_passage_texts(model_name: str, texts: Iterable[str]) -> list[str]:
    return [prepare_passage_text(model_name, text) for text in texts]
