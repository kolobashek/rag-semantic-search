from __future__ import annotations

from types import SimpleNamespace

import pytest

from rag_catalog.core.exact_tokens import numeric_exact_tokens, query_numeric_tokens
from rag_core import RAGSearcher

VARIANTS = ["9980 782471", "9980№782471", "9980 № 782471", "9980782471", "9980-782471", "9980 #782471"]


@pytest.mark.parametrize("query", VARIANTS)
def test_query_numeric_tokens_always_contain_joined_number(query: str) -> None:
    tokens = query_numeric_tokens(query)
    assert "9980782471" in tokens


@pytest.mark.parametrize("query", VARIANTS[:3])
def test_split_variants_yield_identical_token_sets(query: str) -> None:
    assert set(query_numeric_tokens(query)) == {"9980", "782471", "9980782471"}


def test_indexed_text_with_number_sign_matches_joined_query_token() -> None:
    indexed = set(numeric_exact_tokens("Свидетельство 9980 № 782471 выдано"))
    assert set(query_numeric_tokens("9980782471")) <= indexed
    assert set(query_numeric_tokens("9980 782471")) <= indexed


def test_letters_between_groups_prevent_joining() -> None:
    assert "9980782471" not in query_numeric_tokens("9980 и 782471")


class _Scroll:
    def __init__(self) -> None:
        self.filters: list[list[str]] = []

    def scroll(self, **kwargs):
        must = kwargs["scroll_filter"].must
        self.filters.append([cond.match.value for cond in must if cond.key == "numeric_tokens"])
        return [], None


@pytest.mark.parametrize("query", ["СТС 9980 782471", "СТС 9980№782471", "СТС 9980782471"])
def test_numeric_exact_channel_queries_joined_token_first(query: str) -> None:
    s = RAGSearcher.__new__(RAGSearcher)
    s.connected = True
    s.collection_name = "catalog"
    s.config = {}
    s.qdrant = _Scroll()
    s.telemetry = SimpleNamespace()

    s._numeric_exact_search(query=query, limit=20, file_type=None, content_only=False)

    assert s.qdrant.filters[0] == ["9980782471"]
