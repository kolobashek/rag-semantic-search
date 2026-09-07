from __future__ import annotations

from rag_catalog.core import rag_core
from rag_catalog.core.retrieval import bm25, terms
from rag_core import RAGSearcher


def _searcher() -> RAGSearcher:
    s = RAGSearcher.__new__(RAGSearcher)
    s.config = {}
    s.telemetry = None
    return s


def test_term_aliases_have_single_source_with_union_of_keys() -> None:
    assert rag_core._TERM_ALIASES is terms.TERM_ALIASES
    assert bm25._TERM_ALIASES is terms.TERM_ALIASES
    # Keys from the old rag_core copy and the old bm25 copy are both present.
    for key in ("touareg", "туарег", "volkswagen", "фольксваген", "обслуживания", "технических", "vin"):
        assert key in terms.TERM_ALIASES
    assert "шильдик" in terms.TERM_ALIASES["vin"]


def test_term_variants_identical_from_rag_core_and_bm25() -> None:
    s = _searcher()
    for term in ("Touareg", "VIN", "PC300", "об0рудование", "фольксваген", "паспорт"):
        assert s._term_variants(term) == bm25._term_variants(term) == terms.term_variants(term)


def test_tokenize_identical_from_rag_core_and_bm25() -> None:
    s = _searcher()
    text = "Паспорт для PC300 и Ёлка-2 по договору 9980 782471"
    assert s._terms_from_text(text) == bm25.tokenize(text) == terms.tokenize(text)
    assert terms.tokenize(text) == ["паспорт", "pc300", "елка-2", "договору", "9980", "782471"]


def test_term_matches_uses_aliases_and_stems() -> None:
    s = _searcher()
    assert s._term_matches("шильдик экскаватора", "vin") is True
    assert terms.term_matches("шильдик экскаватора", "vin") is True
    assert s._term_matches("обслуживание техники", "обслуживания") is True
    assert s._term_matches("pc3oo паспорт", "pc300") is True
    assert s._term_matches("ничего общего", "паспорт") is False
