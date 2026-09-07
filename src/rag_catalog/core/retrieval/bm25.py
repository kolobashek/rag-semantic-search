"""Small BM25 utilities for metadata/title retrieval."""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from .terms import STOPWORDS as _STOPWORDS
from .terms import TERM_ALIASES as _TERM_ALIASES
from .terms import TOKEN_RE as _TOKEN_RE
from .terms import term_needles as _needles
from .terms import term_variants as _term_variants
from .terms import tokenize

__all__ = [
    "_STOPWORDS",
    "_TERM_ALIASES",
    "_TOKEN_RE",
    "_needles",
    "_term_variants",
    "bm25_rank_indexed_items",
    "bm25_rank_items",
    "prepare_bm25_items",
    "tokenize",
]


def prepare_bm25_items(items: Iterable[Dict[str, Any]]) -> int:
    """Tokenize reusable metadata items once, outside the timed search path."""
    prepared = 0
    for item in items:
        if isinstance(item.get("_bm25_tokens"), list):
            continue
        filename = str(item.get("filename") or "")
        path = str(item.get("path") or "")
        item["_bm25_tokens"] = tokenize(f"{filename} {filename} {path}")
        prepared += 1
    return prepared


def bm25_rank_items(
    items: Iterable[Dict[str, Any]],
    query_terms: Sequence[str],
    *,
    limit: int = 50,
    k1: float = 1.2,
    b: float = 0.75,
    corpus_size: int | None = None,
    average_doc_length: float | None = None,
) -> List[Dict[str, Any]]:
    """Rank file/folder metadata items with BM25 over filename and path.

    This intentionally stays local and dependency-free. It complements semantic
    Qdrant search in retrieval v2 for exact names, numbers, VINs and folder paths.
    """
    query = [term.lower().replace("ё", "е") for term in query_terms if str(term or "").strip()]
    if not query:
        return []

    query_needles = [_needles(term) for term in query]
    token_matches: Dict[str, tuple[bool, ...]] = {}
    matched_docs: List[tuple[Dict[str, Any], int, List[int]]] = []
    df = [0] * len(query)
    total_docs = 0
    total_doc_len = 0
    for item in items:
        # Cache tokenization on the reused filesystem-cache dicts. BM25 still computes
        # query-specific IDF each run, but avoids retokenizing tens of thousands of paths.
        cached_tokens = item.get("_bm25_tokens")
        if isinstance(cached_tokens, list):
            tokens = cached_tokens
        else:
            filename = str(item.get("filename") or "")
            path = str(item.get("path") or "")
            # Filename gets repeated because users usually expect title matches to beat
            # incidental parent path matches.
            tokens = tokenize(f"{filename} {filename} {path}")
            try:
                item["_bm25_tokens"] = tokens
            except Exception:
                pass
        if not tokens:
            continue
        total_docs += 1
        total_doc_len += len(tokens)
        frequencies = [0] * len(query)
        for token in tokens:
            matches = token_matches.get(token)
            if matches is None:
                matches = tuple(any(needle in token for needle in needles) for needles in query_needles)
                token_matches[token] = matches
            for idx, matched in enumerate(matches):
                if matched:
                    frequencies[idx] += 1
        if any(frequencies):
            matched_docs.append((item, len(tokens), frequencies))
            for idx, frequency in enumerate(frequencies):
                if frequency:
                    df[idx] += 1

    if not total_docs:
        return []

    scoring_total_docs = max(total_docs, int(corpus_size or 0))
    avgdl = float(average_doc_length or 0.0) or (total_doc_len / total_docs)
    scored: List[Dict[str, Any]] = []
    for item, doc_len, frequencies in matched_docs:
        score = 0.0
        matched_terms = 0
        for idx, freq in enumerate(frequencies):
            if freq <= 0:
                continue
            matched_terms += 1
            idf = math.log(1.0 + (scoring_total_docs - df[idx] + 0.5) / (df[idx] + 0.5))
            denom = freq + k1 * (1.0 - b + b * doc_len / max(avgdl, 1e-9))
            score += idf * (freq * (k1 + 1.0)) / max(denom, 1e-9)
        if score <= 0.0:
            continue
        ranked = dict(item)
        ranked["bm25_score"] = score
        ranked["bm25_matched_terms"] = matched_terms
        scored.append(ranked)

    if not scored:
        return []

    max_score = max(float(item["bm25_score"]) for item in scored)
    for item in scored:
        normalized = float(item["bm25_score"]) / max(max_score, 1e-9)
        # Keep score on the same broad 0..1 scale as existing lexical metadata.
        item["score"] = round(0.70 + 0.29 * normalized, 6)
        item["rank_reason"] = "BM25 совпадение в имени/пути"

    scored.sort(
        key=lambda item: (
            float(item.get("bm25_score") or 0),
            int(item.get("bm25_matched_terms") or 0),
            1 if str(item.get("kind") or "") == "file" else 0,
            -len(str(item.get("path") or "")),
        ),
        reverse=True,
    )
    return scored[: max(1, int(limit))]


def bm25_rank_indexed_items(
    items: Sequence[Dict[str, Any]],
    candidate_indices: Sequence[int],
    query_terms: Sequence[str],
    *,
    token_docs: Mapping[str, Sequence[int]],
    sorted_tokens: Sequence[str],
    limit: int = 50,
    k1: float = 1.2,
    b: float = 0.75,
    corpus_size: int | None = None,
    average_doc_length: float | None = None,
) -> List[Dict[str, Any]]:
    """Rank cached metadata through token postings instead of rescanning document tokens."""
    query = [term.lower().replace("ё", "е") for term in query_terms if str(term or "").strip()]
    if not query or not candidate_indices:
        return []

    allowed = set(candidate_indices)
    total_docs = 0
    total_doc_len = 0
    doc_lengths: Dict[int, int] = {}
    for item_index in candidate_indices:
        tokens = items[item_index].get("_bm25_tokens")
        if not isinstance(tokens, list) or not tokens:
            continue
        doc_length = len(tokens)
        doc_lengths[item_index] = doc_length
        total_docs += 1
        total_doc_len += doc_length
    if not total_docs:
        return []

    frequencies_by_doc: Dict[int, List[int]] = {}
    df = [0] * len(query)
    query_needles = [_needles(term) for term in query]
    for term_index, needles in enumerate(query_needles):
        frequencies: Dict[int, int] = {}
        for token in sorted_tokens:
            if not any(needle in token for needle in needles):
                continue
            for item_index in token_docs.get(token, ()):
                if item_index in allowed:
                    frequencies[item_index] = frequencies.get(item_index, 0) + 1
        df[term_index] = len(frequencies)
        for item_index, frequency in frequencies.items():
            per_term = frequencies_by_doc.get(item_index)
            if per_term is None:
                per_term = [0] * len(query)
                frequencies_by_doc[item_index] = per_term
            per_term[term_index] = frequency

    scoring_total_docs = max(total_docs, int(corpus_size or 0))
    avgdl = float(average_doc_length or 0.0) or (total_doc_len / total_docs)
    scored: List[Dict[str, Any]] = []
    for item_index in sorted(frequencies_by_doc):
        frequencies = frequencies_by_doc[item_index]
        doc_len = doc_lengths.get(item_index, 0)
        if not doc_len:
            continue
        score = 0.0
        matched_terms = 0
        for term_index, frequency in enumerate(frequencies):
            if frequency <= 0:
                continue
            matched_terms += 1
            idf = math.log(
                1.0 + (scoring_total_docs - df[term_index] + 0.5) / (df[term_index] + 0.5)
            )
            denom = frequency + k1 * (1.0 - b + b * doc_len / max(avgdl, 1e-9))
            score += idf * (frequency * (k1 + 1.0)) / max(denom, 1e-9)
        if score <= 0.0:
            continue
        ranked = dict(items[item_index])
        ranked["bm25_score"] = score
        ranked["bm25_matched_terms"] = matched_terms
        scored.append(ranked)

    if not scored:
        return []
    max_score = max(float(item["bm25_score"]) for item in scored)
    for item in scored:
        normalized = float(item["bm25_score"]) / max(max_score, 1e-9)
        item["score"] = round(0.70 + 0.29 * normalized, 6)
        item["rank_reason"] = "BM25 совпадение в имени/пути"
    scored.sort(
        key=lambda item: (
            float(item.get("bm25_score") or 0),
            int(item.get("bm25_matched_terms") or 0),
            1 if str(item.get("kind") or "") == "file" else 0,
            -len(str(item.get("path") or "")),
        ),
        reverse=True,
    )
    return scored[: max(1, int(limit))]
