"""Small BM25 utilities for metadata/title retrieval."""

from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Sequence

_TOKEN_RE = re.compile(r"[a-zа-яё0-9\-]{2,}", flags=re.IGNORECASE)
_STOPWORDS = {"и", "или", "по", "на", "в", "во", "от", "для", "мне", "нужен", "нужна"}


def tokenize(text: str) -> List[str]:
    """Tokenize mixed Russian/Latin file metadata for lexical retrieval."""
    terms: List[str] = []
    seen: set[str] = set()
    for raw in _TOKEN_RE.findall(text or ""):
        term = raw.lower().replace("ё", "е")
        if term in _STOPWORDS:
            continue
        if term not in seen:
            terms.append(term)
            seen.add(term)
    return terms


def _matches(token: str, query_term: str) -> bool:
    if query_term in token:
        return True
    if len(query_term) >= 5:
        stem = query_term.rstrip("аеиоуыьъйяю")
        return len(stem) >= 4 and stem in token
    return False


def bm25_rank_items(
    items: Iterable[Dict[str, Any]],
    query_terms: Sequence[str],
    *,
    limit: int = 50,
    k1: float = 1.2,
    b: float = 0.75,
) -> List[Dict[str, Any]]:
    """Rank file/folder metadata items with BM25 over filename and path.

    This intentionally stays local and dependency-free. It complements semantic
    Qdrant search in retrieval v2 for exact names, numbers, VINs and folder paths.
    """
    query = [term.lower().replace("ё", "е") for term in query_terms if str(term or "").strip()]
    if not query:
        return []

    docs: List[tuple[Dict[str, Any], List[str]]] = []
    df: Counter[str] = Counter()
    for item in items:
        # Cache tokenization on the reused filesystem-cache dicts. BM25 still computes
        # query-specific IDF each run, but avoids retokenizing tens of thousands of paths.
        cached_tokens = item.get("_bm25_tokens")
        if isinstance(cached_tokens, list):
            tokens = [str(token) for token in cached_tokens]
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
        docs.append((item, tokens))
        unique_tokens = set(tokens)
        for term in query:
            if any(_matches(token, term) for token in unique_tokens):
                df[term] += 1

    if not docs:
        return []

    total_docs = len(docs)
    avgdl = sum(len(tokens) for _item, tokens in docs) / max(1, total_docs)
    scored: List[Dict[str, Any]] = []
    for item, tokens in docs:
        tf = Counter(tokens)
        doc_len = len(tokens)
        score = 0.0
        matched_terms = 0
        for term in query:
            matching_tokens = [token for token in tf if _matches(token, term)]
            freq = sum(tf[token] for token in matching_tokens)
            if freq <= 0:
                continue
            matched_terms += 1
            idf = math.log(1.0 + (total_docs - df[term] + 0.5) / (df[term] + 0.5))
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
