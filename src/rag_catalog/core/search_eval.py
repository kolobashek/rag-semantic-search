"""Offline relevance evaluation helpers for RAG search."""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

_TOKEN_RE = re.compile(r"[a-zа-яё0-9\-]{2,}", flags=re.IGNORECASE)
_STOPWORDS = {"и", "или", "по", "на", "в", "во", "от", "для", "мне", "нужен", "нужна"}
_TERM_ALIASES: Dict[str, List[str]] = {
    "touareg": ["туарег", "фольксваген", "volkswagen", "vw"],
    "туарег": ["touareg", "фольксваген", "volkswagen", "vw"],
    "volkswagen": ["фольксваген", "vw"],
    "фольксваген": ["volkswagen", "vw"],
    "псм": ["паспорт самоходной машины", "электронного паспорта", "паспорт техники"],
    "птс": ["паспорт транспортного средства", "техпаспорт"],
    "стс": ["свидетельство о регистрации", "регистрации"],
    "vin": [
        "шильдик",
        "табличка",
        "заводская табличка",
        "паспорт транспортного средства",
        "свидетельство о регистрации",
        "птс",
        "стс",
    ],
}


@dataclass(frozen=True)
class GoldenQuery:
    query: str
    expected: List[str]
    category: str = "general"


def load_golden_queries(path: str | Path) -> List[GoldenQuery]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Golden set must be a JSON list.")
    out: List[GoldenQuery] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        query = str(item.get("query") or "").strip()
        expected = [str(x).strip() for x in item.get("expected", []) if str(x).strip()]
        category = str(item.get("category") or "general").strip() or "general"
        if query and expected:
            out.append(GoldenQuery(query=query, expected=expected, category=category))
    if not out:
        raise ValueError("Golden set is empty.")
    return out


def _result_text(result: Dict[str, Any]) -> str:
    return " ".join(
        str(result.get(key) or "")
        for key in ("filename", "path", "full_path", "cloud_path", "text")
    ).lower().replace("ё", "е")


def _expected_text(value: str) -> str:
    return str(value or "").lower().replace("ё", "е")


def _stem(term: str) -> str:
    clean = _expected_text(term)
    if len(clean) < 5:
        return clean
    stem = clean.rstrip("аеиоуыьъйяю")
    return stem if len(stem) >= 4 else clean


def _term_variants(term: str) -> List[str]:
    clean = _expected_text(term)
    variants = [clean]
    for alias in _TERM_ALIASES.get(clean, []):
        alias_norm = _expected_text(alias)
        if alias_norm and alias_norm not in variants:
            variants.append(alias_norm)
    stem = _stem(clean)
    if stem and stem != clean:
        variants.append(stem)
    return variants


def _text_matches_expected(text: str, expected: str) -> bool:
    haystack = _expected_text(text)
    if not haystack:
        return False
    for variant in _term_variants(expected):
        if not variant:
            continue
        if " " in variant:
            if variant in haystack:
                return True
            continue
        if variant in haystack:
            return True
        variant_stem = _stem(variant)
        if len(variant_stem) >= 4:
            tokens = [
                token
                for token in _TOKEN_RE.findall(haystack)
                if token not in _STOPWORDS
            ]
            if any(token.startswith(variant_stem) or variant_stem in token for token in tokens):
                return True
    return False


def relevance_vector(results: Iterable[Dict[str, Any]], expected: List[str], *, limit: int) -> List[int]:
    needles = [_expected_text(item) for item in expected if item]
    vector: List[int] = []
    for result in list(results)[: max(1, int(limit))]:
        haystack = _result_text(result)
        vector.append(1 if any(_text_matches_expected(haystack, needle) for needle in needles) else 0)
    return vector


def recall_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    needles = [_expected_text(item) for item in expected if item]
    if not needles:
        return 0.0
    top_texts = [_result_text(item) for item in results[: max(1, int(k))]]
    matched = sum(1 for needle in needles if any(_text_matches_expected(text, needle) for text in top_texts))
    return matched / len(needles)


def mrr_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    for idx, rel in enumerate(relevance_vector(results, expected, limit=k), start=1):
        if rel:
            return 1.0 / idx
    return 0.0


def ndcg_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    rels = relevance_vector(results, expected, limit=k)
    dcg = sum(rel / math.log2(idx + 2) for idx, rel in enumerate(rels))
    ideal_hits = min(sum(rels), max(1, int(k)))
    ideal = sum(1.0 / math.log2(idx + 2) for idx in range(ideal_hits))
    return dcg / ideal if ideal else 0.0


def _percentile(values: List[int], pct: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, math.ceil((float(pct) / 100.0) * len(ordered)) - 1))
    return int(ordered[idx])


def evaluate_search(
    golden: List[GoldenQuery],
    search_fn: Callable[[str, int], List[Dict[str, Any]]],
    *,
    limit: int = 10,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for item in golden:
        started = time.perf_counter()
        results = search_fn(item.query, limit)
        latency_ms = int((time.perf_counter() - started) * 1000)
        rows.append(
            {
                "query": item.query,
                "expected": item.expected,
                "category": item.category,
                "results_count": len(results),
                "recall_at_k": recall_at_k(results, item.expected, k=limit),
                "mrr_at_k": mrr_at_k(results, item.expected, k=limit),
                "ndcg_at_k": ndcg_at_k(results, item.expected, k=limit),
                "latency_ms": latency_ms,
                "top": [
                    {
                        "filename": str(result.get("filename") or ""),
                        "path": str(result.get("path") or result.get("full_path") or result.get("cloud_path") or ""),
                        "score": float(result.get("rank_score", result.get("score") or 0) or 0),
                    }
                    for result in results[:limit]
                ],
            }
        )
    count = max(1, len(rows))
    latencies = [int(row["latency_ms"]) for row in rows]
    zero_results = sum(1 for row in rows if int(row["results_count"] or 0) == 0)
    by_category: Dict[str, Dict[str, Any]] = {}
    for category in sorted({str(row.get("category") or "general") for row in rows}):
        cat_rows = [row for row in rows if str(row.get("category") or "general") == category]
        cat_count = max(1, len(cat_rows))
        cat_latencies = [int(row["latency_ms"]) for row in cat_rows]
        by_category[category] = {
            "queries": len(cat_rows),
            "recall_at_k": sum(row["recall_at_k"] for row in cat_rows) / cat_count,
            "mrr_at_k": sum(row["mrr_at_k"] for row in cat_rows) / cat_count,
            "ndcg_at_k": sum(row["ndcg_at_k"] for row in cat_rows) / cat_count,
            "zero_result_rate": sum(1 for row in cat_rows if int(row["results_count"] or 0) == 0) / cat_count,
            "latency_p50_ms": _percentile(cat_latencies, 50),
            "latency_p95_ms": _percentile(cat_latencies, 95),
        }
    return {
        "queries": len(rows),
        "limit": int(limit),
        "recall_at_k": sum(row["recall_at_k"] for row in rows) / count,
        "mrr_at_k": sum(row["mrr_at_k"] for row in rows) / count,
        "ndcg_at_k": sum(row["ndcg_at_k"] for row in rows) / count,
        "zero_result_rate": zero_results / count,
        "latency_p50_ms": _percentile(latencies, 50),
        "latency_p95_ms": _percentile(latencies, 95),
        "by_category": by_category,
        "rows": rows,
    }
