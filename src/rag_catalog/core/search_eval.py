"""Offline relevance evaluation helpers for RAG search."""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List


@dataclass(frozen=True)
class GoldenQuery:
    query: str
    expected: List[str]


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
        if query and expected:
            out.append(GoldenQuery(query=query, expected=expected))
    if not out:
        raise ValueError("Golden set is empty.")
    return out


def _result_text(result: Dict[str, Any]) -> str:
    return " ".join(
        str(result.get(key) or "")
        for key in ("filename", "path", "full_path", "cloud_path")
    ).lower().replace("ё", "е")


def _expected_text(value: str) -> str:
    return str(value or "").lower().replace("ё", "е")


def relevance_vector(results: Iterable[Dict[str, Any]], expected: List[str], *, limit: int) -> List[int]:
    needles = [_expected_text(item) for item in expected if item]
    vector: List[int] = []
    for result in list(results)[: max(1, int(limit))]:
        haystack = _result_text(result)
        vector.append(1 if any(needle in haystack for needle in needles) else 0)
    return vector


def recall_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    needles = [_expected_text(item) for item in expected if item]
    if not needles:
        return 0.0
    top_texts = [_result_text(item) for item in results[: max(1, int(k))]]
    matched = sum(1 for needle in needles if any(needle in text for text in top_texts))
    return matched / len(needles)


def mrr_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    for idx, rel in enumerate(relevance_vector(results, expected, limit=k), start=1):
        if rel:
            return 1.0 / idx
    return 0.0


def ndcg_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    rels = relevance_vector(results, expected, limit=k)
    dcg = sum(rel / math.log2(idx + 2) for idx, rel in enumerate(rels))
    ideal_hits = min(len([x for x in expected if x]), max(1, int(k)))
    ideal = sum(1.0 / math.log2(idx + 2) for idx in range(ideal_hits))
    return dcg / ideal if ideal else 0.0


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
    return {
        "queries": len(rows),
        "limit": int(limit),
        "recall_at_k": sum(row["recall_at_k"] for row in rows) / count,
        "mrr_at_k": sum(row["mrr_at_k"] for row in rows) / count,
        "ndcg_at_k": sum(row["ndcg_at_k"] for row in rows) / count,
        "latency_p50_ms": sorted(row["latency_ms"] for row in rows)[len(rows) // 2] if rows else 0,
        "rows": rows,
    }
