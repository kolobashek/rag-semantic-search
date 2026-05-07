"""Result fusion utilities for retrieval v2."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


def _result_key(item: Dict[str, Any]) -> str:
    explicit = str(item.get("id") or item.get("point_id") or "").strip()
    if explicit:
        return explicit
    return "::".join(
        str(item.get(key) or "")
        for key in ("cloud_file_id", "full_path", "path", "chunk_index", "type")
    )


def rrf_fuse(
    ranked_lists: Iterable[List[Dict[str, Any]]],
    *,
    limit: int = 10,
    k: int = 60,
) -> List[Dict[str, Any]]:
    """Fuse ranked result lists with Reciprocal Rank Fusion.

    RRF is intentionally score-scale agnostic: dense, sparse and lexical channels can
    contribute without normalizing their raw scores first.
    """
    fused: Dict[str, Dict[str, Any]] = {}
    scores: Dict[str, float] = {}
    for ranked in ranked_lists:
        for rank, item in enumerate(ranked, start=1):
            key = _result_key(item)
            if not key:
                continue
            scores[key] = scores.get(key, 0.0) + 1.0 / (int(k) + rank)
            existing = fused.get(key)
            if existing is None or float(item.get("score") or 0) > float(existing.get("score") or 0):
                fused[key] = dict(item)
    for key, score in scores.items():
        fused[key]["rank_score"] = score
        fused[key]["fusion"] = "rrf"
    return sorted(
        fused.values(),
        key=lambda item: (float(item.get("rank_score") or 0), float(item.get("score") or 0)),
        reverse=True,
    )[: max(1, int(limit))]
