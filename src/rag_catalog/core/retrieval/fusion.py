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
    sources: Dict[str, set[str]] = {}
    channel_ranks: Dict[str, Dict[str, int]] = {}
    evidence_fields = (
        "dense_score",
        "lexical_matched_terms",
        "lexical_query_terms",
        "bm25_matched_terms",
        "bm25_query_terms",
        "fulltext_matched_terms",
        "fulltext_query_terms",
    )
    for channel_index, ranked in enumerate(ranked_lists):
        for rank, item in enumerate(ranked, start=1):
            key = _result_key(item)
            if not key:
                continue
            scores[key] = scores.get(key, 0.0) + 1.0 / (int(k) + rank)
            source = str(item.get("retrieval_source") or f"channel_{channel_index}").strip()
            sources.setdefault(key, set()).add(source)
            channel_ranks.setdefault(key, {})[source] = rank
            existing = fused.get(key)
            if existing is None or float(item.get("score") or 0) > float(existing.get("score") or 0):
                fused[key] = dict(item)
            for field in evidence_fields:
                if item.get(field) is None:
                    continue
                current = fused[key].get(field)
                if current is None or float(item.get(field) or 0) > float(current or 0):
                    fused[key][field] = item.get(field)
    for key, score in scores.items():
        fused[key]["rank_score"] = score
        fused[key]["fusion"] = "rrf"
        fused[key]["retrieval_sources"] = sorted(sources.get(key, set()))
        fused[key]["retrieval_channel_ranks"] = channel_ranks.get(key, {})
    return sorted(
        fused.values(),
        key=lambda item: (float(item.get("rank_score") or 0), float(item.get("score") or 0)),
        reverse=True,
    )[: max(1, int(limit))]
