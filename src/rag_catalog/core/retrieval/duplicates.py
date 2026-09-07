"""Collapse byte-identical documents in the final search output.

The indexer stores ``content_hash`` on every point and marks later copies with
``is_duplicate`` / ``duplicate_of`` (path of the first indexed copy). Search
previously ignored both, so a file copied into three folders occupied three
slots in the result list. Here we keep the best-scoring copy, drop the rest and
attach ``duplicates`` (their paths) plus ``duplicate_count`` to the survivor.
Multiple chunks of the surviving file are kept as they were.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _norm_path(value: Any) -> str:
    return str(value or "").strip().lower().replace("/", "\\")


def _group_key(item: Dict[str, Any], hash_by_path: Dict[str, str]) -> Optional[str]:
    content_hash = str(item.get("content_hash") or "").strip()
    if content_hash:
        return f"hash:{content_hash}"
    duplicate_of = _norm_path(item.get("duplicate_of")) if item.get("is_duplicate") else ""
    if duplicate_of:
        original_hash = hash_by_path.get(duplicate_of)
        return f"hash:{original_hash}" if original_hash else f"path:{duplicate_of}"
    return None


def collapse_duplicate_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return ``results`` with duplicate documents folded into the best-ranked copy."""
    if not results:
        return []
    hash_by_path: Dict[str, str] = {}
    for item in results:
        content_hash = str(item.get("content_hash") or "").strip()
        path = _norm_path(item.get("full_path"))
        if content_hash and path and path not in hash_by_path:
            hash_by_path[path] = content_hash

    keys: List[Optional[str]] = [_group_key(item, hash_by_path) for item in results]
    # A referenced original without its own hash/flag still belongs to the group.
    referenced = {key[len("path:"):] for key in keys if key and key.startswith("path:")}
    for index, item in enumerate(results):
        if keys[index] is None:
            path = _norm_path(item.get("full_path"))
            if path and path in referenced:
                keys[index] = f"path:{path}"

    # Results are already ordered by rank: the first item of a group wins.
    winner_path: Dict[str, str] = {}
    winner_index: Dict[str, int] = {}
    duplicates: Dict[str, List[str]] = {}
    kept: List[Dict[str, Any]] = []
    for index, item in enumerate(results):
        key = keys[index]
        if key is None:
            kept.append(item)
            continue
        path = _norm_path(item.get("full_path"))
        if key not in winner_path:
            winner_path[key] = path
            winner_index[key] = len(kept)
            kept.append(dict(item))
            continue
        if path == winner_path[key]:
            kept.append(item)  # another chunk of the surviving file
            continue
        display_path = str(item.get("full_path") or item.get("path") or "")
        bucket = duplicates.setdefault(key, [])
        if display_path and display_path not in bucket:
            bucket.append(display_path)

    for key, paths in duplicates.items():
        survivor = kept[winner_index[key]]
        survivor["duplicates"] = list(paths)
        survivor["duplicate_count"] = len(paths)
    return kept


__all__ = ["collapse_duplicate_results"]
