from __future__ import annotations

import argparse
from collections import Counter
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict

from qdrant_client import QdrantClient
from qdrant_client.models import OptimizersConfigDiff, PayloadSelectorInclude

from rag_catalog.core.embedding_collections import resolve_embedding_collection_name
from rag_catalog.core.indexing import ensure_payload_indexes
from rag_catalog.core.rag_core import load_config

logger = logging.getLogger(__name__)

_REQUIRED_PAYLOAD_FIELDS = ("type", "text", "full_path", "doc_id", "payload_schema_version")


def _enum_text(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower()


def collection_readiness(
    info: Any,
    *,
    require_fulltext: bool,
    max_unindexed_vectors: int,
) -> Dict[str, Any]:
    schema = getattr(info, "payload_schema", None) or {}
    schema_fields = {str(key) for key in schema.keys()} if hasattr(schema, "keys") else {str(key) for key in schema}
    points = int(getattr(info, "points_count", 0) or 0)
    indexed = int(getattr(info, "indexed_vectors_count", 0) or 0)
    unindexed = max(0, points - indexed)
    status = _enum_text(getattr(info, "status", ""))
    optimizer = _enum_text(getattr(info, "optimizer_status", ""))
    reasons = []
    if status != "green":
        reasons.append(f"collection_status={status or 'unknown'}")
    if optimizer not in {"ok", "green"}:
        reasons.append(f"optimizer_status={optimizer or 'unknown'}")
    if require_fulltext and "text" not in schema_fields:
        reasons.append("fulltext_index_missing")
    if unindexed > max(0, int(max_unindexed_vectors)):
        reasons.append(f"unindexed_vectors={unindexed}")
    return {
        "ready": not reasons,
        "status": status,
        "optimizer_status": optimizer,
        "points_count": points,
        "indexed_vectors_count": indexed,
        "unindexed_vectors": unindexed,
        "max_unindexed_vectors": max(0, int(max_unindexed_vectors)),
        "payload_indexes": sorted(schema_fields),
        "fulltext_ready": "text" in schema_fields,
        "reasons": reasons,
    }


def sample_payload_integrity(
    client: Any,
    *,
    collection_name: str,
    sample_size: int,
) -> Dict[str, Any]:
    limit = min(10_000, max(0, int(sample_size)))
    if limit == 0:
        return {"ok": True, "sample_size": 0, "missing_fields": {}, "types": {}, "schema_versions": {}}
    fields = [*_REQUIRED_PAYLOAD_FIELDS, "chunk_index"]
    points, _offset = client.scroll(
        collection_name=collection_name,
        limit=limit,
        with_payload=PayloadSelectorInclude(include=fields),
        with_vectors=False,
    )
    missing: Counter[str] = Counter()
    types: Counter[str] = Counter()
    versions: Counter[str] = Counter()
    for point in points:
        payload = dict(getattr(point, "payload", None) or {})
        point_type = str(payload.get("type") or "")
        types[point_type or "<missing>"] += 1
        versions[str(payload.get("payload_schema_version") or "<missing>")] += 1
        for field_name in _REQUIRED_PAYLOAD_FIELDS:
            if payload.get(field_name) in (None, "", []):
                missing[field_name] += 1
        if point_type not in {"file_metadata", "folder_metadata"} and payload.get("chunk_index") is None:
            missing["content.chunk_index"] += 1
    if not points:
        missing["sample"] = 1
    return {
        "ok": not missing,
        "sample_size": len(points),
        "requested_sample_size": limit,
        "missing_fields": dict(sorted(missing.items())),
        "types": dict(sorted(types.items())),
        "schema_versions": dict(sorted(versions.items())),
    }


def finalize_collection(
    client: Any,
    *,
    collection_name: str,
    indexing_threshold: int,
    require_fulltext: bool,
    timeout_sec: int,
    poll_seconds: float,
    max_unindexed_vectors: int,
    payload_sample_size: int,
) -> Dict[str, Any]:
    timeout = max(30, int(timeout_sec or 30))
    ensure_payload_indexes(
        client,
        collection_name=collection_name,
        fulltext_enabled=require_fulltext,
        wait=True,
        timeout_sec=timeout,
    )
    client.update_collection(
        collection_name=collection_name,
        optimizers_config=OptimizersConfigDiff(indexing_threshold=max(1, int(indexing_threshold))),
        timeout=timeout,
    )

    deadline = time.monotonic() + timeout
    snapshot: Dict[str, Any] = {}
    while True:
        snapshot = collection_readiness(
            client.get_collection(collection_name),
            require_fulltext=require_fulltext,
            max_unindexed_vectors=max_unindexed_vectors,
        )
        snapshot.update(
            {
                "collection_name": collection_name,
                "indexing_threshold": max(1, int(indexing_threshold)),
            }
        )
        if snapshot["ready"]:
            payload_integrity = sample_payload_integrity(
                client,
                collection_name=collection_name,
                sample_size=payload_sample_size,
            )
            snapshot["payload_integrity"] = payload_integrity
            if not payload_integrity["ok"]:
                snapshot["ready"] = False
                snapshot["reasons"].append("payload_integrity_failed")
            return snapshot
        if time.monotonic() >= deadline:
            return snapshot
        logger.info(
            "Qdrant finalize: indexed=%d/%d, unindexed=%d, reasons=%s",
            snapshot["indexed_vectors_count"],
            snapshot["points_count"],
            snapshot["unindexed_vectors"],
            ",".join(snapshot["reasons"]),
        )
        time.sleep(max(0.1, float(poll_seconds or 1.0)))


def main(argv: list[str] | None = None) -> int:
    cfg = load_config()
    parser = argparse.ArgumentParser(description="Finalize a bulk-loaded Qdrant search collection.")
    parser.add_argument("--collection", default=str(cfg.get("collection_name") or ""))
    parser.add_argument("--url", default=str(cfg.get("qdrant_url") or ""))
    parser.add_argument("--db", default=str(cfg.get("qdrant_db_path") or ""))
    parser.add_argument(
        "--indexing-threshold",
        type=int,
        default=int(cfg.get("qdrant_indexing_threshold") or 20_000),
    )
    parser.add_argument(
        "--max-unindexed-vectors",
        type=int,
        default=int(cfg.get("qdrant_max_unindexed_vectors") or 50_000),
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=int(cfg.get("qdrant_finalize_timeout_sec") or 7_200),
    )
    parser.add_argument("--poll-sec", type=float, default=10.0)
    parser.add_argument(
        "--payload-sample-size",
        type=int,
        default=int(cfg.get("qdrant_payload_audit_sample_size") or 1_000),
    )
    parser.add_argument("--no-fulltext", action="store_true")
    parser.add_argument("--output", default="")
    args = parser.parse_args(argv)

    collection = resolve_embedding_collection_name(
        str(args.collection),
        str(cfg.get("embedding_model") or ""),
        enabled=bool(cfg.get("embedding_collection_versioning", False)),
        suffix=str(cfg.get("embedding_collection_suffix") or ""),
    )
    client = (
        QdrantClient(url=str(args.url), timeout=max(30, int(args.timeout_sec)))
        if str(args.url).strip()
        else QdrantClient(path=str(args.db), timeout=max(30, int(args.timeout_sec)))
    )
    result = finalize_collection(
        client,
        collection_name=collection,
        indexing_threshold=int(args.indexing_threshold),
        require_fulltext=not bool(args.no_fulltext),
        timeout_sec=int(args.timeout_sec),
        poll_seconds=float(args.poll_sec),
        max_unindexed_vectors=int(args.max_unindexed_vectors),
        payload_sample_size=int(args.payload_sample_size),
    )
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if str(args.output or "").strip():
        output = Path(str(args.output)).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    print(text)
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
