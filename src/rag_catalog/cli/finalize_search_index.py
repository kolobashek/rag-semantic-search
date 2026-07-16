from __future__ import annotations

import argparse
import json
import logging
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import (
    FieldCondition,
    Filter,
    MatchAny,
    OptimizersConfigDiff,
    PayloadSelectorInclude,
    Sample,
    SampleQuery,
)

from rag_catalog.core.embedding_collections import resolve_embedding_collection_name
from rag_catalog.core.indexing import ensure_payload_indexes
from rag_catalog.core.qdrant_connection import create_qdrant_client
from rag_catalog.core.rag_core import load_config

logger = logging.getLogger(__name__)

_REQUIRED_PAYLOAD_FIELDS = ("type", "text", "full_path", "doc_id", "payload_schema_version")


def index_runtime_profile(config: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the index-defining runtime values bound to readiness evidence."""
    return {
        "embedding_model": str(config.get("embedding_model") or ""),
        "index_embedding_backend": str(
            config.get("index_embedding_backend") or config.get("embedding_backend") or ""
        ),
        "index_embedding_onnx_provider": str(
            config.get("index_embedding_onnx_provider")
            or config.get("embedding_onnx_provider")
            or ""
        ),
        "index_embedding_onnx_file_name": str(
            config.get("index_embedding_onnx_file_name")
            or config.get("embedding_onnx_file_name")
            or ""
        ),
        "vector_size": int(config.get("vector_size") or 0),
        "chunk_size": int(config.get("chunk_size") or 0),
        "chunk_overlap": int(config.get("chunk_overlap") or 0),
        "index_min_chunk_chars": int(config.get("index_min_chunk_chars") or 0),
        "chunk_group_size": int(config.get("chunk_group_size") or 0),
    }


def _enum_text(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower()


def _payload_index_types(schema: Any) -> Dict[str, str]:
    if not hasattr(schema, "items"):
        return {str(key): "unknown" for key in schema or []}
    types: Dict[str, str] = {}
    for key, value in schema.items():
        data_type = value.get("data_type") if isinstance(value, dict) else getattr(value, "data_type", value)
        types[str(key)] = _enum_text(data_type) or "unknown"
    return types


def collection_readiness(
    info: Any,
    *,
    require_fulltext: bool,
    max_unindexed_vectors: int,
) -> Dict[str, Any]:
    schema = getattr(info, "payload_schema", None) or {}
    schema_types = _payload_index_types(schema)
    schema_fields = set(schema_types)
    text_index_type = schema_types.get("text", "")
    fulltext_ready = text_index_type == "text"
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
    if require_fulltext and not fulltext_ready:
        if text_index_type:
            reasons.append(f"fulltext_index_wrong_type={text_index_type}")
        else:
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
        "payload_index_types": dict(sorted(schema_types.items())),
        "fulltext_ready": fulltext_ready,
        "reasons": reasons,
    }


def _inspect_payload_integrity(
    points: Iterable[Any],
    *,
    min_content_chars: int,
    required_content_fields: tuple[str, ...],
    expected_content_values: Mapping[str, Any],
    missing: Counter[str],
    quality_violations: Counter[str],
    content_quality: Counter[str],
    types: Counter[str],
    versions: Counter[str],
) -> int:
    inspected = 0
    for point in points:
        inspected += 1
        payload = dict(getattr(point, "payload", None) or {})
        point_type = str(payload.get("type") or "")
        types[point_type or "<missing>"] += 1
        versions[str(payload.get("payload_schema_version") or "<missing>")] += 1
        for field_name in _REQUIRED_PAYLOAD_FIELDS:
            if payload.get(field_name) in (None, "", []):
                missing[field_name] += 1
        is_content = point_type not in {"file_metadata", "folder_metadata"}
        if is_content and payload.get("chunk_index") is None:
            missing["content.chunk_index"] += 1
        if is_content:
            for field_name in required_content_fields:
                if payload.get(field_name) in (None, "", []):
                    missing[f"content.{field_name}"] += 1
            for field_name, expected_value in expected_content_values.items():
                actual_value = payload.get(field_name)
                if actual_value not in (None, "", []) and actual_value != expected_value:
                    quality_violations[f"content.{field_name}.unexpected_value"] += 1
            content_quality["sampled"] += 1
            text = str(payload.get("text") or "").strip()
            if text and not re.sub(r"[\W_]+", "", text, flags=re.UNICODE):
                quality_violations["content.separator_only"] += 1
            minimum = max(0, int(min_content_chars or 0))
            if minimum and 0 < len(text) < minimum:
                content_quality["short_under_min"] += 1
                try:
                    chunk_index = int(payload.get("chunk_index") or 0)
                except (TypeError, ValueError):
                    chunk_index = 0
                if chunk_index > 0:
                    quality_violations["content.short_noninitial"] += 1
    return inspected


def _payload_integrity_report(
    *,
    inspected: int,
    requested: int,
    sampling_strategy: str,
    scanned_all: bool,
    missing: Counter[str],
    quality_violations: Counter[str],
    content_quality: Counter[str],
    types: Counter[str],
    versions: Counter[str],
) -> Dict[str, Any]:
    if inspected == 0:
        missing["sample"] += 1
    return {
        "ok": not missing and not quality_violations,
        "sampling_strategy": sampling_strategy,
        "scanned_all": bool(scanned_all),
        "sample_size": inspected,
        "requested_sample_size": requested,
        "missing_fields": dict(sorted(missing.items())),
        "quality_violations": dict(sorted(quality_violations.items())),
        "content_quality": dict(sorted(content_quality.items())),
        "types": dict(sorted(types.items())),
        "schema_versions": dict(sorted(versions.items())),
    }


def sample_payload_integrity(
    client: Any,
    *,
    collection_name: str,
    sample_size: int,
    min_content_chars: int = 0,
    query_filter: Any = None,
    required_content_fields: tuple[str, ...] = (),
    expected_content_values: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    limit = min(10_000, max(0, int(sample_size)))
    if limit == 0:
        return {
            "ok": True,
            "sampling_strategy": "none",
            "scanned_all": False,
            "sample_size": 0,
            "requested_sample_size": 0,
            "missing_fields": {},
            "quality_violations": {},
            "content_quality": {},
            "types": {},
            "schema_versions": {},
        }
    expected_values = dict(expected_content_values or {})
    fields = [
        *_REQUIRED_PAYLOAD_FIELDS,
        "chunk_index",
        *required_content_fields,
        *expected_values,
    ]
    payload_selector = PayloadSelectorInclude(include=fields)
    sampling_strategy = "random"
    try:
        query_kwargs: Dict[str, Any] = {
            "collection_name": collection_name,
            "query": SampleQuery(sample=Sample.RANDOM),
            "limit": limit,
            "with_payload": payload_selector,
            "with_vectors": False,
        }
        if query_filter is not None:
            query_kwargs["query_filter"] = query_filter
        response = client.query_points(
            **query_kwargs,
        )
        points = list(response.points)
    except (AttributeError, TypeError, NotImplementedError, UnexpectedResponse) as exc:
        logger.warning("Qdrant random payload sampling недоступен, использую scroll: %s", exc)
        scroll_kwargs: Dict[str, Any] = {
            "collection_name": collection_name,
            "limit": limit,
            "with_payload": payload_selector,
            "with_vectors": False,
        }
        if query_filter is not None:
            scroll_kwargs["scroll_filter"] = query_filter
        points, _offset = client.scroll(**scroll_kwargs)
        sampling_strategy = "scroll_fallback"
    missing: Counter[str] = Counter()
    quality_violations: Counter[str] = Counter()
    content_quality: Counter[str] = Counter()
    types: Counter[str] = Counter()
    versions: Counter[str] = Counter()
    inspected = _inspect_payload_integrity(
        points,
        min_content_chars=min_content_chars,
        required_content_fields=required_content_fields,
        expected_content_values=expected_values,
        missing=missing,
        quality_violations=quality_violations,
        content_quality=content_quality,
        types=types,
        versions=versions,
    )
    return _payload_integrity_report(
        inspected=inspected,
        requested=limit,
        sampling_strategy=sampling_strategy,
        scanned_all=False,
        missing=missing,
        quality_violations=quality_violations,
        content_quality=content_quality,
        types=types,
        versions=versions,
    )


def scan_payload_integrity(
    client: Any,
    *,
    collection_name: str,
    batch_size: int = 1_000,
    min_content_chars: int = 0,
    query_filter: Any = None,
    required_content_fields: tuple[str, ...] = (),
    expected_content_values: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Inspect every payload matching a filter without loading vectors into memory."""
    limit = min(10_000, max(1, int(batch_size)))
    expected_values = dict(expected_content_values or {})
    fields = [
        *_REQUIRED_PAYLOAD_FIELDS,
        "chunk_index",
        *required_content_fields,
        *expected_values,
    ]
    payload_selector = PayloadSelectorInclude(include=fields)
    missing: Counter[str] = Counter()
    quality_violations: Counter[str] = Counter()
    content_quality: Counter[str] = Counter()
    types: Counter[str] = Counter()
    versions: Counter[str] = Counter()
    inspected = 0
    offset: Any = None
    while True:
        scroll_kwargs: Dict[str, Any] = {
            "collection_name": collection_name,
            "limit": limit,
            "with_payload": payload_selector,
            "with_vectors": False,
        }
        if query_filter is not None:
            scroll_kwargs["scroll_filter"] = query_filter
        if offset is not None:
            scroll_kwargs["offset"] = offset
        points, next_offset = client.scroll(**scroll_kwargs)
        inspected += _inspect_payload_integrity(
            points,
            min_content_chars=min_content_chars,
            required_content_fields=required_content_fields,
            expected_content_values=expected_values,
            missing=missing,
            quality_violations=quality_violations,
            content_quality=content_quality,
            types=types,
            versions=versions,
        )
        if next_offset is None:
            break
        if not points or next_offset == offset:
            raise RuntimeError("Qdrant full payload audit did not advance its scroll offset")
        offset = next_offset
        if inspected and inspected % 100_000 < limit:
            logger.info(
                "Qdrant full payload audit: collection=%s inspected=%d",
                collection_name,
                inspected,
            )
    return _payload_integrity_report(
        inspected=inspected,
        requested=0,
        sampling_strategy="full_scroll",
        scanned_all=True,
        missing=missing,
        quality_violations=quality_violations,
        content_quality=content_quality,
        types=types,
        versions=versions,
    )


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
    min_content_chars: int = 0,
    spreadsheet_sample_size: int = 0,
    spreadsheet_full_audit: bool = False,
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
                min_content_chars=min_content_chars,
            )
            snapshot["payload_integrity"] = payload_integrity
            if not payload_integrity["ok"]:
                snapshot["ready"] = False
                snapshot["reasons"].append("payload_integrity_failed")
            if spreadsheet_sample_size > 0:
                audit_fn = scan_payload_integrity if spreadsheet_full_audit else sample_payload_integrity
                spreadsheet_integrity = audit_fn(
                    client,
                    collection_name=collection_name,
                    **(
                        {"batch_size": spreadsheet_sample_size}
                        if spreadsheet_full_audit
                        else {"sample_size": spreadsheet_sample_size}
                    ),
                    min_content_chars=min_content_chars,
                    query_filter=Filter(
                        must=[
                            FieldCondition(
                                key="extension",
                                match=MatchAny(any=[".xlsx", ".xlsm", ".xls"]),
                            )
                        ]
                    ),
                    required_content_fields=(
                        "sheet",
                        "row_start",
                        "row_end",
                        "spreadsheet_payload_schema_version",
                    ),
                    expected_content_values={"spreadsheet_payload_schema_version": 2},
                )
                snapshot["spreadsheet_integrity"] = spreadsheet_integrity
                if not spreadsheet_integrity["ok"]:
                    snapshot["ready"] = False
                    snapshot["reasons"].append("spreadsheet_integrity_failed")
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
    parser.add_argument(
        "--spreadsheet-sample-size",
        type=int,
        default=int(cfg.get("qdrant_spreadsheet_audit_sample_size") or 500),
        help="Random integrity sample for xlsx/xlsm/xls payloads; 0 disables the spreadsheet gate.",
    )
    parser.add_argument(
        "--spreadsheet-full-audit",
        action="store_true",
        help="Scroll and validate every xlsx/xlsm/xls payload; sample size becomes the page size.",
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
    client = create_qdrant_client(
        url=str(args.url),
        path=str(args.db),
        timeout=max(30, int(args.timeout_sec)),
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
        min_content_chars=int(cfg.get("index_min_chunk_chars") or 120),
        spreadsheet_sample_size=max(0, int(args.spreadsheet_sample_size)),
        spreadsheet_full_audit=bool(args.spreadsheet_full_audit),
    )
    result["index_runtime_profile"] = index_runtime_profile(cfg)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if str(args.output or "").strip():
        output = Path(str(args.output)).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    print(text)
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
