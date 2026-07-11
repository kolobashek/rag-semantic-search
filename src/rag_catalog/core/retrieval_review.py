from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

REVIEW_SCHEMA_VERSION = 1


def load_json_object(path: str | Path) -> Dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def load_json_list(path: str | Path) -> List[Dict[str, Any]]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, list):
        raise ValueError(f"Expected JSON list: {path}")
    return [dict(item) for item in value if isinstance(item, dict)]


def prepare_review_queue(
    golden_rows: List[Dict[str, Any]],
    report: Dict[str, Any],
    *,
    golden_path: str = "",
    report_path: str = "",
    candidate_limit: int = 10,
) -> Dict[str, Any]:
    report_rows = {
        str(row.get("query") or "").strip(): dict(row)
        for row in (report.get("rows") or [])
        if isinstance(row, dict) and str(row.get("query") or "").strip()
    }
    items: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for source in golden_rows:
        query = str(source.get("query") or "").strip()
        if not query or query in seen:
            continue
        seen.add(query)
        report_row = report_rows.get(query, {})
        candidates: list[Dict[str, Any]] = []
        candidate_paths: set[str] = set()
        for rank, candidate in enumerate(report_row.get("top") or [], start=1):
            if not isinstance(candidate, dict):
                continue
            path = str(candidate.get("path") or candidate.get("full_path") or "").strip()
            if not path or path in candidate_paths:
                continue
            candidate_paths.add(path)
            candidates.append(
                {
                    "rank": rank,
                    "path": path,
                    "filename": str(candidate.get("filename") or ""),
                    "page": candidate.get("page"),
                    "score": candidate.get("score"),
                }
            )
            if len(candidates) >= max(1, int(candidate_limit)):
                break
        items.append(
            {
                "query": query,
                "category": str(source.get("category") or "general"),
                "expected_terms": [str(value) for value in (source.get("expected") or []) if str(value).strip()],
                "candidates": candidates,
                "review": {
                    "status": "pending",
                    "reviewed_by": "",
                    "reviewed_at": "",
                    "expect_no_answer": False,
                    "expected_paths": [],
                    "expected_chunks": [],
                    "expected_pages": [],
                    "forbidden": [],
                    "notes": "",
                },
            }
        )
    if not items:
        raise ValueError("Review queue is empty.")
    return {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": {
            "golden_path": golden_path,
            "report_path": report_path,
            "evaluation_profile": dict(report.get("evaluation_profile") or {}),
        },
        "items": items,
    }


def validate_review_queue(
    review_queue: Dict[str, Any],
    *,
    min_no_answer: int = 3,
    min_forbidden: int = 3,
) -> Dict[str, Any]:
    errors: list[Dict[str, Any]] = []
    items = list(review_queue.get("items") or [])
    seen: set[str] = set()
    reviewed = 0
    no_answer_count = 0
    forbidden_count = 0
    positive_count = 0
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            errors.append({"index": index, "error": "item_not_object"})
            continue
        query = str(item.get("query") or "").strip()
        if not query:
            errors.append({"index": index, "error": "query_missing"})
            continue
        if query in seen:
            errors.append({"index": index, "query": query, "error": "query_duplicate"})
            continue
        seen.add(query)
        review = dict(item.get("review") or {})
        if str(review.get("status") or "") != "reviewed":
            errors.append({"index": index, "query": query, "error": "review_pending"})
            continue
        reviewed += 1
        if not str(review.get("reviewed_by") or "").strip():
            errors.append({"index": index, "query": query, "error": "reviewed_by_missing"})
        if not str(review.get("reviewed_at") or "").strip():
            errors.append({"index": index, "query": query, "error": "reviewed_at_missing"})
        expect_no_answer = review.get("expect_no_answer") is True
        expected_paths = [str(value).strip() for value in (review.get("expected_paths") or []) if str(value).strip()]
        expected_chunks = [str(value).strip() for value in (review.get("expected_chunks") or []) if str(value).strip()]
        expected_pages = [value for value in (review.get("expected_pages") or []) if str(value).strip()]
        forbidden = [str(value).strip() for value in (review.get("forbidden") or []) if str(value).strip()]
        if expect_no_answer:
            no_answer_count += 1
            if expected_paths or expected_chunks or expected_pages:
                errors.append({"index": index, "query": query, "error": "no_answer_has_positive_ground_truth"})
        else:
            positive_count += 1
            if not expected_paths:
                errors.append({"index": index, "query": query, "error": "expected_paths_missing"})
        if forbidden:
            forbidden_count += 1
        for page in expected_pages:
            try:
                if int(page) < 1:
                    raise ValueError
            except (TypeError, ValueError):
                errors.append({"index": index, "query": query, "error": "expected_page_invalid", "value": page})
    if no_answer_count < max(0, int(min_no_answer)):
        errors.append(
            {
                "error": "no_answer_coverage_insufficient",
                "actual": no_answer_count,
                "required": max(0, int(min_no_answer)),
            }
        )
    if forbidden_count < max(0, int(min_forbidden)):
        errors.append(
            {
                "error": "forbidden_coverage_insufficient",
                "actual": forbidden_count,
                "required": max(0, int(min_forbidden)),
            }
        )
    return {
        "ok": not errors and bool(items),
        "items": len(items),
        "reviewed": reviewed,
        "pending": max(0, len(items) - reviewed),
        "positive_cases": positive_count,
        "no_answer_cases": no_answer_count,
        "forbidden_cases": forbidden_count,
        "errors": errors,
    }


def finalize_review_queue(
    review_queue: Dict[str, Any],
    *,
    min_no_answer: int = 3,
    min_forbidden: int = 3,
) -> List[Dict[str, Any]]:
    validation = validate_review_queue(
        review_queue,
        min_no_answer=min_no_answer,
        min_forbidden=min_forbidden,
    )
    if not validation["ok"]:
        raise ValueError(json.dumps(validation, ensure_ascii=False, sort_keys=True))
    golden: list[Dict[str, Any]] = []
    for item in review_queue.get("items") or []:
        review = dict(item.get("review") or {})
        expect_no_answer = review.get("expect_no_answer") is True
        golden.append(
            {
                "query": str(item.get("query") or "").strip(),
                "expected": []
                if expect_no_answer
                else [str(value) for value in (item.get("expected_terms") or []) if str(value).strip()],
                "category": str(item.get("category") or "general"),
                "expected_paths": [
                    str(value).strip() for value in (review.get("expected_paths") or []) if str(value).strip()
                ],
                "expected_chunks": [
                    str(value).strip() for value in (review.get("expected_chunks") or []) if str(value).strip()
                ],
                "expected_pages": [int(value) for value in (review.get("expected_pages") or []) if str(value).strip()],
                "forbidden": [str(value).strip() for value in (review.get("forbidden") or []) if str(value).strip()],
                "expect_no_answer": expect_no_answer,
            }
        )
    return golden
