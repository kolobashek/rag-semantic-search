from __future__ import annotations

import pytest

from rag_catalog.core.retrieval_review import finalize_review_queue, prepare_review_queue, validate_review_queue


def _reviewed_item(query: str, *, no_answer: bool = False, forbidden: bool = False) -> dict:
    return {
        "query": query,
        "category": "test",
        "expected_terms": ["договор"],
        "candidates": [],
        "review": {
            "status": "reviewed",
            "reviewed_by": "data-owner",
            "reviewed_at": "2026-07-11T14:00:00+07:00",
            "expect_no_answer": no_answer,
            "expected_paths": [] if no_answer else ["Договоры/Договор.pdf"],
            "expected_chunks": [],
            "expected_pages": [],
            "forbidden": ["Закрыто/Секретный договор.pdf"] if forbidden else [],
            "notes": "",
        },
    }


def test_prepare_review_queue_keeps_candidates_separate_from_ground_truth() -> None:
    queue = prepare_review_queue(
        [{"query": "договор", "expected": ["договор"], "category": "document"}],
        {
            "evaluation_profile": {"retrieval_preset": "legacy"},
            "rows": [
                {
                    "query": "договор",
                    "top": [
                        {"filename": "Договор.pdf", "path": "Договоры/Договор.pdf", "score": 0.9},
                        {"filename": "Договор.pdf", "path": "Договоры/Договор.pdf", "score": 0.8},
                    ],
                }
            ],
        },
    )

    assert queue["items"][0]["candidates"] == [
        {
            "rank": 1,
            "path": "Договоры/Договор.pdf",
            "filename": "Договор.pdf",
            "page": None,
            "score": 0.9,
        }
    ]
    assert queue["items"][0]["review"]["expected_paths"] == []
    assert queue["items"][0]["review"]["status"] == "pending"


def test_review_validation_requires_human_labels_no_answer_and_acl_cases() -> None:
    queue = {"items": [_reviewed_item("positive", forbidden=True), _reviewed_item("negative", no_answer=True)]}

    valid = validate_review_queue(queue, min_no_answer=1, min_forbidden=1)
    assert valid["ok"] is True

    golden = finalize_review_queue(queue, min_no_answer=1, min_forbidden=1)
    assert golden[0]["expected_paths"] == ["Договоры/Договор.pdf"]
    assert golden[0]["forbidden"] == ["Закрыто/Секретный договор.pdf"]
    assert golden[1]["expect_no_answer"] is True
    assert golden[1]["expected"] == []

    with pytest.raises(ValueError):
        finalize_review_queue(queue, min_no_answer=2, min_forbidden=2)


def test_pending_or_contradictory_review_cannot_finalize() -> None:
    pending = _reviewed_item("pending")
    pending["review"]["status"] = "pending"
    contradictory = _reviewed_item("negative", no_answer=True)
    contradictory["review"]["expected_paths"] = ["Unexpected.pdf"]
    result = validate_review_queue({"items": [pending, contradictory]}, min_no_answer=1, min_forbidden=0)

    assert result["ok"] is False
    errors = {item["error"] for item in result["errors"]}
    assert "review_pending" in errors
    assert "no_answer_has_positive_ground_truth" in errors
