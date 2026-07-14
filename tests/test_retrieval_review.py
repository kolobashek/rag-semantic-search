from __future__ import annotations

import pytest

from rag_catalog.core.retrieval_review import (
    append_expected_chunk_text,
    finalize_review_queue,
    load_json_object,
    prepare_review_queue,
    save_review_queue_atomic,
    validate_review_queue,
)

_SMALL_COVERAGE = {
    "min_items": 0,
    "min_document_grounded": 0,
    "min_content_grounded": 0,
    "min_categories": 0,
}


def test_append_expected_chunk_text_normalizes_and_deduplicates_excerpt() -> None:
    assert append_expected_chunk_text("Первый фрагмент", " Второй\n  фрагмент ") == (
        "Первый фрагмент\nВторой фрагмент"
    )
    assert append_expected_chunk_text(
        "Первый фрагмент\nВторой фрагмент",
        "Второй фрагмент",
    ) == "Первый фрагмент\nВторой фрагмент"


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
                        {
                            "filename": "Договор.pdf",
                            "path": "Договоры/Договор.pdf",
                            "score": 0.9,
                            "excerpt": "Оплата  в течение\n10 дней.",
                        },
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
            "excerpt": "Оплата в течение 10 дней.",
        }
    ]
    assert queue["items"][0]["review"]["expected_paths"] == []
    assert queue["items"][0]["review"]["status"] == "pending"


def test_review_validation_requires_human_labels_no_answer_and_acl_cases() -> None:
    queue = {"items": [_reviewed_item("positive", forbidden=True), _reviewed_item("negative", no_answer=True)]}

    valid = validate_review_queue(
        queue,
        min_no_answer=1,
        min_forbidden=1,
        **_SMALL_COVERAGE,
    )
    assert valid["ok"] is True

    golden = finalize_review_queue(
        queue,
        min_no_answer=1,
        min_forbidden=1,
        **_SMALL_COVERAGE,
    )
    assert golden[0]["expected_paths"] == ["Договоры/Договор.pdf"]
    assert golden[0]["forbidden"] == ["Закрыто/Секретный договор.pdf"]
    assert golden[1]["expect_no_answer"] is True
    assert golden[1]["expected"] == []

    with pytest.raises(ValueError):
        finalize_review_queue(
            queue,
            min_no_answer=2,
            min_forbidden=2,
            **_SMALL_COVERAGE,
        )


def test_pending_or_contradictory_review_cannot_finalize() -> None:
    pending = _reviewed_item("pending")
    pending["review"]["status"] = "pending"
    contradictory = _reviewed_item("negative", no_answer=True)
    contradictory["review"]["expected_paths"] = ["Unexpected.pdf"]
    result = validate_review_queue(
        {"items": [pending, contradictory]},
        min_no_answer=1,
        min_forbidden=0,
        **_SMALL_COVERAGE,
    )

    assert result["ok"] is False
    errors = {item["error"] for item in result["errors"]}
    assert "review_pending" in errors
    assert "no_answer_has_positive_ground_truth" in errors


def test_default_review_validation_requires_broad_human_labels() -> None:
    narrow = validate_review_queue(
        {"items": [_reviewed_item("positive"), _reviewed_item("negative", no_answer=True)]}
    )
    errors = {item["error"] for item in narrow["errors"]}
    assert {
        "review_item_coverage_insufficient",
        "no_answer_coverage_insufficient",
        "content_grounded_coverage_insufficient",
        "category_coverage_insufficient",
    } <= errors

    items = []
    for index in range(50):
        no_answer = index < 10
        item = _reviewed_item(
            f"query-{index}",
            no_answer=no_answer,
            forbidden=10 <= index < 13,
        )
        item["category"] = f"category-{index % 6}"
        if 10 <= index < 20:
            item["review"]["expected_chunks"] = [f"verified fragment {index}"]
        items.append(item)

    broad = validate_review_queue({"items": items})
    assert broad["ok"] is True
    assert broad["document_grounded_cases"] == 40
    assert broad["content_grounded_cases"] == 10
    assert broad["categories"] == 6
    assert len(finalize_review_queue({"items": items})) == 50


def test_review_queue_save_is_atomic_and_keeps_backup(tmp_path) -> None:
    path = tmp_path / "review.json"
    first = {"schema_version": 1, "items": [_reviewed_item("first")]}
    second = {"schema_version": 1, "items": [_reviewed_item("second")]}

    backup = save_review_queue_atomic(path, first)
    assert not backup.exists()
    backup = save_review_queue_atomic(path, second)

    assert load_json_object(path)["items"][0]["query"] == "second"
    assert load_json_object(backup)["items"][0]["query"] == "first"
    assert not list(tmp_path.glob("*.tmp"))

    with pytest.raises(ValueError):
        save_review_queue_atomic(path, {"schema_version": 999, "items": []})
