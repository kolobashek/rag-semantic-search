from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from rag_catalog.core.retrieval_review import (
    finalize_review_queue,
    load_json_list,
    load_json_object,
    prepare_review_queue,
    validate_review_queue,
)
from rag_catalog.core.search_eval import (
    DEFAULT_MIN_CONTENT_GROUNDED_CASES,
    DEFAULT_MIN_DOCUMENT_GROUNDED_CASES,
    DEFAULT_MIN_EVAL_CATEGORIES,
    DEFAULT_MIN_EVAL_QUERIES,
    DEFAULT_MIN_NO_ANSWER_CASES,
)


def _add_coverage_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--min-items", type=int, default=DEFAULT_MIN_EVAL_QUERIES)
    parser.add_argument("--min-no-answer", type=int, default=DEFAULT_MIN_NO_ANSWER_CASES)
    parser.add_argument("--min-forbidden", type=int, default=3)
    parser.add_argument(
        "--min-document-grounded",
        type=int,
        default=DEFAULT_MIN_DOCUMENT_GROUNDED_CASES,
    )
    parser.add_argument(
        "--min-content-grounded",
        type=int,
        default=DEFAULT_MIN_CONTENT_GROUNDED_CASES,
    )
    parser.add_argument("--min-categories", type=int, default=DEFAULT_MIN_EVAL_CATEGORIES)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare and validate human Retrieval v3 ground truth.")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--golden", required=True)
    prepare.add_argument("--report", required=True)
    prepare.add_argument("--output", required=True)
    prepare.add_argument("--candidate-limit", type=int, default=10)
    validate = sub.add_parser("validate")
    validate.add_argument("review")
    _add_coverage_arguments(validate)
    validate.add_argument("--verbose", action="store_true")
    finalize = sub.add_parser("finalize")
    finalize.add_argument("review")
    finalize.add_argument("--output", required=True)
    _add_coverage_arguments(finalize)
    args = parser.parse_args(argv)

    if args.command == "prepare":
        output = Path(args.output).expanduser().resolve()
        if output.exists():
            raise SystemExit(f"Review queue уже существует: {output}")
        queue = prepare_review_queue(
            load_json_list(args.golden),
            load_json_object(args.report),
            golden_path=str(Path(args.golden).expanduser().resolve()),
            report_path=str(Path(args.report).expanduser().resolve()),
            candidate_limit=max(1, int(args.candidate_limit)),
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(queue, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"items": len(queue["items"]), "output": str(output)}, ensure_ascii=True, indent=2))
        return 0

    review_path = Path(args.review).expanduser().resolve()
    queue = load_json_object(review_path)
    validation = validate_review_queue(
        queue,
        min_items=max(0, int(args.min_items)),
        min_no_answer=max(0, int(args.min_no_answer)),
        min_forbidden=max(0, int(args.min_forbidden)),
        min_document_grounded=max(0, int(args.min_document_grounded)),
        min_content_grounded=max(0, int(args.min_content_grounded)),
        min_categories=max(0, int(args.min_categories)),
    )
    if args.command == "validate":
        summary = {key: value for key, value in validation.items() if key != "errors"}
        summary["error_counts"] = dict(Counter(str(error.get("error") or "unknown") for error in validation["errors"]))
        if args.verbose:
            summary["errors"] = validation["errors"]
        print(json.dumps(summary, ensure_ascii=True, indent=2))
        return 0 if validation["ok"] else 2

    output = Path(args.output).expanduser().resolve()
    if output.exists():
        raise SystemExit(f"Golden output уже существует: {output}")
    golden = finalize_review_queue(
        queue,
        min_items=max(0, int(args.min_items)),
        min_no_answer=max(0, int(args.min_no_answer)),
        min_forbidden=max(0, int(args.min_forbidden)),
        min_document_grounded=max(0, int(args.min_document_grounded)),
        min_content_grounded=max(0, int(args.min_content_grounded)),
        min_categories=max(0, int(args.min_categories)),
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(golden, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"items": len(golden), "output": str(output)}, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
