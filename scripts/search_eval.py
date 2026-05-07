"""Run offline relevance evaluation against the configured RAG searcher."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from rag_catalog.core.rag_core import RAGSearcher, load_config
from rag_catalog.core.search_eval import evaluate_search, load_golden_queries


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate search relevance on a golden query set.")
    parser.add_argument("--golden", default="eval/search_golden.json", help="Path to golden JSON.")
    parser.add_argument("--limit", type=int, default=10, help="Top-k for metrics.")
    parser.add_argument("--output", default="", help="Optional JSON report path.")
    parser.add_argument("--fail-under-recall", type=float, default=0.0, help="Exit 1 if mean Recall@k is lower.")
    args = parser.parse_args()

    cfg = load_config()
    searcher = RAGSearcher(cfg)
    golden = load_golden_queries(args.golden)

    def _search(query: str, limit: int) -> List[Dict[str, Any]]:
        return searcher.search(query, limit=limit)

    report = evaluate_search(golden, _search, limit=max(1, int(args.limit)))
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text, encoding="utf-8")
    print(text)
    if float(report["recall_at_k"]) < float(args.fail_under_recall):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
