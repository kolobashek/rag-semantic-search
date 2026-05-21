"""Run offline relevance evaluation against the configured RAG searcher."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from rag_catalog.core.rag_core import RAGSearcher, load_config
from rag_catalog.core.search_eval import evaluate_search, load_golden_queries


def _parse_config_value(value: str) -> Any:
    raw = str(value or "").strip()
    lower = raw.lower()
    if lower in {"true", "yes", "on"}:
        return True
    if lower in {"false", "no", "off"}:
        return False
    try:
        if "." not in raw:
            return int(raw)
        return float(raw)
    except ValueError:
        return raw


def _apply_config_overrides(config: Dict[str, Any], items: List[str]) -> Dict[str, Any]:
    out = dict(config)
    for item in items:
        if "=" not in str(item):
            raise ValueError(f"Invalid --config-set value: {item!r}. Expected key=value.")
        key, value = str(item).split("=", 1)
        clean_key = key.strip()
        if not clean_key:
            raise ValueError(f"Invalid --config-set value: {item!r}. Empty key.")
        out[clean_key] = _parse_config_value(value)
    return out



def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate search relevance on a golden query set.")
    parser.add_argument("--golden", default="eval/search_golden.json", help="Path to golden JSON.")
    parser.add_argument("--limit", type=int, default=10, help="Top-k for metrics.")
    parser.add_argument("--output", default="", help="Optional JSON report path.")
    parser.add_argument("--markdown-output", default="", help="Optional Markdown summary path.")
    parser.add_argument("--fail-under-recall", type=float, default=0.0, help="Exit 1 if mean Recall@k is lower.")
    parser.add_argument("--no-warmup", action="store_true", help="Do not warm embedder/filesystem caches before timing.")
    parser.add_argument("--warmup-query", default="карточка предприятия", help="Query text used for eval warmup.")
    parser.add_argument(
        "--config-set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override config value for this eval run. Can be repeated.",
    )
    args = parser.parse_args()

    cfg = _apply_config_overrides(load_config(), list(args.config_set or []))
    searcher = RAGSearcher(cfg)
    golden = load_golden_queries(args.golden)
    if not args.no_warmup:
        try:
            searcher.embedder.encode(str(args.warmup_query or "warmup"), normalize_embeddings=True)
            if hasattr(searcher, "_refresh_fs_cache"):
                searcher._refresh_fs_cache()
        except Exception as exc:
            print(f"warning: search eval warmup failed: {exc}", file=sys.stderr)

    def _search(query: str, limit: int) -> List[Dict[str, Any]]:
        return searcher.search(query, limit=limit)

    report = evaluate_search(golden, _search, limit=max(1, int(args.limit)))
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text, encoding="utf-8")
    if args.markdown_output:
        md_path = Path(args.markdown_output)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        rows = [
            "# Search Evaluation Report",
            "",
            f"- Queries: {report['queries']}",
            f"- Limit: {report['limit']}",
            f"- Recall@k: {report['recall_at_k']:.3f}",
            f"- MRR@k: {report['mrr_at_k']:.3f}",
            f"- nDCG@k: {report['ndcg_at_k']:.3f}",
            f"- Zero-result rate: {report['zero_result_rate']:.3f}",
            f"- Latency p50: {report['latency_p50_ms']} ms",
            f"- Latency p95: {report['latency_p95_ms']} ms",
            "",
            "## By Category",
            "",
            "| Category | Queries | Recall | MRR | nDCG | Zero-result | p50 ms | p95 ms |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for category, metrics in sorted(report.get("by_category", {}).items()):
            safe_category = str(category).replace("|", r"\|")
            rows.append(
                f"| {safe_category} | {metrics['queries']} | {metrics['recall_at_k']:.3f} | "
                f"{metrics['mrr_at_k']:.3f} | {metrics['ndcg_at_k']:.3f} | "
                f"{metrics['zero_result_rate']:.3f} | {metrics['latency_p50_ms']} | {metrics['latency_p95_ms']} |"
            )
        rows.extend([
            "",
            "## Queries",
            "",
            "| Category | Query | Recall | MRR | nDCG | Results | Latency ms |",
            "|---|---|---:|---:|---:|---:|---:|",
        ])
        for row in report["rows"]:
            category = str(row.get("category") or "general").replace("|", r"\|")
            query = str(row["query"]).replace("|", r"\|")
            rows.append(
                f"| {category} | {query} | {row['recall_at_k']:.3f} | {row['mrr_at_k']:.3f} | "
                f"{row['ndcg_at_k']:.3f} | {row['results_count']} | {row['latency_ms']} |"
            )
        md_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(text)
    if float(report["recall_at_k"]) < float(args.fail_under_recall):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
