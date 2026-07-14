"""Run offline relevance evaluation against the configured RAG searcher."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from rag_catalog.cli.finalize_search_index import collection_readiness
from rag_catalog.cli.pilot_gate import source_fingerprint
from rag_catalog.core.rag_core import RAGSearcher, apply_retrieval_preset, load_config
from rag_catalog.core.search_eval import (
    DEFAULT_MIN_CONTENT_GROUNDED_CASES,
    DEFAULT_MIN_DOCUMENT_GROUNDED_CASES,
    DEFAULT_MIN_EVAL_CATEGORIES,
    DEFAULT_MIN_EVAL_QUERIES,
    DEFAULT_MIN_NO_ANSWER_CASES,
    evaluate_retrieval_decision,
    evaluate_search,
    load_golden_queries,
)

_EVALUATION_PROTOCOL_VERSION = "search-eval-v2"


def _evaluation_fingerprints(golden_path: Path, *, source: str, limit: int) -> Dict[str, Any]:
    golden_fingerprint = hashlib.sha256(golden_path.read_bytes()).hexdigest()
    protocol = {
        "version": _EVALUATION_PROTOCOL_VERSION,
        "limit": max(1, int(limit)),
    }
    protocol_json = json.dumps(protocol, sort_keys=True, separators=(",", ":"))
    combined = hashlib.sha256(
        f"{source}\0{golden_fingerprint}\0{protocol_json}".encode("utf-8")
    ).hexdigest()
    return {
        "source_fingerprint": source,
        "golden_fingerprint": golden_fingerprint,
        "evaluation_protocol": protocol,
        "evaluation_fingerprint": combined,
    }


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


def _parse_named_values(items: List[str], *, option_name: str) -> Dict[str, Any]:
    parsed: Dict[str, Any] = {}
    for item in items:
        if "=" not in str(item):
            raise ValueError(f"Invalid {option_name} value: {item!r}. Expected key=value.")
        key, value = str(item).split("=", 1)
        clean_key = key.strip()
        if not clean_key:
            raise ValueError(f"Invalid {option_name} value: {item!r}. Empty key.")
        parsed[clean_key] = _parse_config_value(value)
    return parsed


def _apply_config_overrides(config: Dict[str, Any], items: List[str]) -> Dict[str, Any]:
    out = dict(config)
    parsed = _parse_named_values(items, option_name="--config-set")
    out.update(parsed)
    explicit_keys = set(parsed)
    if "retrieval_preset" in explicit_keys:
        return apply_retrieval_preset(out, explicit_keys)
    return out


def _enforce_eval_runtime_contracts(config: Dict[str, Any]) -> Dict[str, Any]:
    """Keep optional retrieval stages observable instead of silently falling back."""
    out = dict(config)
    if bool(out.get("retrieval_fulltext_enabled", False)):
        out["retrieval_fulltext_fail_open"] = False
    if bool(out.get("retrieval_reranker_enabled", False)):
        out["retrieval_reranker_fail_open"] = False
    return out


def _format_metric(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.3f}"


def _apply_acl_evidence(
    report: Dict[str, Any],
    evidence: Dict[str, Any],
    *,
    evidence_path: str,
    current_source_fingerprint: str,
) -> Dict[str, Any]:
    """Merge fresh authenticated search ACL evidence into retrieval metrics."""
    if not evidence.get("ok"):
        raise ValueError("ACL evidence artifact is not successful.")
    artifact_fingerprint = str(evidence.get("source_fingerprint") or "")
    if not current_source_fingerprint or artifact_fingerprint != current_source_fingerprint:
        raise ValueError("ACL evidence source fingerprint does not match current sources.")
    checked = int(evidence.get("acl_results_checked") or 0)
    leakage_rate = float(evidence.get("acl_leakage_rate") or 0.0)
    if checked <= 0:
        raise ValueError("ACL evidence contains no checked forbidden search results.")
    if not 0.0 <= leakage_rate <= 1.0:
        raise ValueError("ACL evidence leakage rate must be between 0 and 1.")

    existing_checked = int(report.get("acl_results_checked") or 0)
    existing_rate = float(report.get("acl_leakage_rate") or 0.0)
    total_checked = existing_checked + checked
    total_leaks = existing_rate * existing_checked + leakage_rate * checked
    merged = dict(report)
    merged["acl_results_checked"] = total_checked
    merged["acl_leakage_rate"] = total_leaks / max(1, total_checked)
    merged["acl_evidence"] = {
        "path": evidence_path,
        "source_fingerprint": artifact_fingerprint,
        "results_checked": checked,
        "leakage_rate": leakage_rate,
    }
    return merged


def _validate_index_readiness_evidence(
    evidence: Dict[str, Any],
    *,
    evidence_path: str,
    collection_name: str,
    live_readiness: Dict[str, Any],
) -> Dict[str, Any]:
    """Bind a full finalization artifact to the current live collection state."""
    reasons: List[str] = []
    artifact_collection = str(evidence.get("collection_name") or "")
    expected_collection = str(collection_name or "")
    live_collection = str(live_readiness.get("collection_name") or "")
    artifact_points = int(evidence.get("points_count") or 0)
    live_points = int(live_readiness.get("points_count") or 0)
    artifact_indexed = int(evidence.get("indexed_vectors_count") or 0)
    live_indexed = int(live_readiness.get("indexed_vectors_count") or 0)
    payload_integrity = evidence.get("payload_integrity")
    payload = payload_integrity if isinstance(payload_integrity, dict) else {}
    spreadsheet_integrity = evidence.get("spreadsheet_integrity")
    spreadsheet = spreadsheet_integrity if isinstance(spreadsheet_integrity, dict) else {}

    if evidence.get("ready") is not True:
        reasons.append("artifact_not_ready")
    if not expected_collection or artifact_collection != expected_collection:
        reasons.append("artifact_collection_mismatch")
    if live_collection != expected_collection:
        reasons.append("live_collection_mismatch")
    if live_readiness.get("ready") is not True:
        reasons.append("live_collection_not_ready")
    if artifact_points <= 0 or artifact_points != live_points:
        reasons.append("points_count_mismatch")
    if artifact_indexed <= 0 or live_indexed < artifact_indexed:
        reasons.append("indexed_vectors_regressed")
    if payload.get("ok") is not True or int(payload.get("sample_size") or 0) <= 0:
        reasons.append("payload_integrity_missing_or_failed")
    if spreadsheet.get("ok") is not True:
        reasons.append("spreadsheet_integrity_failed")
    if spreadsheet.get("scanned_all") is not True:
        reasons.append("spreadsheet_full_audit_missing")
    if str(spreadsheet.get("sampling_strategy") or "") != "full_scroll":
        reasons.append("spreadsheet_audit_not_full_scroll")
    if int(spreadsheet.get("sample_size") or 0) <= 0:
        reasons.append("spreadsheet_audit_empty")
    if reasons:
        raise ValueError("Index readiness evidence is invalid: " + ", ".join(reasons))

    return {
        "ok": True,
        "path": evidence_path,
        "collection_name": expected_collection,
        "points_count": artifact_points,
        "indexed_vectors_count": artifact_indexed,
        "live_points_count": live_points,
        "live_indexed_vectors_count": live_indexed,
        "payload_integrity": payload,
        "spreadsheet_integrity": spreadsheet,
    }



def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate search relevance on a golden query set.")
    parser.add_argument("--golden", default="eval/search_golden.json", help="Path to golden JSON.")
    parser.add_argument("--limit", type=int, default=10, help="Top-k for metrics.")
    parser.add_argument("--output", default="", help="Optional JSON report path.")
    parser.add_argument("--markdown-output", default="", help="Optional Markdown summary path.")
    parser.add_argument("--fail-under-recall", type=float, default=0.0, help="Exit 1 if mean Recall@k is lower.")
    parser.add_argument("--baseline-report", default="", help="Optional baseline JSON for candidate regression checks.")
    parser.add_argument(
        "--acl-evidence",
        default="",
        help="Fresh pilot UI smoke JSON containing authenticated search ACL evidence.",
    )
    parser.add_argument(
        "--index-readiness-evidence",
        default="",
        help="Full finalization JSON produced with --spreadsheet-full-audit.",
    )
    parser.add_argument("--decision-output", default="", help="Optional JSON path for the GO/NO_GO decision.")
    parser.add_argument("--enforce-decision-gate", action="store_true", help="Exit 1 when the retrieval decision is NO_GO.")
    parser.add_argument("--max-p95-ms", type=int, default=3000)
    parser.add_argument("--max-p95-ratio", type=float, default=1.5)
    parser.add_argument("--max-recall-drop", type=float, default=0.0)
    parser.add_argument("--min-precision-at-k", type=float, default=0.5)
    parser.add_argument("--min-top1-accuracy", type=float, default=0.8)
    parser.add_argument("--max-irrelevant-rate", type=float, default=0.5)
    parser.add_argument("--max-precision-drop", type=float, default=0.0)
    parser.add_argument("--max-top1-drop", type=float, default=0.0)
    parser.add_argument("--max-acl-leakage", type=float, default=0.0)
    parser.add_argument("--min-no-answer-accuracy", type=float, default=0.8)
    parser.add_argument("--min-ground-truth-coverage", type=float, default=0.5)
    parser.add_argument("--min-eval-queries", type=int, default=DEFAULT_MIN_EVAL_QUERIES)
    parser.add_argument("--min-no-answer-cases", type=int, default=DEFAULT_MIN_NO_ANSWER_CASES)
    parser.add_argument(
        "--min-document-grounded-cases",
        type=int,
        default=DEFAULT_MIN_DOCUMENT_GROUNDED_CASES,
    )
    parser.add_argument(
        "--min-content-grounded-cases",
        type=int,
        default=DEFAULT_MIN_CONTENT_GROUNDED_CASES,
    )
    parser.add_argument("--min-categories", type=int, default=DEFAULT_MIN_EVAL_CATEGORIES)
    parser.add_argument("--require-faithfulness", action="store_true")
    parser.add_argument("--no-warmup", action="store_true", help="Do not warm embedder/filesystem caches before timing.")
    parser.add_argument("--warmup-query", default="карточка предприятия", help="Query text used for eval warmup.")
    parser.add_argument(
        "--config-set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override config value for this eval run. Can be repeated.",
    )
    parser.add_argument(
        "--require-profile",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Require an exact resolved evaluation-profile value. Can be repeated.",
    )
    args = parser.parse_args()

    cfg = _enforce_eval_runtime_contracts(
        _apply_config_overrides(load_config(), list(args.config_set or []))
    )
    required_profile = _parse_named_values(
        list(args.require_profile or []),
        option_name="--require-profile",
    )
    searcher = RAGSearcher(cfg)
    try:
        collection_info = searcher.qdrant.get_collection(searcher.collection_name)
        live_index_readiness = {
            **collection_readiness(
                collection_info,
                require_fulltext=bool(cfg.get("retrieval_fulltext_enabled", False)),
                max_unindexed_vectors=int(cfg.get("qdrant_max_unindexed_vectors") or 50_000),
            ),
            "collection_name": searcher.collection_name,
        }
    except Exception as exc:
        live_index_readiness = {
            "ready": False,
            "collection_name": searcher.collection_name,
            "reasons": [f"readiness_probe_failed:{type(exc).__name__}"],
        }
    index_quality_evidence: Dict[str, Any] = {
        "ok": False,
        "collection_name": searcher.collection_name,
        "reasons": ["index_readiness_evidence_missing"],
    }
    if str(args.index_readiness_evidence or "").strip():
        evidence_path = Path(str(args.index_readiness_evidence)).expanduser().resolve()
        try:
            evidence_value = json.loads(evidence_path.read_text(encoding="utf-8"))
            if not isinstance(evidence_value, dict):
                raise ValueError("Index readiness evidence must be a JSON object.")
            index_quality_evidence = _validate_index_readiness_evidence(
                evidence_value,
                evidence_path=str(evidence_path),
                collection_name=searcher.collection_name,
                live_readiness=live_index_readiness,
            )
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            print(f"invalid index readiness evidence: {exc}", file=sys.stderr)
            return 2
    golden_path = Path(args.golden).expanduser().resolve()
    golden = load_golden_queries(golden_path)
    current_source_fingerprint = source_fingerprint(ROOT)
    evaluation_fingerprints = _evaluation_fingerprints(
        golden_path,
        source=current_source_fingerprint,
        limit=max(1, int(args.limit)),
    )
    if not args.no_warmup:
        try:
            searcher.embedder.encode(str(args.warmup_query or "warmup"), normalize_embeddings=True)
            searcher.warm_retrieval_cache()
        except Exception as exc:
            print(f"warning: search eval warmup failed: {exc}", file=sys.stderr)

    progress = {"index": 0}

    def _search(query: str, limit: int) -> List[Dict[str, Any]]:
        progress["index"] += 1
        current = progress["index"]
        started = time.perf_counter()
        print(f"[eval {current}/{len(golden)}] start: {query}", file=sys.stderr, flush=True)
        try:
            return searcher.search(query, limit=limit)
        finally:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            print(f"[eval {current}/{len(golden)}] done: {elapsed_ms} ms", file=sys.stderr, flush=True)

    report = evaluate_search(golden, _search, limit=max(1, int(args.limit)))
    report.update(evaluation_fingerprints)
    if str(args.acl_evidence or "").strip():
        evidence_path = Path(str(args.acl_evidence)).expanduser().resolve()
        try:
            evidence_value = json.loads(evidence_path.read_text(encoding="utf-8"))
            if not isinstance(evidence_value, dict):
                raise ValueError("ACL evidence must be a JSON object.")
            report = _apply_acl_evidence(
                report,
                evidence_value,
                evidence_path=str(evidence_path),
                current_source_fingerprint=current_source_fingerprint,
            )
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            print(f"invalid ACL evidence: {exc}", file=sys.stderr)
            return 2
    report["evaluation_profile"] = {
        "retrieval_preset": str(cfg.get("retrieval_preset") or "legacy"),
        "retrieval_pipeline": str(cfg.get("retrieval_pipeline") or "legacy"),
        "bm25_enabled": bool(cfg.get("retrieval_bm25_enabled")),
        "bm25_top_k": int(cfg.get("retrieval_bm25_top_k") or 0),
        "fulltext_enabled": bool(cfg.get("retrieval_fulltext_enabled")),
        "fulltext_top_k": int(cfg.get("retrieval_fulltext_top_k") or 0),
        "embedding_model": str(cfg.get("embedding_model") or ""),
        "embedding_backend": str(cfg.get("embedding_backend") or ""),
        "relevance_gate_enabled": bool(cfg.get("retrieval_relevance_gate_enabled")),
        "min_dense_score": float(cfg.get("retrieval_min_dense_score") or 0.0),
        "single_term_min_dense_score": float(cfg.get("retrieval_single_term_min_dense_score") or 0.0),
        "reranker_enabled": bool(cfg.get("retrieval_reranker_enabled")),
        "reranker_model": str(cfg.get("retrieval_reranker_model") or ""),
        "reranker_backend": str(cfg.get("retrieval_reranker_backend") or ""),
        "collection_name": searcher.collection_name,
    }
    report["index_readiness"] = live_index_readiness
    report["index_quality_evidence"] = index_quality_evidence
    baseline = None
    if args.baseline_report:
        baseline = json.loads(Path(args.baseline_report).read_text(encoding="utf-8"))
    decision = evaluate_retrieval_decision(
        report,
        baseline=baseline,
        min_recall=max(0.0, float(args.fail_under_recall or 0.875)),
        min_precision=max(0.0, float(args.min_precision_at_k)),
        min_top1_accuracy=max(0.0, float(args.min_top1_accuracy)),
        max_irrelevant_rate=max(0.0, float(args.max_irrelevant_rate)),
        max_recall_drop=max(0.0, float(args.max_recall_drop)),
        max_precision_drop=max(0.0, float(args.max_precision_drop)),
        max_top1_drop=max(0.0, float(args.max_top1_drop)),
        max_p95_ms=max(1, int(args.max_p95_ms)),
        max_p95_ratio=max(1.0, float(args.max_p95_ratio)),
        max_acl_leakage=max(0.0, float(args.max_acl_leakage)),
        min_no_answer_accuracy=max(0.0, float(args.min_no_answer_accuracy)),
        min_ground_truth_coverage=max(0.0, float(args.min_ground_truth_coverage)),
        min_eval_queries=max(0, int(args.min_eval_queries)),
        min_no_answer_cases=max(0, int(args.min_no_answer_cases)),
        min_document_grounded_cases=max(0, int(args.min_document_grounded_cases)),
        min_content_grounded_cases=max(0, int(args.min_content_grounded_cases)),
        min_categories=max(0, int(args.min_categories)),
        require_faithfulness=bool(args.require_faithfulness),
        required_profile=required_profile,
    )
    report["retrieval_decision"] = decision
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
            f"- Categories: {report['categories_count']}",
            f"- No-answer cases: {report['no_answer_cases']}",
            f"- Document-grounded cases: {report['document_grounded_cases']}",
            f"- Content-grounded cases: {report['content_grounded_cases']}",
            f"- Source fingerprint: {report['source_fingerprint']}",
            f"- Golden fingerprint: {report['golden_fingerprint']}",
            f"- Evaluation fingerprint: {report['evaluation_fingerprint']}",
            f"- Limit: {report['limit']}",
            f"- Recall@k: {report['recall_at_k']:.3f}",
            f"- Precision@k: {report['precision_at_k']:.3f}",
            f"- Irrelevant result rate: {report['irrelevant_rate_at_k']:.3f}",
            f"- Top-1 accuracy: {report['top1_accuracy']:.3f}",
            f"- MRR@k: {report['mrr_at_k']:.3f}",
            f"- nDCG@k: {report['ndcg_at_k']:.3f}",
            f"- Zero-result rate: {report['zero_result_rate']:.3f}",
            f"- Latency p50: {report['latency_p50_ms']} ms",
            f"- Latency p95: {report['latency_p95_ms']} ms",
            f"- Document hit rate: {report['document_hit_rate']}",
            f"- Chunk hit rate: {report['chunk_hit_rate']}",
            f"- Page hit rate: {report['page_hit_rate']}",
            f"- No-answer accuracy: {report['no_answer_accuracy']}",
            f"- ACL results checked: {report['acl_results_checked']}",
            f"- ACL leakage rate: {report['acl_leakage_rate']:.6f}",
            f"- Ground-truth coverage: {report['ground_truth_coverage']:.3f}",
            f"- Evaluated results: {report['evaluated_results_count']}",
            f"- Reranked results: {report['reranked_results_count']}",
            f"- Reranker coverage: {_format_metric(report['reranker_coverage'])}",
            f"- Retrieval source counts: {json.dumps(report['retrieval_source_counts'], ensure_ascii=False, sort_keys=True)}",
            f"- Retrieval decision: {decision['decision']}",
            "",
            "## By Category",
            "",
            "| Category | Queries | Recall | Precision | Top-1 | MRR | nDCG | Zero-result | p50 ms | p95 ms |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for category, metrics in sorted(report.get("by_category", {}).items()):
            safe_category = str(category).replace("|", r"\|")
            rows.append(
                f"| {safe_category} | {metrics['queries']} | {metrics['recall_at_k']:.3f} | "
                f"{metrics['precision_at_k']:.3f} | {metrics['top1_accuracy']:.3f} | "
                f"{metrics['mrr_at_k']:.3f} | {metrics['ndcg_at_k']:.3f} | "
                f"{metrics['zero_result_rate']:.3f} | {metrics['latency_p50_ms']} | {metrics['latency_p95_ms']} |"
            )
        rows.extend([
            "",
            "## Queries",
            "",
            "| Category | Query | Recall | Precision | Top-1 | MRR | nDCG | Results | Reranked | Latency ms |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ])
        for row in report["rows"]:
            category = str(row.get("category") or "general").replace("|", r"\|")
            query = str(row["query"]).replace("|", r"\|")
            rows.append(
                f"| {category} | {query} | {_format_metric(row['recall_at_k'])} | "
                f"{_format_metric(row['precision_at_k'])} | {_format_metric(row['top1_relevant'])} | "
                f"{_format_metric(row['mrr_at_k'])} | {_format_metric(row['ndcg_at_k'])} | "
                f"{row['results_count']} | {row['reranked_results_count']} | {row['latency_ms']} |"
            )
        md_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    if args.decision_output:
        decision_path = Path(args.decision_output)
        decision_path.parent.mkdir(parents=True, exist_ok=True)
        decision_path.write_text(json.dumps(decision, ensure_ascii=False, indent=2), encoding="utf-8")
    print(text)
    if float(report["recall_at_k"]) < float(args.fail_under_recall):
        return 1
    if args.enforce_decision_gate and not decision["ok"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
