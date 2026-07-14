"""Offline relevance evaluation helpers for RAG search."""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

_TOKEN_RE = re.compile(r"[a-zа-яё0-9\-]{2,}", flags=re.IGNORECASE)
_STOPWORDS = {"и", "или", "по", "на", "в", "во", "от", "для", "мне", "нужен", "нужна"}
_TERM_ALIASES: Dict[str, List[str]] = {
    "touareg": ["туарег", "фольксваген", "volkswagen", "vw"],
    "туарег": ["touareg", "фольксваген", "volkswagen", "vw"],
    "volkswagen": ["фольксваген", "vw"],
    "фольксваген": ["volkswagen", "vw"],
    "обслуживания": ["обслуживание", "техническое обслуживание", "услуги", "ремонт", "сервис"],
    "технических": ["технические", "техническое обслуживание", "услуги", "ремонт", "сервис"],
    "реквизит": ["реквизиты", "карточка предприятия", "карточка организации", "карточка"],
    "реквизиты": ["реквизит", "карточка предприятия", "карточка организации", "карточка"],
    "псм": ["паспорт самоходной машины", "электронного паспорта", "паспорт техники"],
    "птс": ["паспорт транспортного средства", "техпаспорт"],
    "стс": ["свидетельство о регистрации", "регистрации"],
    "vin": [
        "шильдик",
        "табличка",
        "заводская табличка",
        "паспорт транспортного средства",
        "свидетельство о регистрации",
        "птс",
        "стс",
    ],
}


@dataclass(frozen=True)
class GoldenQuery:
    query: str
    expected: List[str]
    category: str = "general"
    expected_paths: List[str] | None = None
    expected_chunks: List[str] | None = None
    expected_pages: List[int] | None = None
    forbidden: List[str] | None = None
    expect_no_answer: bool = False


def load_golden_queries(path: str | Path) -> List[GoldenQuery]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Golden set must be a JSON list.")
    out: List[GoldenQuery] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        query = str(item.get("query") or "").strip()
        expected = [str(x).strip() for x in item.get("expected", []) if str(x).strip()]
        category = str(item.get("category") or "general").strip() or "general"
        expected_paths = [str(x).strip() for x in item.get("expected_paths", []) if str(x).strip()]
        expected_chunks = [str(x).strip() for x in item.get("expected_chunks", []) if str(x).strip()]
        expected_pages = [int(x) for x in item.get("expected_pages", []) if str(x).strip()]
        forbidden = [str(x).strip() for x in item.get("forbidden", []) if str(x).strip()]
        expect_no_answer = bool(item.get("expect_no_answer"))
        if query and (expected or expected_paths or expected_chunks or expected_pages or forbidden or expect_no_answer):
            out.append(
                GoldenQuery(
                    query=query,
                    expected=expected,
                    category=category,
                    expected_paths=expected_paths,
                    expected_chunks=expected_chunks,
                    expected_pages=expected_pages,
                    forbidden=forbidden,
                    expect_no_answer=expect_no_answer,
                )
            )
    if not out:
        raise ValueError("Golden set is empty.")
    return out


def _result_text(result: Dict[str, Any]) -> str:
    return " ".join(
        str(result.get(key) or "")
        for key in ("filename", "path", "full_path", "cloud_path", "text")
    ).lower().replace("ё", "е")


def _result_path_text(result: Dict[str, Any]) -> str:
    return " ".join(
        str(result.get(key) or "") for key in ("filename", "path", "full_path", "cloud_path")
    ).lower().replace("ё", "е")


def _result_chunk_text(result: Dict[str, Any]) -> str:
    return " ".join(
        str(result.get(key) or "") for key in ("text", "chunk_text", "content", "snippet")
    ).lower().replace("ё", "е")


def _result_page(result: Dict[str, Any]) -> int | None:
    for key in ("page", "page_number", "page_no"):
        value = result.get(key)
        if value is None or str(value).strip() == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _expected_hit(texts: Iterable[str], expected: List[str]) -> bool:
    return any(_text_matches_expected(text, needle) for text in texts for needle in expected)


def _expected_text(value: str) -> str:
    return str(value or "").lower().replace("ё", "е")


def _stem(term: str) -> str:
    clean = _expected_text(term)
    if len(clean) < 5:
        return clean
    stem = clean.rstrip("аеиоуыьъйяю")
    return stem if len(stem) >= 4 else clean


def _term_variants(term: str) -> List[str]:
    clean = _expected_text(term)
    variants = [clean]
    for alias in _TERM_ALIASES.get(clean, []):
        alias_norm = _expected_text(alias)
        if alias_norm and alias_norm not in variants:
            variants.append(alias_norm)
    stem = _stem(clean)
    if stem and stem != clean:
        variants.append(stem)
    return variants


def _text_matches_expected(text: str, expected: str) -> bool:
    haystack = _expected_text(text)
    if not haystack:
        return False
    for variant in _term_variants(expected):
        if not variant:
            continue
        if " " in variant:
            if variant in haystack:
                return True
            continue
        if variant in haystack:
            return True
        variant_stem = _stem(variant)
        if len(variant_stem) >= 4:
            tokens = [
                token
                for token in _TOKEN_RE.findall(haystack)
                if token not in _STOPWORDS
            ]
            if any(token.startswith(variant_stem) or variant_stem in token for token in tokens):
                return True
    return False


def _is_entity_expected(value: str) -> bool:
    text = _expected_text(value)
    return bool(re.search(r"\d", text))


def _result_matches_expected(text: str, expected: str, all_expected: List[str]) -> bool:
    if _text_matches_expected(text, expected):
        return True
    expected_norm = _expected_text(expected)
    if expected_norm not in {"паспорт", "паспорта", "псм", "техпаспорт"}:
        return False
    haystack = _expected_text(text)
    if ".pdf" not in haystack:
        return False
    return any(
        other != expected and _is_entity_expected(other) and _text_matches_expected(haystack, other)
        for other in all_expected
    )


def relevance_vector(results: Iterable[Dict[str, Any]], expected: List[str], *, limit: int) -> List[int]:
    needles = [_expected_text(item) for item in expected if item]
    vector: List[int] = []
    for result in list(results)[: max(1, int(limit))]:
        haystack = _result_text(result)
        vector.append(1 if needles and all(_result_matches_expected(haystack, needle, needles) for needle in needles) else 0)
    return vector


def recall_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    needles = [_expected_text(item) for item in expected if item]
    if not needles:
        return 0.0
    top_texts = [_result_text(item) for item in results[: max(1, int(k))]]
    matched = sum(1 for needle in needles if any(_result_matches_expected(text, needle, needles) for text in top_texts))
    return matched / len(needles)


def precision_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    """Return the share of returned top-k results matching the complete intent."""
    rels = relevance_vector(results, expected, limit=k)
    return sum(rels) / len(rels) if rels else 0.0


def mrr_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    for idx, rel in enumerate(relevance_vector(results, expected, limit=k), start=1):
        if rel:
            return 1.0 / idx
    return 0.0


def ndcg_at_k(results: List[Dict[str, Any]], expected: List[str], *, k: int) -> float:
    rels = relevance_vector(results, expected, limit=k)
    dcg = sum(rel / math.log2(idx + 2) for idx, rel in enumerate(rels))
    ideal_hits = min(sum(rels), max(1, int(k)))
    ideal = sum(1.0 / math.log2(idx + 2) for idx in range(ideal_hits))
    return dcg / ideal if ideal else 0.0


def _percentile(values: List[int], pct: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, math.ceil((float(pct) / 100.0) * len(ordered)) - 1))
    return int(ordered[idx])


def _mean_defined(rows: List[Dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return sum(values) / len(values) if values else None


def evaluate_search(
    golden: List[GoldenQuery],
    search_fn: Callable[[str, int], List[Dict[str, Any]]],
    *,
    limit: int = 10,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for item in golden:
        started = time.perf_counter()
        results = search_fn(item.query, limit)
        latency_ms = int((time.perf_counter() - started) * 1000)
        limited_results = results[: max(1, int(limit))]
        expected_paths = list(item.expected_paths or [])
        expected_chunks = list(item.expected_chunks or [])
        expected_pages = list(item.expected_pages or [])
        forbidden = list(item.forbidden or [])
        path_texts = [_result_path_text(result) for result in limited_results]
        chunk_texts = [_result_chunk_text(result) for result in limited_results]
        pages = [_result_page(result) for result in limited_results]
        acl_leaks = sum(1 for result in limited_results if _expected_hit([_result_text(result)], forbidden)) if forbidden else 0
        relevance_recall = recall_at_k(results, item.expected, k=limit) if item.expected and not item.expect_no_answer else None
        relevance_precision = (
            precision_at_k(results, item.expected, k=limit)
            if item.expected and not item.expect_no_answer
            else None
        )
        relevance_mrr = mrr_at_k(results, item.expected, k=limit) if item.expected and not item.expect_no_answer else None
        relevance_ndcg = ndcg_at_k(results, item.expected, k=limit) if item.expected and not item.expect_no_answer else None
        top1_relevant = (
            float(bool(relevance_vector(results, item.expected, limit=1)[0]))
            if item.expected and not item.expect_no_answer and limited_results
            else (0.0 if item.expected and not item.expect_no_answer else None)
        )
        rows.append(
            {
                "query": item.query,
                "expected": item.expected,
                "category": item.category,
                "results_count": len(results),
                "recall_at_k": relevance_recall,
                "precision_at_k": relevance_precision,
                "irrelevant_rate_at_k": (1.0 - relevance_precision) if relevance_precision is not None else None,
                "top1_relevant": top1_relevant,
                "mrr_at_k": relevance_mrr,
                "ndcg_at_k": relevance_ndcg,
                "document_hit": 1.0 if expected_paths and _expected_hit(path_texts, expected_paths) else (0.0 if expected_paths else None),
                "chunk_hit": 1.0 if expected_chunks and _expected_hit(chunk_texts, expected_chunks) else (0.0 if expected_chunks else None),
                "page_hit": 1.0 if expected_pages and any(page in expected_pages for page in pages) else (0.0 if expected_pages else None),
                "no_answer_ok": (1.0 if not results else 0.0) if item.expect_no_answer else None,
                "acl_leaks": acl_leaks,
                "acl_results_checked": len(limited_results) if forbidden else 0,
                "latency_ms": latency_ms,
                "top": [
                    {
                        "filename": str(result.get("filename") or ""),
                        "path": str(result.get("path") or result.get("full_path") or result.get("cloud_path") or ""),
                        "page": _result_page(result),
                        "score": float(result.get("rank_score", result.get("score") or 0) or 0),
                    }
                    for result in results[:limit]
                ],
            }
        )
    relevance_rows = [row for row in rows if row.get("recall_at_k") is not None]
    count = max(1, len(relevance_rows))
    latencies = [int(row["latency_ms"]) for row in rows]
    zero_results = sum(1 for row in rows if int(row["results_count"] or 0) == 0)
    by_category: Dict[str, Dict[str, Any]] = {}
    for category in sorted({str(row.get("category") or "general") for row in rows}):
        cat_rows = [row for row in rows if str(row.get("category") or "general") == category]
        cat_relevance_rows = [row for row in cat_rows if row.get("recall_at_k") is not None]
        cat_count = max(1, len(cat_relevance_rows))
        cat_latencies = [int(row["latency_ms"]) for row in cat_rows]
        by_category[category] = {
            "queries": len(cat_rows),
            "recall_at_k": sum(float(row["recall_at_k"]) for row in cat_relevance_rows) / cat_count,
            "precision_at_k": sum(float(row["precision_at_k"]) for row in cat_relevance_rows) / cat_count,
            "irrelevant_rate_at_k": sum(float(row["irrelevant_rate_at_k"]) for row in cat_relevance_rows)
            / cat_count,
            "top1_accuracy": sum(float(row["top1_relevant"]) for row in cat_relevance_rows) / cat_count,
            "mrr_at_k": sum(float(row["mrr_at_k"]) for row in cat_relevance_rows) / cat_count,
            "ndcg_at_k": sum(float(row["ndcg_at_k"]) for row in cat_relevance_rows) / cat_count,
            "zero_result_rate": sum(1 for row in cat_rows if int(row["results_count"] or 0) == 0) / max(1, len(cat_rows)),
            "latency_p50_ms": _percentile(cat_latencies, 50),
            "latency_p95_ms": _percentile(cat_latencies, 95),
        }
    return {
        "queries": len(rows),
        "categories_count": len(by_category),
        "no_answer_cases": sum(1 for item in golden if item.expect_no_answer),
        "document_grounded_cases": sum(1 for item in golden if item.expected_paths),
        "content_grounded_cases": sum(
            1 for item in golden if item.expected_chunks or item.expected_pages
        ),
        "acl_cases": sum(1 for item in golden if item.forbidden),
        "limit": int(limit),
        "recall_at_k": sum(float(row["recall_at_k"]) for row in relevance_rows) / count,
        "precision_at_k": sum(float(row["precision_at_k"]) for row in relevance_rows) / count,
        "irrelevant_rate_at_k": sum(float(row["irrelevant_rate_at_k"]) for row in relevance_rows) / count,
        "top1_accuracy": sum(float(row["top1_relevant"]) for row in relevance_rows) / count,
        "mrr_at_k": sum(float(row["mrr_at_k"]) for row in relevance_rows) / count,
        "ndcg_at_k": sum(float(row["ndcg_at_k"]) for row in relevance_rows) / count,
        "zero_result_rate": zero_results / max(1, len(rows)),
        "document_hit_rate": _mean_defined(rows, "document_hit"),
        "chunk_hit_rate": _mean_defined(rows, "chunk_hit"),
        "page_hit_rate": _mean_defined(rows, "page_hit"),
        "no_answer_accuracy": _mean_defined(rows, "no_answer_ok"),
        "acl_results_checked": sum(int(row["acl_results_checked"]) for row in rows),
        "acl_leakage_rate": (
            sum(int(row["acl_leaks"]) for row in rows)
            / max(1, sum(int(row["acl_results_checked"]) for row in rows))
        ),
        "ground_truth_coverage": sum(
            1
            for item in golden
            if item.expected_paths or item.expected_chunks or item.expected_pages or item.forbidden or item.expect_no_answer
        )
        / max(1, len(golden)),
        "faithfulness_evaluated": False,
        "latency_p50_ms": _percentile(latencies, 50),
        "latency_p95_ms": _percentile(latencies, 95),
        "by_category": by_category,
        "rows": rows,
    }


def evaluate_retrieval_decision(
    candidate: Dict[str, Any],
    *,
    baseline: Dict[str, Any] | None = None,
    min_recall: float = 0.875,
    min_precision: float = 0.5,
    min_top1_accuracy: float = 0.8,
    max_irrelevant_rate: float = 0.5,
    max_recall_drop: float = 0.0,
    max_precision_drop: float = 0.0,
    max_top1_drop: float = 0.0,
    max_p95_ms: int = 3000,
    max_p95_ratio: float = 1.5,
    max_acl_leakage: float = 0.0,
    min_no_answer_accuracy: float = 0.8,
    min_ground_truth_coverage: float = 0.5,
    min_eval_queries: int = 50,
    min_no_answer_cases: int = 10,
    min_document_grounded_cases: int = 20,
    min_content_grounded_cases: int = 10,
    min_categories: int = 6,
    require_faithfulness: bool = False,
) -> Dict[str, Any]:
    """Produce a deterministic GO/NO_GO gate for a shadow retrieval candidate."""
    checks: List[Dict[str, Any]] = []

    def add(name: str, ok: bool, actual: Any, expected: str) -> None:
        checks.append({"name": name, "ok": bool(ok), "actual": actual, "expected": expected})

    recall = float(candidate.get("recall_at_k") or 0.0)
    precision = float(candidate.get("precision_at_k") or 0.0)
    top1_accuracy = float(candidate.get("top1_accuracy") or 0.0)
    irrelevant_rate = float(candidate.get("irrelevant_rate_at_k") or 0.0)
    p95 = int(candidate.get("latency_p95_ms") or 0)
    acl_leakage = float(candidate.get("acl_leakage_rate") or 0.0)
    acl_results_checked = int(candidate.get("acl_results_checked") or 0)
    coverage = float(candidate.get("ground_truth_coverage") or 0.0)
    queries = int(candidate.get("queries") or 0)
    no_answer_cases = int(candidate.get("no_answer_cases") or 0)
    document_grounded_cases = int(candidate.get("document_grounded_cases") or 0)
    content_grounded_cases = int(candidate.get("content_grounded_cases") or 0)
    categories_count = int(candidate.get("categories_count") or 0)
    add("eval_query_breadth", queries >= min_eval_queries, queries, f">={min_eval_queries}")
    add(
        "no_answer_case_breadth",
        no_answer_cases >= min_no_answer_cases,
        no_answer_cases,
        f">={min_no_answer_cases}",
    )
    add(
        "document_grounded_breadth",
        document_grounded_cases >= min_document_grounded_cases,
        document_grounded_cases,
        f">={min_document_grounded_cases}",
    )
    add(
        "content_grounded_breadth",
        content_grounded_cases >= min_content_grounded_cases,
        content_grounded_cases,
        f">={min_content_grounded_cases}",
    )
    add(
        "category_breadth",
        categories_count >= min_categories,
        categories_count,
        f">={min_categories}",
    )
    add("recall_floor", recall >= min_recall, recall, f">={min_recall}")
    add("precision_floor", precision >= min_precision, precision, f">={min_precision}")
    add("top1_accuracy", top1_accuracy >= min_top1_accuracy, top1_accuracy, f">={min_top1_accuracy}")
    add(
        "irrelevant_result_rate",
        irrelevant_rate <= max_irrelevant_rate,
        irrelevant_rate,
        f"<={max_irrelevant_rate}",
    )
    add("latency_budget", p95 <= int(max_p95_ms), p95, f"<={int(max_p95_ms)} ms")
    add("acl_evidence", acl_results_checked > 0, acl_results_checked, ">0 checked forbidden results")
    add("acl_leakage", acl_leakage <= max_acl_leakage, acl_leakage, f"<={max_acl_leakage}")
    add("ground_truth_coverage", coverage >= min_ground_truth_coverage, coverage, f">={min_ground_truth_coverage}")
    index_readiness = candidate.get("index_readiness")
    readiness = index_readiness if isinstance(index_readiness, dict) else {}
    add(
        "index_readiness",
        readiness.get("ready") is True,
        {
            "ready": readiness.get("ready") is True,
            "collection_name": str(readiness.get("collection_name") or ""),
            "reasons": list(readiness.get("reasons") or ["readiness_evidence_missing"]),
        },
        "ready=true",
    )

    no_answer = candidate.get("no_answer_accuracy")
    add(
        "no_answer_accuracy",
        no_answer is not None and float(no_answer) >= min_no_answer_accuracy,
        no_answer,
        f">={min_no_answer_accuracy}",
    )
    if require_faithfulness:
        add(
            "faithfulness_evaluated",
            bool(candidate.get("faithfulness_evaluated")),
            bool(candidate.get("faithfulness_evaluated")),
            "true",
        )
    if baseline is not None:
        baseline_recall = float(baseline.get("recall_at_k") or 0.0)
        recall_drop = baseline_recall - recall
        add("recall_regression", recall_drop <= max_recall_drop, recall_drop, f"<={max_recall_drop}")
        baseline_precision_raw = baseline.get("precision_at_k")
        baseline_precision = float(baseline_precision_raw) if baseline_precision_raw is not None else None
        precision_drop = baseline_precision - precision if baseline_precision is not None else None
        add(
            "precision_regression",
            precision_drop is not None and precision_drop <= max_precision_drop,
            precision_drop,
            f"<={max_precision_drop}",
        )
        baseline_top1_raw = baseline.get("top1_accuracy")
        baseline_top1 = float(baseline_top1_raw) if baseline_top1_raw is not None else None
        top1_drop = baseline_top1 - top1_accuracy if baseline_top1 is not None else None
        add(
            "top1_regression",
            top1_drop is not None and top1_drop <= max_top1_drop,
            top1_drop,
            f"<={max_top1_drop}",
        )
        baseline_p95 = int(baseline.get("latency_p95_ms") or 0)
        p95_ratio = p95 / baseline_p95 if baseline_p95 > 0 else None
        add(
            "latency_regression",
            p95_ratio is not None and p95_ratio <= max_p95_ratio,
            p95_ratio,
            f"<={max_p95_ratio}x baseline",
        )
    ok = all(bool(check["ok"]) for check in checks)
    return {
        "decision": "GO" if ok else "NO_GO",
        "ok": ok,
        "checks": checks,
        "candidate": {
            "queries": queries,
            "categories_count": categories_count,
            "no_answer_cases": no_answer_cases,
            "document_grounded_cases": document_grounded_cases,
            "content_grounded_cases": content_grounded_cases,
            "recall_at_k": recall,
            "precision_at_k": precision,
            "irrelevant_rate_at_k": irrelevant_rate,
            "top1_accuracy": top1_accuracy,
            "latency_p95_ms": p95,
            "acl_leakage_rate": acl_leakage,
            "acl_results_checked": acl_results_checked,
            "no_answer_accuracy": no_answer,
            "ground_truth_coverage": coverage,
            "index_readiness": readiness or None,
        },
    }
