"""Single source of truth for query-term normalization.

``rag_core`` and ``retrieval.bm25`` used to keep private copies of the alias
table, tokenizer and term-variant expansion which silently drifted apart.
Everything term-related now lives here and is re-exported by both modules.
"""

from __future__ import annotations

import re
from typing import Dict, List

TOKEN_RE = re.compile(r"[a-zа-яё0-9\-]{2,}", flags=re.IGNORECASE)
STOPWORDS = frozenset({"и", "или", "по", "на", "в", "во", "от", "для", "мне", "нужен", "нужна"})
_VOWELS = "аеиоуыьъйяю"

TERM_ALIASES: Dict[str, List[str]] = {
    "touareg": ["туарег", "volkswagen", "фольксваген", "vw"],
    "туарег": ["touareg", "volkswagen", "фольксваген", "vw"],
    "volkswagen": ["фольксваген", "vw"],
    "фольксваген": ["volkswagen", "vw"],
    "обслуживания": ["обслуживание", "техническое обслуживание", "услуги", "ремонт", "сервис"],
    "технических": ["технические", "техническое обслуживание", "услуги", "ремонт", "сервис"],
    "vin": ["шильдик", "табличка", "заводская табличка"],
}


def normalize_term(term: str) -> str:
    return str(term or "").lower().replace("ё", "е")


def tokenize(text: str) -> List[str]:
    """Tokenize mixed Russian/Latin text: lowercase, ё→е, stopwords out, order kept, unique."""
    terms: List[str] = []
    seen: set[str] = set()
    for raw in TOKEN_RE.findall(text or ""):
        term = normalize_term(raw)
        if term in STOPWORDS or term in seen:
            continue
        seen.add(term)
        terms.append(term)
    return terms


def term_stem(term: str) -> str:
    """Crude Russian stem used for prefix matching (only for terms of 5+ chars)."""
    if len(term) < 5:
        return term
    stem = term.rstrip(_VOWELS)
    return stem if len(stem) >= 4 else term


def term_variants(term: str) -> List[str]:
    """Aliases plus Latin/Cyrillic ``o`` ↔ ``0`` confusions for vehicle codes."""
    clean = normalize_term(term)
    variants = [clean]
    for alias in TERM_ALIASES.get(clean, []):
        alias_norm = normalize_term(alias)
        if alias_norm and alias_norm not in variants:
            variants.append(alias_norm)
    if "0" in clean or re.search(r"[oо].*\d|\d.*[oо]", clean, flags=re.IGNORECASE):
        for src, dst in (("o", "0"), ("о", "0"), ("0", "o"), ("0", "о")):
            alt = clean.replace(src, dst)
            if alt and alt not in variants:
                variants.append(alt)
        for idx, char in enumerate(clean):
            if char == "0":
                for dst in ("o", "о"):
                    alt = f"{clean[:idx]}{dst}{clean[idx + 1:]}"
                    if alt and alt not in variants:
                        variants.append(alt)
    return variants


def term_needles(term: str) -> tuple[str, ...]:
    """Variants plus their stems — substrings worth probing in a haystack."""
    needles: list[str] = []
    for variant in term_variants(term):
        if variant and variant not in needles:
            needles.append(variant)
        if len(variant) >= 5:
            stem = variant.rstrip(_VOWELS)
            if len(stem) >= 4 and stem not in needles:
                needles.append(stem)
    return tuple(needles)


def term_matches(haystack: str, term: str) -> bool:
    return any(needle in haystack for needle in term_needles(term))


__all__ = [
    "STOPWORDS",
    "TERM_ALIASES",
    "TOKEN_RE",
    "normalize_term",
    "term_matches",
    "term_needles",
    "term_stem",
    "term_variants",
    "tokenize",
]
