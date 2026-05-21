"""Exact-match helpers for identifiers and legacy ZIP filenames."""

from __future__ import annotations

import re
from typing import Iterable, List


_MOJIBAKE_MARKERS = set("αΓπΣσΩδ∞φτ≈√ⁿ²■ «»")


def numeric_exact_tokens(text: str, *, max_tokens: int = 200) -> List[str]:
    """Return stable numeric tokens for exact document-number search.

    Besides individual digit groups, this also stores adjacent groups joined
    together, so ``9941 210904`` matches source text extracted as
    ``9941�210904`` or split across table cells.
    """
    value = str(text or "")
    matches = list(re.finditer(r"\d{2,}", value))
    tokens: list[str] = []
    seen: set[str] = set()

    def add(token: str) -> None:
        if len(token) < 3 or len(token) > 32 or token in seen:
            return
        seen.add(token)
        tokens.append(token)

    for match in matches:
        add(match.group(0))

    for idx, match in enumerate(matches[:-1]):
        combined = match.group(0)
        last_end = match.end()
        for next_match in matches[idx + 1 : idx + 4]:
            gap = value[last_end : next_match.start()]
            if len(gap) > 5 or re.search(r"[A-Za-zА-Яа-яЁё]", gap):
                break
            combined += next_match.group(0)
            add(combined)
            last_end = next_match.end()
            if len(tokens) >= max_tokens:
                return tokens[:max_tokens]

    return tokens[:max_tokens]


def query_numeric_tokens(query: str) -> List[str]:
    """Numeric tokens worth using as exact-match filters for a user query."""
    return [token for token in numeric_exact_tokens(query, max_tokens=40) if len(token) >= 4]


def _cyrillic_score(text: str) -> int:
    return sum(1 for char in text if "\u0400" <= char <= "\u04ff")


def repair_mojibake_text(text: str) -> str:
    """Repair common CP866-in-ZIP mojibake decoded by Python as CP437.

    Old Windows ZIPs often store Cyrillic names without the UTF-8 flag. The
    standard library decodes those bytes as CP437, producing strings like
    ``Åα«Γ``. Re-encoding as CP437 and decoding as CP866 restores the name.
    """
    value = str(text or "")
    if not value or not any(char in _MOJIBAKE_MARKERS or ord(char) < 32 for char in value):
        return value
    try:
        repaired = value.encode("cp437").decode("cp866")
    except UnicodeError:
        return value
    if _cyrillic_score(repaired) > _cyrillic_score(value):
        return repaired
    return value


def repair_zip_member_name(name: str) -> str:
    parts = str(name or "").replace("\\", "/").split("/")
    return "/".join(repair_mojibake_text(part) for part in parts)


def add_numeric_tokens(payload: dict, *texts: Iterable[str] | str) -> dict:
    joined_parts: list[str] = []
    for item in texts:
        if isinstance(item, str):
            joined_parts.append(item)
        else:
            joined_parts.extend(str(part or "") for part in item)
    tokens = numeric_exact_tokens("\n".join(joined_parts))
    if tokens:
        payload["numeric_tokens"] = tokens
    return payload
