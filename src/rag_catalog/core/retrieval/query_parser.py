"""Query operators: ``"phrase"``, ``-word``, ``type:``, ``path:``, ``after:``, ``before:``.

The parser turns the raw user string into :class:`ParsedQuery`; the remaining
free text (``terms``) is what the retrieval channels actually search for.
:func:`apply_operator_filters` is the post-filter applied to channel results.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional

_PHRASE_RE = re.compile(r"\"([^\"]*)\"|«([^»]*)»|“([^”]*)”")
_OPERATOR_RE = re.compile(
    r"(?<!\S)(?P<op>type|path|after|before|ext|расширение|путь|после|до)\s*:\s*"
    r"(?:\"(?P<quoted>[^\"]*)\"|(?P<bare>\S+))",
    flags=re.IGNORECASE,
)
_EXCLUDE_RE = re.compile(r"(?<!\S)[-−–](?P<word>[^\s\"«»“”-][^\s\"«»“”]*)")
_DATE_FORMATS = ("%Y-%m-%d", "%d.%m.%Y", "%Y.%m.%d", "%Y/%m/%d", "%d/%m/%Y", "%Y%m%d")
_OP_CANON = {
    "type": "type",
    "ext": "type",
    "расширение": "type",
    "path": "path",
    "путь": "path",
    "after": "after",
    "после": "after",
    "before": "before",
    "до": "before",
}


def _norm(text: Any) -> str:
    return str(text or "").lower().replace("ё", "е")


def _parse_date(value: str) -> Optional[date]:
    clean = str(value or "").strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(clean, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_file_type(value: str) -> Optional[str]:
    clean = str(value or "").strip().lower().lstrip(".")
    clean = re.sub(r"[^a-z0-9]", "", clean)
    return f".{clean}" if clean else None


@dataclass
class ParsedQuery:
    terms: str = ""
    phrases: List[str] = field(default_factory=list)
    excluded: List[str] = field(default_factory=list)
    file_type: Optional[str] = None
    path_contains: Optional[str] = None
    after: Optional[date] = None
    before: Optional[date] = None
    raw: str = ""

    @property
    def has_operators(self) -> bool:
        return bool(
            self.phrases
            or self.excluded
            or self.file_type
            or self.path_contains
            or self.after
            or self.before
        )

    @property
    def has_result_filters(self) -> bool:
        """Operators that need a post-filter over results (type: goes to Qdrant instead)."""
        return bool(self.phrases or self.excluded or self.path_contains or self.after or self.before)

    @property
    def search_text(self) -> str:
        """Free text plus phrases — what retrieval channels should look for."""
        parts = [self.terms, *self.phrases]
        return " ".join(part for part in parts if part).strip()

    def as_dict(self) -> Dict[str, Any]:
        return {
            "terms": self.terms,
            "phrases": list(self.phrases),
            "excluded": list(self.excluded),
            "file_type": self.file_type,
            "path_contains": self.path_contains,
            "after": self.after.isoformat() if self.after else None,
            "before": self.before.isoformat() if self.before else None,
        }


def parse_query(text: str) -> ParsedQuery:
    raw = str(text or "")
    parsed = ParsedQuery(raw=raw)
    rest = raw

    def take_operator(match: re.Match[str]) -> str:
        op = _OP_CANON.get(match.group("op").lower(), "")
        value = match.group("quoted") if match.group("quoted") is not None else match.group("bare")
        value = str(value or "").strip()
        if not op or not value:
            return " "
        if op == "type":
            parsed.file_type = _normalize_file_type(value) or parsed.file_type
        elif op == "path":
            parsed.path_contains = value
        elif op == "after":
            parsed.after = _parse_date(value) or parsed.after
            if parsed.after is None:
                return f" {value} "
        elif op == "before":
            parsed.before = _parse_date(value) or parsed.before
            if parsed.before is None:
                return f" {value} "
        return " "

    rest = _OPERATOR_RE.sub(take_operator, rest)

    def take_phrase(match: re.Match[str]) -> str:
        phrase = next((group for group in match.groups() if group is not None), "")
        phrase = " ".join(str(phrase).split())
        if phrase:
            parsed.phrases.append(phrase)
        return " "

    rest = _PHRASE_RE.sub(take_phrase, rest)

    def take_exclusion(match: re.Match[str]) -> str:
        word = match.group("word").strip().strip(".,;:!?")
        if word:
            parsed.excluded.append(word)
        return " "

    rest = _EXCLUDE_RE.sub(take_exclusion, rest)
    parsed.terms = " ".join(rest.replace('"', " ").split())
    return parsed


def _default_modified_to_dt(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value))
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{9,}(\.\d+)?", text):
        try:
            return datetime.fromtimestamp(float(text))
        except (OverflowError, OSError, ValueError):
            return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _word_present(haystack: str, word: str) -> bool:
    return re.search(rf"(?<![a-zа-я0-9]){re.escape(word)}(?![a-zа-я0-9])", haystack) is not None


def apply_operator_filters(
    parsed: ParsedQuery,
    results: List[Dict[str, Any]],
    *,
    modified_to_dt: Optional[Callable[[Any], Optional[datetime]]] = None,
    stats: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """Drop results that violate query operators. Order is preserved."""
    if not parsed.has_result_filters:
        return list(results)
    to_dt = modified_to_dt or _default_modified_to_dt
    phrases = [_norm(phrase) for phrase in parsed.phrases if phrase]
    excluded = [_norm(word) for word in parsed.excluded if word]
    path_needle = _norm(parsed.path_contains).replace("\\", "/") if parsed.path_contains else ""
    after_dt = datetime.combine(parsed.after, datetime.min.time()) if parsed.after else None
    # ``before:`` is exclusive: everything modified strictly before that day.
    before_dt = datetime.combine(parsed.before, datetime.min.time()) if parsed.before else None
    kept: List[Dict[str, Any]] = []

    def reject(reason: str) -> None:
        if stats is not None:
            stats[reason] = stats.get(reason, 0) + 1

    for item in results:
        filename = _norm(item.get("filename"))
        path = _norm(item.get("path"))
        full_path = _norm(item.get("full_path"))
        text = _norm(item.get("text"))
        if phrases:
            hay = f"{filename}\n{text}"
            if not all(phrase in hay for phrase in phrases):
                reject("phrase_missing")
                continue
        if excluded:
            hay = f"{filename}\n{path}\n{full_path}\n{text}"
            if any(_word_present(hay, word) for word in excluded):
                reject("excluded_word")
                continue
        if path_needle:
            hay = f"{path}\n{full_path}".replace("\\", "/")
            if path_needle not in hay:
                reject("path_mismatch")
                continue
        if after_dt is not None or before_dt is not None:
            modified = to_dt(item.get("modified"))
            if modified is None:
                reject("modified_unknown")
                continue
            if after_dt is not None and modified < after_dt:
                reject("before_after_bound")
                continue
            if before_dt is not None and modified >= before_dt:
                reject("after_before_bound")
                continue
        kept.append(item)
    return kept


__all__ = ["ParsedQuery", "apply_operator_filters", "parse_query"]
