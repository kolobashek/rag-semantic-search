"""Structured extraction contract for indexable text blocks."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class TextBlock:
    """A text fragment with source-local provenance."""

    text: str
    page: Optional[int] = None
    sheet: str = ""
    row_start: Optional[int] = None
    row_end: Optional[int] = None
    slide: Optional[int] = None
    section: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExtractedDocument:
    """Extractor result composed of text blocks instead of service marker lines."""

    blocks: tuple[TextBlock, ...]
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return "\n\n".join(block.text for block in self.blocks if block.text.strip())


_MARKER_RE = re.compile(r"^\s*(Страница|Лист|Строка|Слайд):\s*(.*?)\s*$", flags=re.IGNORECASE)


def blocks_from_legacy_text(text: str) -> list[TextBlock]:
    """Convert legacy marker-prefixed extractor text into structured blocks."""
    raw = str(text or "")
    if not raw.strip():
        return []

    blocks: list[TextBlock] = []
    lines: list[str] = []
    page: Optional[int] = None
    sheet = ""
    row_start: Optional[int] = None
    slide: Optional[int] = None

    def _flush() -> None:
        nonlocal lines
        block_text = "\n".join(lines).strip()
        if block_text:
            blocks.append(
                TextBlock(
                    text=block_text,
                    page=page,
                    sheet=sheet,
                    row_start=row_start,
                    row_end=row_start,
                    slide=slide,
                )
            )
        lines = []

    for line in raw.splitlines():
        match = _MARKER_RE.match(line)
        if not match:
            lines.append(line)
            continue
        _flush()
        marker = match.group(1).lower()
        value = match.group(2).strip()
        if marker == "страница":
            page = _parse_int(value)
            row_start = None
        elif marker == "лист":
            sheet = value[:160]
            row_start = None
        elif marker == "строка":
            row_start = _parse_int(value)
        elif marker == "слайд":
            slide = _parse_int(value)
    _flush()

    if blocks:
        return blocks
    return [TextBlock(text=raw.strip())]


def document_from_legacy_text(text: str) -> ExtractedDocument:
    return ExtractedDocument(blocks=tuple(blocks_from_legacy_text(text)))


def _parse_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
