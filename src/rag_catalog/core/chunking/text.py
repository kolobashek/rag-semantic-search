"""Text chunking helpers used by the file indexer.

The functions are intentionally stateless so indexers, tests, and future
extractors can share the same chunking rules without importing `index_rag.py`.
"""

from __future__ import annotations


def chunk_text(text: str, *, chunk_size: int, chunk_overlap: int) -> list[str]:
    """Split text into overlapping chunks.

    For tiny `chunk_size` values the legacy sliding-window behavior is kept
    exactly; tests and edge-case guards depend on that deterministic output.
    For production-size chunks, the splitter prefers paragraph/sentence
    boundaries while guaranteeing forward progress.
    """
    if not text:
        return []

    if chunk_size < 120:
        step = max(1, chunk_size - chunk_overlap)
        chunks: list[str] = []
        start = 0
        while start < len(text):
            chunk = text[start : start + chunk_size]
            if chunks and len(chunk) <= chunk_overlap:
                break
            chunks.append(chunk)
            if start + chunk_size >= len(text):
                break
            start += step
        return chunks

    step = max(1, chunk_size - chunk_overlap)
    chunks: list[str] = []
    start = 0
    while start < len(text):
        max_end = min(len(text), start + chunk_size)
        end = semantic_chunk_end(text, start=start, max_end=max_end, chunk_size=chunk_size)
        chunk = text[start:end]
        if chunks and len(chunk) <= chunk_overlap:
            break
        chunks.append(chunk)
        if end >= len(text):
            break
        start = max(start + 1, end - chunk_overlap, start + step if end <= start else start + 1)
    return chunks


def semantic_chunk_end(text: str, *, start: int, max_end: int, chunk_size: int) -> int:
    """Return a chunk end offset, preferring paragraph or sentence boundaries."""
    if max_end >= len(text):
        return len(text)
    min_end = start + max(1, int(chunk_size * 0.60))
    if min_end >= max_end:
        return max_end

    window = text[min_end:max_end]
    boundary_offsets = [
        window.rfind("\n\n"),
        window.rfind("\n"),
        max(window.rfind(". "), window.rfind("! "), window.rfind("? "), window.rfind("… ")),
        max(window.rfind(".\n"), window.rfind("!\n"), window.rfind("?\n"), window.rfind("…\n")),
    ]
    best = max(boundary_offsets)
    if best < 0:
        return max_end
    return min(max_end, min_end + best + 1)
