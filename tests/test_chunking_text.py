from __future__ import annotations

from rag_catalog.core.chunking import chunk_text, semantic_chunk_end


def test_chunk_text_keeps_legacy_tiny_window_behavior() -> None:
    assert chunk_text("abcdefghij", chunk_size=4, chunk_overlap=1) == ["abcd", "defg", "ghij"]


def test_chunk_text_prefers_paragraph_boundary() -> None:
    first = (
        "Первый абзац содержит достаточно текста для проверки границы. "
        "Он должен закончиться до начала следующего абзаца."
    )
    second = "Второй абзац должен начинаться отдельным смысловым блоком."
    chunks = chunk_text(first + "\n\n" + second, chunk_size=140, chunk_overlap=12)

    assert chunks
    assert chunks[0].rstrip().endswith(".")
    assert "Второй абзац" not in chunks[0]


def test_semantic_chunk_end_falls_back_to_max_end() -> None:
    text = "x" * 200
    assert semantic_chunk_end(text, start=0, max_end=100, chunk_size=100) == 100
