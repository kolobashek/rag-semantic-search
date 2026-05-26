from __future__ import annotations

from pathlib import Path

from rag_catalog.core.extractors.files import extract_html


def test_extract_html_returns_visible_text(tmp_path: Path) -> None:
    path = tmp_path / "saved.html"
    path.write_text(
        "<html><head><style>.x{}</style><script>hidden()</script></head>"
        "<body><h1>Заголовок</h1><p>Строка&nbsp;текста</p></body></html>",
        encoding="utf-8",
    )

    text = extract_html(path)

    assert "Заголовок" in text
    assert "Строка текста" in text
    assert "hidden" not in text
