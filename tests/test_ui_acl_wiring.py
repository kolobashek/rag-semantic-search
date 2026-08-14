"""ACL-проводка в веб-интерфейсе: RAG-ответ и просмотрщик файлов.

RAG-ответ выполняет собственный поиск внутри RAGSearcher, а просмотрщик читает
офисные и текстовые файлы на сервере, минуя защищённый /api/view-file. Обе точки
обязаны проверять права Cloud Drive, иначе список результатов отфильтрован, а
содержимое закрытых документов всё равно доходит до пользователя.
"""

from __future__ import annotations

from pathlib import Path

import rag_catalog.ui.helpers as ui_helpers
from rag_catalog.ui.helpers import _cd_acl_allows_local_file, _cd_acl_result_filter

_ACL_CFG = {"cloud_drive_db_path": "cloud_drive.db", "catalog_path": r"O:\Обмен"}
_SECRET = r"O:\Обмен\Кадры\зарплаты.xlsx"
_OPEN = r"O:\Обмен\Общее\план.docx"


def _allow_only(monkeypatch, allowed: set) -> None:
    monkeypatch.setattr(
        ui_helpers,
        "_filter_cloud_drive_search_results",
        lambda cfg, user, results: [
            item for item in results if str(item.get("full_path") or "") in allowed
        ],
    )


def test_acl_result_filter_drops_forbidden_documents(monkeypatch) -> None:
    _allow_only(monkeypatch, {_OPEN})
    result_filter = _cd_acl_result_filter(_ACL_CFG, {"username": "ivan", "role": "user"})

    filtered = result_filter([{"full_path": _SECRET}, {"full_path": _OPEN}])

    assert [item["full_path"] for item in filtered] == [_OPEN]


def test_viewer_denies_file_without_access(monkeypatch) -> None:
    _allow_only(monkeypatch, {_OPEN})
    user = {"username": "ivan", "role": "user"}

    assert _cd_acl_allows_local_file(_ACL_CFG, user, _OPEN) is True
    assert _cd_acl_allows_local_file(_ACL_CFG, user, _SECRET) is False
    assert _cd_acl_allows_local_file(_ACL_CFG, user, "") is False


def test_viewer_allows_everything_without_cloud_drive() -> None:
    assert _cd_acl_allows_local_file({}, {"username": "ivan", "role": "user"}, _SECRET) is True


def test_web_passes_acl_filter_into_rag_answer() -> None:
    """Структурная защита: вызов answer_documents без result_filter — это утечка."""
    source = (Path(__file__).resolve().parent.parent
              / "src" / "rag_catalog" / "ui" / "nice_app.py").read_text(encoding="utf-8")

    marker = "searcher_for_answer.answer_documents"
    assert marker in source
    call_start = source.index(marker)
    call_tail = source[call_start:call_start + 300]
    assert "result_filter=" in call_tail, "answer_documents в вебе вызван без ACL-фильтра"


def test_viewer_checks_acl_before_rendering_preview() -> None:
    source = (Path(__file__).resolve().parent.parent
              / "src" / "rag_catalog" / "ui" / "nice_app.py").read_text(encoding="utf-8")

    start = source.index("def open_file_viewer(")
    body = source[start:start + 1200]
    assert "_cd_acl_allows_local_file" in body, "просмотрщик не проверяет права Cloud Drive"
