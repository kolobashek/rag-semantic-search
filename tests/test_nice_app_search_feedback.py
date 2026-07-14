from __future__ import annotations

from rag_catalog.ui import nice_app
from rag_catalog.ui.state import PageState


class _Telemetry:
    def __init__(self) -> None:
        self.feedback: list[dict] = []

    def log_search_feedback(self, **kwargs) -> None:
        self.feedback.append(kwargs)


def test_implicit_result_use_is_recorded_as_neutral_feedback(monkeypatch) -> None:
    telemetry = _Telemetry()
    app_events: list[tuple[str, str, dict]] = []
    state = PageState(
        cfg={},
        searched_query="contract 42",
        current_user={"username": "Admin"},
    )
    result = {
        "filename": "contract.docx",
        "full_path": r"O:\Contracts\contract.docx",
        "score": 0.91,
    }

    monkeypatch.setattr(nice_app, "_get_telemetry", lambda _state: telemetry)
    monkeypatch.setattr(nice_app, "_username", lambda _state: "admin")
    monkeypatch.setattr(
        nice_app,
        "_log_app_event",
        lambda _state, feature, action, **kwargs: app_events.append((feature, action, kwargs["details"])),
    )

    nice_app._record_implicit_search_result_use(
        state,
        result,
        index=3,
        reason="local_download",
    )

    assert telemetry.feedback == [
        {
            "username": "admin",
            "source": "nicegui",
            "query": "contract 42",
            "result_path": r"O:\Contracts\contract.docx",
            "result_title": "contract.docx",
            "feedback": 0,
            "result_rank": 3,
            "result_score": 0.91,
            "details": {
                "screen": "search",
                "reason": "local_download",
                "cloud_file_id": "",
                "cloud_version_id": "",
                "cloud_path": "",
                "source": "filesystem",
            },
        }
    ]
    assert app_events == [
        (
            "search",
            "result_use",
            {
                "screen": "search",
                "reason": "local_download",
                "cloud_file_id": "",
                "cloud_version_id": "",
                "cloud_path": "",
                "source": "filesystem",
                "path": r"O:\Contracts\contract.docx",
                "query": "contract 42",
            },
        )
    ]


def test_implicit_cloud_result_use_stays_neutral(monkeypatch) -> None:
    telemetry = _Telemetry()
    state = PageState(cfg={}, searched_query="invoice")
    result = {
        "filename": "invoice.xlsx",
        "path": "Shared/invoice.xlsx",
        "cloud_file_id": "file-1",
        "cloud_version_id": "version-2",
        "cloud_path": "Shared/invoice.xlsx",
    }

    monkeypatch.setattr(nice_app, "_get_telemetry", lambda _state: telemetry)
    monkeypatch.setattr(nice_app, "_username", lambda _state: "user")
    monkeypatch.setattr(nice_app, "_log_app_event", lambda *_args, **_kwargs: None)

    nice_app._record_implicit_search_result_use(
        state,
        result,
        index=1,
        reason="cloud_download",
    )

    assert telemetry.feedback[0]["feedback"] == 0
    assert telemetry.feedback[0]["details"] == {
        "screen": "search",
        "reason": "cloud_download",
        "cloud_file_id": "file-1",
        "cloud_version_id": "version-2",
        "cloud_path": "Shared/invoice.xlsx",
        "source": "cloud_drive",
    }


def test_ui_query_expansion_requires_global_feature_flag() -> None:
    assert not nice_app._ui_llm_expand_configured({"llm_enabled": True})
    assert nice_app._ui_llm_expand_configured(
        {"llm_enabled": True, "llm_search_expand_enabled": True}
    )
    assert not nice_app._ui_llm_expand_enabled(
        {"llm_enabled": True},
        available=True,
        user_enabled=True,
    )
    assert not nice_app._ui_llm_expand_enabled(
        {"llm_enabled": True, "llm_search_expand_enabled": True},
        available=False,
        user_enabled=True,
    )
    assert nice_app._ui_llm_expand_enabled(
        {"llm_enabled": True, "llm_search_expand_enabled": True},
        available=True,
        user_enabled=True,
    )


def test_search_empty_hints_do_not_report_healthy_index_as_broken() -> None:
    assert nice_app._search_empty_hints(
        "несуществующий документ",
        content_only=False,
        title_only=False,
        file_type="Все",
    ) == ["Измените запрос или фильтры"]

    hints = nice_app._search_empty_hints(
        "очень длинный запрос из многих ключевых слов",
        content_only=True,
        title_only=False,
        file_type="PDF",
    )
    assert hints == [
        "Снимите фильтр «Только содержимое» или «Только название»",
        "Попробуйте сбросить фильтр типа файла «PDF»",
        "Сократите запрос до ключевых слов",
    ]
    assert all("Qdrant" not in hint for hint in hints)
