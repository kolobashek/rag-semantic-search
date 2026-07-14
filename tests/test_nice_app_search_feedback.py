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
