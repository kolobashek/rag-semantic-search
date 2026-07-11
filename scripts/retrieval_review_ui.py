from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from nicegui import ui

from rag_catalog.core.retrieval_review import (
    load_json_object,
    save_review_queue_atomic,
    validate_review_queue,
)


def _lines(value: str) -> list[str]:
    return [line.strip() for line in str(value or "").splitlines() if line.strip()]


def _pages(value: str) -> list[int]:
    pages: list[int] = []
    for token in re.split(r"[,;\s]+", str(value or "").strip()):
        if not token:
            continue
        page = int(token)
        if page < 1:
            raise ValueError("Номера страниц должны быть положительными.")
        if page not in pages:
            pages.append(page)
    return pages


def run_review_ui(
    review_path: Path,
    *,
    host: str,
    port: int,
    show: bool,
    min_no_answer: int,
    min_forbidden: int,
) -> None:
    queue = load_json_object(review_path)
    state = {"index": 0}
    items = queue.get("items") or []
    if not items:
        raise RuntimeError("Review queue пуста.")

    ui.add_head_html(
        """
        <style>
        body { background:#0d0f12; color:#e8eaed; }
        .review-shell { width:min(1180px, 100%); margin:0 auto; padding:18px 20px 40px; }
        .review-toolbar { position:sticky; top:0; z-index:10; background:#0d0f12ee; border-bottom:1px solid #2a2d33; }
        .review-candidate { display:grid; grid-template-columns:48px minmax(0,1fr) 92px 104px; gap:10px; align-items:center; border-bottom:1px solid #25282e; padding:9px 0; }
        .review-path { overflow-wrap:anywhere; color:#aeb4bf; font-size:12px; }
        .review-meta { color:#9097a3; font-size:12px; }
        @media(max-width:700px) {
          .review-shell { padding:12px; }
          .review-candidate { grid-template-columns:36px minmax(0,1fr); }
          .review-candidate .q-checkbox { grid-column:auto; }
        }
        </style>
        """
    )
    ui.dark_mode(True)

    with ui.column().classes("review-shell gap-4"):
        with ui.row().classes("review-toolbar w-full items-center gap-3 py-3 no-wrap"):
            ui.label("Retrieval ground truth").classes("text-lg font-medium")
            progress_label = ui.label("").classes("review-meta")
            ui.space()
            add_no_answer_button = ui.button(icon="playlist_add", color=None).props(
                "flat round dense aria-label=Добавить_no-answer data-testid=add-no-answer"
            )
            reviewer_input = ui.input("Reviewer").props("dense outlined data-testid=reviewer").classes("w-48")
            previous_button = ui.button(icon="arrow_back", color=None).props(
                "flat round dense aria-label=Предыдущий data-testid=previous"
            )
            next_button = ui.button(icon="arrow_forward", color=None).props(
                "flat round dense aria-label=Следующий data-testid=next"
            )

        validation_label = ui.label("").classes("review-meta")
        content = ui.column().classes("w-full gap-4")

        with ui.dialog() as add_no_answer_dialog, ui.card().classes("w-full max-w-xl"):
            ui.label("Добавить no-answer запрос").classes("text-lg font-medium")
            no_answer_query = ui.input("Запрос").props("outlined data-testid=new-no-answer-query").classes("w-full")
            no_answer_category = ui.input("Категория", value="no_answer").props("outlined").classes("w-full")
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Отмена", on_click=add_no_answer_dialog.close).props("flat")
                confirm_no_answer_button = ui.button("Добавить", icon="add").props(
                    "unelevated data-testid=confirm-no-answer"
                )

        def validation_snapshot() -> dict[str, Any]:
            return validate_review_queue(queue, min_no_answer=min_no_answer, min_forbidden=min_forbidden)

        def refresh_progress() -> None:
            result = validation_snapshot()
            progress_label.set_text(
                f"{state['index'] + 1}/{len(items)} · reviewed {result['reviewed']} · pending {result['pending']}"
            )
            validation_label.set_text(
                f"No-answer: {result['no_answer_cases']}/{min_no_answer} · "
                f"Forbidden: {result['forbidden_cases']}/{min_forbidden}"
            )
            previous_button.set_enabled(state["index"] > 0)
            next_button.set_enabled(state["index"] < len(items) - 1)

        def goto(index: int) -> None:
            state["index"] = max(0, min(int(index), len(items) - 1))
            render_item()

        def next_pending(after_index: int) -> int:
            for offset in range(1, len(items) + 1):
                candidate = (after_index + offset) % len(items)
                review = dict(items[candidate].get("review") or {})
                if str(review.get("status") or "") != "reviewed":
                    return candidate
            return min(after_index + 1, len(items) - 1)

        previous_button.on_click(lambda: goto(state["index"] - 1))
        next_button.on_click(lambda: goto(state["index"] + 1))
        add_no_answer_button.on_click(add_no_answer_dialog.open)

        def add_no_answer() -> None:
            query = str(no_answer_query.value or "").strip()
            if not query:
                ui.notify("Введите запрос.", type="warning")
                return
            item = {
                "query": query,
                "category": str(no_answer_category.value or "no_answer").strip() or "no_answer",
                "expected_terms": [],
                "candidates": [],
                "review": {
                    "status": "pending",
                    "reviewed_by": "",
                    "reviewed_at": "",
                    "expect_no_answer": True,
                    "expected_paths": [],
                    "expected_chunks": [],
                    "expected_pages": [],
                    "forbidden": [],
                    "notes": "",
                },
            }
            items.append(item)
            save_review_queue_atomic(review_path, queue)
            no_answer_query.set_value("")
            add_no_answer_dialog.close()
            goto(len(items) - 1)
            ui.notify("No-answer запрос добавлен. Подтвердите его разметку.", type="positive")

        confirm_no_answer_button.on_click(add_no_answer)

        def render_item() -> None:
            content.clear()
            item = items[state["index"]]
            review = dict(item.get("review") or {})
            expected_paths = set(str(value) for value in (review.get("expected_paths") or []))
            forbidden_paths = set(str(value) for value in (review.get("forbidden") or []))
            candidate_paths = {
                str(candidate.get("path") or "")
                for candidate in (item.get("candidates") or [])
                if str(candidate.get("path") or "")
            }
            selected: dict[str, Any] = {}
            forbidden_selected: dict[str, Any] = {}
            with content:
                with ui.row().classes("w-full items-start gap-3"):
                    with ui.column().classes("gap-1 flex-1"):
                        ui.label(str(item.get("query") or "")).classes("text-2xl font-semibold").props(
                            "data-testid=query"
                        )
                        ui.label(
                            f"{item.get('category') or 'general'} · terms: "
                            + ", ".join(str(value) for value in (item.get("expected_terms") or []))
                        ).classes("review-meta")
                    status = str(review.get("status") or "pending")
                    ui.badge("reviewed" if status == "reviewed" else "pending", color="positive" if status == "reviewed" else "warning")

                no_answer = ui.checkbox(
                    "Нет корректного ответа в корпусе",
                    value=bool(review.get("expect_no_answer")),
                ).props("data-testid=no-answer")
                ui.label("Кандидаты baseline не являются ground truth. Отметьте решение reviewer.").classes("review-meta")

                for candidate in item.get("candidates") or []:
                    path = str(candidate.get("path") or "")
                    with ui.element("div").classes("review-candidate"):
                        ui.label(f"#{candidate.get('rank')}").classes("review-meta")
                        with ui.column().classes("gap-0 min-w-0"):
                            ui.label(str(candidate.get("filename") or Path(path).name)).classes("text-sm")
                            ui.label(path).classes("review-path")
                        selected[path] = ui.checkbox("Relevant", value=path in expected_paths).props("dense")
                        forbidden_selected[path] = ui.checkbox("Forbidden", value=path in forbidden_paths).props("dense")

                extra_expected = ui.textarea(
                    "Другие expected paths — по одному на строку",
                    value="\n".join(sorted(expected_paths - candidate_paths)),
                ).props("outlined autogrow data-testid=extra-expected").classes("w-full")
                expected_chunks = ui.textarea(
                    "Expected chunks — характерные фрагменты",
                    value="\n".join(str(value) for value in (review.get("expected_chunks") or [])),
                ).props("outlined autogrow data-testid=expected-chunks").classes("w-full")
                expected_pages = ui.input(
                    "Expected pages — через запятую",
                    value=", ".join(str(value) for value in (review.get("expected_pages") or [])),
                ).props("outlined data-testid=expected-pages").classes("w-full")
                extra_forbidden = ui.textarea(
                    "Другие forbidden paths/маркеры — по одному на строку",
                    value="\n".join(sorted(forbidden_paths - candidate_paths)),
                ).props("outlined autogrow data-testid=extra-forbidden").classes("w-full")
                notes = ui.textarea("Комментарий reviewer", value=str(review.get("notes") or "")).props(
                    "outlined autogrow data-testid=notes"
                ).classes("w-full")

                def enforce_exclusive(path: str, *, relevant_changed: bool) -> None:
                    if relevant_changed and bool(selected[path].value):
                        forbidden_selected[path].set_value(False)
                    elif not relevant_changed and bool(forbidden_selected[path].value):
                        selected[path].set_value(False)

                for path in selected:
                    selected[path].on_value_change(lambda _event, path=path: enforce_exclusive(path, relevant_changed=True))
                    forbidden_selected[path].on_value_change(
                        lambda _event, path=path: enforce_exclusive(path, relevant_changed=False)
                    )

                def save_current() -> None:
                    reviewer = str(reviewer_input.value or "").strip()
                    if not reviewer:
                        ui.notify("Укажите reviewer.", type="warning")
                        return
                    try:
                        is_no_answer = bool(no_answer.value)
                        paths = [] if is_no_answer else [path for path, checkbox in selected.items() if checkbox.value]
                        if not is_no_answer:
                            paths.extend(path for path in _lines(str(extra_expected.value or "")) if path not in paths)
                        forbidden = [path for path, checkbox in forbidden_selected.items() if checkbox.value]
                        forbidden.extend(
                            path for path in _lines(str(extra_forbidden.value or "")) if path not in forbidden
                        )
                        if not is_no_answer and not paths:
                            ui.notify("Выберите relevant path или отметьте no-answer.", type="warning")
                            return
                        review.update(
                            {
                                "status": "reviewed",
                                "reviewed_by": reviewer,
                                "reviewed_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                                "expect_no_answer": is_no_answer,
                                "expected_paths": paths,
                                "expected_chunks": [] if is_no_answer else _lines(str(expected_chunks.value or "")),
                                "expected_pages": [] if is_no_answer else _pages(str(expected_pages.value or "")),
                                "forbidden": forbidden,
                                "notes": str(notes.value or "").strip(),
                            }
                        )
                        item["review"] = review
                        save_review_queue_atomic(review_path, queue)
                        ui.notify("Разметка сохранена.", type="positive")
                        goto(next_pending(state["index"]))
                    except (OSError, ValueError) as exc:
                        ui.notify(f"Не удалось сохранить: {exc}", type="negative")

                with ui.row().classes("w-full items-center gap-2"):
                    ui.button("Сохранить и дальше", icon="save", on_click=save_current).props(
                        "unelevated data-testid=save-review"
                    )
                    ui.button(
                        "Проверить весь набор",
                        icon="fact_check",
                        on_click=lambda: ui.notify(
                            "Review set готов." if validation_snapshot()["ok"] else "Есть pending или недостаточно coverage.",
                            type="positive" if validation_snapshot()["ok"] else "warning",
                        ),
                    ).props("outline")
            refresh_progress()

        render_item()

    ui.run(
        title="Retrieval Ground Truth Review",
        host=host,
        port=port,
        show=show,
        reload=False,
        dark=True,
        storage_secret="retrieval-review-local-only",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local UI for human Retrieval v3 ground-truth review.")
    parser.add_argument("review")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8092)
    parser.add_argument("--no-show", action="store_true")
    parser.add_argument("--min-no-answer", type=int, default=3)
    parser.add_argument("--min-forbidden", type=int, default=3)
    args = parser.parse_args(argv)
    run_review_ui(
        Path(args.review).expanduser().resolve(),
        host=str(args.host),
        port=int(args.port),
        show=not args.no_show,
        min_no_answer=max(0, int(args.min_no_answer)),
        min_forbidden=max(0, int(args.min_forbidden)),
    )
    return 0


if __name__ == "__main__":
    main()
