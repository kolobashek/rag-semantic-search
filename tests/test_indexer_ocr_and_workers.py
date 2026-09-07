from __future__ import annotations

import os

import pytest

from rag_catalog.core.index_rag import resolve_ocr_mode, resolve_read_workers


@pytest.mark.parametrize(
    ("cfg_skip", "no_ocr", "force_ocr", "expected_skip", "reason_part"),
    [
        (False, False, False, False, "index_skip_ocr=false"),
        (True, False, False, True, "index_skip_ocr=true"),
        (False, True, False, True, "--no-ocr"),
        (True, True, False, True, "--no-ocr"),
        (False, False, True, False, "--force-ocr"),
        (True, False, True, False, "--force-ocr"),
        (False, True, True, False, "приоритет"),
        (True, True, True, False, "приоритет"),
    ],
)
def test_ocr_mode_is_config_default_with_flag_overrides(
    cfg_skip: bool, no_ocr: bool, force_ocr: bool, expected_skip: bool, reason_part: str
) -> None:
    skip, reason = resolve_ocr_mode({"index_skip_ocr": cfg_skip}, no_ocr=no_ocr, force_ocr=force_ocr)
    assert skip is expected_skip
    assert reason_part in reason


def test_ocr_mode_default_is_enabled_when_key_missing() -> None:
    skip, _ = resolve_ocr_mode({})
    assert skip is False


def test_ocr_mode_does_not_depend_on_stage() -> None:
    # Этап не передаётся в функцию вовсе: одинаковый результат для любого stage.
    for cfg_skip in (False, True):
        results = {resolve_ocr_mode({"index_skip_ocr": cfg_skip})[0] for _stage in ("metadata", "small", "large", "all")}
        assert results == {cfg_skip}


def test_read_workers_zero_means_auto(monkeypatch) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: 8)
    assert resolve_read_workers(0) == 8
    assert resolve_read_workers(None) == 8
    assert resolve_read_workers("0") == 8
    assert resolve_read_workers(-3) == 8


def test_read_workers_auto_is_bounded(monkeypatch) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: 1)
    assert resolve_read_workers(0) == 2
    monkeypatch.setattr(os, "cpu_count", lambda: 64)
    assert resolve_read_workers(0) == 12
    monkeypatch.setattr(os, "cpu_count", lambda: None)
    assert resolve_read_workers(0) == 4


def test_read_workers_explicit_value_is_kept(monkeypatch) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: 8)
    assert resolve_read_workers(3) == 3
    assert resolve_read_workers("5") == 5
    assert resolve_read_workers(40) == 40
