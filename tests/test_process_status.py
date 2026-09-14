import os

import pytest

from rag_catalog.core import consistency_audit, ocr_pdfs, telemetry_db
from rag_catalog.core.process_status import process_is_alive
from rag_catalog.ui import system


@pytest.mark.parametrize("probe", [process_is_alive, consistency_audit._pid_alive,
                                   telemetry_db._pid_alive, ocr_pdfs._is_process_alive, system._is_process_alive])
def test_process_probe_never_sends_a_signal(monkeypatch, probe):
    def forbidden(*args):
        raise AssertionError("a liveness probe must not call os.kill")
    monkeypatch.setattr(os, "kill", forbidden)
    assert probe(os.getpid()) is True
    assert probe(0) is False
    assert probe(-1) is False
