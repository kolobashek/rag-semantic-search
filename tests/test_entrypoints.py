from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_entrypoint(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=PROJECT_ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=30,
        check=False,
    )


def test_cli_entrypoint_help_commands() -> None:
    expected = {
        "rag_search.py": "--content-only",
        "rag_search_fixed.py": "--content-only",
        "index_rag.py": "--stage",
        "ocr_pdfs.py": "--dry-run",
    }

    for script, marker in expected.items():
        result = _run_entrypoint(script, "--help")
        output = result.stdout + result.stderr
        assert result.returncode == 0, output
        assert "usage:" in output
        assert marker in output


def test_legacy_import_shims_alias_package_modules() -> None:
    import index_rag as root_index_rag
    import ocr_pdfs as root_ocr_pdfs
    import rag_core as root_rag_core
    import rag_search as root_rag_search
    import rag_search_fixed as root_rag_search_fixed
    import telegram_bot as root_telegram_bot
    import telemetry_db as root_telemetry_db
    import user_auth_db as root_user_auth_db
    import windows_app as root_windows_app
    from rag_catalog.cli import rag_search as package_rag_search
    from rag_catalog.cli import rag_search_fixed as package_rag_search_fixed
    from rag_catalog.core import index_rag as package_index_rag
    from rag_catalog.core import ocr_pdfs as package_ocr_pdfs
    from rag_catalog.core import rag_core as package_rag_core
    from rag_catalog.core import telemetry_db as package_telemetry_db
    from rag_catalog.core import user_auth_db as package_user_auth_db
    from rag_catalog.integrations import telegram_bot as package_telegram_bot
    from rag_catalog.ui import windows_app as package_windows_app

    assert root_rag_search.main is package_rag_search.main
    assert root_rag_search_fixed.main is package_rag_search_fixed.main
    assert root_index_rag.RAGIndexer is package_index_rag.RAGIndexer
    assert root_ocr_pdfs.find_scanned_pdfs is package_ocr_pdfs.find_scanned_pdfs
    assert root_telegram_bot.process_message is package_telegram_bot.process_message
    assert root_windows_app.RAGWindow is package_windows_app.RAGWindow
    assert root_rag_core.RAGSearcher is package_rag_core.RAGSearcher
    assert root_user_auth_db.UserAuthDB is package_user_auth_db.UserAuthDB
    assert root_telemetry_db.TelemetryDB is package_telemetry_db.TelemetryDB
