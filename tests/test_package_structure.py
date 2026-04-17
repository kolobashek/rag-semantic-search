from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def test_root_shims_export_package_objects() -> None:
    import rag_core as root_rag_core
    import user_auth_db as root_user_auth_db
    from rag_catalog.core import rag_core as package_rag_core
    from rag_catalog.core import user_auth_db as package_user_auth_db

    assert root_rag_core.RAGSearcher is package_rag_core.RAGSearcher
    assert root_rag_core.load_config is package_rag_core.load_config
    assert root_user_auth_db.UserAuthDB is package_user_auth_db.UserAuthDB


def test_config_and_icon_remain_project_root_assets() -> None:
    from rag_catalog.core.rag_core import CONFIG_FILE
    from rag_catalog.ui.windows_app import APP_ICON_PATH

    assert CONFIG_FILE == PROJECT_ROOT / "config.json"
    assert APP_ICON_PATH == PROJECT_ROOT / "icon.ico"


def test_entrypoint_shims_exist_for_backward_compatibility() -> None:
    for filename in (
        "app_ui.py",
        "windows_app.py",
        "rag_search.py",
        "rag_search_fixed.py",
        "index_rag.py",
        "ocr_pdfs.py",
        "telegram_bot.py",
    ):
        assert (PROJECT_ROOT / filename).is_file()
