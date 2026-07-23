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


def test_windows_11_shell_package_is_fail_closed_and_uses_matching_com_class() -> None:
    shell_source = (
        PROJECT_ROOT / "clients" / "windows-shell-extension" / "RagCloudShell.cpp"
    ).read_text(encoding="utf-8")
    manifest = (
        PROJECT_ROOT / "packaging" / "cloud-files-shell" / "AppxManifest.xml"
    ).read_text(encoding="utf-8")
    build_script = (
        PROJECT_ROOT / "packaging" / "build_cloud_files_shell.ps1"
    ).read_text(encoding="utf-8")

    assert "IExplorerCommand" in shell_source
    assert "windows.fileExplorerContextMenus" in manifest
    assert "B732C5DB-B14F-4F22-A729-1DA4E430E1DD" in manifest
    assert "0xb732c5db, 0xb14f, 0x4f22" in shell_source
    assert "signtool.exe" in build_script.lower()
    assert "verify /pa /v" in build_script
    assert "$PackagePath.sha256" in build_script


def test_cloud_files_executable_declares_windows_compatibility_and_uninstall() -> None:
    client_dir = PROJECT_ROOT / "clients" / "windows-cloud-files"
    project = (client_dir / "RagCloudFiles.csproj").read_text(encoding="utf-8")
    manifest = (client_dir / "app.manifest").read_text(encoding="utf-8")
    bootstrap = (client_dir / "WindowsBootstrap.cs").read_text(encoding="utf-8")

    assert "<ApplicationManifest>app.manifest</ApplicationManifest>" in project
    assert "TSK.RagCloudFiles" in manifest
    assert "{8e0f7a12-bfb3-4fe8-b9a5-48fd50a15a9a}" in manifest
    assert 'level="asInvoker"' in manifest
    assert "CurrentVersion\\Uninstall\\RAGCloudFiles" in bootstrap
    assert '"UninstallString"' in bootstrap
    assert "--apply-uninstall" in bootstrap
