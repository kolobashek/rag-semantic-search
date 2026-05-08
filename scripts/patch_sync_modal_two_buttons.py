"""Replace single download link in install modal with two platform links."""
from pathlib import Path

src_path = Path('src/rag_catalog/ui/settings_view.py')
src = src_path.read_text(encoding='utf-8')

# ── Admin modal: replace the single link with two ui.link elements ────────────
old_dl_step = (
    '                ui.label("Шаг 1 — скачать").classes("font-semibold text-sm")\n'
    '                with ui.row().classes("gap-2 items-center"):\n'
    '                    _dl_token_holder: list[str] = [""]\n'
    '                    _dl_href = ui.link(\n'
    '                        "rag_sync_client.py",\n'
    '                        target="_blank",\n'
    '                    ).classes("rag-path text-sm underline")\n'
)
new_dl_step = (
    '                ui.label("Шаг 1 — скачать").classes("font-semibold text-sm")\n'
    '                with ui.row().classes("gap-3 items-center flex-wrap"):\n'
    '                    _dl_win_link = ui.link("Windows (.exe)", target="#", new_tab=True)\n'
    '                    _dl_win_link.classes("rag-path text-sm")\n'
    '                    with _dl_win_link:\n'
    '                        ui.icon("window", size="14px")\n'
    '                    _dl_py_link = ui.link("Python (.py)", target="#", new_tab=True)\n'
    '                    _dl_py_link.classes("rag-meta text-sm")\n'
    '                    with _dl_py_link:\n'
    '                        ui.icon("code", size="14px")\n'
    '                ui.label(\n'
    '                    "Windows: установщик с мастером настройки. "\n'
    '                    "Python: мультиплатформенный скрипт (pip install requests watchdog)."\n'
    '                ).classes("rag-meta text-xs")\n'
)
assert old_dl_step in src, "admin download step anchor not found"
src = src.replace(old_dl_step, new_dl_step, 1)

# ── Admin modal: update open_install_dialog to set link targets ───────────────
old_open_fn = (
    '            async def open_install_dialog() -> None:\n'
    '                try:\n'
    '                    origin = await ui.run_javascript("window.location.origin")\n'
    '                except Exception:\n'
    '                    origin = "http://localhost:8080"\n'
    '                tok = str(app.storage.user.get("auth_token") or "…").strip()\n'
    '                _run_cmd_input.set_value(\n'
    '                    f"python rag_sync_client.py --server {origin} --token {tok}"\n'
    '                )\n'
    '                _dl_href.target = f"{origin}/api/cloud-drive/sync/client-download?auth_token={tok}"\n'
    '                _install_dlg.open()\n'
)
new_open_fn = (
    '            async def open_install_dialog() -> None:\n'
    '                try:\n'
    '                    origin = await ui.run_javascript("window.location.origin")\n'
    '                except Exception:\n'
    '                    origin = "http://localhost:8080"\n'
    '                tok = str(app.storage.user.get("auth_token") or "…").strip()\n'
    '                _base = f"{origin}/api/cloud-drive/sync/client-download?auth_token={tok}"\n'
    '                _dl_win_link.target = f"{_base}&format=exe"\n'
    '                _dl_py_link.target = f"{_base}&format=py"\n'
    '                _run_cmd_input.set_value(\n'
    '                    f"python rag_sync_client.py --server {origin} --token {tok}"\n'
    '                )\n'
    '                _install_dlg.open()\n'
)
assert old_open_fn in src, "open_install_dialog anchor not found"
src = src.replace(old_open_fn, new_open_fn, 1)

src_path.write_text(src, encoding='utf-8')
print(f"Done. New size: {len(src)} chars")
