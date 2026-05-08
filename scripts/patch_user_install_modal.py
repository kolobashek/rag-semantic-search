"""Fix user Cloud Sync install modal to show MSI, EXE and PY download options."""
from pathlib import Path

src_path = Path('src/rag_catalog/ui/settings_view.py')
src = src_path.read_text(encoding='utf-8')

old_modal = (
    '                                with ui.dialog() as _udlg, ui.card().classes("p-5 gap-3 w-full max-w-lg"):\n'
    '                                    ui.label("Установка sync-клиента").classes("text-base font-semibold")\n'
    '                                    ui.label(\n'
    '                                        "Скачайте скрипт, установите зависимости и запустите на своём компьютере."\n'
    '                                    ).classes("rag-meta text-sm")\n'
    '                                    ui.separator()\n'
    '                                    ui.label("1. Скачать скрипт").classes("font-semibold text-sm")\n'
    '                                    _dl_url = f"{_origin}/api/cloud-drive/sync/client-download?auth_token={_tok}"\n'
    '                                    ui.link("rag_sync_client.py", target=_dl_url).classes("rag-path text-sm underline")\n'
    '                                    ui.label("2. Установить зависимости").classes("font-semibold text-sm mt-1")\n'
    '                                    with ui.row().classes("w-full gap-1 items-center"):\n'
    '                                        _pip2 = ui.input(value="pip install requests watchdog").props("readonly dense outlined").classes("flex-1 font-mono text-xs")\n'
    '                                        ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(f"navigator.clipboard.writeText({repr(_pip2.value)})" )).props("flat dense round").tooltip("Копировать")\n'
    '                                    ui.label("3. Запустить").classes("font-semibold text-sm mt-1")\n'
    '                                    with ui.row().classes("w-full gap-1 items-center"):\n'
    '                                        _run2 = ui.input(value=_cmd).props("readonly dense outlined").classes("flex-1 font-mono text-xs")\n'
    '                                        ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(f"navigator.clipboard.writeText({repr(_run2.value)})" )).props("flat dense round").tooltip("Копировать")\n'
    '                                    ui.button("Закрыть", on_click=_udlg.close).props("flat dense")\n'
)

new_modal = (
    '                                with ui.dialog() as _udlg, ui.card().classes("p-5 gap-4 w-full max-w-lg"):\n'
    '                                    ui.label("Установка sync-клиента").classes("text-base font-semibold")\n'
    '                                    ui.label(\n'
    '                                        "Скачайте установщик и запустите на своём компьютере. "\n'
    '                                        "Сервер и токен уже вписаны в установщик."\n'
    '                                    ).classes("rag-meta text-sm")\n'
    '                                    ui.separator()\n'
    '                                    ui.label("Шаг 1 — скачать").classes("font-semibold text-sm")\n'
    '                                    _base_url = f"{_origin}/api/cloud-drive/sync/client-download?auth_token={_tok}"\n'
    '                                    with ui.row().classes("gap-3 items-center flex-wrap"):\n'
    '                                        ui.link("Windows MSI", target=f"{_base_url}&format=msi", new_tab=True).classes("rag-path text-sm")\n'
    '                                        ui.label("·").classes("rag-meta")\n'
    '                                        ui.link("Windows EXE (установщик)", target=f"{_base_url}&format=exe", new_tab=True).classes("rag-path text-sm")\n'
    '                                        ui.label("·").classes("rag-meta")\n'
    '                                        ui.link("Python .py (Linux/Mac)", target=f"{_base_url}&format=py", new_tab=True).classes("rag-meta text-sm")\n'
    '                                    ui.label(\n'
    '                                        "MSI: тихая установка, поддержка групповых политик. "\n'
    '                                        "EXE: мастер настройки с полями сервера и токена."\n'
    '                                    ).classes("rag-meta text-xs")\n'
    '                                    ui.separator()\n'
    '                                    ui.label("Для Python-скрипта: запустить").classes("font-semibold text-sm")\n'
    '                                    with ui.row().classes("w-full gap-1 items-center"):\n'
    '                                        _run2 = ui.input(value=_cmd).props("readonly dense outlined").classes("flex-1 font-mono text-xs")\n'
    '                                        ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(f"navigator.clipboard.writeText({repr(_run2.value)})" )).props("flat dense round").tooltip("Копировать")\n'
    '                                    ui.button("Закрыть", on_click=_udlg.close).props("flat dense")\n'
)

assert old_modal in src, "user install modal anchor not found"
src = src.replace(old_modal, new_modal, 1)

# Also update the admin modal step-1 description to mention MSI
old_step1_note = (
    '                ui.label(\n'
    '                    "Windows: установщик с мастером настройки. "\n'
    '                    "Python: мультиплатформенный скрипт (pip install requests watchdog)."\n'
    '                ).classes("rag-meta text-xs")\n'
)
new_step1_note = (
    '                with ui.row().classes("gap-3 items-center flex-wrap"):\n'
    '                    _dl_msi_link = ui.link("Windows MSI", target="#", new_tab=True).classes("rag-path text-sm")\n'
    '                    ui.label("·").classes("rag-meta")\n'
    '                    _dl_win_link = ui.link("Windows EXE (установщик)", target="#", new_tab=True).classes("rag-path text-sm")\n'
    '                    ui.label("·").classes("rag-meta")\n'
    '                    _dl_py_link = ui.link("Python .py", target="#", new_tab=True).classes("rag-meta text-sm")\n'
    '                ui.label(\n'
    '                    "MSI: тихая установка, поддержка групповых политик. "\n'
    '                    "EXE: мастер настройки. Python: Linux/macOS."\n'
    '                ).classes("rag-meta text-xs")\n'
)
assert old_step1_note in src, "admin step-1 note anchor not found"
src = src.replace(old_step1_note, new_step1_note, 1)

# Update open_install_dialog to set _dl_msi_link too
old_open = (
    '                _base = f"{origin}/api/cloud-drive/sync/client-download?auth_token={tok}"\n'
    '                _dl_win_link.target = f"{_base}&format=exe"\n'
    '                _dl_py_link.target = f"{_base}&format=py"\n'
)
new_open = (
    '                _base = f"{origin}/api/cloud-drive/sync/client-download?auth_token={tok}"\n'
    '                _dl_msi_link.target = f"{_base}&format=msi"\n'
    '                _dl_win_link.target = f"{_base}&format=exe"\n'
    '                _dl_py_link.target = f"{_base}&format=py"\n'
)
assert old_open in src, "open_install_dialog base anchor not found"
src = src.replace(old_open, new_open, 1)

# Remove duplicate link rows left from previous patch
old_links_dup = (
    '                with ui.row().classes("gap-3 items-center flex-wrap"):\n'
    '                    _dl_win_link = ui.link("Windows (.exe)", target="#", new_tab=True)\n'
    '                    _dl_win_link.classes("rag-path text-sm")\n'
    '                    with _dl_win_link:\n'
    '                        ui.icon("window", size="14px")\n'
    '                    _dl_py_link = ui.link("Python (.py)", target="#", new_tab=True)\n'
    '                    _dl_py_link.classes("rag-meta text-sm")\n'
    '                    with _dl_py_link:\n'
    '                        ui.icon("code", size="14px")\n'
)
if old_links_dup in src:
    src = src.replace(old_links_dup, '', 1)

src_path.write_text(src, encoding='utf-8')
print(f"Done. New size: {len(src)} chars")
