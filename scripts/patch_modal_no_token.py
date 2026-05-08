"""Remove token requirement from install modals — auth now happens via browser."""
from pathlib import Path

src_path = Path('src/rag_catalog/ui/settings_view.py')
src = src_path.read_text(encoding='utf-8')

# ── 1. Admin modal: step-2 (pip install) is now Windows-only note,
#       step-3 run command no longer shows --token ────────────────────────────
old_step2 = (
    '                ui.label("Шаг 2 — установить зависимости").classes("font-semibold text-sm mt-1")\n'
    '                with ui.row().classes("w-full gap-1 items-center"):\n'
    '                    _pip_cmd = ui.input(value="pip install requests watchdog").props(\n'
    '                        \'readonly dense outlined\'\n'
    '                    ).classes("flex-1 font-mono text-xs")\n'
    '                    ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(\n'
    '                        f"navigator.clipboard.writeText({repr(_pip_cmd.value)})"\n'
    '                    )).props("flat dense round").tooltip("Копировать")\n'
    '\n'
    '                ui.label("Шаг 3 — запустить").classes("font-semibold text-sm mt-1")\n'
    '                _run_cmd_input = ui.input(value="python rag_sync_client.py --server … --token …").props(\n'
    '                    \'readonly dense outlined\'\n'
    '                ).classes("w-full font-mono text-xs")\n'
    '                with ui.row().classes("w-full justify-end gap-2"):\n'
    '                    ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(\n'
    '                        f"navigator.clipboard.writeText({repr(_run_cmd_input.value)})"\n'
    '                    )).props("flat dense round").tooltip("Копировать команду")\n'
    '                    ui.button("Закрыть", on_click=_install_dlg.close).props("flat dense")\n'
    '\n'
    '                ui.label(\n'
    '                    "Токен — сессионный токен любого активного пользователя. "\n'
    '                    "Клиент сохранит его в ~/.rag_sync/config.json после первого запуска."\n'
    '                ).classes("rag-meta text-xs mt-1")\n'
)
new_step2 = (
    '                ui.label("Шаг 2 — только для Python-скрипта: зависимости").classes("font-semibold text-sm mt-1")\n'
    '                with ui.row().classes("w-full gap-1 items-center"):\n'
    '                    _pip_cmd = ui.input(value="pip install requests watchdog").props(\n'
    '                        \'readonly dense outlined\'\n'
    '                    ).classes("flex-1 font-mono text-xs")\n'
    '                    ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(\n'
    '                        f"navigator.clipboard.writeText({repr(_pip_cmd.value)})"\n'
    '                    )).props("flat dense round").tooltip("Копировать")\n'
    '\n'
    '                ui.label("Шаг 3 — запустить").classes("font-semibold text-sm mt-1")\n'
    '                _run_cmd_input = ui.input(value="python rag_sync_client.py --server …").props(\n'
    '                    \'readonly dense outlined\'\n'
    '                ).classes("w-full font-mono text-xs")\n'
    '                with ui.row().classes("w-full justify-end gap-2"):\n'
    '                    ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(\n'
    '                        f"navigator.clipboard.writeText({repr(_run_cmd_input.value)})"\n'
    '                    )).props("flat dense round").tooltip("Копировать команду")\n'
    '                    ui.button("Закрыть", on_click=_install_dlg.close).props("flat dense")\n'
    '\n'
    '                with ui.row().classes("gap-2 items-start mt-1"):\n'
    '                    ui.icon("info", size="16px").classes("text-indigo-400 mt-0.5")\n'
    '                    ui.label(\n'
    '                        "При первом запуске клиент откроет браузер — войдите в RAG Catalog "\n'
    '                        "обычным способом и введите код подтверждения. Токен сохранится автоматически."\n'
    '                    ).classes("rag-meta text-xs")\n'
)
assert old_step2 in src, "admin step-2 anchor not found"
src = src.replace(old_step2, new_step2, 1)

# ── 2. Admin open_install_dialog: update run command (no --token) ─────────────
old_run_set = (
    '                _run_cmd_input.set_value(\n'
    '                    f"python rag_sync_client.py --server {origin} --token {tok}"\n'
    '                )\n'
)
new_run_set = (
    '                _run_cmd_input.set_value(\n'
    '                    f"python rag_sync_client.py --server {origin}"\n'
    '                )\n'
)
assert old_run_set in src, "run_cmd_input.set_value anchor not found"
src = src.replace(old_run_set, new_run_set, 1)

# ── 3. User modal: update run command and note ────────────────────────────────
old_user_cmd = (
    '                                _cmd = f"python rag_sync_client.py --server {_origin} --token {_tok}"\n'
)
new_user_cmd = (
    '                                _cmd = f"python rag_sync_client.py --server {_origin}"\n'
)
assert old_user_cmd in src, "user _cmd anchor not found"
src = src.replace(old_user_cmd, new_user_cmd, 1)

# ── 4. User modal: update description ────────────────────────────────────────
old_user_desc = (
    '                                    ui.label(\n'
    '                                        "Скачайте установщик и запустите на своём компьютере. "\n'
    '                                        "Сервер и токен уже вписаны в установщик."\n'
    '                                    ).classes("rag-meta text-sm")\n'
)
new_user_desc = (
    '                                    ui.label(\n'
    '                                        "Скачайте установщик и запустите на своём компьютере. "\n'
    '                                        "При первом запуске откроется браузер для входа — токен не нужен."\n'
    '                                    ).classes("rag-meta text-sm")\n'
)
assert old_user_desc in src, "user modal description anchor not found"
src = src.replace(old_user_desc, new_user_desc, 1)

# ── 5. User modal: update run command hint ────────────────────────────────────
old_user_run_label = '                                    ui.label("Для Python-скрипта: запустить").classes("font-semibold text-sm")\n'
new_user_run_label = '                                    ui.label("Python-скрипт: запустить (браузер откроется автоматически)").classes("font-semibold text-sm")\n'
assert old_user_run_label in src, "user run label anchor not found"
src = src.replace(old_user_run_label, new_user_run_label, 1)

src_path.write_text(src, encoding='utf-8')
print(f"Done. New size: {len(src)} chars")
