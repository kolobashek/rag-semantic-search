"""Patch settings_view.py to add sync client download modal."""
from pathlib import Path

src_path = Path('src/rag_catalog/ui/settings_view.py')
src = src_path.read_text(encoding='utf-8')

# ── 1. Add download modal + button to render_admin_cloud_sync_settings ────────
# Insert right after the separator that follows the cd_enabled guard
old_admin_sep = (
    '            ui.separator()\n'
    '\n'
    '            svc = _cd_get_service(state.cfg)\n'
)
new_admin_sep = (
    '            ui.separator()\n'
    '\n'
    '            # ── Client download ──────────────────────────────────────────\n'
    '            with ui.dialog() as _install_dlg, ui.card().classes("p-5 gap-4 w-full max-w-lg"):\n'
    '                ui.label("Установка sync-клиента").classes("text-base font-semibold")\n'
    '                ui.label(\n'
    '                    "Скачайте скрипт на компьютер пользователя, установите зависимости "\n'
    '                    "и запустите с параметрами сервера и токена."\n'
    '                ).classes("rag-meta text-sm")\n'
    '                ui.separator()\n'
    '\n'
    '                ui.label("Шаг 1 — скачать").classes("font-semibold text-sm")\n'
    '                with ui.row().classes("gap-2 items-center"):\n'
    '                    _dl_token_holder: list[str] = [""]\n'
    '                    _dl_href = ui.link(\n'
    '                        "rag_sync_client.py",\n'
    '                        target="_blank",\n'
    '                    ).classes("rag-path text-sm underline")\n'
    '\n'
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
    '\n'
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
    '\n'
    '            with ui.row().classes("w-full justify-end"):\n'
    '                ui.button(\n'
    '                    "Скачать клиент", icon="download", on_click=open_install_dialog\n'
    '                ).props("outline dense").classes("text-indigo-400")\n'
    '\n'
    '            ui.separator()\n'
    '\n'
    '            svc = _cd_get_service(state.cfg)\n'
)
assert old_admin_sep in src, "admin separator anchor not found"
src = src.replace(old_admin_sep, new_admin_sep, 1)

# ── 2. Add download button to the user cloud_sync section ─────────────────────
# Insert after the status badge row (after .classes("text-xs"))
old_user_sep = (
    '                        ui.separator()\n'
    '                        ui.label("Мои папки синхронизации").classes("font-semibold")\n'
)
new_user_sep = (
    '                        with ui.row().classes("w-full justify-end mt-1"):\n'
    '                            async def _open_user_install_dlg() -> None:\n'
    '                                try:\n'
    '                                    _origin = await ui.run_javascript("window.location.origin")\n'
    '                                except Exception:\n'
    '                                    _origin = "http://localhost:8080"\n'
    '                                _tok = str(app.storage.user.get("auth_token") or "…").strip()\n'
    '                                _cmd = f"python rag_sync_client.py --server {_origin} --token {_tok}"\n'
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
    '                                _udlg.open()\n'
    '                            ui.button("Скачать клиент", icon="download", on_click=_open_user_install_dlg).props("outline dense").classes("text-indigo-400")\n'
    '\n'
    '                        ui.separator()\n'
    '                        ui.label("Мои папки синхронизации").classes("font-semibold")\n'
)
assert old_user_sep in src, "user separator anchor not found"
src = src.replace(old_user_sep, new_user_sep, 1)

src_path.write_text(src, encoding='utf-8')
print(f"Done. New size: {len(src)} chars")
