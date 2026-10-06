"""Administrator access to a registered computer's latest client log."""
from datetime import datetime

from nicegui import ui

from rag_catalog.core.client_diagnostics import ClientDiagnosticsDB
from rag_catalog.core.cloud_drive import CloudDriveService

from .state import _get_auth_db, _log_app_event, _username


def show_client_diagnostics(state, client: dict):
    def require_admin():
        user = _get_auth_db(state).get_user(username=_username(state))
        if not user or user.get('role') != 'admin' or user.get('status') != 'active':
            ui.notify('Журналы доступны только администратору.', type='negative')
            return False
        return True

    if not require_admin():
        return
    client_id = str(client['id'])
    if CloudDriveService.from_config(state.cfg).registry.get_sync_client(client_id) is None:
        ui.notify('Клиент больше не зарегистрирован.', type='negative')
        return
    store = ClientDiagnosticsDB.from_config(state.cfg)
    with ui.dialog() as dialog, ui.card().classes('w-[800px] max-w-[95vw] p-4 gap-3'):
        ui.label(f"Журнал: {client.get('display_name') or client.get('device_id')}").classes('text-lg font-semibold')
        ui.label(str(client.get('username') or '')).classes('rag-meta')
        status = ui.label().classes('text-sm')
        text = ui.textarea().props('readonly outlined rows=16').classes('w-full font-mono text-xs')

        def refresh():
            if not require_admin():
                timer.deactivate()
                dialog.close()
                return
            row = store.read(client_id)
            date = datetime.fromtimestamp(row['uploaded_at']).strftime('%d.%m.%Y %H:%M:%S') if row['uploaded_at'] else ''
            message = f"Получен {date} · версия {row['app_version']}" if date else 'Журнал ещё не получен'
            if row['request_id']:
                message += ' · ожидается ответ компьютера'
            status.set_text(message)
            value = row['log_text'][-16000:]
            if text.value != value:
                text.set_value(value)
            download.set_enabled(bool(row['uploaded_at']))

        def request():
            if not require_admin():
                return
            store.request(client_id, _username(state))
            _log_app_event(state, 'settings', 'client_log_request', details={'client_id': client_id})
            refresh()

        def save():
            if not require_admin():
                return
            row = store.read(client_id)
            if row['uploaded_at']:
                _log_app_event(state, 'settings', 'client_log_download', details={'client_id': client_id})
                ui.download.content(row['log_text'].encode('utf-8'), filename='RagCloudFiles.log', media_type='text/plain')

        with ui.row().classes('w-full gap-2'):
            ui.button('Запросить свежий журнал', icon='sync', on_click=request)
            download = ui.button('Скачать', icon='download', on_click=save).props('outline')
            ui.space()
            ui.button('Закрыть', on_click=dialog.close).props('flat')
        timer = ui.timer(5, refresh)
        dialog.on('hide', timer.deactivate)
        refresh()
    dialog.open()
