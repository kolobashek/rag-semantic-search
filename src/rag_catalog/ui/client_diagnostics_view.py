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
        health_status = ui.label().classes('text-sm whitespace-pre-wrap')
        update_status = ui.label().classes('text-sm')
        recovery_status = ui.label().classes('text-sm')
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
            health = store.read_health(client_id)
            if health['last_seen_at']:
                seen = datetime.fromtimestamp(health['last_seen_at']).strftime('%d.%m.%Y %H:%M:%S')
                phase = {'registration': 'регистрация', 'authorization': 'ожидание входа',
                         'namespace': 'подготовка файлов', 'running': 'работает',
                         'failed': 'ошибка запуска/работы', 'stopped': 'остановлен'}.get(health['phase'], health['phase'])
                prefix = 'На связи' if health['fresh'] else 'Нет свежей связи; последнее известное состояние'
                health_status.set_text(
                    f"{prefix}: {seen} · версия {health['app_version']} · {phase} · {health['state']}"
                    + (f"\nПоследняя переданная ошибка: {health['last_error']}" if health['last_error'] else ''))
            else:
                health_status.set_text('Текущее состояние неизвестно: сигнал состояния ещё не получен. Данные журнала исторические.')
            value = row['log_text'][-16000:]
            if text.value != value:
                text.set_value(value)
            download.set_enabled(bool(row['uploaded_at']))
            update = store.read_update(client_id)
            update_status.set_text(
                (f"Обновлён до {update['reported_version']}" if update.get('completed_at')
                 else f"Ожидается обновление до {update['target_version']}") if update else '')
            recovery = store.read_recovery(client_id)
            recovery_status.set_text(('Новая папка подготовлена; результат синхронизации смотрите в статусе.'
                                      if recovery['switched_at'] else 'Запрошено восстановление в новой папке (команда действует сутки).')
                                     if recovery else '')

        def request_recovery():
            if not require_admin():
                return
            with ui.dialog() as confirmation, ui.card().classes('max-w-lg'):
                ui.label('Восстановить облачную папку?').classes('font-semibold')
                ui.label('Клиент 0.6.7+ создаст новую папку рядом со старой. Исходная папка и локальные изменения '
                         'останутся на месте; они не будут автоматически перенесены в новую. Содержимое облачных '
                         'файлов загружается при открытии или согласно выбранным офлайн-настройкам.')
                def confirm():
                    if require_admin():
                        store.request_recovery(client_id, _username(state))
                        _log_app_event(state, 'settings', 'client_recovery_request', details={'client_id': client_id})
                        refresh()
                    confirmation.close()
                with ui.row():
                    ui.button('Создать новую папку', icon='create_new_folder', on_click=confirm)
                    ui.button('Отмена', on_click=confirmation.close).props('flat')
            confirmation.open()

        def request_update():
            if not require_admin():
                return
            from .api import _CLOUD_FILES_VERSION
            store.request_update(client_id, _username(state), _CLOUD_FILES_VERSION)
            _log_app_event(state, 'settings', 'client_update_request', details={'client_id': client_id})
            ui.notify('Обновление запрошено. Клиент 0.6.4+ проверит команду при подключении.')
            refresh()

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
            ui.button('Обновить клиент', icon='system_update', on_click=request_update).props('outline')
            ui.button('Восстановить папку', icon='restore', on_click=request_recovery).props('outline')
            ui.space()
            ui.button('Закрыть', on_click=dialog.close).props('flat')
        timer = ui.timer(5, refresh)
        dialog.on('hide', timer.deactivate)
        refresh()
    dialog.open()
