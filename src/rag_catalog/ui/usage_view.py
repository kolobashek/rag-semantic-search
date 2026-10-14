"""Admin product usage dashboard, loaded outside the UI event loop."""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone

from nicegui import run, ui

from rag_catalog.core.usage_analytics import UsageAnalyticsDB, analytics_path, collector

from .usage_api import require_user

SCREEN_LABELS = {"search": "Поиск", "explorer": "Файлы", "shifts": "Смены", "jobs": "Задачи",
                 "index": "Индекс", "settings": "Настройки", "stats": "Статистика"}
KIND_LABELS = {"click": "Нажатие", "page_view": "Переход", "operation": "Операция",
               "timing": "Замер", "error": "Ошибка"}
ACTION_LABELS = {
    "page.open": "Открытие раздела", "page.ready": "Первая отрисовка страницы",
    "page.response": "Ответ сервера страницы", "page.transition": "Переход до отрисовки экрана",
    "interaction.response": "Отклик на нажатие (от 16 мс)", "diagnostics.render_done": "Построение экрана на сервере",
    "search.submit": "Запуск поиска", "search.complete": "Поиск завершён", "search.run_quick": "Быстрый поиск",
    "search.run_quick_timeout": "Таймаут быстрого поиска", "search.run_full": "Полный поиск",
    "search.export_csv": "Выгрузка поиска", "files.open_file": "Открыть файл", "files.open_folder": "Открыть папку",
    "files.download": "Скачать файл", "files.share": "Поделиться", "files.copy": "Копировать",
    "files.cut": "Вырезать", "files.delete": "Удалить", "files.archive": "Архивировать",
    "files.hide": "Скрыть", "files.unhide": "Показать", "files.favorite": "Избранное",
    "files.upload": "Загрузить файл", "files.versions": "Версии файла", "files.rename": "Переименовать",
    "files.move": "Переместить", "client.download": "Скачать приложение",
    "files.preview_response": "Получение файла для просмотра",
    "cd_explorer.folder_sizes_loaded": "Размеры папок", "cd_explorer.folder_sizes_failed": "Ошибка расчёта размеров",
    "shifts.list": "Загрузка смен", "shifts.users": "Список сотрудников", "shifts.references": "Справочники смен",
    "shifts.delete": "Удаление смены", "shifts.transition": "Изменение статуса смены", "shifts.export_csv": "Выгрузка смен",
    "browser.javascript": "Ошибка JavaScript", "browser.promise": "Ошибка браузерной операции",
    "connection.lost": "Потеря соединения",
}
VERBS = {"refresh": "Обновить", "more": "Дополнительные действия", "close": "Закрыть", "save": "Сохранить",
         "cancel": "Отмена", "create": "Создать", "edit": "Изменить", "filter": "Фильтр", "select": "Выбрать",
         "help": "Справка", "theme": "Тема", "logout": "Выход", "tab": "Вкладка", "expand": "Раскрыть список",
         "button": "Другой элемент", "checkbox": "Переключатель", "menu": "Меню", "previous": "Назад",
         "next": "Далее", "approve": "Утвердить", "reject": "Отклонить", "history": "История",
         "export": "Выгрузить", "manage": "Управление", "submit": "На проверку", "revert": "В черновик"}
ACTION_LABELS.update({f"control.{key}": label for key, label in VERBS.items()})
ACTION_LABELS.update({f"navigation.{key}": f"Перейти: {label}" for key, label in SCREEN_LABELS.items()})
ACTION_LABELS.update({f"shifts.{key}": f"Смены: {label.lower()}" for key, label in VERBS.items()})
for _prefix in ("cd_explorer", "explorer"):
    for _action in ("open_file", "open_folder", "download", "upload", "rename", "move", "archive", "hide", "unhide"):
        ACTION_LABELS[f"{_prefix}.{_action}"] = ACTION_LABELS[f"files.{_action}"]
    for _action, _label in {
        "open_cloud_preview": "Просмотр файла", "reindex_file": "Повторная индексация файла",
        "restore": "Восстановление", "clipboard_copy": "Копирование", "clipboard_cut": "Вырезание",
        "paste": "Вставка", "share_public_link": "Создание публичной ссылки",
        "share_public_link_revoke": "Отзыв публичной ссылки", "share_internal_grant": "Предоставление доступа",
        "share_internal_revoke": "Отзыв доступа", "delete_selected": "Удаление выбранного",
        "load_more": "Загрузить ещё", "change_setting": "Параметры файлов", "save_settings": "Сохранение параметров",
        "breadcrumb": "Переход по пути", "up": "Родительская папка", "create_folder": "Создание папки",
    }.items():
        ACTION_LABELS[f"{_prefix}.{_action}"] = _label
ACTION_LABELS.update({"favorites.add": "В избранное", "favorites.remove": "Из избранного",
                      "saved_search.add": "Сохранить запрос", "saved_search.remove": "Удалить сохранённый запрос",
                      "ui.theme_toggle": "Изменить тему", "search.toggle_ai_expand": "Переключить AI-поиск"})


def _display_rows(rows):
    result = []
    for row in rows:
        item = dict(row)
        item["screen"] = SCREEN_LABELS.get(item.get("screen"), item.get("screen", ""))
        item["action"] = ACTION_LABELS.get(item.get("action"), item.get("action", ""))
        item["kind"] = KIND_LABELS.get(item.get("kind"), item.get("kind", ""))
        item["source"] = {"browser": "Браузер", "server": "Сервер"}.get(item.get("source"), "")
        if "ok" in item:
            item["result"] = ({"click": "Нажато", "page_view": "Открыто", "timing": "Измерено"}.get(row.get("kind"), "Успешно")
                              if item["ok"] else "Ошибка")
        for field in ("ts", "last_seen"):
            if field in item:
                item[field] = datetime.fromtimestamp(item[field], timezone.utc).strftime("%d.%m.%Y %H:%M:%S")
        result.append(item)
    return result


def render_usage_panel(state):
    generation = 0
    latest = {}

    def load():
        require_user(state.cfg, state.auth_token, admin=True)
        return UsageAnalyticsDB(analytics_path(state.cfg)).report(
            days=int(period.value), username=user.value or "", screen=screen.value or "")

    async def refresh():
        nonlocal generation, latest
        generation += 1
        ticket = generation
        progress.set_visibility(True)
        try:
            report = await run.io_bound(load)
            if ticket != generation:
                return
            latest = report
            user.set_options({"": "Все сотрудники", **{name: name for name in report["usernames"]}})
            status.set_text("Сбор включён · хранение 90 дней, до 200 000 событий" if
                            state.cfg.get("usage_analytics_enabled", True) is not False else "Сбор отключён")
            dropped = collector(analytics_path(state.cfg)).dropped
            if dropped:
                status.set_text(status.text + f" · пропущено событий: {dropped}")
            body.clear()
            with body:
                overview = report["overview"]
                with ui.row().classes("w-full gap-8 flex-wrap"):
                    for key, label in (("users", "Сотрудников"), ("clicks", "Нажатий"), ("views", "Переходов"), ("errors", "Ошибок")):
                        with ui.column().classes("gap-1"):
                            ui.label(label).classes("rag-meta")
                            ui.label(str(overview[key])).classes("text-2xl font-semibold")
                if not overview["events"]:
                    ui.label("За выбранный период событий нет.").classes("rag-meta")
                    return
                ui.echart({
                    "tooltip": {"trigger": "axis"}, "legend": {"data": ["Нажатия", "Переходы", "Ошибки"]},
                    "xAxis": {"type": "category", "data": [r["day"] for r in report["daily"]]},
                    "yAxis": {"type": "value", "minInterval": 1},
                    "series": [{"name": name, "type": "bar", "data": [r[key] for r in report["daily"]]}
                               for name, key in (("Нажатия", "clicks"), ("Переходы", "views"), ("Ошибки", "errors"))],
                }).classes("w-full h-64")
                table("Сотрудники", report["users"], [("username", "Сотрудник"), ("clicks", "Нажатия"),
                      ("views", "Переходы"), ("operations", "Операции"), ("errors", "Ошибки"), ("last_seen", "Последнее событие, UTC")])
                table("Популярные действия", report["actions"], [("screen", "Раздел"), ("action", "Действие"),
                      ("kind", "Тип"), ("count", "Количество"), ("users", "Сотрудников"), ("errors", "Ошибок")])
                table("Скорость операций, мс", report["timings"], [("screen", "Раздел"), ("action", "Операция"),
                      ("source", "Источник"), ("count", "Замеров"), ("avg_ms", "Среднее"),
                      ("p50_ms", "Медиана"), ("p95_ms", "p95"), ("max_ms", "Максимум")])
                table("Последние 200 событий", report["recent"], [("ts", "Время, UTC"), ("username", "Сотрудник"),
                      ("screen", "Раздел"), ("action", "Действие"), ("kind", "Тип"),
                      ("duration_ms", "Время, мс"), ("result", "Результат")], errors=True)
        except Exception:
            if ticket == generation:
                latest = {}
                body.clear()
                with body:
                    ui.label("Не удалось загрузить аналитику. Проверьте доступ и повторите.").classes("text-negative")
        finally:
            if ticket == generation:
                progress.set_visibility(False)

    def table(title, rows, columns, *, errors=False):
        ui.label(title).classes("text-base font-semibold mt-3")
        element = ui.table(columns=[dict(name=key, field=key, label=label, align="left", sortable=True)
                                    for key, label in columns], rows=_display_rows(rows),
                           pagination=10).classes("w-full max-w-full").props("flat bordered wrap-cells")
        if errors:
            element.add_slot("body-cell-result", '''<q-td :props="props">
                <span :class="props.row.ok ? '' : 'text-negative'">{{props.value}}</span></q-td>''')

    async def export():
        try:
            await run.io_bound(require_user, state.cfg, state.auth_token, admin=True)
        except Exception:
            ui.notify("Недостаточно прав", type="negative")
            return
        rows = _display_rows(latest.get("recent", []))
        stream = io.StringIO()
        fields = ["ts", "username", "screen", "action", "kind", "duration_ms", "result"]
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: ("'" + value if isinstance(value, str) and value.startswith(("=", "+", "-", "@"))
                                   else value) for key, value in row.items()})
        ui.download(stream.getvalue().encode("utf-8-sig"), "usage-events.csv", media_type="text/csv")

    with ui.row().classes("w-full items-end gap-3 flex-wrap"):
        period = ui.select({1: "24 часа", 7: "7 дней", 30: "30 дней", 90: "90 дней"}, value=7,
                           label="Период", on_change=refresh).classes("w-40")
        user = ui.select({"": "Все сотрудники"}, value="", label="Сотрудник", on_change=refresh).classes("w-60")
        screen = ui.select({"": "Все разделы", **SCREEN_LABELS}, value="", label="Раздел", on_change=refresh).classes("w-48")
        ui.button(icon="refresh", on_click=refresh).props("flat round aria-label='Обновить аналитику'").tooltip("Обновить")
        ui.button(icon="download", on_click=export).props("flat round aria-label='Скачать события CSV'").tooltip("Последние 200 событий в CSV")
    status = ui.label("").classes("rag-meta")
    progress = ui.linear_progress().props("indeterminate").classes("w-full")
    body = ui.column().classes("w-full min-w-0 gap-3")
    ui.timer(0.1, refresh, once=True)
