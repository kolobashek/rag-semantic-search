# Roadmap: Cloud Drive

## Цель

Преобразовать текущий RAG-каталог из приложения, работающего поверх файловой шары, в полноценный продукт уровня "свой Dropbox/Google Drive" с:

- центральным registry файлов и папок;
- управляемым storage backend;
- web-клиентом как основным рабочим интерфейсом;
- API как базовым контрактом для интеграций и sync-клиента;
- фоновыми задачами индексации, OCR, preview и импорта;
- дальнейшей возможностью добавить desktop sync client.

## Архитектурные принципы

- `Registry` и бизнес-логика файлов живут отдельно от файловой системы.
- `Web` является главным клиентом первой очереди.
- `API` обязателен до появления sync-клиента.
- `Storage` абстрагирован: сначала `local`, затем `S3/MinIO`.
- `O:\Обмен` рассматривается как источник импорта/совместимости, а не как source of truth.
- Все долгие операции должны иметь наблюдаемый progress/status и возможность безопасного восстановления.

## Этапы

### Этап 1. Stabilize Foundation

Статус: `in progress`

Цель:
- закрепить текущий registry/storage foundation;
- убрать "немые" фоновые операции;
- довести bootstrap и scheduler до эксплуатационного уровня.

Выход:
- Cloud Drive bootstrap прозрачен в UI;
- scheduler работает как серверный фон;
- базовые ошибки и статусы видны в системе.

Выполнено:
- добавлен Cloud Drive registry/storage foundation;
- добавлен CLI bootstrap/init/stats;
- добавлен admin UI для Cloud Drive;
- добавлены tooltips и нормальные имена полей;
- bootstrap вынесен в фоновое выполнение;
- добавлен live progress в UI;
- scheduler перенесён из page timer в серверный фон;
- scheduler переведён на локальное время сервера;
- bootstrap переведён на `cloud_jobs` job-модель;
- статус bootstrap читается из registry job, а не только из runtime JSON.
- добавлены `cancel/retry` для bootstrap jobs;
- добавлена история последних bootstrap jobs в admin UI;
- добавлен recovery для `running/pending` bootstrap jobs после рестарта.
- добавлены read-only API endpoints для bootstrap status и списка bootstrap jobs.
- добавлены schema migrations для Cloud Drive registry (`v1 -> v2`);
- bootstrap jobs получили SQL-поля `started_at/finished_at`, теперь жизненный цикл jobs наблюдаем не только через `payload`.
- добавлен storage health-check contract для backend;
- добавлен read-only API endpoint для health-check storage backend.
- добавлены первые registry-backed file operations API endpoints:
  - `GET /api/cloud-drive/node`
  - `GET /api/cloud-drive/list`
- Cloud Drive service получил read-only методы `get_node()` и `list_directory()` для explorer/API слоя.
- добавлен registry-backed endpoint создания папки:
  - `POST /api/cloud-drive/folders`
- Cloud Drive service получил `create_folder()` с валидацией конфликтов имени и родительского каталога.
- добавлен registry-backed endpoint скачивания файла:
  - `GET /api/cloud-drive/download`
- Cloud Drive service получил `get_download_descriptor()` для local storage backend.
- добавлен registry-backed endpoint загрузки файла:
  - `POST /api/cloud-drive/upload`
- Cloud Drive service получил `upload_file()` для импорта одного файла в существующий каталог registry.
- добавлен endpoint просмотра версий файла:
  - `GET /api/cloud-drive/versions`
- Cloud Drive service и registry получили `list_versions()` / `list_file_versions()` для чтения истории версий по пути файла.

Осталось в этапе:
- cleanup/удаление legacy runtime state artifacts после подтверждённой миграции.

### Этап 2. Registry-backed Explorer

Цель:
- перевести проводник с `os.walk` на `cloud_folders/cloud_files`;
- отделить UI навигации от прямого чтения файловой системы;
- подготовить единый каталог для поиска, предпросмотра и ACL.

Выход:
- explorer читает из registry;
- текущий путь, хлебные крошки, дерево, сортировка и фильтры работают поверх Cloud Drive.

### Этап 3. File Operations and Upload

Цель:
- сделать Cloud Drive usable как продукт, а не только как импортированный каталог;
- добавить создание папок, загрузку файлов, versioning, download, delete/move/rename.

Выход:
- пользователь может работать с файлами через web без прямой зависимости от `O:\`.

### Этап 4. Search and Index Integration

Цель:
- связать registry, Qdrant, OCR и lexical search в единый pipeline;
- перевести поиск на registry-first модель.

Выход:
- поиск, preview, indexing и OCR работают по registry/file-version сущностям.

### Этап 5. API and External Clients

Цель:
- вынести файловые операции в стабильный HTTP API;
- подготовить платформу для Telegram-интеграций и будущего sync client.

Выход:
- backend не завязан на NiceGUI как единственную точку входа.

### Этап 6. Desktop Sync Client

Цель:
- реализовать Windows sync-agent для локальной синхронизации папок пользователей.

Выход:
- появляется второй полноценный клиент после web.

## Разделение работ: Codex / Claude

Ниже разделение сделано примерно поровну по объёму и сложности. Принцип такой:

- `Codex` берёт системный backend, data model, runtime, CLI, API, фоновые задачи, миграции и интеграционные гарантии.
- `Claude` берёт основной объём product/UI: проводник, пользовательские сценарии, настройки, визуальную логику, usability, flow и часть клиентской интеграции.

Это не строгое "backend vs frontend", а более практичное разделение по зонам ответственности.

## Backlog по исполнителям

### Codex

#### 1. Runtime и foundation hardening

- [x] Довести bootstrap state до полноценной job-модели в `cloud_jobs`.
- [x] Добавить отдельные статусы `pending/running/completed/failed/cancelled`.
- [x] Привязать bootstrap/import/reindex к `job_id`, а не только к runtime JSON.
- [x] Добавить cancellable long-running jobs.
- [x] Убрать остаточные page-bound timer зависимости.
- [x] Нормализовать scheduler по локальному времени/таймзоне и покрыть тестами.
- [x] Убрать runtime fallback из bootstrap status чтения; source of truth = `cloud_jobs`.

#### 2. Registry model и storage contracts

- Доработать schema registry:
  - move/rename support;
  - soft delete / restore;
  - version metadata;
  - file hash / dedup hooks;
  - storage backend metadata.
- [x] Добавить миграции schema version для Cloud Drive.
- Подготовить поддержку `S3/MinIO` как реального backend, а не только заготовки.
- [x] Добавить health-check storage backend.

#### 3. API слой

- Вынести файловые операции в FastAPI endpoints:
  - [x] list folders/files;
  - [x] get node;
  - [x] create folder;
  - [x] upload;
  - [x] download;
  - rename/move/delete;
  - [x] versions;
  - jobs/status.
- Подготовить auth/authorization hooks для API.
- [x] Добавить endpoint bootstrap status / job status.

#### 4. Search/index integration

- Перевести индексацию на registry entities:
  - file_id;
  - version_id;
  - source_path/storage_key;
  - index state per version.
- Перестроить pipeline:
  - import -> extract -> OCR -> chunk -> embeddings -> Qdrant/lexical.
- Связать cloud registry с `index_state`/telemetry.
- Добавить retry/requeue на уровне job model.

#### 5. Sync backend prerequisites

- Реализовать conflict model и version comparison contract.
- Добавить file change feed / delta endpoints.
- Добавить audit trail для sync операций.
- Подготовить серверную часть selective sync.

### Claude

#### 1. Cloud Drive admin UX

Примечание: Codex уже реализовал `render_admin_cloud_drive_settings()` со всей базовой функциональностью
(config dirty-tracking, live progress bar, jobs history, cancel/retry per job, 3s auto-refresh, stats panel, tooltips).
Задача Claude — довести UX до brandbook-уровня поверх готового backend.

- [x] Нормальные подписи и tooltips — ✅ сделано Codex
- [x] progress/status (live progress bar, file count, current path) — ✅ сделано Codex
- [x] кнопки cancel/retry — ✅ сделано Codex
- [x] журнал последних операций — ✅ сделано Codex
- [x] Привести блок к brandbook/wireframe v2:
  - [x] статус-бейджи `cd-status-badge` (pending/running/done/error/cancelled) с иконкой;
  - [x] иконки статусов (schedule/sync/check_circle/error/cancel);
  - [x] карточки jobs `cd-jobs-card` с прогресс-баром, truncated path/error;
  - [x] stats tiles с иконками folder/description/history/pending.
- [x] Добавить понятные empty/error/loading states:
  - [x] empty state stats (cloud_off + текст);
  - [x] empty state bootstrap status (cloud_upload + текст);
  - [x] empty state jobs history (history icon + текст);
  - [x] error state jobs когда нет cloud_drive_db_path (settings icon);
  - [x] error state jobs когда CloudDriveService.from_config выбрасывает.
- [x] cleanup настроек и терминологии — кнопки переименованы в русские, tooltips очищены от developer notes, уведомления переведены на продуктовый язык.

_Коммиты: f046509 (feat(ui): Cloud Drive admin UX polish — Sprint 1), 9b85dd6 (feat(ui): Cloud Drive admin — Sprint 1 terminology cleanup)_

#### 2. Explorer on registry

- [x] Перевести экран проводника на чтение из Cloud Drive registry:
  - `_cd_get_service()` — активирует registry-mode когда `cloud_drive_enabled`;
  - `_cd_list_children()`, `_cd_breadcrumb_chain()` — data-helpers над registry API;
  - `render_explorer_screen()` диспетчирует в `_render_cd_explorer()` или legacy os-walk.
- [x] Пересобрать layout:
  - [x] дерево (рекурсивный обход `list_child_folders`, active/ancestor state);
  - [x] breadcrumbs (toolbar: вверх + цепочка папок + refresh);
  - [x] список/таблица (Таблица и Список view modes);
  - [x] свойства (панель: имя, path, кол-во папок/файлов, активные фильтры);
  - [x] фильтры (по имени, расширению, сортировка по имени/размеру/дате, порядок).
- [x] Синхронизировать состояние: `explorer_cd_path` как единый source of truth для tree/breadcrumbs/entries.
- [x] Убрать дубли пути: навигация через folder.path из registry, не через filesystem.
- [x] Empty states: registry empty (cloud_off), folder empty (folder_open), star-favorites.
- [ ] Drag-and-drop upload (ждёт file operations API от Codex — Sprint 3).

_Коммит: 6678ff5 (feat(ui): Cloud Drive registry-backed explorer — Sprint 2)_

#### 3. User file workflows

- UI для:
  - [x] создания папки — диалог `_cd_new_folder_dialog`, кнопка в toolbar;
  - [x] загрузки файлов — диалог `_cd_upload_dialog` с `ui.upload`, кнопка в toolbar;
  - [x] drag-and-drop — `ui.upload` с auto_upload поддерживает drag-and-drop;
  - [ ] rename/move/delete — ждёт backend от Codex;
  - [x] версий файла — диалог `_cd_versions_dialog`, кнопка history на каждом файле;
  - [x] скачивания — кнопка download на файлах с storage_key → `/api/cloud-drive/download`;
  - [ ] предпросмотра — ждёт backend (index/OCR pipeline).
- Визуальный статус фоновых задач по файлу:
  - [ ] indexing;
  - [ ] OCR;
  - [ ] preview;
  - [ ] ошибки.

_Коммит: f73fb36 (feat(ui): Cloud Drive Sprint 3 — upload, versions, download UI)_

#### 4. Search UX over Cloud Drive

- Перевести search UI на Cloud Drive сущности.
- Сделать быстрый вывод:
  - папки;
  - файлы;
  - lazy semantic layer.
- Визуально связать результаты поиска с каталогом, версией, preview и действиями.
- Довести карточки и историю запросов под новую модель.

#### 5. Клиентская логика sync/scenarios

- Спроектировать UX для будущего desktop sync client:
  - что видит пользователь;
  - как настраивает локальную папку;
  - как отображаются конфликты;
  - как работает выборочная синхронизация.
- Подготовить UI admin/user flows для этого ещё до реализации клиента.

## Зависимости между работами

### Блокеры Codex -> Claude

- API и registry contracts нужны до полноценного перехода explorer/search на Cloud Drive.
- Job model нужна до полноценного UX прогресса и управления задачами.
- Storage backend metadata нужна до UI версий/перемещений/синхронизации.

### Блокеры Claude -> Codex

- Стабильные пользовательские flow и понятные UX states полезны до окончательной фиксации API.
- Реальные сценарии explorer/search/upload помогут выявить, каких endpoints и полей не хватает.

## Порядок исполнения

### Sprint 1

Codex:
- [x] job model для bootstrap/import;
- [x] scheduler hardening;
- [x] registry migrations;
- [x] bootstrap status API.
- [x] cancel/retry bootstrap jobs;
- [x] stale bootstrap recovery.

Claude:
- [x] brandbook/wireframe v2 для jobs history (статус-бейджи, иконки, карточки);
- [x] empty/error/loading states в Cloud Drive admin;
- [x] cleanup настроек и терминологии.

### Sprint 2

Codex:
- registry-backed data access layer;
- file operations API;
- storage health and contract.

Claude:
- [x] explorer на registry;
- [x] breadcrumbs/tree/list integration;
- [x] visual states и actions.

### Sprint 3

Codex:
- [x] upload backend + API;
- [x] download backend + API;
- [x] versions backend + API;
- [x] create folder backend + API;
- [ ] delete/move/rename backend;
- [ ] search/index registry integration.

Claude:
- [x] upload UI (диалог + toolbar кнопка + empty state shortcut);
- [x] versioning UI (история версий на каждом файле);
- [x] download UI (кнопка для файлов в storage);
- [x] create folder UI (диалог + toolbar кнопка);
- [ ] file actions UI (rename/move/delete — ждёт Codex);
- [ ] search UX adaptation (ждёт Codex search/index integration).

### Sprint 4

Codex:
- sync backend prerequisites;
- delta/feed API;
- audit/conflict contracts.

Claude:
- sync UX;
- admin/user sync settings;
- product polish для cloud workflows.

## Definition of Done

Cloud Drive можно считать первой рабочей версией, когда:

- web-клиент позволяет жить без прямой работы через `O:\Обмен`;
- explorer и search читают из registry, а не из `os.walk`;
- upload/download/versioning работают через registry + storage backend;
- индексатор и OCR работают по registry/job model;
- все долгие операции имеют observable status и recoverable execution;
- API покрывает web и Telegram-интеграции;
- локальная файловая шара становится только источником миграции/совместимости.
