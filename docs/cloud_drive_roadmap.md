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
- добавлены registry-backed endpoints перемещения, переименования и удаления:
  - `POST /api/cloud-drive/move`
  - `POST /api/cloud-drive/rename`
  - `POST /api/cloud-drive/delete`
- Cloud Drive service и registry получили операции `move_node()` / `delete_node()` с обновлением storage key для local storage.
- добавлены базовые session-based auth/authorization hooks для Cloud Drive API:
  - read endpoints требуют валидную пользовательскую сессию;
  - write endpoints требуют валидную пользовательскую сессию;
  - admin endpoints (`bootstrap-status`, `bootstrap-jobs`, `storage-health`) требуют роль `admin`.
- добавлены общие admin endpoints для `cloud_jobs`:
  - `GET /api/cloud-drive/jobs`
  - `GET /api/cloud-drive/job`
  - `GET /api/cloud-drive/job-latest`
- Cloud Drive service получил общие методы `get_job()` / `get_latest_job()` / `list_jobs()` для API и дальнейшей интеграции UI.
- добавлен первый registry-to-index bridge endpoint:
  - `POST /api/cloud-drive/reindex`
- Cloud Drive service получил `enqueue_reindex()` c возвратом `reindex` job для file/version сущности.
- добавлен обработчик lifecycle для registry jobs:
  - `POST /api/cloud-drive/job-run`
- Cloud Drive service получил `run_reindex_job()` для `reindex/cleanup` jobs.
- `upload/move/rename/delete` автоматически ставят `reindex` и/или `cleanup` jobs для затронутых файлов.
- Reindex handler вызывает текущий `RAGIndexer` для файлов из `catalog_path` и local Cloud Drive storage.
- Qdrant payload и `index_state.db` получили Cloud Drive identity: `cloud_file_id`, `cloud_version_id`, `cloud_path`, `storage_key`.
- `cleanup` jobs удаляют Qdrant points по Cloud Drive identity.
- добавлен API статусов по файлам:
  - `GET /api/cloud-drive/file-statuses`
- добавлен retry/requeue API для registry jobs:
  - `POST /api/cloud-drive/job-retry`

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
  - [x] rename/move/delete;
  - [x] versions;
  - [x] jobs/status.
- [x] Подготовить базовые auth/authorization hooks для API.
- [x] Добавить endpoint bootstrap status / job status.

#### 4. Search/index integration

- Перевести индексацию на registry entities:
  - file_id;
  - version_id;
  - source_path/storage_key;
  - index state per version.
- [x] Добавить первый registry-driven `reindex` job endpoint для file/version сущностей.
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
- [x] Drag-and-drop upload — постоянная зона в основном контенте проводника (ui.upload, flat bordered).

_Коммит: 6678ff5 (feat(ui): Cloud Drive registry-backed explorer — Sprint 2)_

#### 3. User file workflows

- UI для:
  - [x] создания папки — диалог `_cd_new_folder_dialog`, кнопка в toolbar;
  - [x] загрузки файлов — диалог `_cd_upload_dialog` с `ui.upload`, кнопка в toolbar;
  - [x] drag-and-drop — `ui.upload` с auto_upload поддерживает drag-and-drop;
  - [x] rename/move/delete — контекстное меню (more_vert) на каждой строке файла/папки, диалоги переименования и подтверждения удаления;
  - [x] версий файла — диалог `_cd_versions_dialog`, кнопка history на каждом файле;
  - [x] скачивания — кнопка download на файлах с storage_key → `/api/cloud-drive/download`;
  - [ ] предпросмотра — ждёт backend (index/OCR pipeline).
- Визуальный статус фоновых задач по файлу:
  - [x] indexing;
  - [x] OCR;
  - [x] preview;
  - [x] ошибки.

_Коммит: f73fb36 (feat(ui): Cloud Drive Sprint 3 — upload, versions, download UI)_

#### 4. Search UX over Cloud Drive

- [x] Сделать быстрый вывод из реестра:
  - [x] папки (depth ≤ 3, до 5 совпадений по имени);
  - [x] файлы (корневой уровень, до 5 совпадений по имени);
  - [ ] lazy semantic layer — ждёт Codex index/OCR pipeline.
- [x] Карточка «Cloud Drive» показывается над семантическими результатами.
- [x] Клик по папке → переход в explorer на эту папку.
- [x] Клик по файлу → открытие viewer (если файл на диске).
- [ ] Визуально связать результаты поиска с версией, preview и действиями — ждёт Codex.
- [x] История запросов под новую Cloud Drive модель.

_Коммит: 487abb8 (feat(ui): Cloud Drive search hints — registry name matching in search)_

#### 5. Клиентская логика sync/scenarios

- Спроектировать UX для будущего desktop sync client:
  - [x] что видит пользователь — статус клиента (connected/disconnected), список пар папок;
  - [x] как настраивает локальную папку — диалог добавления пары (local path + Cloud Drive picker + conflict policy);
  - [x] как отображаются конфликты — журнал конфликтов в admin settings (placeholder для backend);
  - [x] как работает выборочная синхронизация — checkbox list всех top-level папок реестра.
- [x] Подготовить UI admin/user flows для этого ещё до реализации клиента.

_Коммиты: 63bc6f2 (feat(ui): Cloud Drive Sync client admin settings), d2435c1 (feat(ui): Cloud Sync user settings section)_

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
- [x] delete/move/rename backend (POST /api/cloud-drive/move, /rename, /delete);
- [x] search/index registry integration: queue + job lifecycle + local storage-aware indexing готовы.

Claude:
- [x] upload UI (диалог + toolbar кнопка + empty state shortcut);
- [x] versioning UI (история версий на каждом файле);
- [x] download UI (кнопка для файлов в storage);
- [x] create folder UI (диалог + toolbar кнопка);
- [x] details panel: свойства + Действия (new folder/upload) + Фильтры;
- [x] header breadcrumbs: синхронизированы с текущим cd_path;
- [x] search quick-match: SQL LIKE по реестру над семантическими результатами;
- [x] file actions UI (rename/move/delete) — контекстное меню на каждой строке файла/папки;
- [x] move-to-folder dialog — список всех папок реестра, ui.select picker, вызов svc.move_node();
- [x] drag-and-drop upload — постоянная зона в нижней части проводника, авто-загрузка через tempfile;
- [ ] search UX adaptation (ждёт Codex search/index integration).

### Sprint 4

Codex:
- sync backend prerequisites;
- delta/feed API;
- audit/conflict contracts.

Claude:
- [x] sync UX — scaffold: connected clients panel, empty states, status badge;
- [x] admin sync settings — `render_admin_cloud_sync_settings()`: folder pairs, conflict policy, selective sync, conflict journal;
- [x] user sync settings — `cloud_sync_user` section в панели настроек: статус клиента, список пар папок;
- [x] product polish для cloud workflows — drop zone CSS, search hints улучшены (parent path + "Show in Explorer"), context-menu delete highlight.

## External Review Backlog

Этот блок собран из внешних ревью и используется как приоритетный вход в следующие спринты. Дубли с уже выполненными задачами не повторяются.

### P0: первый рабочий релиз Cloud Drive

Codex:
- [x] Закрыть local end-to-end `Cloud Drive -> reindex job -> extract/OCR/chunk/embed -> Qdrant/search payload`.
- [x] Сделать обработчик `reindex` jobs, а не только постановку задачи.
- [x] Связать `cloud_files.id` / `cloud_file_versions.id` с telemetry; `index_state` и Qdrant payload уже связаны.
- [x] На upload/move/rename/delete автоматически ставить нужные reindex/cleanup jobs.
- [x] Сделать local storage-aware indexing для файлов, загруженных в Cloud Drive storage вне `catalog_path`.
- [x] Добавить per-file job status API для indexing/OCR/preview/error.
- [x] Добавить retry/requeue для registry jobs.
- [x] Добавить cleanup job для удалённых/старых Qdrant points.

Claude:
- [x] Показать per-file indexing/OCR/preview/error status в проводнике.
- [x] Показать per-file status в карточках поиска.
- [x] Доделать lazy semantic layer в Cloud Drive search после backend-интеграции.
- [x] Связать search results с version, preview и file actions.

### P1: безопасность и эксплуатационная готовность

Codex:
- [x] Убрать production-риск `admin/admin`: forced password rotation, explicit bootstrap admin или запрет default admin без dev-mode.
- [x] Перевести Cloud Drive API на session cookie / `Authorization: Bearer`; `auth_token` оставлен как legacy fallback до чистки внешних клиентов.
- [x] Добавить folder/file ACL hooks в Cloud Drive API и search filters.
- [x] Добавить audit trail для Cloud Drive операций: view/download/upload/delete/move/rename/search.
- [x] Добавить CI quality gate: `pytest`, `py_compile`, launcher smoke, docker smoke.
- [x] Подключить `ruff` и постепенно включать правила без массового churn.
- [x] Зафиксировать dependency lock / reproducible install path.

Claude:
- [x] Показать пользователю понятные состояния доступа: нет прав, сессия истекла, нужна смена пароля.
- [x] Добавить admin UX для audit/security событий.

### P1: качество поиска и измеримость

Codex:
- [x] Добавить offline relevance benchmark: golden queries, expected docs, `Recall@k`, `MRR`, `nDCG`, latency p50/p95.
- [x] Добавить retrieval v2 feature flag.
- [x] Вынести retrieval pipeline в `src/rag_catalog/core/retrieval/`.
- [x] Добавить RRF fusion поверх dense + текущего lexical.
- [ ] Добавить BM25/sparse слой или Qdrant sparse vector path.
- [ ] Добавить reranker stage для top-N результатов.
- [ ] Подготовить migration path для новых embedding models (`bge-m3`, `multilingual-e5-*`) через версионированные Qdrant collections.
- [ ] Улучшить chunking: paragraph/sentence-aware минимум, затем structural chunking для DOCX/PDF/XLSX.
- [ ] Добавить parent-child retrieval и provenance: page/sheet/row/section.
- [ ] Добавить grouping/diversity: лимит чанков на документ и MMR-подобную диверсификацию.

Claude:
- [x] Добавить UI для eval results: сравнение пайплайнов, топ провалов, latency.
- [x] Обновить UX поиска под retrieval v2: source grouping, provenance, preview snippets.

### P2: LLM/RAG assistant

Codex:
- [ ] Подключить `llm.expand_query()` в основной search pipeline через config flag и telemetry.
- [ ] Добавить RAG answer mode backend: answer + citations + "не знаю" при слабых источниках.
- [ ] Добавить verifier/gating для фактов: числа, даты, единицы измерения, конфликтующие источники.
- [ ] Добавить Telegram assistant mode: question detection, answer with sources, follow-up context.

Claude:
- [x] Добавить web-режим "Ответ по документам" с источниками и кликабельными цитатами.
- [x] Добавить действия "Пояснить по этому документу" и "Сводка по выбранным".

### P2: архитектурная поддерживаемость

Codex:
- [ ] Разрезать `index_rag.py` на `indexing/`, `extractors/`, `chunking/`, `qdrant_writer`.
- [ ] Вынести Cloud Drive API из `nice_app.py` в отдельный модуль.
- [ ] Вынести auth helpers из `nice_app.py`.
- [ ] Доделать миграцию root shims в тонкие entrypoints и убрать legacy-дубли.

Claude:
- [x] Разрезать UI: nice_app.py 6085→1820 lines; extracted explorer_view, settings_view, stats_view, index_view.
- [x] Привести analytics/telemetry UI к отдельному рабочему экрану для доменного улучшения поиска.

### P2: Cloud Drive production storage and sync

Codex:
- [ ] Реализовать production S3/MinIO path: upload/download, presigned URLs, health, failure modes.
- [ ] Добавить immutable storage keys и path-independent `doc_id`.
- [ ] Добавить dedup by checksum.
- [ ] Добавить soft delete/trash/restore.
- [ ] Добавить file change feed / delta endpoints.
- [ ] Добавить conflict model и selective sync backend.
- [ ] Добавить cleanup job для удалённых/старых Qdrant points.

Claude:
- [ ] Добавить trash/restore UX.
- [ ] Добавить sync conflict resolution UX.
- [x] Добавить saved searches, favorites и collections после стабилизации ACL/search.

## Definition of Done

Cloud Drive можно считать первой рабочей версией, когда:

- web-клиент позволяет жить без прямой работы через `O:\Обмен`;
- explorer и search читают из registry, а не из `os.walk`;
- upload/download/versioning работают через registry + storage backend;
- индексатор и OCR работают по registry/job model;
- все долгие операции имеют observable status и recoverable execution;
- API покрывает web и Telegram-интеграции;
- локальная файловая шара становится только источником миграции/совместимости.
