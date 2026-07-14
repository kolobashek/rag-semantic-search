# RAG Каталог

Внутренний каталог документов с семантическим поиском, Cloud Drive, OCR, Telegram-ботом и web-интерфейсом на NiceGUI.

Основной стек:

- Qdrant — векторная база.
- `sentence-transformers` — embeddings.
- NiceGUI — основной web UI на `http://127.0.0.1:8080`.
- SQLite — telemetry, users, index state, Cloud Drive registry.
- Tesseract + Poppler — OCR для сканов и PDF без текстового слоя.
- Docker Compose — локальный/контейнерный запуск Qdrant, web и bot.

## Быстрый Старт

```powershell
pip install -r requirements.txt
pip install -e .
Copy-Item config.example.json config.json
python -m rag_catalog.cli.launcher start
```

Windows desktop wrapper installs PyQt6 only when requested:

```powershell
pip install -e .[desktop]
rag-windows-app
```

Если пакет ещё не установлен в окружение, можно запустить из checkout без установки:

```powershell
$env:PYTHONPATH = "$PWD\src"
python -m rag_catalog.cli.launcher start
```

Проверка статуса и остановка:

```powershell
python -m rag_catalog.cli.launcher status
python -m rag_catalog.cli.launcher stop
python -m rag_catalog.cli.launcher restart
python -m rag_catalog.cli.launcher restart-web
python -m rag_catalog.cli.launcher restart-bot --bot on
python -m rag_catalog.cli.launcher support-bundle --output runtime/support.zip
```

Лаунчер поднимает:

- web UI на `127.0.0.1:8080`;
- локальный Docker Qdrant, если `qdrant_url` указывает на localhost и порт не занят;
- Telegram-бот, если `telegram_enabled=true` и задан токен.

Legacy entrypoints сохранены как совместимые shims:

```powershell
python nice_app.py
python index_rag.py --help
python rag_search.py --help
python telegram_bot.py
```

## Конфигурация

Рабочий файл — `config.json`, шаблон — `config.example.json`. Для изолированного запуска можно явно задать другой файл через `RAG_CONFIG_PATH`.

Минимально проверить:

- `catalog_path` — исходный каталог документов.
- `qdrant_url` — Qdrant endpoint, обычно `http://localhost:6333`.
- `qdrant_db_path` — локальное состояние индекса.
- Qdrant server/client держим в одной minor-линейке: compose использует `qdrant/qdrant:v1.17.1`, Python client `qdrant-client>=1.17.1,<1.18`.
- `telemetry_db_path`, `users_db_path` — локальные SQLite БД.
- `telegram_enabled`, `telegram_bot_token`, `telegram_bot_link` — если нужен бот.
- `cloud_drive_enabled`, `cloud_drive_db_path`, `cloud_drive_storage_root` — если нужен Cloud Drive.
- `ui_reconnect_timeout_sec` — окно восстановления NiceGUI-сессии после транспортного сбоя; рекомендуемое значение `5` секунд.
- `launcher_web_start_timeout_sec` — readiness budget запуска web под фоновой нагрузкой; default `30`, допустимо `10..120` секунд.

Не коммитить реальные токены, локальные базы, storage, логи и runtime-state.

### Первый Администратор

Приложение не создаёт известный пароль `admin/admin` автоматически. Для первичного bootstrap задайте временный пароль перед первым запуском:

```powershell
$env:RAG_BOOTSTRAP_ADMIN_PASSWORD = "temporary-long-password"
python -m rag_catalog.cli.launcher restart
```

После входа под `admin` смените пароль в настройках. Если нужно запретить bootstrap даже при заданной переменной:

```powershell
$env:RAG_DISABLE_DEFAULT_ADMIN = "1"
```

## Архитектура

Ключевая структура:

```text
src/rag_catalog/core/          search, indexing, OCR, auth, telemetry, cloud drive
src/rag_catalog/core/indexing/ indexing stages and stage runner
src/rag_catalog/core/retrieval retrieval v2, fusion, rerank path
src/rag_catalog/ui/            NiceGUI screens, API routes, helpers
src/rag_catalog/integrations/  Telegram integration
src/rag_catalog/cli/           launcher and CLI entrypoints
assets/brand/                  logo, favicon, brand assets and design notes
scripts/                       maintenance scripts
packaging/                     PyInstaller specs
```

Root-level Python files are compatibility shims. New code should import package modules directly, for example:

```python
from rag_catalog.core.rag_core import RAGSearcher, load_config
from rag_catalog.core.cloud_drive import CloudDriveService
```

## Индексация

Основные стадии:

| Stage | Что делает | Когда использовать |
|---|---|---|
| `metadata` | имена, пути, размеры, даты | быстрый старт поиска по названиям |
| `small` | содержимое DOCX/XLSX/XLS и небольших PDF | основной полнотекстовый слой |
| `large` | большие PDF, сканы, OCR | полное покрытие тяжёлых файлов |
| `all` | все стадии | первичный или полный прогон |

Команды:

```powershell
python index_rag.py --stage metadata
python index_rag.py --stage small
python index_rag.py --stage large
python index_rag.py --stage all
python index_rag.py --cleanup
python index_rag.py --dry-run --stage all
python index_rag.py --watch --stage small
python index_rag.py --quality-report
python index_rag.py --recreate --stage all
```

Индексное состояние хранится в SQLite `index_state.db`, а не в JSON. Это снижает риск file-lock ошибок на Windows и позволяет безопаснее продолжать долгие прогоны.

Поддерживаемые форматы: `.doc`, `.docx`, `.xls`, `.xlsx`, `.xlsm`, `.pdf`, `.pptx`, `.rtf`, `.txt`, `.csv`, `.html`, `.htm`, изображения с OCR и архивы `.zip`, `.7z`, `.tar`, `.tar.gz`, `.tgz`, `.tar.bz2`, `.tbz`, `.tbz2`, `.tar.xz`, `.txz`, `.rar` с такими файлами внутри.

Для старых бинарных `.doc` используется быстрый bundled `antiword` из зафиксированной зависимости `doc2txt`. LibreOffice служит резервным headless-конвертером и автоматически находится в стандартной установке Windows; для нестандартного пути задайте `RAG_SOFFICE_CMD`. Без обоих инструментов применяется только аварийный бинарный fallback, который не гарантирует полноту текста.

## OCR

Рекомендуемый вариант — portable binaries внутри проекта:

```text
tools/tesseract/tesseract.exe
tools/tesseract/bin/tesseract.exe
tools/poppler/Library/bin/*
tools/poppler/bin/*
```

Явные override в `config.json`:

- `ocr_tesseract_cmd`
- `ocr_poppler_bin`
- `ocr_max_image_pages` — лимит кадров/страниц для OCR многостраничных изображений.
- `ocr_pdf_batch_pages` — число PDF-страниц, одновременно рендерящихся для OCR (по умолчанию 8).
- `ocr_rapid_fallback_enabled` — разрешить fallback RapidOCR → Tesseract; отключите для чистого GPU-бенчмарка.

Или через env:

- `RAG_TESSERACT_CMD`
- `RAG_POPPLER_BIN`

Проверка:

```powershell
tesseract --version
pdftoppm -v
python ocr_pdfs.py --url http://localhost:6333
```

## Cloud Drive

Cloud Drive — registry-backed файловый слой: папки, файлы, версии, storage backend, jobs, sync contracts и интеграция с поиском.

Поддержано:

- local storage;
- S3/MinIO adapter contract, healthcheck, presigned download path;
- bootstrap/import metadata and files;
- upload/download/versions;
- create folder, rename, move;
- soft delete, trash, restore;
- immutable storage keys and checksum dedup;
- reindex and cleanup jobs with durable leases and stale-job recovery;
- registry-backed ACL/RBAC: user/group/role grants for path, folder, file, `viewer/editor/admin` access levels;
- user groups: immutable group id, active/archived lifecycle, membership management and session/search ACL propagation;
- Explorer sharing: выдача и отзыв внутренних доступов, `who has access`, управляемые public links со сроком действия;
- index coverage diagnostics: registry files vs current `index_state.db`;
- sync clients, folder pairs, selective sync, conflicts;
- Cloud Drive hints in search and registry-backed explorer;
- Cloud Drive search results are filtered by Cloud Drive access before RAG use.

### S3 / MinIO Storage

При выборе `cloud_drive_storage: "s3"` Cloud Drive хранит содержимое файлов в объектном хранилище.
В таком режиме обязательно нужен `cloud_drive_bucket` — это контейнер объектов, без него backend не знает куда писать/читать данные.

Минимальные ключи в `config.json`:

- `cloud_drive_storage`: `s3`
- `cloud_drive_bucket`: имя bucket, например `rag-catalog`
- `cloud_drive_s3_access_key`, `cloud_drive_s3_secret_key`
- `cloud_drive_s3_endpoint`: для MinIO (например `http://127.0.0.1:9000`), для AWS S3 можно оставить пустым
- `cloud_drive_s3_region`: для MinIO обычно `us-east-1`

UI:

- `Настройки -> Cloud Drive -> Хранилище файлов = S3 / MinIO` (дальше появятся поля bucket/endpoint/keys)
- сохраните настройки и нажмите `Инициализировать реестр`

CLI:

```powershell
python cloud_drive.py init --enable
python cloud_drive.py stats
python cloud_drive.py bootstrap --max-files 1000
python cloud_drive.py import-source-add --name "Сканер" --source-path "\\server\scanner" --target-path "Входящие/Сканер"
python cloud_drive.py import-source-run <source_id> --run-now
python cloud_drive.py compact-versions
python cloud_drive.py backup --output runtime/backups/cloud-drive.zip
python cloud_drive.py verify-backup runtime/backups/cloud-drive.zip
python cloud_drive.py restore-drill runtime/backups/cloud-drive.zip
python cloud_drive.py preflight --mode upgrade --backup-dir runtime/backups
python cloud_drive.py restore runtime/backups/cloud-drive.zip --target-dir runtime/restore-check
python cloud_drive.py provider-backup --output-dir runtime/backups/s3-provider-latest --workers 8
python cloud_drive.py provider-reconcile runtime/backups/s3-provider-latest
python cloud_drive.py provider-reconcile runtime/backups/s3-provider-latest --apply
python cloud_drive.py provider-verify runtime/backups/s3-provider-latest
python cloud_drive.py provider-restore-drill runtime/backups/s3-provider-latest --sample-size 25
```

Для local storage backup использует online SQLite snapshot, включает object files и SHA-256 manifest, а секреты в `config.snapshot.json` заменяет на `[REDACTED]`. Успешный `restore-drill` создаёт рядом с архивом проверяемый artifact, который учитывается в admin storage health. Для S3/MinIO `provider-backup` сохраняет согласованные SQLite snapshots и полный набор объектов с SHA-256. `provider-reconcile` сначала в dry-run режиме находит старые registry keys и исчезнувшие source-файлы; `--apply` перепривязывает только объекты с совпавшим content SHA-256 и мягко удаляет только записи при доступном source drive. `provider-verify` проверяет все хеши и сохраняет artifact, привязанный к manifest SHA-256. После этого `provider-restore-drill` делает content round-trip выборки через временный bucket и удаляет его, не перечитывая весь snapshot повторно.

HTTP API принимает или создаёт `X-Correlation-ID`, возвращает его клиенту и добавляет в API logs и Cloud Drive audit events.
Telemetry SQLite использует rollback journal: web, bot и background indexer могут писать параллельно без общей WAL/SHM пары, которая ранее блокировала reconnect после принудительного перезапуска процесса.

Admin UI:

- `Настройки -> Cloud Drive`
- `Настройки -> Sync клиент`

Основные API группы:

```text
GET  /api/cloud-drive/node
GET  /api/cloud-drive/list
POST /api/cloud-drive/folders
POST /api/cloud-drive/upload
GET  /api/cloud-drive/download
GET  /api/cloud-drive/preview
GET  /api/cloud-drive/versions
POST /api/cloud-drive/move
POST /api/cloud-drive/rename
POST /api/cloud-drive/delete
GET  /api/cloud-drive/trash
POST /api/cloud-drive/restore
POST /api/cloud-drive/reindex
POST /api/cloud-drive/permissions
GET  /api/cloud-drive/jobs
POST /api/cloud-drive/jobs/recover-stale
GET  /api/cloud-drive/import-sources
POST /api/cloud-drive/import-sources
POST /api/cloud-drive/import-sources/run
GET  /api/cloud-drive/changes
GET  /api/cloud-drive/storage-health
GET  /api/operations/health
GET  /api/cloud-drive/index-coverage
GET  /api/user-groups
POST /api/user-groups
PATCH /api/user-groups
POST /api/user-groups/members
DELETE /api/user-groups/members
GET  /api/cloud-drive/permissions
DELETE /api/cloud-drive/permissions
POST /api/cloud-drive/share-links
GET  /api/cloud-drive/share-links
DELETE /api/cloud-drive/share-links
/api/cloud-drive/sync/*
```

Если таблица `cloud_permissions` пуста, Cloud Drive сохраняет прежний open-access режим для совместимости.
После первой grant-записи доступ к API проверяется по registry ACL с наследованием прав от папок.
Разрешённые чувствительные операции и ACL-отказы записываются в audit telemetry; для публичных ссылок хранится только отпечаток токена, сам токен в журнал не попадает.

Публичные ссылки по умолчанию выключены. Администратор включает их через `Настройки -> Cloud Drive -> Разрешить публичные ссылки` или `cloud_drive_public_links_enabled=true`. Существующие ссылки перестают открываться, когда policy выключена, но остаются видимыми администратору для отзыва.

## Поиск И RAG

Доступно:

- быстрые совпадения по именам папок/файлов;
- semantic search через Qdrant;
- retrieval v2 feature flags;
- lexical/BM25 metadata channel;
- RRF fusion;
- optional reranker;
- grouping by document;
- query expansion через LLM feature flag;
- RAG answer mode with citations and weak-source fallback;
- Telegram assistant mode.

Release retrieval preset: set `retrieval_preset=release_v2` to enable retrieval v2 + BM25/RRF defaults. Reranker remains opt-in (`retrieval_reranker_enabled=true`) until latency/eval thresholds are accepted. Interactive search can fail open when full-text or reranking is unavailable (`retrieval_fulltext_fail_open=true`, `retrieval_reranker_fail_open=true`), while `scripts/search_eval.py` forces both enabled stages to fail closed so a broken channel cannot produce a candidate artifact.
Evaluation artifacts record both `retrieval_preset` and the effective `retrieval_pipeline`; CLI `--config-set retrieval_preset=release_v2` reapplies preset defaults unless a pipeline setting is explicitly overridden. Startup warmup prepares the metadata token index and BM25 tokens so the warm-search SLO does not include avoidable per-query scans of the full catalog.
Baseline comparisons also require the same non-empty evaluation fingerprint, derived from the current source fingerprint, exact golden-file SHA-256, evaluation protocol version and `top-k` limit. A stale, fingerprint-less or differently configured `--baseline-report` keeps the decision at `NO_GO`.
Numeric identifiers are resolved from the indexed `numeric_tokens` payload. Live spreadsheet scanning is disabled in the request path; `numeric_exact_fs_fallback_enabled=true` is an emergency compatibility option for an old incomplete index and can make a query slow or dependent on source-drive availability.

Embedding migration without overwriting the old collection:

1. Set a new model and vector size in `config.json`, for example `embedding_model=BAAI/bge-m3`, `vector_size=1024`.
2. Set `embedding_collection_versioning=true` and optionally `embedding_collection_suffix=bge_m3`.
3. Run indexing; data goes to `catalog__bge_m3` while the old `catalog` collection remains available.
4. Run `python scripts/search_eval.py --golden eval/search_golden.json --limit 10 --output runtime/eval/bge_m3.json --markdown-output runtime/eval/bge_m3.md`.
5. Switch users to the new collection only after quality and latency are accepted.

Retrieval v3 evaluation accepts optional per-query `expected_paths`, `expected_chunks`, `expected_pages`, `forbidden` and `expect_no_answer`. Candidate reports include recall, precision, irrelevant-result rate, top-1 accuracy, document/chunk/page hit rate, no-answer accuracy, ACL leakage, ground-truth coverage, retrieval-source counts, actual reranker coverage and the exact retrieval profile. Precision requires each returned result to match the complete expected intent, so one relevant document followed by unrelated results no longer passes unnoticed. Compare a shadow candidate with the baseline and produce a machine-readable decision:

```powershell
python scripts/search_eval.py --golden eval/retrieval_v3_golden.json --limit 10 `
  --config-set retrieval_preset=release_v2 `
  --config-set collection_name=catalog_shadow_v3 `
  --require-profile retrieval_preset=release_v2 `
  --require-profile retrieval_pipeline=v2 `
  --require-profile collection_name=catalog_shadow_v3 `
  --baseline-report runtime/eval/legacy.json `
  --output runtime/eval/shadow-v3.json `
  --decision-output runtime/eval/shadow-v3-decision.json `
  --fail-under-recall 0.875 --max-p95-ms 3000 --max-p95-ratio 1.5 `
  --min-precision-at-k 0.5 --min-top1-accuracy 0.8 --max-irrelevant-rate 0.5 `
  --max-acl-leakage 0 --min-no-answer-accuracy 0.8 `
  --min-ground-truth-coverage 0.5 --enforce-decision-gate
```

`--require-faithfulness` остаётся блокирующим gate, пока к eval не подключён answer/citation evaluator; retrieval-only отчёт намеренно не выдаёт текстовую релевантность за faithfulness.

Ground truth нельзя автоматически выводить из текущего top-k. Для ручной разметки создаётся review queue: кандидаты показаны отдельно, а `expected_paths` остаются пустыми до решения data owner.

```powershell
python scripts/retrieval_review.py prepare `
  --golden eval/search_golden.json `
  --report runtime/eval/retrieval-v3-legacy.json `
  --output runtime/eval/retrieval-v3-review.json

python scripts/retrieval_review_ui.py runtime/eval/retrieval-v3-review.json --port 8092

python scripts/retrieval_review.py validate runtime/eval/retrieval-v3-review.json
python scripts/retrieval_review.py finalize runtime/eval/retrieval-v3-review.json `
  --output eval/retrieval_v3_golden.json
```

Локальный UI привязан к `127.0.0.1`, сохраняет каждое решение атомарно и держит предыдущую версию рядом в `.bak`. В нём можно отметить relevant/forbidden кандидатов, добавить отсутствующий путь или создать отдельный no-answer запрос. Для каждого элемента reviewer задаёт `status=reviewed`, `reviewed_by`, `reviewed_at` и либо `expected_paths`, либо `expect_no_answer=true`. Финализация и retrieval gate по умолчанию требуют минимум 50 запросов, 10 no-answer, 20 document-grounded, 10 chunk/page-grounded, 6 категорий и 3 forbidden/ACL cases. Retrieval GO также требует `index_readiness=true`, ненулевой `acl_results_checked`, resolved profile с включённым relevance gate и корректными порогами. Если profile включает BM25, full-text или reranker, отчёт обязан подтвердить фактическое участие соответствующих каналов; для reranker требуется 100% покрытие всех оценённых top-k результатов. Отсутствие readiness или нулевая утечка без ACL ground truth не считаются доказательством готовности.

Для автоматического gate передайте свежий authenticated smoke через `--acl-evidence runtime/pilot-ui-smoke/<run>/pilot-ui-smoke.json`. CLI отклонит неуспешный или устаревший артефакт с другим fingerprint исходников.

### Paid Pilot Release Gate

Authenticated UI smoke поднимает отдельный временный contour, не создаёт пользователей в рабочей БД и проверяет login, сохранение search state, groups, ACL success/deny audit с correlation ID, все основные маршруты и responsive layout на 480/900/1280 px:

```powershell
python scripts/pilot_ui_smoke.py
```

Runner использует установленный Chrome/Edge либо путь из `--browser-executable`. Python Playwright входит в `dev` dependencies; при отсутствии браузера можно выполнить `playwright install chromium`.

Итоговый gate объединяет operations health, свежий verified restore, UI/ACL/audit smoke, полный pytest, retrieval evidence и подписанный acceptance:

```powershell
python -m rag_catalog.cli.pilot_gate --write-signoff-template
python -m rag_catalog.cli.pilot_gate --run-tests `
  --retrieval-artifact runtime/eval/retrieval-v3-pilot.json
```

Команда возвращает успешный код только при решении `GO`; полный отчёт сохраняется в `runtime/pilot-gates/`. UI и pytest artifacts содержат SHA-256 fingerprint файлов `src/tests/scripts/pyproject.toml`, поэтому любое изменение кода автоматически требует повторного smoke/test run. Заполнять sign-off заранее нельзя: имена ответственных, customer acceptance и update rehearsal фиксируются после фактической приёмки.

CLI:

```powershell
python rag_search.py --query "карточка предприятия" --limit 10
python rag_search.py --query "паспорт" --type .pdf --limit 5
python rag_search.py --url http://localhost:6333 --query "PC300"
```

Onboarding, acceptance, release/rollback и incident procedure для выделенного клиентского контура: [Paid Dedicated Pilot Runbook](docs/PILOT_RUNBOOK.md).

## Docker

Подготовка:

```powershell
Copy-Item config.docker.example.json config.docker.json
```

Запуск web + Qdrant:

```powershell
docker compose up -d --build qdrant web
```

Docker image ставит runtime-зависимости, чтобы индексатор меньше зависел от host-ПК:

- OCR: `tesseract-ocr`, `tesseract-ocr-rus`, `poppler-utils`;
- архивы: `libarchive-tools`/`bsdtar`, `p7zip-full`, `unar` (`.rar` читается через `bsdtar` или `7z`/`7zz`/`7za`);
- legacy Office: `antiword`, `catdoc`, headless LibreOffice components.

Если локальные порты заняты, переопредели их через env vars:

```powershell
$env:QDRANT_PORT = "16333"
$env:RAG_WEB_PORT = "18080"
docker compose up -d --build qdrant web
```

Если `config.docker.json` лежит не рядом с compose-файлом:

```powershell
$env:RAG_CONFIG_PATH = "C:\path\to\config.docker.json"
```

Для release smoke без host bind mounts, которые иногда ломаются в Docker Desktop на Windows-дисках:

```powershell
docker compose -p semanticsearch_smoke -f docker-compose.yml -f docker-compose.smoke.yml up -d qdrant web
docker compose -p semanticsearch_smoke -f docker-compose.yml -f docker-compose.smoke.yml down --remove-orphans
```

### Docker + MinIO (S3 storage for Cloud Drive)

Если хочешь S3-совместимое хранилище “вместе с окружением”, подними MinIO через compose profile `storage`:

```powershell
docker compose --profile storage up -d minio minio-init
```

По умолчанию MinIO поднимается на:

- S3 API: `http://localhost:9000`
- Console: `http://localhost:9001` (логин/пароль берутся из `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`)

Порты можно переопределить через `MINIO_PORT` и `MINIO_CONSOLE_PORT`.

Шаблон `config.docker.example.json` уже настроен на MinIO (`cloud_drive_storage=s3`, endpoint `http://minio:9000`, bucket `rag-catalog`).
Скопируй его в `config.docker.json` и при необходимости переопредели credentials через env vars:

```powershell
$env:MINIO_ROOT_USER = "minioadmin"
$env:MINIO_ROOT_PASSWORD = "minioadmin123"
$env:MINIO_BUCKET = "rag-catalog"
```

Telegram bot:

```powershell
docker compose --profile bot up -d --build bot
```

Indexer:

```powershell
docker compose --profile tools run --rm indexer
```

Логи и остановка:

```powershell
docker compose logs -f web
docker compose down
```

Если каталог в контейнере монтируется не тем же путём, лучше переиндексировать из контейнера, чтобы пути в Qdrant соответствовали runtime окружению.

## Runtime Данные

Не коммитятся:

```text
data/
runtime/
logs/
tools/tesseract/
tools/poppler/
*.db
*.db-wal
*.db-shm
config.json
config.docker.json
```

Типичные локальные данные:

| Что | Где |
|---|---|
| telemetry/users/index state/cloud drive DB | `data/` |
| launcher state and pid files | `runtime/` |
| logs | `logs/` |
| Cloud Drive local storage | `data/cloud_storage` |

## Логи И Диагностика

Новые runtime-логи пишутся сегментами в:

```text
logs/history/<лог>/<YYYY-MM-DD>/*.log
```

Сегментация выполняется:

- по новому запуску web/bot/index/OCR;
- по дню;
- по лимиту размера файла для Python logging handlers.

UI читает историю бесшовно: новые сегменты и старые legacy-файлы (`logs/*.log`, `logs/runtime/*.log`, старый путь `log_file` из `config.json`) показываются как одна история. В модальном окне логов индекса доступны фильтры по уровню, датам и текстовый поиск по истории.

SQLite runtime общий для telemetry/users/cloud registry: соединения включают `busy_timeout`, WAL и `synchronous=NORMAL`, но не валят процесс при временной гонке повторного `journal_mode=WAL` на Windows, если база уже в WAL.

## Проверки

Полный прогон:

```powershell
python -m pytest -q
python -m ruff check src tests scripts
python scripts/search_eval.py --golden eval/search_golden.json --limit 10 --fail-under-recall 0.875
```

Быстрая проверка entrypoints/modules:

```powershell
python -m py_compile app_ui.py nice_app.py rag_core.py index_rag.py telegram_bot.py windows_app.py run_automation.py
python -m pytest -q tests\test_entrypoints.py
```

Cloud Drive focused tests:

```powershell
python -m pytest -q tests\test_cloud_drive_registry.py tests\test_cloud_drive_storage.py tests\test_nice_app_explorer.py
```

Release smoke:

```powershell
python -m rag_catalog.cli.launcher status
$env:QDRANT_PORT = "16333"
$env:RAG_WEB_PORT = "18080"
docker compose -p semanticsearch_smoke -f docker-compose.yml -f docker-compose.smoke.yml up -d qdrant web
docker compose -p semanticsearch_smoke -f docker-compose.yml -f docker-compose.smoke.yml down --remove-orphans
```

Cleanup локальных артефактов:

```powershell
.\scripts\clean_project.ps1
```

## Статус Roadmap

- Текущий delivery-план до платного dedicated pilot и полноценного cloud service: [docs/CLOUD_SERVICE_ROADMAP.md](docs/CLOUD_SERVICE_ROADMAP.md).
- Полный продуктовый горизонт: [docs/PRODUCT_ROADMAP.md](docs/PRODUCT_ROADMAP.md).
- История и критерии закрытого internal release: [docs/RELEASE_ROADMAP.md](docs/RELEASE_ROADMAP.md).
