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

Рабочий файл — `config.json`, шаблон — `config.example.json`.

Минимально проверить:

- `catalog_path` — исходный каталог документов.
- `qdrant_url` — Qdrant endpoint, обычно `http://localhost:6333`.
- `qdrant_db_path` — локальное состояние индекса.
- Qdrant server/client держим в одной minor-линейке: compose использует `qdrant/qdrant:v1.17.1`, Python client `qdrant-client>=1.17.1,<1.18`.
- `telemetry_db_path`, `user_db_path` — локальные SQLite БД.
- `telegram_enabled`, `telegram_bot_token`, `telegram_bot_link` — если нужен бот.
- `cloud_drive_enabled`, `cloud_drive_db_path`, `cloud_drive_storage_root` — если нужен Cloud Drive.

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
python index_rag.py --quality-report
python index_rag.py --recreate --stage all
```

Индексное состояние хранится в SQLite `index_state.db`, а не в JSON. Это снижает риск file-lock ошибок на Windows и позволяет безопаснее продолжать долгие прогоны.

Поддерживаемые форматы: `.doc`, `.docx`, `.xls`, `.xlsx`, `.pdf`, `.pptx`, `.rtf`, `.txt`, `.csv`, изображения с OCR и `.zip`-архивы с такими файлами внутри.

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
- reindex and cleanup jobs;
- sync clients, folder pairs, selective sync, conflicts;
- Cloud Drive hints in search and registry-backed explorer.

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
python cloud_drive.py compact-versions
```

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
GET  /api/cloud-drive/versions
POST /api/cloud-drive/move
POST /api/cloud-drive/rename
POST /api/cloud-drive/delete
GET  /api/cloud-drive/trash
POST /api/cloud-drive/restore
POST /api/cloud-drive/reindex
GET  /api/cloud-drive/jobs
GET  /api/cloud-drive/changes
GET  /api/cloud-drive/storage-health
/api/cloud-drive/sync/*
```

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

Release retrieval preset: set `retrieval_preset=release_v2` to enable retrieval v2 + BM25/RRF defaults. Reranker remains opt-in (`retrieval_reranker_enabled=true`) until latency/eval thresholds are accepted.

Embedding migration without overwriting the old collection:

1. Set a new model and vector size in `config.json`, for example `embedding_model=BAAI/bge-m3`, `vector_size=1024`.
2. Set `embedding_collection_versioning=true` and optionally `embedding_collection_suffix=bge_m3`.
3. Run indexing; data goes to `catalog__bge_m3` while the old `catalog` collection remains available.
4. Run `python scripts/search_eval.py --golden eval/search_golden.json --limit 10 --output runtime/eval/bge_m3.json --markdown-output runtime/eval/bge_m3.md`.
5. Switch users to the new collection only after quality and latency are accepted.

CLI:

```powershell
python rag_search.py --query "карточка предприятия" --limit 10
python rag_search.py --query "паспорт" --type .pdf --limit 5
python rag_search.py --url http://localhost:6333 --query "PC300"
```

## Docker

Подготовка:

```powershell
Copy-Item config.docker.example.json config.docker.json
```

Запуск web + Qdrant:

```powershell
docker compose up -d --build qdrant web
```

### Docker + MinIO (S3 storage for Cloud Drive)

Если хочешь S3-совместимое хранилище “вместе с окружением”, подними MinIO через compose profile `storage`:

```powershell
docker compose --profile storage up -d minio minio-init
```

По умолчанию MinIO поднимается на:

- S3 API: `http://localhost:9000`
- Console: `http://localhost:9001` (логин/пароль берутся из `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`)

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

Cleanup локальных артефактов:

```powershell
.\scripts\clean_project.ps1
```

## Статус Roadmap

Предыдущий Cloud Drive roadmap закрыт. История сохранена в Git. Текущий план доведения до релиза: [docs/RELEASE_ROADMAP.md](docs/RELEASE_ROADMAP.md).

Кандидаты после первого релиза:

- production-hardening sync client;
- deeper structural chunking for DOCX/PDF/XLSX;
- Qdrant sparse vector path;
- stronger RAG verifier for conflicting sources;
- UI polish after реального использования Cloud Drive.
