# RAG Catalog

Internal semantic search tool for DOCX, XLSX, XLS, and PDF catalogs.

The project uses Qdrant for vector search, `sentence-transformers` for
embeddings, NiceGUI for the main web UI, Streamlit as the legacy web UI, PyQt6
for the Windows UI, and optional OCR for scanned PDF files.

## Quick Start

```powershell
pip install -r requirements.txt
python index_rag.py --help
python rag_search.py --help
python nice_app.py
# единый запуск сервисов (web + qdrant + telegram при необходимости)
python -m rag_catalog.cli.launcher start
```

Legacy Streamlit UI is still available with `streamlit run app_ui.py`.

## Unified Launcher

Unified launcher controls web UI, Qdrant, and Telegram bot from one command.

```powershell
python -m rag_catalog.cli.launcher status
python -m rag_catalog.cli.launcher start
python -m rag_catalog.cli.launcher stop
python -m rag_catalog.cli.launcher restart
```

Behavior:

- starts web UI on `127.0.0.1:8080`
- starts local Docker Qdrant only when `qdrant_url` points to local host and port is down
- skips bot by default if `telegram_enabled=false` or token is empty

Use `config.example.json` as the template for local machine settings in
`config.json`. Keep real tokens, paths, logs, databases, and build artifacts out
of Git.

Docker deployment is available via `docker-compose.yml`; see `docs/DOCKER.md`.

## OCR Runtime (Bundled Tools)

OCR does not depend on global `PATH` anymore if binaries are placed in project
`tools/`:

- `tools/tesseract/tesseract.exe` (or `tools/tesseract/bin/tesseract.exe`)
- `tools/poppler/Library/bin` (or `tools/poppler/bin`)

Optional explicit overrides in `config.json`:

- `ocr_tesseract_cmd`
- `ocr_poppler_bin`

## Cloud Drive

Cloud Drive adds a central registry of folders/files plus a managed storage backend.

Current supported backends:

- `local`
- `s3` / `minio` (backend contract and health-check; production rollout still in progress)

Main config keys in `config.json`:

- `cloud_drive_enabled`
- `cloud_drive_db_path`
- `cloud_drive_storage`
- `cloud_drive_storage_root`
- `cloud_drive_bucket`
- `cloud_drive_s3_endpoint`
- `cloud_drive_s3_region`
- `cloud_drive_s3_access_key`
- `cloud_drive_s3_secret_key`

Quick start:

```powershell
python cloud_drive.py init --enable
python cloud_drive.py stats
python cloud_drive.py bootstrap --max-files 1000
```

Admin UI:

- `Settings -> Cloud Drive`
- actions: `Инициализировать реестр`, `Статистика`, `Импортировать структуру`, `Импортировать структуру и файлы`

Current API endpoints:

- `GET /api/cloud-drive/node`
- `GET /api/cloud-drive/list`
- `POST /api/cloud-drive/folders`
- `POST /api/cloud-drive/upload`
- `GET /api/cloud-drive/download`
- `GET /api/cloud-drive/versions`
- `POST /api/cloud-drive/move`
- `POST /api/cloud-drive/rename`
- `POST /api/cloud-drive/delete`
- `POST /api/cloud-drive/reindex`
- `GET /api/cloud-drive/jobs`
- `GET /api/cloud-drive/job`
- `GET /api/cloud-drive/job-latest`
- `GET /api/cloud-drive/bootstrap-status`
- `GET /api/cloud-drive/bootstrap-jobs`
- `GET /api/cloud-drive/storage-health`

## Documentation

- Full Russian operations guide: `ИНСТРУКЦИЯ.md`
- Architecture notes: `docs/ARCHITECTURE.md`
- Development checks: `docs/DEVELOPMENT.md`

## Checks

```powershell
python -m pytest -q tests
python -m py_compile app_ui.py nice_app.py rag_core.py index_rag.py telegram_bot.py windows_app.py run_automation.py
python test_imports.py
```
