# Release Roadmap

Цель: довести текущий RAG Catalog до стабильного внутреннего релиза, где поиск, Cloud Drive, индексация, безопасность и эксплуатация работают предсказуемо и проверяются автоматически.

## Release Gate

Релиз считается готовым, когда выполнены все условия:

- security: нет дефолтного `admin/admin`, сессии работают без передачи auth token в URL, роли и доступы документированы;
- search: выбран production retrieval preset, качество подтверждено eval-набором, p95 latency зафиксирована;
- indexing: nightly schedules реально запускаются, остановка/повторный запуск не теряют прогресс, ошибки видны в UI и логах;
- Cloud Drive: upload/download/version/move/delete/reindex проходят end-to-end, S3/MinIO healthcheck и import status понятны администратору;
- UI: search, explorer, index, settings и analytics не ломаются на mobile/tablet/desktop;
- ops: launcher, Docker compose, CI, README и config examples воспроизводимо поднимают стек;
- tests: CI green, focused smoke для launcher/docker/search/cloud/index проходит.

## P0 До Релиза

### 1. Security Hardening

Owner: Codex.

- DONE 2026-05-14: убрать silent bootstrap `admin/admin` по умолчанию.
- DONE 2026-05-14: bootstrap admin оставлен только через явный `RAG_BOOTSTRAP_ADMIN_PASSWORD`; silent first-run password отключён.
- DONE 2026-05-14: убрана поддержка `auth_token` через query-параметры в Cloud Drive API; остаются browser session и `Authorization: Bearer`.
- DONE 2026-05-14: `/api/view-file` закрыт auth-проверкой; path traversal покрыт тестом.
- DONE 2026-05-14: Cloud Drive download/sync endpoints сверены: auth/header flow активен, path ACL применяется; download auth покрыт тестом.
- DONE 2026-05-14: audit events сверены: login/logout в `auth_events`, Cloud Drive download/upload/delete/restore/reindex и settings changes в `app_events`.

Done criteria:

- fresh install без bootstrap password не создаёт известный пароль;
- тесты покрывают auth-token leak и protected endpoint access;
- README описывает first-run admin setup.

### 2. Retrieval Production Preset

Owner: Codex.

- Выбрать production embedding path: текущий `all-MiniLM-L6-v2` оставить legacy, новую модель вести через versioned collection.
- DONE 2026-05-14: README описывает migration plan для новой embedding-модели через `embedding_collection_versioning` и eval before switch.
- DONE 2026-05-14: добавлен конфигурируемый `retrieval_preset=release_v2` для retrieval v2 + BM25/RRF defaults.
- DONE 2026-05-14: reranker оставлен opt-in и не включается release preset без latency/eval замеров.
- DONE 2026-05-14: baseline eval снят на 32 запросах (`runtime/eval/baseline.*` локально): Recall@10=0.875, zero-result=0.000, steady-state p50=472 ms, p95=919 ms; cold-start первого запроса ~20 сек из-за загрузки модели.
- DONE 2026-05-14: release gate rerun после index/cloud fixes: `pytest -q` = 375 passed; `search_eval` latest = Recall@10 0.875, zero-result 0.000, p50 790 ms, p95 1684 ms. Один cold/slow folder query 27.8s остаётся performance-риск для P1.
- DONE 2026-05-14: NiceGUI теперь переиспользует общий `RAGSearcher` между сессиями и запускает фоновый warmup embedder + name/path cache при старте (`search_warmup_enabled=true`).

Done criteria:

- DONE 2026-05-14: есть config preset для legacy и release retrieval; `config.example.json` и Docker example синхронизированы.
- DONE 2026-05-14: baseline vs `release_v2` сравнен локально. Baseline: Recall@10=0.875, p50=472 ms, p95=919 ms. Release_v2 before BM25 cache: Recall@10=0.875, p50=1365 ms, p95=13127 ms. After BM25 token cache: Recall@10=0.875, p50=501 ms, p95=942 ms. Решение: `release_v2` допустим как opt-in preset; default переключать только после согласования thresholds.
- UI показывает режим поиска и не скрывает fallback/ошибки Qdrant/LLM.

### 3. Search Evaluation Gate

Owner: Codex.

- DONE 2026-05-14: `eval/search_golden.json` расширен до 32 запросов по exact/folder/document/OCR-like сценариям.
- DONE 2026-05-14: golden cases разделены по категориям (`folder_or_name`, `exact_number_or_vehicle`, `document_type`, `ocr_or_scan`, `semantic_business`, `general`); eval считает `by_category`. Cloud Drive-specific cases добавить после стабилизации registry search fixtures.
- Запускать `scripts/search_eval.py` в CI как optional/manual gate сначала, затем как required для retrieval changes.
- DONE 2026-05-14: eval report включает Recall/MRR/nDCG, zero-result rate, latency p50/p95; CLI умеет JSON и Markdown artifacts.
- DONE 2026-05-14: `python scripts/search_eval.py --golden eval/search_golden.json --limit 10 --output runtime/eval/latest.json --markdown-output runtime/eval/latest.md` проходит локально на текущем индексе.

Done criteria:

- DONE 2026-05-14: eval можно запустить одной командой локально: `python scripts/search_eval.py --golden eval/search_golden.json --limit 10`.
- DONE 2026-05-14: результат сохраняется в JSON/Markdown artifact через `--output` и `--markdown-output`.
- пороги качества ещё нужно согласовать; `release_v2` latency regression должен быть устранён до required CI gate.

### 4. Indexing And OCR Reliability

Owner: Codex.

- DONE 2026-05-14: scheduler больше не теряет запуск из-за узкого окна ±1 минута; daily/hourly catch-up покрыт тестами.
- DONE 2026-05-14: индексный UI теперь видит runtime marker сразу после старта процесса, даже если `index_runs/index_stage_progress` ещё не созданы; это убирает задержку переключения кнопок/статуса после нажатия start.
- DONE 2026-05-14: `active_stages` теперь включает только stage rows со статусом `running`; completed/failed этапы остаются в latest summary, но не показываются как активные задачи.
- DONE 2026-05-14: XLSX extraction больше не падает на файлах без `xl/sharedStrings.xml`; добавлен ZIP/XML fallback и regression-тест на повреждённую структуру workbook.
- DONE 2026-05-14: stop для index/OCR завершает дерево дочерних процессов, а не только root PID; это снижает риск зависших OCR/pdf helper-процессов после остановки.
- DONE 2026-05-14: SQLite runtime больше не валит web/bot/scheduler при `PRAGMA journal_mode=WAL` -> `disk I/O error`; добавлен fallback на текущий/default journal mode и regression-тест.
- DONE 2026-05-14: failed/cancelled stage summary теперь сохраняет `run_id/run_note`, а pipeline UI показывает короткую причину последнего сбоя прямо в строке этапа.
- Проверить фактическую ночную индексацию на telemetry после следующего ночного окна: lock, active process, last run reason.
- Оставить одно действие для активного этапа: stop; следующий start продолжает по state DB.
- Добавить retry failed files / failed phase UX: список ошибок, файл, exception, кнопка retry scope.
- OCR вынести в явную очередь или job list: pending/running/failed/done по файлам.
- Проверить Qdrant timeout behavior на small/large chunks; не скрывать stage failure без ERROR логов.

Done criteria:

- stage status в UI совпадает с telemetry/index_state;
- остановка процесса не оставляет ложный `running`;
- failed stage открывает релевантный log segment и список файлов.

### 5. Cloud Drive End-To-End

Owner: Codex + Claude.

Codex:

- DONE 2026-05-14: проверен и покрыт regression-тестами reindex/cleanup job lifecycle для upload/move/rename/delete/restore. Move/rename ставит cleanup старого пути и reindex нового, delete ставит cleanup, restore ставит reindex.
- DONE 2026-05-14: S3/MinIO storage adapter проверен тестами: bucket init, healthcheck, presigned download, missing boto3 message, config validation. `boto3` добавлен в CI lock для воспроизводимого S3 smoke.
- DONE 2026-05-14: проверено, что Cloud Drive local storage/data не tracked в Git; `.gitignore` дополнен явными правилами `data/cloud_storage/`, `cloud_storage/`, `cloud_drive.db*`.
- DONE 2026-05-14: cleanup job удаляет Qdrant points по `cloud_file_id`/`cloud_path`; restore возвращает файл через reindex job. Остался release smoke на реальном Qdrant.

Claude:

- Довести admin Cloud Drive UI: progress, jobs, errors, storage warnings, S3/MinIO подсказки.
- Довести explorer Cloud Drive UX: actions, versions, trash, restore, conflict states.

Done criteria:

- сценарий upload -> reindex -> search -> download работает;
- move/rename не создают дубли в поиске;
- delete убирает результат из поиска, restore возвращает.

## P1 Release Polish

### 6. UI Stabilization Against Hi-Fi

Owner: Claude, Codex reviews.

- Header: привести desktop/tablet/mobile к `hi-fi-rag-search.html` без съезда элементов.
- Navigation: mobile menu всегда доступно, desktop nav не ломает width.
- Explorer: дерево с раскрытием/сворачиванием, корректным текущим путём и скроллом; table view без наложений.
- Index: pipeline rows одинаковой ширины, проценты вместо float, понятные statuses.
- Settings: опасные ops-настройки с предупреждениями и tooltips.

Done criteria:

- smoke screenshots для search/explorer/index/settings на 480, 900, 1280 px;
- нет горизонтального наложения и недоступных меню.

### 7. RAG Answer Hardening

Owner: Codex.

- Усилить verifier: числа, даты, суммы, веса, conflicting sources.
- При слабом evidence выводить “не нашёл подтверждения”, а не уверенный ответ.
- Добавить source provenance: файл, страница/лист/строка, chunk id.
- В Telegram явно отделить search result от generated answer.

Done criteria:

- тесты на unsupported/conflicting facts;
- UI показывает sources рядом с answer;
- Telegram не выдаёт unsupported facts как факт.

### 8. Documentation And Config Freeze

Owner: Codex.

- README оставить операционным, без длинной истории.
- DONE 2026-05-14: `config.example.json` и `config.docker.example.json` синхронизированы с `DEFAULT_CONFIG`; добавлен regression-тест на полноту ключей.
- Документировать launcher, Docker + MinIO, first-run admin, OCR deps, release checks.
- DONE 2026-05-14: `requirements-ci.lock` дополнен `qdrant-client==1.17.1`, чтобы launcher smoke не падал на `ModuleNotFoundError`.
- DONE 2026-05-14: README quick start теперь устанавливает пакет через `pip install -e .`; добавлен fallback запуска с `PYTHONPATH=src` для fresh checkout.
- DONE 2026-05-14: launcher `restart` ждёт освобождения web-порта после stop, чтобы не оставлять web down из-за race между остановкой процесса и проверкой 8080.
- DONE 2026-05-14: bot/web startup hardened against SQLite WAL setup failures on Windows/external drives.
- DONE 2026-05-14: launcher status больше не показывает последнюю INFO-строку как `bot.last_error`; fallback оставлен только для failed-to-start диагностики.
- Решить, нужен ли отдельный runtime lock.

Done criteria:

- fresh clone commands проходят по README;
- `python -m rag_catalog.cli.launcher --help` и Docker smoke green в CI.

## P2 После Первого Релиза

### 9. Document ACL / RBAC V2

Owner: Codex.

- Роли `viewer/editor/admin`.
- Folder/file ACL in Cloud Drive registry.
- Qdrant payload filter by allowed groups/path scopes.
- Audit report by user/action/document.

### 10. Structural Chunking V2

Owner: Codex.

- DOCX sections/tables.
- PDF page/block provenance.
- XLSX sheet/row/table chunks.
- Parent-child retrieval: child search, parent context for RAG.

### 11. Architecture Split

Owner: Codex + Claude.

- Дальше резать `nice_app.py` на feature modules.
- Вынести API/auth/cloud/search/index UI boundaries.
- Добавить mypy или pyright по постепенно расширяемому scope.

## Suggested Execution Order

1. Security hardening.
2. Indexing/OCR reliability.
3. Cloud Drive end-to-end.
4. Eval gate + retrieval preset.
5. UI stabilization.
6. Documentation/config freeze.
7. Release candidate smoke and tag.

## Release Candidate Checklist

- `git status --short` clean.
- `python -m pytest -q` green or documented non-release blockers fixed.
- `python -m ruff check src tests` green.
- `python -m rag_catalog.cli.launcher restart` starts web, Qdrant and bot as expected.
- `python scripts/search_eval.py --golden eval/search_golden.json --limit 10` passes agreed thresholds.
- Docker compose smoke: web + Qdrant + optional MinIO.
- Manual smoke: login, search, RAG answer, explorer, Cloud Drive upload/download/reindex, index start/stop/resume, Telegram search.
