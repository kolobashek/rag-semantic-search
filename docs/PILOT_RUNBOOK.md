# Paid Dedicated Pilot Runbook

Статус: рабочий runbook для первого выделенного клиентского контура.

## Границы Пилота

- Один клиент и один выделенный deployment contour.
- Данные клиента, registry, users, telemetry, object storage и search index не разделяются с другими клиентами.
- Public links выключены по умолчанию и включаются только письменным решением владельца данных.
- В pilot входят Cloud Drive, поиск, preview/download, версии, корзина, sharing, groups/ACL, index jobs и admin operations.
- Не входят: multi-tenant SaaS, online billing, SSO/SCIM, legal hold, мобильный sync и гарантированный cross-region DR.
- Команда поддержки не открывает содержимое документов без согласованного incident access; support bundle должен быть redacted.

## Ответственные

| Роль | Ответственность |
|---|---|
| Data owner клиента | Разрешает источники, группы, public-link policy, retention и incident access |
| Pilot admin клиента | Users/groups/ACL, import sources, jobs, audit review |
| Service operator | Install/update, backup, restore, health, capacity, incident response |
| Product owner | Acceptance criteria, known limitations, go/no-go пилота |

До запуска должны быть назначены люди на все четыре роли и указан аварийный канал связи.

## До Установки

1. Зафиксировать hostname, storage backend, объём данных, окно индексации и hardware profile.
2. Определить RPO/RTO. Внутренний pilot target: RPO <= 24 часа, RTO <= 8 часов.
3. Согласовать список import sources и владельца каждого источника.
4. Согласовать user/group matrix и проверить, что default access не шире требуемого.
5. Сохранить credentials вне Git и передавать их отдельным защищённым каналом.
6. Выполнить:

```powershell
python -m rag_catalog.cli.cloud_drive preflight --mode fresh-install --min-free-gb 10
python -m rag_catalog.cli.launcher start
python -m rag_catalog.cli.launcher status
```

Preflight и launcher status должны завершиться без failed checks. Исключения оформляются как известный риск с владельцем и сроком.

## Первичная Настройка

1. Создать отдельного admin и минимум одного обычного пользователя; сменить временные пароли.
2. Создать группы до выдачи массовых path grants.
3. Выдать минимальные `viewer/editor/admin` права и выполнить негативную проверку закрытого пути.
4. Зарегистрировать import sources. Первый импорт запускать с ограничением количества файлов.
5. Сверить registry/storage/index coverage до полного bootstrap.
6. Включать public links только после проверки expiration/revoke и audit.
7. Записать текущий retrieval preset, embedding model, collection и hardware profile.

## Обязательная Приёмка

Обычный пользователь без помощи администратора:

- входит и восстанавливает session после краткого reconnect;
- загружает файл и видит понятный busy/loading state;
- находит файл по имени, точному номеру и содержимому;
- открывает preview и скачивает исходник;
- создаёт новую версию и видит историю;
- делится с пользователем или группой;
- удаляет файл в корзину и восстанавливает его;
- не видит и не находит закрытый ACL path.

Администратор:

- создаёт/архивирует группу и меняет membership;
- выдаёт и отзывает permission;
- создаёт и отзывает expiring public link, если policy включена;
- видит success и denied audit events с correlation ID;
- видит storage health, index coverage, queue/jobs и backup freshness;
- экспортирует redacted diagnostic bundle без credentials и содержимого документов.

Оператор:

```powershell
python -m rag_catalog.cli.cloud_drive backup --output runtime/backups/pilot-acceptance.zip
python -m rag_catalog.cli.cloud_drive verify-backup runtime/backups/pilot-acceptance.zip
python -m rag_catalog.cli.cloud_drive restore-drill runtime/backups/pilot-acceptance.zip
python -m rag_catalog.cli.cloud_drive preflight --mode upgrade --backup-dir runtime/backups
```

Local storage pilot не принимается без `restore-drill` artifact. Для S3/MinIO отдельно прикладывается успешная provider-native object restore procedure.

## Release Gate

Перед обновлением:

1. Убедиться, что нет незавершённого destructive job или schema migration.
2. Создать и проверить свежий backup.
3. Запустить upgrade preflight.
4. Зафиксировать текущий commit/image, schema versions и rollback target.
5. Выполнить focused tests и smoke основных маршрутов.
6. Обновить один contour, проверить login/search/preview/download/write/audit.
7. При ACL leak, hard reload, потере search state, повреждении DB или failed restore остановить rollout.

Исполняемый evidence gate:

```powershell
python scripts/pilot_ui_smoke.py
python -m rag_catalog.cli.pilot_gate --write-signoff-template
python -m rag_catalog.cli.pilot_gate --run-tests `
  --retrieval-artifact runtime/eval/retrieval-v3-pilot.json
```

`pilot_gate` обязан вернуть `GO`. `NO_GO` нельзя переопределять устным решением: отсутствующий retrieval ground truth, failed UI/ACL/audit smoke, stale restore или незаполненный sign-off остаются блокерами до появления нового artifact.

Rollback означает возврат предыдущего кода плюс восстановление совместимого state. Простое переключение Git commit не считается rollback procedure для необратимой migration.

## Инциденты

| Severity | Пример | Первая реакция | Цель обновлений статуса |
|---|---|---:|---:|
| SEV-1 | ACL leak, потеря/повреждение документов, полный outage | 15 мин | каждые 30 мин |
| SEV-2 | Search/preview недоступен, массовые ошибки jobs, backup stale | 1 час | каждые 2 часа |
| SEV-3 | Частичная деградация или UX defect с workaround | 1 рабочий день | ежедневно |

Первые действия:

1. Зафиксировать время, contour, пользователя, correlation ID и действие.
2. Не перезапускать циклически сервис до сохранения log/support evidence.
3. При подозрении на DB/WAL повреждение остановить writers и сделать incident copy до recovery.
4. При ACL finding отключить sharing/public links и проверить все access paths.
5. После восстановления выполнить smoke и записать data-loss window относительно RPO.

## Завершение Пилота

Пилот готов к коммерческому решению, когда acceptance checklist подписан клиентом, есть минимум один успешный update и restore drill, retrieval thresholds подтверждены на размеченном наборе, а открытые SEV-1/SEV-2 отсутствуют.

При выходе клиента оператор предоставляет согласованный export, отзывает credentials/public links/sessions и удаляет данные только после письменного подтверждения data owner. Факт удаления и перечень затронутых stores фиксируются в audit/handoff.
