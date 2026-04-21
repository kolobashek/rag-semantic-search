# Docker

Контейнерный запуск поднимает три роли:

- `qdrant` - векторная база.
- `web` - NiceGUI-интерфейс на `http://localhost:8080`.
- `bot` - Telegram-бот, запускается отдельным profile.

## Подготовка

Создайте локальный конфиг из шаблона:

```powershell
Copy-Item config.docker.example.json config.docker.json
```

В `config.docker.json` оставьте контейнерные пути:

```json
{
  "catalog_path": "/data/catalog",
  "qdrant_db_path": "/data/state",
  "qdrant_url": "http://qdrant:6333"
}
```

Для Telegram заполните `telegram_bot_token` и `telegram_bot_link`.
`config.docker.json` содержит секреты и не должен попадать в Git.

Если каталог находится не в `O:\Обмен`, задайте путь перед запуском:

```powershell
$env:RAG_CATALOG_PATH = "D:/Docs/Catalog"
```

Для Docker Desktop на Windows диск с каталогом должен быть доступен Docker.

## Запуск web и Qdrant

```powershell
docker compose up -d --build qdrant web
```

Откройте `http://localhost:8080`.

## Запуск Telegram-бота

Перед запуском контейнерного бота остановите локальный `telegram_bot.py`.
Telegram long polling не должен работать в двух процессах с одним токеном.

```powershell
docker compose --profile bot up -d --build bot
```

## Индексация из контейнера

```powershell
docker compose --profile tools run --rm indexer
```

Важно: если раньше индекс строился на Windows с путями вида `O:\...`, для полноценной работы скачивания и просмотра файлов в контейнере лучше переиндексировать каталог из контейнера. Тогда в Qdrant будут пути вида `/data/catalog/...`.

Если нужно полностью пересобрать коллекцию:

```powershell
docker compose --profile tools run --rm indexer rag-index --url http://qdrant:6333 --recreate
```

## Проверка

```powershell
docker compose ps
docker compose logs -f web
docker compose logs -f bot
```

Остановить:

```powershell
docker compose down
```
