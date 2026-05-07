# Agent Workflow

> Это вспомогательный документ. Авторитетный источник правил — `AGENTS.md` в корне проекта.
> При расхождении приоритет у `AGENTS.md`.

## Модель ветвления

`main` — единственная рабочая ветка. Небольшие валидированные этапы коммитятся напрямую в `main`.

Отдельная ветка (`claude/<task>` или `codex/<task>`) нужна **только** для:
- рискованных или долгосрочных изменений;
- работы, которая конфликтует с параллельными изменениями другого агента.

Не держите параллельные архитектурные эксперименты после принятия решения — архивируйте или удаляйте ветку.

## Worktree-расположение

| Путь | Владелец | Назначение |
|---|---|---|
| `D:\...\Semantic search\` | Интеграция | Основная копия; тесты, сборки, релизы |
| `.claude\worktrees\*` | Claude | Ветки Claude (только для долгих/рискованных задач) |
| `.codex\worktrees\*` | Codex | Ветки Codex (только для долгих/рискованных задач) |

Обе папки агентов перечислены в `.gitignore` и никогда не коммитятся.

## Начало сессии

```powershell
git fetch --all
git status --short
git log --oneline --graph --all --decorate -20
```

Если работа долгая/рискованная — создайте worktree:

```powershell
# Claude
git worktree add ".claude\worktrees\<task-name>" -b claude/<task-name>
# Codex
git worktree add ".codex\worktrees\<task-name>" -b codex/<task-name>
```

Для коротких этапов — работайте прямо в основном рабочем каталоге.

## Завершение этапа

```powershell
python -m pytest -q tests/   # тесты для затронутой области
git add <files>
git commit -m "..."
git push origin main          # или origin <branch>
```

## Синхронизация между агентами

```powershell
git fetch --all
git log --oneline --all -20

# Взять конкретный коммит
git cherry-pick <sha>

# Смержить ветку
git merge codex/<task-name>
git merge claude/<task-name>
```

## Ключевые правила

- Не редактируйте worktree другого агента.
- Перед началом проверяйте `git status` и `git fetch`.
- При наличии stash сразу документируйте причину и удаляйте после завершения.
- Не смешивайте runtime-данные, сгенерированные БД и изменения исходного кода в одном коммите.
- `docs/cloud_drive_roadmap.md` — source of truth по задачам Cloud Drive; обновляйте после каждого этапа.
- `WORKLOG.md` — для значимых передач между агентами и операционных заметок.
