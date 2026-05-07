# Worklog

## Current Operating Mode

- Primary catalog: `D:\Docs\Claude\Projects\Semantic search`
- Canonical branch: `main`
- Current collaboration model: short validated stages committed and pushed directly to `main`
- Cloud Drive source of truth: `docs/cloud_drive_roadmap.md`

## Agent Ownership

- Codex: backend, APIs, data model, jobs, storage contracts, auth, launcher, tests, CI, index/search/OCR integration.
- Claude: NiceGUI UX, explorer/search screens, admin/user settings, product flows, client-side sync UX.

## Runtime State

- Runtime DBs live under `data/`.
- Launcher/runtime state lives under `runtime/`.
- These folders are intentionally not committed.

## Archived State

- Old `pre-sync` stash was archived into branch `archive/pre-sync-stash` and removed from `git stash`.
- `main.lock.bak` was removed because its commit is already contained in `main`.

## Update Template

Append this block only for significant handoffs, migrations, or operational state:

```text
Date:
Agent:
Branch:
Base commit:
Task:
Status:
Validation:
Notes:
```

## Recent Operational Notes

```text
Date: 2026-05-06
Agent: Codex
Branch: main
Task: move runtime SQLite/state from O:\qdrant_db to project data directory
Status: completed
Validation: launcher restart; /index smoke; state totals verified after copy
Notes: app still uses O:\Обмен as catalog source, but telemetry/users/index_state DBs are local under data/
```

```text
Date: 2026-05-07
Agent: Codex + Claude
Branch: main
Task: Cloud Drive foundation, API, admin UI, explorer UI, file actions, jobs/status, auth hooks
Status: in progress according to docs/cloud_drive_roadmap.md
Validation: focused Cloud Drive tests per stage; launcher restart smoke after backend/API changes
Notes: remaining P0 is end-to-end Cloud Drive reindex -> index/search pipeline
```

```text
Date: 2026-05-07
Agent: Claude
Branch: main
Task: docs actualization — ИНСТРУКЦИЯ.md и docs/AGENT_WORKFLOW.md
Status: completed
Validation: n/a (doc-only)
Notes: ИНСТРУКЦИЯ.md переписана под текущий стек (NiceGUI launcher, data/, staged indexing, Cloud Drive).
       docs/AGENT_WORKFLOW.md приведён в соответствие с AGENTS.md (модель direct-to-main).
```
