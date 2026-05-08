# Agent Workflow

This repository is shared by Codex, Claude, and the user. These rules apply to the whole repo unless a deeper `AGENTS.md` overrides them.

## Operating Model

- `main` is the canonical working line for this project.
- Small validated stages may be committed directly to `main` in the primary catalog.
- Use a separate `codex/<task>` or `claude/<task>` branch only for risky, long-running, or conflicting work.
- Do not keep parallel architecture experiments alive after a decision is made; archive or delete them.
- Completed roadmap/history lives in Git. Create a new roadmap only for a new large delivery phase; do not keep stale task lists as permanent docs.

## Agent Ownership

- Codex owns backend/system work by default:
  - data model and migrations;
  - storage contracts;
  - API endpoints;
  - auth, jobs, recovery, launcher;
  - index/search/OCR integration;
  - tests and CI.
- Claude owns product/UI work by default:
  - NiceGUI screens and workflows;
  - explorer/search UX;
  - admin/user settings;
  - visual states and copy;
  - client-side sync UX.
- If one agent touches the other agent's area, keep the change narrow and mention it in the commit message or final handoff.

## Commit Discipline

- Commit after each coherent stage.
- Push `main` after a successful stage unless the user explicitly asks to keep changes local.
- Keep commits focused; do not mix runtime data, generated DBs, and source changes.
- Do not commit `data/`, `runtime/`, logs, tokens, local DB WAL/SHM files, storage objects, or machine-specific caches.
- Update `README.md` only for user-facing operational changes, not for every internal refactor.
- Keep markdown lean. Prefer updating `README.md` or this file over creating another `.md`.

## Sync Discipline

- Before starting work, run at least:
  - `git status --short`
  - `git fetch`
  - compare local `main` with `origin/main` when needed.
- Prefer fast-forward/pull only when the worktree is clean or pending changes are understood.
- Never overwrite another agent's uncommitted work.
- If a stash is created, immediately record why it exists and remove or archive it before ending the cleanup.

## Validation

- Run focused tests for the changed area before each commit.
- For Cloud Drive API/backend changes, prefer targeted tests in:
  - `tests/test_cloud_drive_registry.py`
  - `tests/test_cloud_drive_storage.py`
  - `tests/test_cloud_drive_cli.py`
  - `tests/test_nice_app_explorer.py`
- Run `py_compile` for changed Python entrypoints/modules.
- If full `pytest` has known unrelated failures, state that explicitly and run the relevant focused subset.

## Launcher and Runtime

- Use the launcher for local stack control:
  - `python -m rag_catalog.cli.launcher status`
  - `python -m rag_catalog.cli.launcher restart`
- The primary catalog is `D:\Docs\Claude\Projects\Semantic search`.
- Runtime artifacts are local state, not source:
  - `data/`
  - `runtime/`
  - `logs/`

## Handoff Notes

Use commit messages and final responses for normal handoff. Create a new temporary markdown handoff only when it is genuinely needed for a long migration or incident, and delete or fold it into `README.md` when the work is complete.

Record only facts that help the next agent continue:

- current branch/commit;
- active service state;
- known test failures;
- pending blockers;
- archived branches/stashes.
