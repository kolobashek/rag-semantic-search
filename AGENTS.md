# Agent Workflow

This repository is used by multiple coding agents. Follow these rules for the whole repo unless a deeper `AGENTS.md` overrides them.

## Branch discipline

- Never work directly on `main`.
- Claude uses branches named `claude/<task>`.
- Codex uses branches named `codex/<task>`.
- Start every task from the current `main` unless the user explicitly asks otherwise.
- Merge into `main` only after the task is validated and the user asks to sync or merge.

## Worktree discipline

- Prefer one worktree per active branch.
- Do not reuse a worktree that is already attached to another branch if that risks hidden local state.
- If `main` is checked out in the primary catalog, do not force-switch it for task work; use a separate worktree branch instead.

## Logging and handoff

- Update `WORKLOG.md` when you start or finish a significant task.
- Record: agent, branch, base commit, short task summary, current status.
- If you stash local changes during sync, record the stash name in `WORKLOG.md`.

## Git hygiene

- Keep commits focused and readable.
- Do not rewrite another agent's history unless the user explicitly requests it.
- Prefer merge or cherry-pick over manual file copying between branches.
- Before syncing the primary catalog, inspect `git status`, `git branch --all`, and a short graph log.

## Validation

- Run the most relevant tests for the changed area before merging.
- If full test suite is feasible, prefer running it before syncing `main`.
