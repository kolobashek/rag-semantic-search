# Agent Workflow

## Worktree Layout

| Location | Owner | Purpose |
|---|---|---|
| `D:\...\Semantic search\` | Integration | Stable copy, tests, builds, releases |
| `.claude\worktrees\*` | Claude | Claude's working branches |
| `.codex\worktrees\*` | Codex | Codex's working branches |

Both agent directories are in `.gitignore` — they are never committed.

## Rules

- Agents do **not** edit each other's worktrees.
- Agents do **not** commit directly to `main` (except deliberate integration merges approved by the user).
- Every agent commits to its own branch: `claude/<task>` or `codex/<task>`.
- Integration happens through `git merge` or `git cherry-pick` into `main`.
- The main working directory is used for integration, running tests, building, and verifying — not for parallel development.

## Starting a Session

```powershell
git fetch --all
git log --oneline --graph --all --decorate -20
git status
```

Create a fresh worktree from latest main:

```powershell
# Claude
git worktree add ".claude\worktrees\<task-name>" -b claude/<task-name>

# Codex
git worktree add ".codex\worktrees\<task-name>" -b codex/<task-name>
```

## Finishing a Session

```powershell
python -m pytest -q tests/    # run tests
git add <files>
git commit -m "..."
git push origin <branch>
```

Then open a PR or ask the user to merge.

## Seeing Each Other's Changes

```powershell
git fetch --all
git log --oneline --all -20

# Take a specific commit
git cherry-pick <sha>

# Merge a branch
git merge codex/<task-name>
git merge claude/<task-name>
```

## Why Not Work in the Same Directory

- Simultaneous edits produce a shared dirty state — impossible to tell whose change is whose.
- No clean per-agent commit trail.
- Risk of one agent overwriting the other's uncommitted work.
