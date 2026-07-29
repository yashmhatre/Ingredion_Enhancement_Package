# Contributing to the Ingredion Enhancement Project

Thanks for your interest in contributing! This document covers everything
you need to get set up locally, find something to work on, and submit
changes.

---

## Table of Contents

- [Ways to Contribute](#ways-to-contribute)
- [Local Development Setup](#local-development-setup)
- [Finding Something to Work On](#finding-something-to-work-on)
- [Claiming an Issue](#claiming-an-issue)
- [Branches and the promotion flow](#branches-and-the-promotion-flow)
- [Commit Message Guidelines](#commit-message-guidelines)
- [Making Changes](#making-changes)
- [Testing Requirements](#testing-requirements)
- [Documentation Expectations](#documentation-expectations)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Code Review Process](#code-review-process)
- [Questions](#questions)

---

## Ways to Contribute

- Pick up an open [issue](../../issues) labeled `good first issue` or `help wanted`
- Report bugs by opening a new issue (use the **Task** or **Feature Request** template)
- Improve documentation (README, code comments, docs/ folder, this file)
- Add or improve test coverage
- Propose new features by opening a **Feature Request** issue before starting work

## Local Development Setup

```bash
# 1. Clone the repo
git clone https://github.com/yashmhatre/Ingredion_Enhancement_Package.git
cd Ingredion_Enhancement_Package/bronze_layer

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows

# 3. Install dependencies (includes pyspark, delta-spark, pytest and build
#    via the dev extra). `build` is what `databricks bundle deploy` uses to
#    package the wheel locally before uploading it - without it a deploy
#    fails with "No module named build" before reaching the workspace.
pip install -e ".[dev]"

# 4. Run tests to confirm your setup works
pytest
```

If you're setting up the Azure/Databricks environment from scratch
(storage account, Unity Catalog, external volumes), see
[azure_setup.md](../azure_setup.md) at the repo root — a validated,
step-by-step runbook.

## Finding Something to Work On

1. Go to the [Issues](../../issues) tab
2. Filter by label:
   - `good first issue` — small, well-scoped, low context needed
   - `help wanted` — actively looking for contributors
   - `bug` / `enhancement` / `testing` — component-based filtering
3. Check the [Project board](../../projects) — issues in the **Ready**
   column are scoped and available; issues in **Backlog** may still need
   refinement before starting

## Claiming an Issue

- Comment on the issue (e.g., "I'd like to take this on") before starting work, to avoid duplicate effort
- If it's not assigned to you within a day or two, feel free to self-assign
- If you start an issue but can't finish it, leave a comment so someone else can pick it up

## Branches and the promotion flow

Three long-lived branches, one per deployment environment:

| Branch | Environment | Unity Catalog catalog | Deployed as |
|---|---|---|---|
| `dev` | development | `ingredion_en_dev` | the deploying user |
| `staging` | pre-production | `ingredion_en_staging` | staging service principal |
| `main` | **production** | `ingredion_en_prod` | prod service principal |

Changes flow one way, and every arrow is a pull request:

```
feature/*  →  dev  →  staging  →  main (prod)
```

**Branch your work off `dev`, and open your PR against `dev`** — not `main`.
`main` is production; nothing lands there except a promotion from `staging`,
or an urgent hotfix (see below).

Use a short, descriptive branch name prefixed by type:

```
feature/<short-description>     e.g. feature/multi-format-reader
fix/<short-description>         e.g. fix/jsonl-discovery
test/<short-description>        e.g. test/json-reader-validation
docs/<short-description>        e.g. docs/update-readme
refactor/<short-description>    e.g. refactor/rename-bronze-layer
```

### Hotfixes

A fix that unblocks production may go straight to `main`. It must then be
**back-merged into `dev` and `staging` immediately**, or those branches
silently carry the bug and will reintroduce it at the next promotion. This
is the known cost of environment branches: they drift whenever `main` moves
independently, and nothing detects that automatically.

## Commit Message Guidelines

Keep commits focused and descriptive:

```
<type>: <short summary>

<optional longer description>
```

Types: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`

Example:
```
feat: retry-limit before quarantining permanently-failing files

Files that fail ingestion (not just the archival move) are now tracked
across runs and quarantined after N consecutive failures, instead of
retrying forever with no signal that a human needs to intervene.
```

## Making Changes

1. Create a branch off `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. **Open or reference a task/issue before starting significant work** —
   this project follows a task-first workflow: draft the issue (context,
   what needs to be done, acceptance criteria), then resolve it
   incrementally, one file/step at a time, validating before moving on.
   For anything beyond a trivial fix, this saves rework and keeps intent
   traceable.
3. Make your changes, keeping them scoped to the linked issue
4. Follow existing code style/conventions in the repo — in particular:
   - Config additions to `IngestionConfig` should be additive with sane
     defaults (never break existing config files)
   - New reader/writer logic should follow the existing dual-environment
     pattern (`dbutils` when available, plain Python fallback otherwise —
     see `directory_ingestion.py`'s `_try_dbutils_ls` / `_try_posix_ls`
     for the pattern to follow)
5. Add or update tests for any new behavior
6. Update documentation (README, docstrings, `docs/` folder) if your
   change affects usage, setup, or discovered a non-obvious gotcha others
   are likely to hit

## Testing Requirements

This project has two layers of testing — know which one your change needs:

**1. Local pytest suite** (`bronze_layer/tests/`)
```bash
cd bronze_layer
pytest
```
Covers config validation, flatten/quality logic, directory ingestion,
file archival, retry-limit behavior, folder-as-table, and the run-level
audit trail. Uses a local, Delta-enabled `SparkSession` — no Databricks
connection needed, and the suite is also environment-aware enough to run
directly on a Databricks cluster if needed (see `conftest.py`'s `spark`
and `json_test_dir` fixtures for how that works).

**2. Real-environment validation** (`bronze_layer/docs/` + associated
notebooks), needed when a change touches:
- Reading behavior against real cloud storage (see
  `notebooks/validate_json_reader.py` and
  [testing_json_reader.md](docs/testing_json_reader.md))
- Deployment or job configuration (`databricks.yml`, notebook entrypoints)
  — see [testing_end_to_end_deployment.md](docs/testing_end_to_end_deployment.md)
  for the kind of validation expected before considering a deployment
  change done

**Before opening a PR:**
- All local pytest tests pass
- If your change affects deployment or real-storage behavior, note what
  real-environment validation (if any) was done in the PR description
- If you hit a genuine environment gotcha (stale config value, path
  mismatch, unexpected Spark behavior) while testing, document it — see
  below

## Documentation Expectations

If you discover something non-obvious while building or testing a
change — a Spark behavior that surprised you, an environment
misconfiguration, a bug in test infrastructure itself — write it down.
The `docs/` folder's testing files are full of exactly this kind of
finding, and they've repeatedly saved the next person from repeating the
same debugging session. A good entry includes: what was expected, what
actually happened, why, and how it was resolved.

## Submitting a Pull Request

1. Push your branch:
   ```bash
   git push origin feature/your-feature-name
   ```
2. Open a PR against **`dev`** — the PR template will auto-populate; fill it out completely.
   (Only promotions and hotfixes target `staging` or `main`.)
3. Link the related issue using `Closes #<issue-number>` in the PR description
4. CI runs automatically via GitHub Actions on every PR
   (`.github/workflows/ci.yml`) — two required checks:
   - **`test`** — the full pytest suite
   - **`wheel`** — the package builds, ships no test/notebook/config files,
     reports a version matching its filename, and imports standalone
   CI runs on PRs into `dev`, `staging` and `main`, so each gate in the
   promotion chain is checked rather than only the last one.
5. Request review — at least 1 approval required before merging into `main`

## Code Review Process

- At least one approving review is required before merging
- Address review comments with additional commits rather than force-pushing, unless asked to squash
- Once approved and CI passes, a maintainer will merge the PR

## Questions

- Open a [Discussion](../../discussions) if enabled, or comment directly on the relevant issue
- For anything unclear about expected behavior/schema, ask before implementing — it saves rework on both sides