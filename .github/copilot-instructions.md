# GitHub Copilot instructions

Repo-wide instructions for Copilot in this repository. This file is
deliberately thin: it points at the documents that own each subject rather
than restating them, following the same rule the rest of this repo's docs
follow (see `docs/README.md`). **The code wins over any document, including
this one.**

## Read these first

- **`AGENTS.md`** (repo root) — the actual entry point. What this repo is,
  the branch model, the non-negotiable rules, and which doc owns what.
  Copilot reads `AGENTS.md` natively; everything in it applies here.
- **`docs/agent_governance.md`** — the four approval tiers, the seven-agent
  roster, and the dispatch ladder.

## Approval tiers apply to Copilot exactly as they do to any other agent

Tiers bind **by action, not by role and not by substrate.** A deploy drafted
in a Copilot chat mode is exactly as Tier 2 as one drafted anywhere else.

- **Tier 0** — read, search, explain, run `./scripts/verify.sh`, draft on a
  branch, open a PR into `dev`.
- **Tier 1** — merging into `dev`; changes to `IngestionConfig`'s public
  shape, `ci.yml`, notebooks, or `resources/*.yml`; any suppression.
- **Tier 2** — staging/prod deploys, `GRANT`/`REVOKE`/`DROP`/`VACUUM`,
  `run_as` changes, promotion PRs. **Draft and stop.** These need Yash naming
  the specific action *before* it runs.
- **Tier 3** — credentials, IAM/RBAC, hotfixes to `main`, weakening a gate.
  Never autonomous, not even as a drafted executable step.

If it's unclear which tier something falls into, treat it as the higher tier
and ask. Green CI is a precondition for asking, never a substitute for asking.

## Path ownership

Three roles own the guarded paths, and each has a chat mode:

| Path | Chat mode |
| --- | --- |
| `bronze_layer/bronze_ingest/` | `builder` |
| `bronze_layer/notebooks/`, `tests/test_notebooks.py` | `notebook-qa` |
| `databricks.yml`, `bronze_layer/resources/*.yml` | `platform` (drafts only) |

`scripts/check_path_ownership.py` enforces this at commit and in CI. It is
not advisory — a commit touching a guarded path from the wrong role fails.

## Getting the chat modes

They aren't in this repo. The real agent definitions are proprietary and live
in a private config repo, pinned by `agents.lock`; `scripts/bootstrap_agents.sh`
fetches them and renders the Copilot-lane ones into the gitignored
`.github/chatmodes/`. Run it once after cloning:

```bash
export AGENT_CONFIG_TOKEN=<read-only token for the private config repo>
./scripts/bootstrap_agents.sh
```

Then pick the mode from the chat mode dropdown in VS Code. See
`.github/chatmodes/README.md` for what does and doesn't survive the
rendering.

## Before you say something is done

```bash
./scripts/verify.sh
```

It reports three outcomes per gate — PASS, FAIL, and **NOT RUN**. A NOT RUN
gate is not a pass. Say explicitly which gates could not run rather than
reporting a green you don't have; on Windows the Spark suite is CI-only, and
a 9-second CI "pass" is the change-detection gate skipping the suite, not a
run.
