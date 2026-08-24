# AGENTS.md

This file is the top-level operating guide for AI coding agents in this repo. Read it before making changes. The code wins over any doc, and the current owning doc wins over a duplicate or older note.

## Scope

This repo is the Ingredion ELT package for Databricks + Unity Catalog.

- `bronze_layer/` is the live, deployed, tested layer.
- `silver_layer/` is not implemented; treat it as non-working code.
- `gold_layer/` does not exist yet.

If a task assumes Silver or Gold behavior, stop and validate it against `docs/roadmap.md` and the current issue state before coding.

`docs/archive/` and `bronze_layer/docs/archive/` are historical context only. Read the banner, then use the current owning doc instead of inferring behavior from archived material.

## Required workflow

- Work inside the correct owner and path boundaries.
- Keep changes small, explicit, and traceable.
- Prefer the current doc owner over creating a second copy.
- Preserve compatibility, defaults, and user-facing behavior unless the task says otherwise.
- Add or update tests for behavior changes.
- Verify the source of truth before proceeding.

## Approval and risk rules

- Deployments to staging or prod require explicit human approval.
- `GRANT`, `REVOKE`, `DROP`, and `VACUUM` are controlled actions, not routine edits.
- Credential handling and promotion PRs require named sign-off.
- Do not edit `databricks.yml` or Azure deployment config without reading `azure_setup.md` and the deployment section in `bronze_layer/README.md` first.
- Yash signs off on staging/prod deploys, credentials, GRANT/REVOKE/DROP/VACUUM actions, and promotion PRs. He does not implement in the normal path.

## Repo rules

- `docs/README.md` is the map of ownership; use it before creating new docs.
- `docs/roadmap.md` is the sequencing source of truth; check it before reordering work.
- Use the branch flow `feature/*` -> `dev` -> `staging` -> `main`.
- Commit format: `<type>: <short summary>`, where `type` is one of `feat`, `fix`, `test`, `docs`, `refactor`, or `chore`.
- Do not add flattening or reshaping logic in `bronze_layer`; nested JSON stays nested by design.
- Config changes must stay additive and backward-compatible.
- Suppressions need a clear reason and a narrow scope. Do not widen the net to hide a new issue.
- Tests are required for behavior changes, including notebook changes.

## Writing gate: unslop

Use the local `unslop` skill for every agent-authored writing task: Markdown, docs, comments, issue or PR text, release notes, summaries, agent instructions, prompts, and generated handoff text. This applies to every agent and subagent, including Claude, Copilot, and local-model lanes.

Before returning or committing prose:

1. Read `.agents/skills/unslop/SKILL.md` and the routed command file.
2. Use `cleanup` when the user asked for review or suggestions without a rewrite.
3. Use `rewrite` when the user asked to improve, clean up, humanize, or finalize text.
4. Use `teach` only when building a reusable voice profile.
5. Preserve facts, names, dates, quantities, code, citations, scope, uncertainty, attribution, and technical meaning.
6. Validate the result using the unslop contract. If the skill's validator cannot run, report that instead of claiming the prose passed.

Do not silently rewrite text when the user asked for an audit. Prefer a no-op when a defect is uncertain. Keep the final prose direct, factual, and specific.

## Source-of-truth docs

Use these before making decisions:

- `docs/README.md` for ownership and the doc map
- `docs/roadmap.md` for sequencing and priorities
- `docs/building_an_agent_team.md` for how the agent roster gets built and scoped
- `CONTRIBUTING.md` for dev setup and repo-specific expectations
- `azure_setup.md` before changing cloud or deployment assumptions

## Verification before completion

Do not claim a task is complete without fresh evidence.

Run the smallest relevant verification for the touched area, including:

- `pytest` for behavior changes
- `ruff format --check` and `ruff check` for Python style
- `mypy` for typed code paths
- `bandit` and `pip-audit` when relevant
- notebook checks when notebook behavior changes

If a verification step cannot run, say so explicitly and do not report it as green.

The code wins. The repo rules win. The prose stays direct and truthful.
