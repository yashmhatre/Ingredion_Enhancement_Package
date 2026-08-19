# .claude/agents/

This directory is not the source of truth for the repo's agent definitions. The real prompt content and tool grants live in the private `ingredion-agent-config` repo and are fetched here by `scripts/bootstrap_agents.sh`.

This directory is gitignored except for this README. Do not commit real agent files here.

## Local setup

```bash
export AGENT_CONFIG_TOKEN=<read-only token for ingredion-agent-config>
./scripts/bootstrap_agents.sh
```

This reads the pinned version from `agents.lock` and fetches that exact version into `.claude/agents/`.

## CI

CI uses a read-only token for the same private repo. It does not use a broad personal token.

## Why this exists

See `docs/private_agent_architecture.md` for the reasoning. This repo keeps agent prompt content out of git and pins the fetch to a known version instead of copying secrets or proprietary instructions into the repo.

## Current team

The current roster is defined in `docs/agent_governance.md`.

Short version:

- Yash signs off and does not implement
- `orchestrator` routes work and merges Tier 1 changes into `dev`
- `architect` handles business reconciliation and design
- `reviewer` handles escalation-path review
- `builder`, `notebook-qa`, and `platform` implement
- `scout` produces brief summaries only

Tier 2 and Tier 3 actions still require Yash's explicit named sign-off.
