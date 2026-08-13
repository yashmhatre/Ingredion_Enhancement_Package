# .claude/agents/ — populated at fetch time, not committed

The real agent definitions (prompts, tool grants, workflow instructions) for
this project are proprietary and live in the private
`yashmhatre/ingredion-agent-config` repo, not here. This directory is
gitignored except for this README — everything else in it is written by
`scripts/bootstrap_agents.sh` and must never be committed.

## First-time setup (local dev)

```bash
export AGENT_CONFIG_TOKEN=<a token scoped to read-only access on ingredion-agent-config>
./scripts/bootstrap_agents.sh
```

This reads the pinned version from `agents.lock` at the repo root and
fetches exactly that version's agent files into this directory. Re-run it
any time `agents.lock` changes.

## CI

The same script runs as a setup step using a CI-scoped credential (a
fine-grained PAT or deploy key with read-only access to the one private
repo, stored as a repo/environment secret — never a broad personal token).

## Why this exists

See `docs/private_agent_architecture.md` for the full comparison of options
and why this project uses a private authoring repo + a pinned, versioned
fetch instead of committing agent content directly here, encrypted or not.

## Current team

See `docs/agent_governance.md`'s "The team" section for the full table, the
lanes, and the dispatch ladder. Short version: Yash (Senior Principal Data
Engineer) signs off and does not route or implement. `orchestrator` routes
and merges all Tier 1 work into `dev`; `architect` designs; `reviewer` reviews
the escalation paths; `builder`, `notebook-qa`, and `platform` implement;
`scout` produces cheap briefs on a local model. **Seven subagents in total**,
pinned at `agents.lock`'s current version.

Merging into `dev` is the agent team's authority. Tier 2/3 actions
(staging/prod deploys, GRANT/DROP/VACUUM, credentials, and the promotion PRs
`dev` → `staging` and `staging` → `main`) require Yash's own named sign-off —
that authority sits with no agent.
