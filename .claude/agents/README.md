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

## Current org structure

See `docs/agent_governance.md`'s "Agent org chart" section for who reports
to whom and what each role does. Short version: Yash (Principal Data
Engineer) → Business Analyst + Solution Architect (jointly overseeing
Data Engineer, QA Engineer, Data Analyst) and DevOps Lead (overseeing
DevOps Engineer, Platform Engineer).
