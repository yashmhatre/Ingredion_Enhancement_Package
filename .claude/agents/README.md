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
