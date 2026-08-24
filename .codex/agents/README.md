# `.codex/agents/` — generated from the private agent config

Codex project-scoped custom agents are generated here by
`scripts/bootstrap_agents.sh`. The seven roles and their prompt bodies come
from the same private, `agents.lock`-pinned source used for Claude Code; only
this README is committed.

Run the bootstrap from the repository root after cloning or whenever
`agents.lock` changes:

```bash
export AGENT_CONFIG_TOKEN=<read-only token for ingredion-agent-config>
./scripts/bootstrap_agents.sh
```

The generated TOML files are proprietary and gitignored. `AGENTS.md` remains
the public source of truth for ownership and approval rules.
