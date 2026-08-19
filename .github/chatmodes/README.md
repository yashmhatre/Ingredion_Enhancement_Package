# .github/chatmodes/

This directory is generated from the private agent config and is not the source of truth. It is gitignored, and only this README is committed.

Use `scripts/bootstrap_agents.sh` to regenerate it whenever `agents.lock` changes.

## Setup

```bash
export AGENT_CONFIG_TOKEN=<read-only token for the private config repo>
./scripts/bootstrap_agents.sh
```

## Which roles appear here

Only Copilot-lane roles are rendered here. Today that is `builder`, `notebook-qa`, and `platform`.

`orchestrator`, `architect`, and `reviewer` run in the Claude lane; `scout` runs on a local model. They are not rendered here.

## Important limits

Copilot chat modes are not Claude subagents, and some features do not carry over:

- delegation is not equivalent to `Agent(...)`
- `maxTurns` does not exist as a direct equivalent
- the PreToolUse write-lockdown hook is not available

Because of that, path ownership is enforced by `scripts/check_path_ownership.py` in CI and at commit time, not by the mode itself.

## Models

The generated frontmatter does not hard-code a model. The model name must match the Copilot picker exactly, and that list varies by plan. The generated file carries a recommendation instead of a fixed value.

The base model is effectively the low-cost fallback path when premium requests run out.
