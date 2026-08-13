# .github/chatmodes/ — generated at fetch time, not committed

VS Code Copilot custom chat modes for this project's Copilot-lane agents.
Everything here except this README is written by
`scripts/generate_copilot_chatmodes.py` (invoked by
`scripts/bootstrap_agents.sh`) and is **gitignored**.

The real agent definitions are proprietary and live in the private
`yashmhatre/ingredion-agent-config` repo, pinned by `agents.lock`. `.github/`
is committed, so the same "never commit real agent-config content" rule that
covers `.claude/agents/*.md` covers this directory too — see
`docs/private_agent_architecture.md`.

## Setup

```bash
export AGENT_CONFIG_TOKEN=<read-only token for the private config repo>
./scripts/bootstrap_agents.sh
```

Re-run it whenever `agents.lock` changes. `scripts/verify.sh` will tell you
when your local copy has drifted from the pin.

## Which agents appear here

Only the ones whose definition declares the Copilot lane — today `builder`,
`notebook-qa`, and `platform`. Lane is read from the definition itself rather
than configured here, so a role that changes lane upstream changes what gets
generated with no edit to this repo.

`orchestrator`, `architect`, and `reviewer` run in the Claude lane; `scout`
runs on a local model. They are not rendered as chat modes.

## What doesn't survive the rendering

Copilot chat modes are not Claude subagents, and three things are genuinely
lost rather than approximated:

- **Delegation.** `Agent(...)` grants have no equivalent — a chat mode cannot
  spawn another chat mode. Each generated mode says so in its header. Where
  its instructions say to hand work to another role, it should ask you to
  switch modes, not do that role's work itself.
- **`maxTurns`.** No equivalent. Costs wall-clock time rather than budget:
  in agent mode one *user prompt* is one premium request regardless of how
  many tool calls happen inside it.
- **The PreToolUse write-lockdown hook.** Copilot has no such mechanism.
  This is why path ownership is enforced by `scripts/check_path_ownership.py`
  at commit time and in CI instead — deterministic, and it works for every
  substrate including a human editing by hand.

Tool grants are mapped, not dropped: `Read` → `codebase`, `Grep`/`Glob` →
`search`, `Edit`/`Write` → `editFiles`, `Bash` → `runCommands`, `WebSearch` →
`fetch`.

## Models

No `model:` line is emitted. The frontmatter takes an identifier that must
match Copilot's picker exactly, and that list varies by plan and changes over
time — emitting a wrong one silently breaks the mode. Each generated file
carries a recommendation in its header instead; pick from the real list.

On Copilot Pro the useful rule is that the base model is **0x — unlimited**,
so it costs nothing against the premium allowance. Running out of premium
requests doesn't stop Copilot; it drops you to that base model.
