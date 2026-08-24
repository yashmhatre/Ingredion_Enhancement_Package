# GitHub Copilot instructions

This file is intentionally short. It points to the authority docs instead of repeating them. The code wins over any document here, including this one.

## Read these first

- `AGENTS.md` — project rules, branch flow, approval rules, and the entry point for any coding agent
- `docs/README.md` — which document owns each subject

## Approval rules

These apply to Copilot exactly as they do to any other agent. See `AGENTS.md`'s
"Approval and risk rules" for the current list of actions that need Yash's
sign-off before they run. If a task could be higher risk than it looks, treat
it as higher risk and ask.

## Path ownership

`scripts/check_path_ownership.py` enforces path ownership (its `OWNERSHIP`
table) in CI and at commit time. It is not advisory.

## Chat modes

The real definitions live in a private repo and are pinned by `agents.lock`. `scripts/bootstrap_agents.sh` fetches them and renders the Copilot-lane files into the gitignored `.github/chatmodes/` directory.

```bash
export AGENT_CONFIG_TOKEN=<read-only token for the private config repo>
./scripts/bootstrap_agents.sh
```

Then pick the generated mode in VS Code.

## Writing gate

Use the local `.agents/skills/unslop` skill for every writing task, including docs, Markdown, comments, issue and PR text, release notes, summaries, prompts, and agent handoffs. This rule applies when Copilot writes for itself or on behalf of another agent.

- Use `cleanup` for audit-only review or suggestions.
- Use `rewrite` for requested cleanup or final prose.
- Read the skill's routed command file before acting.
- Preserve facts, code, names, dates, quantities, scope, uncertainty, attribution, and technical meaning.
- Validate the result using the skill contract and report any validator that could not run.

## Before saying a change is done

```bash
./scripts/verify.sh
```

This reports PASS, FAIL, and NOT RUN. A NOT RUN gate is not a pass. Say which gates could not run instead of claiming green status you do not have.
