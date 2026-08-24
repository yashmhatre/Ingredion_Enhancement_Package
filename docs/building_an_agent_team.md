# Building an agent team from scratch

`docs/agent_governance.md` is gone. It described a seven-role org chart with
four approval tiers, written by an AI and never fully driven by hand. This
doc replaces it with something you build yourself, one piece at a time, and
can read end to end in a sitting.

Nothing here is binding until you write it down somewhere an agent actually
reads (`AGENTS.md`, a `.claude/agents/*.md` file, `CODEOWNERS`). A rule that
exists only in a doc no agent loads is not a rule.

## 1. Start with one agent, not a roster

Don't design the org chart first. Pick the single task you do most often and
wish you could hand off — reviewing a PR, drafting a notebook test, triaging
issues — and build one agent for that. Add a second agent only once the
first one has done real work and you know what it's missing.

A role earns its place by doing something you'd otherwise do by hand.
"Router," "coordinator," and "lead" roles that only pass work to other
agents are the first thing to cut if your team ever feels top-heavy — they
existed here before and did no work of their own.

## 2. Write the agent definition

A Claude Code subagent is one Markdown file in `.claude/agents/` (or the
private-repo equivalent this project already uses — see
`docs/private_agent_architecture.md`). The frontmatter is the whole
contract:

```markdown
---
name: notebook-qa
description: Reviews and tests changes under bronze_layer/notebooks/. Use for notebook behavior changes.
tools: Read, Edit, Bash, Grep, Glob
model: sonnet
---

You own bronze_layer/notebooks/ and tests/test_notebooks.py.

- Run the notebook test suite before reporting a change as done.
- Flag any change to bronze_layer/bronze_ingest/ as out of scope — that's a
  different agent's path.
- Never merge. Report your findings and stop.
```

Keep each field honest:

- **`name`** — lowercase, hyphenated, and stable. Scripts and hooks can key
  off this string; renaming it later is a real change, not a typo fix.
- **`description`** — what the agent is *for*, specific enough that you (or
  Claude, when routing) can tell it apart from every other agent at a
  glance. "Handles backend stuff" fails that test; "Reviews and tests
  changes under bronze_layer/notebooks/" passes it.
- **`tools`** — the actual list, not `*`. An agent that only reviews doesn't
  need `Edit`. An agent that only reads doesn't need `Bash`. Narrow tools
  are the cheapest safety net you have.
- **`model`** — match the model to the judgment required. Deterministic
  checks (lint, test, format) don't need your most expensive model; design
  review does.

## 3. Give it a scope, not a title

The old roster wrote ownership into prose ("`builder` owns
`bronze_layer/bronze_ingest/`") and left enforcement to a separate policy
document. Do both in the same place:

- The agent's own file says what it owns, in its system prompt, as shown
  above.
- If you want that scope enforced rather than just stated, `scripts/check_path_ownership.py`
  already does this for this repo: it reads an `Agent: <name>` trailer on
  each commit and rejects one that touches a path it doesn't own. Add a row
  to its `OWNERSHIP` list for a new agent's paths; that's the entire
  registration step.

Scope by path, not by vague responsibility ("owns quality"). A path is
checkable by a script. A responsibility is only checkable by you reading
every diff.

## 4. Decide what needs your sign-off, in plain language

Skip the four-tier taxonomy. Write down two lists instead, directly in
`AGENTS.md`'s "Approval and risk rules" section:

1. **Things any agent can do without asking** — reading, drafting on a
   branch, opening a PR into `dev`, running your test/lint/format suite.
2. **Things that need you, by name, before they run** — anything that
   deploys, grants or revokes access, deletes data, touches a credential,
   or leaves `dev` for `staging`/`main`.

If an action doesn't obviously belong on list 2, it defaults to list 1 —
but say so explicitly rather than leaving it to be inferred. The two-list
version is shorter than a tier table and answers the only question that
ever actually comes up: does this need me before it runs, or not.

## 5. Wire up the guardrails you actually want enforced

None of these are required to have an agent team — add them only for the
risk you're actually worried about:

- **`CODEOWNERS`** — makes a review mandatory on paths you list, if branch
  protection requires it. Good for "a human must look at this before it
  merges."
- **`scripts/check_path_ownership.py`** — makes ownership a commit-time
  check instead of a claim in a doc. Good for "this agent must not touch
  that path," enforced the same way for a human, Claude, or Copilot.
- **A `PreToolUse` hook** (see `scripts/hooks/`) — the weakest of the three,
  because it only runs inside one vendor's runtime and fails open if the
  session starts from the wrong directory. Fine as a fast local nudge;
  never rely on it as the only gate.

Pick the cheapest mechanism that actually stops the mistake you're
guarding against. A doc that says "don't do X" stops nothing on its own.

## 6. Test the agent before you trust it

Invoke it directly on a real, small task and read what it produces before
handing it anything that matters:

```bash
# From Claude Code, once the .claude/agents/ file exists:
# "use the notebook-qa agent to review my last commit"
```

Watch for the failure modes that show up early: an agent whose `tools` list
is too broad and edits something outside its stated scope; a `description`
vague enough that it gets picked for the wrong task; a scope that overlaps
another agent's, so two agents both think they own the same path.

## 7. Keep it small and let it grow from evidence

Add an agent, a tier, or a guardrail only when something you actually hit
justifies it — not because a bigger team looks more complete. This repo's
last roster had eleven roles; four of them existed only to pass work to
other agents. A role earns a place on your team by doing work, not by
filling out an org chart.

When you do add a role, write down *why*, next to the definition — the
failure or gap that made it necessary. That note is what stops the next
person (or the next you) from re-adding a role you already cut on purpose.

## Checklist

- [ ] One task you do by hand that's worth handing off
- [ ] One `.claude/agents/<name>.md` file: `name`, a specific `description`,
      a narrow `tools` list, a scope stated in the prompt body
- [ ] That scope registered wherever you enforce it (`OWNERSHIP` in
      `scripts/check_path_ownership.py`, a `CODEOWNERS` line, or both)
- [ ] Two short lists in `AGENTS.md`: what needs no sign-off, what needs you
      by name
- [ ] A real test run against a small task, read end to end
- [ ] A one-line reason recorded for every role you add, so the next
      addition has to clear the same bar
