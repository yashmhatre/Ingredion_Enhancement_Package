# AI agent governance

Who reviews what, and what an AI coding agent may do in this repository
without a human in the loop first. Written for Yash (Principal Data
Engineer on this project), who is the human reviewer of record for every
tier below that isn't fully autonomous.

This doc governs *agent behavior*; it doesn't replace `CONTRIBUTING.md`
(the human-and-agent-shared contribution workflow) or `AGENTS.md` (general
operating instructions for any coding agent in this repo). Read those first.
If this doc and `AGENTS.md` ever disagree, treat that as a bug and fix one
of them — don't quietly follow whichever is more convenient.

**The governing principle:** agents in this repo propose and draft; the
Principal Data Engineer approves anything that spends money, touches a
credential, changes who can see production data, or moves code into
staging/prod. Nothing below is about agent capability — it's about where
the human's judgment is required regardless of how capable the agent is.

---

## The four tiers

### Tier 0 — fully autonomous, no approval needed

- Reading, searching, and explaining any file in the repo.
- Running the local test/lint/type/security suite (`pytest`, `ruff`, `mypy`,
  `bandit`, `pip-audit`) and reporting results.
- Drafting code changes on a local branch or an already-open feature branch.
- Opening or updating a PR **into `dev`** (never merging it).
- `databricks bundle validate` (any target) — read-only, resolves config,
  touches no compute or data.

### Tier 1 — draft freely, but don't merge without review

- Any PR merge into `dev`.
- Any change to `IngestionConfig`'s public shape (new/changed fields,
  validation rules).
- Any change to `.github/workflows/ci.yml`.
- Any suppression of a ruff/mypy/bandit/pip-audit finding
  (`# noqa`, `# nosec`, `--ignore-vuln`) — the agent may draft it with a
  reason, but Yash reviews the reason before it merges, per the
  no-blanket-suppression rule in `AGENTS.md`.
- Any change to a notebook or `bronze_layer/resources/*.yml` — given this
  repo's history (#144, #145), these get read carefully even with tests
  attached.

Agents should get a Tier 1 change fully green on the local quality gates
(via `devops-engineer`) *before* asking for review — that's table
stakes, not a substitute for the review itself.

### Tier 2 — draft only; requires Yash's explicit, named sign-off before executing

Not "review the PR after the fact" — these need a yes *before* the action
runs, because some of them aren't reversible by a follow-up PR:

- `databricks bundle deploy -t staging` or `-t prod`
- Any `GRANT` / `REVOKE` / `DROP` / `VACUUM` / destructive `DELETE` or
  `ALTER ... DROP COLUMN` against Unity Catalog
- Any change to `run_as_service_principal` values, `run_as` blocks, or
  target definitions in `databricks.yml`
- A promotion PR: `dev` → `staging` or `staging` → `main`
- Any Azure portal action in `azure_setup.md` not already checked off there
  (creating resources, changing role assignments, registering providers)
- Enabling Change Data Feed, changing `VACUUM` retention, or any change with
  a stated irreversible-history consequence (see
  `docs/bronze_silver_contract.md` §1 on why CDF retention is exactly this
  kind of decision)

An agent hitting a Tier 2 action should draft the exact command/SQL/diff,
state the blast radius (which environment, which principals, what happens
if it's wrong), and stop — present it for a specific yes, not proceed on a
general "sounds good."

### Tier 3 — never autonomous, no drafting shortcut

- Creating, rotating, or displaying service principal credentials or any
  secret.
- Any IAM/RBAC change in Azure or the Databricks account console
  (`Service Principal: User` role grants, `Use` grants, workspace admin).
- A hotfix commit directly to `main`.
- Removing or weakening a CI gate (making a currently-blocking check
  non-blocking, or vice versa without discussion — the reverse direction is
  Tier 1).

For Tier 3, an agent's job is to explain what needs to happen and who
(Yash, or whoever holds the relevant Azure/Databricks account role) needs to
do it — not to attempt a workaround that achieves the same end state through
a side door.

---

## How this maps to the subagents in `.claude/agents/`

| Subagent | Typical tier of its own work |
| --- | --- |
| `data-engineer` | Tier 0 drafting, Tier 1 merge |
| `qa-engineer` | Tier 0 drafting, Tier 1 merge |
| `devops-engineer` | Tier 0 (verification only; narrow suppression drafts are Tier 1) |
| `platform-engineer` | Drafts Tier 1/2/3 actions; **never executes Tier 2/3 itself** |
| `business-analyst` | Tier 0 (captures, reconciles, and documents business asks in `docs/business_requirements.md`); never writes code and never files an issue for a case still "Proposed" or "Under review" — turning an accepted case into engineering work is Yash's call, not this agent's |

If a general-purpose agent session (not one of the above) ends up touching
deploy config, credentials, or a promotion branch, it should still follow
this tiering — the tier is about the *action*, not which agent happens to be
driving.

## What "explicit sign-off" looks like in practice

A Tier 2/3 action is approved when Yash names the specific action in the
conversation — "go ahead and deploy to staging," "yes, run that GRANT" —
not when a PR sits open unreviewed, not when an earlier message said
"looks good" about something else, and not when CI is green. Green CI is a
precondition for asking, never a substitute for asking.

## Escalation default

If it's unclear which tier an action falls into, treat it as the higher
tier and ask. Being asked unnecessarily costs a minute; running an
unreviewed `GRANT` or promotion doesn't undo itself.

## Keeping this current

This doc is a **living document** in the same sense `docs/README.md`
defines the term: update it in place as the tiering proves wrong in
practice, don't let a stale copy of the rules sit next to the real ones.
If you (any agent or human) find this doc and an agent's actual behavior
disagreeing, that's worth flagging the same way a docs/architecture drift
would be under `docs/README.md`'s existing rule.
