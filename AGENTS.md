# AGENTS.md

Instructions for AI coding agents (Claude Code, Codex, Cursor, Copilot, etc.)
working in this repository. Read this first — it is short on purpose and
points to the document that actually owns each subject rather than repeating
it, following the same rule the rest of this repo's docs already follow (see
`docs/README.md`). **The code wins over any document, including this one.**

---

## What this repo is, right now

An ELT pipeline package for Ingredion's data platform, built as independent,
self-contained layers on Databricks + Unity Catalog. Today that means:

- **`bronze_layer/`** — real, deployed, tested. A config-driven package
  (`bronze_ingest/`) that loads JSON into governed Delta bronze tables, plus
  its own tests, docs, and Asset Bundle resources.
- **`silver_layer/`** — not built. A README and an archived module
  (`_archive/flattener.py`) only. Do not treat anything under `silver_layer/`
  as working code.
- **`gold_layer/`** — does not exist yet.

If a task description assumes silver/gold pipeline logic exists, stop and
check `docs/roadmap.md` and the open issues — it's almost certainly aspirational,
not current state.

**Anything under `docs/archive/` (and `bronze_layer/docs/archive/`) is a
point-in-time record, not current guidance.** Each archived file carries its
own banner explaining what superseded it and why it was kept rather than
deleted — read the banner, but don't treat the archived content itself as a
still-open finding or a design to follow. If you need the current version of
something an archived doc discusses, go to the doc that owns that subject
today per the table below (or `docs/README.md`'s full index) — never infer
current behavior from an archived file without checking the issue or living
doc it points to. `docs/current_behavior.md` and `docs/architecture_review_2026-07.md`
now live at `docs/archive/` under this rule; `bronze_layer/docs/testing_json_reader.md`
and `bronze_layer/docs/testing_end_to_end_deployment.md` now live at
`bronze_layer/docs/archive/`.

**If you're not sure whether a request is a technical task or a business
ask, start at `docs/overview.md`** — a plain-language companion doc aimed at
non-engineers, and the front door for anyone who isn't already fluent in this
repo's architecture. New business asks belong in `docs/business_requirements.md`
(owned by the `architect` subagent below), not as a silent addition to the
roadmap. The architect reconciles candidates into validated cases before they
become implementation work.

## Agent roster and approval tiers

This repo runs **7 purpose-built subagents**, defined by function and
substrate rather than by an org-chart metaphor — see
`docs/agent_governance.md` for the full table and the approval tiers. Short
version: Yash, the Project Lead (human, a Senior Principal Data Engineer),
**signs off; he does not route work and does not implement.** The boundary is
the branch — everything into `dev` is the agent team's call, everything out of
it (`dev` → `staging`, `staging` → `main`) is Yash's. `orchestrator` is the
routing layer that exists so he doesn't have to be — it delegates, merges
**all** Tier 1 work into `dev` once CI is genuinely green and any escalation
path has its `reviewer` verdict, and assembles sign-off packets for Tier 2/3.
`architect` turns an ask into a reconciled case and then a
technical design, and owns `docs/business_requirements.md`. `reviewer` gives
an adversarial pre-merge read on the escalation paths, and is separate from
`orchestrator` precisely so the agent that routes work doesn't also bless it.
`builder` (`bronze_ingest/`), `notebook-qa` (`notebooks/` + notebook tests),
and `platform` (deploy config, drafts only) implement. `scout` runs on a free
local model and produces briefs — **its output is an index, never evidence.**
Prefer the matching subagent over general-purpose editing when one fits.

**The actual agent definitions — prompts, tool grants, workflow
instructions — are proprietary and are not stored in this repo, including in
its git history.** They live in the private `ingredion-agent-config` repo.
`scripts/bootstrap_agents.sh` fetches the pinned source into gitignored
`.claude/agents/` and renders the Codex and Copilot equivalents — run it once
after cloning, per `.claude/agents/README.md`. `agents.lock` pins which version
is currently in use. See `docs/private_agent_architecture.md` for the full
reasoning and the options considered.

**`docs/agent_governance.md` owns what any agent (subagent or not) may do
without a human sign-off versus what requires the Project Lead's explicit
go-ahead first** — deploys to staging/prod, GRANT/DROP/VACUUM SQL,
credential handling, and promotion PRs all require that sign-off regardless
of which agent is doing the work, or which one is coordinating it. Read it
before touching anything in `databricks.yml`, `bronze_layer/resources/*.yml`,
or `azure_setup.md`.

## Agent skills

This repo has the [`mattpocock/skills`](https://github.com/mattpocock/skills) pack installed
(35 skills, in the gitignored `.agents/skills/`, pinned by the committed `skills-lock.json`;
restore with `npx skills@latest add mattpocock/skills` and then
`python scripts/configure_agent_skills.py`). Those skills read the three files
below for their per-repo configuration. All three are hand-editable — re-run
`/setup-matt-pocock-skills` only to switch issue trackers or start over.

### Issue tracker

GitHub issues on `yashmhatre/Ingredion_Enhancement_Package`, via the `gh` CLI; prefer the
`context-scout` MCP server for reads. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, label strings unchanged: `needs-triage`, `needs-info`,
`ready-for-agent`, `ready-for-human`, `wontfix`. All five now exist, and they stack on the
existing area/kind/priority/stage labels rather than replacing them. See
`docs/agents/triage-labels.md`.

### Domain docs

Single-context. Decision records live in **`docs/decisions/`, not `docs/adr/`** — do not
create the latter. Records there are binding and Tier 2, so a skill may **draft** one marked
`Proposed — awaiting Tier 2 sign-off` but must never write a sign-off line itself. See
`docs/agents/domain.md`.

### Skills vs. the roster

The pack ships skills that duplicate roles the 7-agent roster above already owns. **The
roster wins.** A skill is a way of doing the work; it does not reassign who owns it or
change an approval tier.

| Pack skill | Defers to | Rule |
| --- | --- | --- |
| `two-axis-code-review` | `reviewer` | Exploratory reads only. The pre-merge verdict on an escalation path is `reviewer`'s, and `reviewer` never reviews its own work. This is the pack's upstream `code-review`, renamed locally. |
| `implement`, `tdd`, `prototype` | `builder`, `notebook-qa` | Code in `bronze_ingest/` is `builder`'s; `notebooks/` and `tests/test_notebooks.py` are `notebook-qa`'s. |
| `to-tickets`, `to-spec`, `to-questionnaire`, `triage`, `wayfinder` | `orchestrator`, `architect` | `orchestrator` routes work; `architect` owns intake into `docs/business_requirements.md`. A skill may produce tickets — it does not decide who picks them up. |
| `git-guardrails-claude-code`, `setup-pre-commit` | `platform` | Touch CI, hooks, and deploy config. Drafts only; Tier 2 sign-off applies. |
| `research`, `grilling`, `grill-with-docs`, `domain-modeling`, `codebase-design`, `diagnosing-bugs`, `handoff`, `teach`, `writing-*` | — | No roster equivalent. Use freely. |

Two standing cautions:

- **Four skills are TypeScript/Node-only and inert here** — `migrate-to-shoehorn`,
  `setup-ts-deep-modules`, `scaffold-exercises`, and `setup-pre-commit` (Husky + lint-staged).
  This is a Python/Databricks repo. Don't spend a session trying to apply them.
- **The pack's upstream `code-review` is installed here as
  `two-axis-code-review`**, leaving Claude Code's built-in `/code-review` and
  `/code-review ultra` unshadowed. Re-run `scripts/configure_agent_skills.py`
  after restoring or updating the pack.

### Codex agent team

`scripts/bootstrap_agents.sh` renders the same seven private, pinned role
definitions into `.codex/agents/*.toml`. Codex uses `orchestrator` as the front
door and delegates to `architect`, `reviewer`, `builder`, `notebook-qa`,
`platform`, and `scout` according to `docs/agent_governance.md`. Generated TOML
contains proprietary prompt content and is gitignored; `.codex/agents/README.md`
documents setup. Custom agents do not change approval tiers or path ownership.

No skill overrides `docs/agent_governance.md`. Deploys, `GRANT`/`DROP`/`VACUUM`, credential
handling, and promotion PRs need the Project Lead's named sign-off no matter which skill or
agent is driving.

## Where to find things (don't duplicate — go read the owner)

| Question | Owning doc |
| --- | --- |
| I'm not an engineer — where do I start? | `docs/overview.md` |
| Where do new business asks/feature requests get captured and reconciled? | `docs/business_requirements.md` |
| Why don't I see the actual agent prompts in this repo? | `docs/private_agent_architecture.md` (plus `.claude/agents/README.md` and `.codex/agents/README.md`) |
| How do I set up, configure, run the bronze package? | `bronze_layer/README.md` |
| How do I set up my local dev environment (Java/Python/Spark/Windows prereqs), find an issue, branch, commit, open a PR? | `CONTRIBUTING.md` |
| What changed in a release, and what do I need to do before deploying it? | `CHANGELOG.md` |
| What's the deployment target/variable/run-as layout? | `databricks.yml` (header comments — the file *is* the source of truth) |
| Design rationale for the bronze architecture and remaining hardening phases? | `bronze_layer/docs/architecture.md` |
| What order is the remaining work in, and why? | `docs/roadmap.md` — **check this before picking up any task** |
| First-time Azure/Databricks environment setup? | `azure_setup.md` |
| What does bronze promise silver, and what must silver be built to expect? | `docs/bronze_silver_contract.md` |
| Build-vs-buy decisions already made (DQX, Lakeflow, etc.)? | `docs/buy_vs_build_2026-08.md` |

The full index, with the "living vs. point-in-time" distinction, is
`docs/README.md`. If two docs disagree, that file's ownership table decides
which one wins; if that's still ambiguous, open GitHub issues are the tiebreak.

## Fast path to running things

```bash
cd bronze_layer
pip install -e ".[dev]"
pytest                                    # full suite (needs Java 17 + Spark; see CONTRIBUTING.md if it fails)
pytest tests/test_notebooks.py            # notebook contract tests only — no Spark/Java needed, <1s
pytest --cov=bronze_ingest --cov-report=term-missing   # coverage is reported, never enforced — no --cov-fail-under

ruff format bronze_ingest tests notebooks         # apply formatting — do this before committing
ruff check bronze_ingest tests notebooks          # lint
mypy bronze_ingest notebooks                      # types
bandit -r bronze_ingest                           # security scan
pip-audit --skip-editable                         # dependency CVEs
```

All tool config lives in `bronze_layer/pyproject.toml` — no separate
`.ruff.toml` / `mypy.ini` / `pytest.ini`. If a test needs a live workspace or
real cloud storage, it isn't in `pytest` — check `bronze_layer/docs/testing_*.md`
instead of trying to make it pass locally.

If Spark/Java setup fails locally (especially on Windows), don't try to work
around it ad hoc — `CONTRIBUTING.md`'s "Local Development Setup" section
documents five specific, order-sensitive prerequisites and the exact error
each missing one produces. Read it before improvising a fix.

## Non-negotiable rules for changes in this repo

**Task-first.** For anything beyond a trivial fix, there should be an open
GitHub issue describing what and why before you start. If one doesn't exist
and the change is non-trivial, say so and propose opening one rather than
starting silent, large-scope work.

**Branch model — a strict one-way flow, and every arrow is a PR:**
```
feature/*  →  dev  →  staging  →  main (prod)
```
Branch off `dev` and open PRs against `dev`. Never target `main` or `staging`
directly — those only receive promotions or a documented hotfix (see
`CONTRIBUTING.md` § "Hotfixes"). Never commit straight to `main`.

**Commit style:** `<type>: <short summary>` where type is one of `feat`,
`fix`, `test`, `docs`, `refactor`, `chore`. Keep commits scoped to one thing.

**Tests are mandatory for behavior changes, notebooks included.**
`bronze_layer/notebooks/` holds the code Databricks jobs actually run — both
known live production defects in this repo's history lived there, in the
untested gap. A change to a notebook needs a test in `tests/test_notebooks.py`
exactly as a change to the package does. Don't add a notebook dependency
without declaring it (see CONTRIBUTING.md's pandas story for why).

**Bronze stays flattening-free, by design.** Nested JSON is preserved as-is;
reshaping is Silver's job (`docs/bronze_silver_contract.md` §4). Don't add
flattening/reshaping logic to `bronze_layer` to solve a Silver-shaped problem.

**Config changes are additive.** New `IngestionConfig` fields need sane
defaults and must never break an existing config file. Follow the existing
`dbutils`-when-available / plain-Python-fallback dual-environment pattern
(see `directory_ingestion.py`'s `_try_dbutils_ls` / `_try_posix_ls`) for any
new reader/writer logic.

**Suppressions carry a reason, and never widen the net.** A lint/type/security
finding that's genuinely fine gets an inline suppression naming the specific
rule and why (`# noqa: BLE001 - ...`, `# nosec B608 - ...`). Never disable a
rule globally, lower a severity threshold, or widen an allowlist to make one
finding go away — that silently covers whatever comes next too.

**Docs: fix the owner, don't create a second copy.** If you learn something
non-obvious while working (a Spark gotcha, an environment misconfiguration, a
stale claim), write it into the doc that owns that subject per the table
above — not into a new file, and not as a comment nobody will find. If you
find two docs making conflicting claims, that itself is worth flagging or
fixing (see issue #161's pattern in `docs/architecture_review_2026-07.md`).

**Don't touch cloud/Azure infrastructure or `databricks.yml` targets without
reading `azure_setup.md` and the "Deployment" section of `bronze_layer/README.md`
first.** The environment model (one catalog, isolation by schema, per-target
service principals) is deliberate and has a documented known gap (source
volumes aren't isolated per environment — see `databricks.yml`'s header). Don't
"fix" that gap as a drive-by; it's tracked (#160).

**Never commit real agent-config content.** `.claude/agents/*.md` and
`.codex/agents/*.toml` (other than their `README.md` files) are gitignored on
purpose — they are populated by `scripts/bootstrap_agents.sh` from the private
`ingredion-agent-config` repo. If `git status` ever shows one of those files as
untracked-and-stageable with real prompt content in it, the gitignore or fetch
script has drifted — fix that before committing anything else.

**A brief is not a finding.** `scout` runs on a small local model, which
produces fluent, confident, wrong summaries. Its output tells you where to
look; it never substitutes for looking. The moment a decision rests on a
`scout` summary being correct, verify the source first.

## What to work on

`docs/roadmap.md` is the living source of truth for sequencing — it's
re-audited against the code, not against issue text, so trust it over an
issue's own description of "done vs. not done." As of its last audit: bronze's
correctness work is complete and promoted to `main`; open work is phased
starting with two decisions that need no workspace (#163, #162), then Azure
provisioning (#112 chain), then operational maturity, then new features.
**If you're told to "pick something," `docs/roadmap.md`'s "Suggested order"
section is the answer** — don't reorder it without a stated reason.

## Verification before calling something done

- `pytest` passes (or you've documented in the PR why a specific test can't
  run in this environment).
- `ruff format --check`, `ruff check`, `mypy`, `bandit`, `pip-audit` all pass
  on touched files, or findings are suppressed with a reason per the rule above.
- If the change touches deployment (`databricks.yml`, `resources/*.yml`,
  notebooks) or real-storage read behavior, note in the PR what
  real-environment validation was done — local pytest alone doesn't cover
  Unity Catalog surfaces (Volumes, tags, Auto Loader, `information_schema`).
- Coverage is reported, not gated — don't chase a specific percentage, but
  don't regress an already-tested path either.
