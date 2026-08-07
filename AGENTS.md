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
(owned by the `business-analyst` subagent below), not as a silent addition to
the roadmap. If there's no real ask on hand yet, `business-stakeholder` can
research a sourced candidate — but even then, `business-analyst` is the only
path from candidate to validated case.

## Agent roster and approval tiers

This repo runs **10 purpose-built subagents**, organized as a real reporting
structure rather than a flat list — see `docs/agent_governance.md`'s "Agent
org chart" for the full diagram. Short version: Yash, the Project Lead
(human), sits at the top. `principal-data-engineer` is the managerial and
senior-technical layer directly below Yash — the three branches below
(`business-stakeholder`; `business-analyst` + `solution-architect`;
`devops-lead`) all report to it, and it holds standing authority to review
and merge Tier 1 PRs into `dev` on its own, escalating anything that feels
like more than Tier 1 rather than self-approving. Yash still holds every
Tier 2/3 sign-off directly — that authority does not shift.
`business-stakeholder` researches and proposes candidate opportunities when
no real ask exists yet, and always hands them to `business-analyst` rather
than skipping ahead. `business-analyst` and `solution-architect` are peers
who jointly direct `data-engineer`, `qa-engineer`, and `data-analyst` on the
delivery side. `devops-lead` sequences `devops-engineer` and
`platform-engineer` on the DevOps side. Prefer the matching subagent over
general-purpose editing when one fits. `business-analyst` captures and
reconciles business asks against what's already built or decided (from a
human *or* from `business-stakeholder`'s research — same reconciliation
either way); `solution-architect` turns an Approved case into a technical
design. Neither `solution-architect` nor `business-stakeholder` writes
pipeline code themselves.

**The actual agent definitions — prompts, tool grants, workflow
instructions — are proprietary and are not stored in this repo, including in
its git history.** They live in the private `ingredion-agent-config` repo
and are fetched into the gitignored `.claude/agents/` directory by
`scripts/bootstrap_agents.sh` — run it once after cloning, per
`.claude/agents/README.md`. `agents.lock` pins which version is currently in
use. See `docs/private_agent_architecture.md` for the full reasoning and the
options considered.

**`docs/agent_governance.md` owns what any agent (subagent or not) may do
without a human sign-off versus what requires the Project Lead's explicit
go-ahead first** — deploys to staging/prod, GRANT/DROP/VACUUM SQL,
credential handling, and promotion PRs all require that sign-off regardless
of which agent is doing the work, or which one is coordinating it. Read it
before touching anything in `databricks.yml`, `bronze_layer/resources/*.yml`,
or `azure_setup.md`.

## Where to find things (don't duplicate — go read the owner)

| Question | Owning doc |
| --- | --- |
| I'm not an engineer — where do I start? | `docs/overview.md` |
| Where do new business asks/feature requests get captured and reconciled? | `docs/business_requirements.md` |
| Why don't I see the actual agent prompts in this repo? | `docs/private_agent_architecture.md` (and `.claude/agents/README.md`) |
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

**Never commit real agent-config content.** `.claude/agents/*.md` (other
than `README.md`) is gitignored on purpose — it's populated by
`scripts/bootstrap_agents.sh` from the private `ingredion-agent-config` repo.
If `git status` ever shows one of those files as untracked-and-stageable
with real prompt content in it, that's a sign the gitignore or the fetch
script has drifted — fix that before committing anything else.

**A researched candidate is not a validated case.** `business-stakeholder`
can propose a sourced opportunity when no real ask exists, but it never
skips `business-analyst`'s reconciliation, never gets a fabricated owner or
impact figure, and never reaches `solution-architect` or an implementing
agent directly. Treat any candidate the same as an unreconciled human ask
until `business-analyst` says otherwise.

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
