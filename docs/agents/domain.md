# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

This repo is **single-context** — one `CONTEXT.md` at the root, one decision-record
directory. There is no `CONTEXT-MAP.md` and no per-package context; don't create either.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root — the domain glossary.
- **`docs/decisions/`** — read the decision records that touch the area you're about to work
  in. **This repo uses `docs/decisions/`, not `docs/adr/`.** Do not create `docs/adr/`; it
  would put two decision-record directories in a repo whose whole documentation convention
  is that exactly one document owns each subject.
- **`docs/README.md`** — the ownership table. It says which document is authoritative for
  each subject. Read it whenever two docs disagree.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't
suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs`
and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually
get resolved.

`CONTEXT.md` now exists and is the current single-context glossary.

## File structure

```
/
├── CONTEXT.md                                   ← created lazily by /domain-modeling
├── docs/
│   ├── README.md                                ← ownership table; the tiebreak
│   ├── decisions/                               ← binding decision records (Tier 2)
│   │   ├── 2026-08_autonomous_remediation.md
│   │   └── 2026-08_ai_genie_architecture.md
│   └── agents/                                  ← this directory
├── bronze_layer/
└── silver_layer/
```

Records in `docs/decisions/` are named `YYYY-MM_short_slug.md`, not `NNNN-kebab-title.md`.
Match the existing convention.

## Writing decision records — draft only

**`docs/decisions/` is not an ordinary ADR directory.** Per `docs/README.md`, a record there
is *binding once signed off*, and per `docs/agent_governance.md` it carries a **named Project
Lead sign-off under Tier 2**. A decision stays in force until a later record supersedes it,
and it is amended by writing the amendment into it — never by rewriting the reasoning.

So when a skill produces a decision record:

- **Read freely.** Cite records, and flag contradictions (see below).
- **Write as a draft only.** Give the record an explicit status line of
  `**Status:** Proposed — awaiting Tier 2 sign-off`, and name no signer.
- **Never self-mark as signed off.** `**Signed off** by <name>, <date>` is written only
  after Yash actually signs off. An agent writing that line is forging an approval.
- **Never amend a signed-off record's reasoning.** Propose a superseding record instead.
- **Never mark a `superseded-pending-verification` item as resolved** without the in-workspace
  verification that label is waiting on.

An agent may draft; only the Project Lead binds.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a
hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms
the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing
language the project doesn't use (reconsider) or there's a real gap (note it for
`/domain-modeling`).

Where `CONTEXT.md` and another document disagree, `docs/README.md`'s ownership table decides.
If that is still ambiguous, **open GitHub issues are the tiebreak**.

## Flag decision conflicts

If your output contradicts an existing record in `docs/decisions/`, surface it explicitly
rather than silently overriding:

> _Contradicts `docs/decisions/2026-08_autonomous_remediation.md` (the eligible fix-class set
> starts empty) — but worth reopening because…_

Surfacing it is the whole job. Do not resolve the conflict yourself: a signed-off record
outranks any skill's output, and reopening one is a Tier 2 decision.

## Point-in-time documents are not current

`docs/README.md` splits documentation into *living*, *decision records*, and *point-in-time*.
The point-in-time set (`docs/archive/`, `docs/architecture_ai_metadata_2026-08.md`,
`bronze_layer/docs/archive/`) records what was verified and when — it does **not** describe
today's code. Don't cite it as current behaviour, and don't update it to match new
behaviour. Supersede it instead.
