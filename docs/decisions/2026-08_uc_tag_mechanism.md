# Unity Catalog tags — which mechanism, and the gate that must ship with it

The design decision **#64** needs before any code. #64 was split on 2026-08-08:
it keeps tag *application*; ABAC *enforcement* went to **#238**. That split is
what makes this document necessary, because the two are only separable on
paper — see §3.

**Status: proposed. Not signed off.**

Written against `dev` @ `b7fbdb1`, verified against workspace
`adb-7405607398572130` on 2026-08-11.

---

## 1. What is actually in the workspace

Verified by invoking, not by reading docs:

```
$ databricks tag-policies list-tag-policies -p <profile> --output json
70 tag policies
  class.* taxonomy: 63  -> class.credit_card, class.email_address,
                           class.iban_code, class.ip_address,
                           class.location, class.name, ...
  others: system.certification_status, ai.model_creator, ai.model_family,
          sap.PersonalData.entitySemantics, ...
```

```
GET /api/2.1/unity-catalog/policies/table/<table>   -> HTTP 200
```

Two things follow, and both change the shape of #64:

1. **A governed PII vocabulary already exists.** 63 `class.*` keys. #64 does
   not need to invent tag names; it needs to decide whether it is applying
   *governed* keys (which have policies attached) or *free-form* ones.
2. **ABAC is live.** A governed tag is not a label. It is a key that a policy
   can bind access rules to.

### A method note, because this nearly went wrong again

The first probe here was a hand-built `curl` at
`/api/2.1/unity-catalog/tag-policies`, which returned `ENDPOINT_NOT_FOUND`.
That reads as "the feature is absent" and is false — the real path differs, and
the CLI knows it. This is the identical mistake that produced **three false
FAILs** in Amendment 2 of `2026-08_ai_genie_architecture.md`, corrected by
Amendment 3.

**Rule, restated because it has now cost two sessions:** probe capabilities
through `databricks <command>` (with `--debug` to see the real path). A
guessed URL that 404s proves nothing.

## 2. The mechanism: DDL or the REST assignment API

| | `ALTER TABLE ... SET TAGS` (DDL) | `entity-tag-assignments` (REST) |
| --- | --- | --- |
| Works on Databricks Runtime | Yes | Yes |
| Works on OSS/local Delta | **No** — `ParseException` | N/A, it is an HTTP call |
| Testable by this suite | **No.** It executes through `spark.sql`, and the local session cannot parse it at all | **Yes**, behind an injectable client |
| Idempotency | Needs `information_schema` tag views to diff — also DBR-only | Diffable against a `GET`, no SQL needed |

`catalog_metadata.py`'s module docstring already states the DDL problem
plainly: `SET TAGS` and the `information_schema` tag views *"are Databricks
Runtime features that raise ParseException on OSS/local Delta, so a tagging
implementation cannot be executed — let alone verified — by this package's test
suite."*

**Decision: the REST assignment API, behind an injectable client.**

The deciding argument is not that REST is nicer. It is that **this repo already
has the pattern, and already chose it for exactly this reason.** `ai_metadata.py`
defines a `MetadataDrafter` Protocol with two implementations and a fake in
tests (#241), precisely so that a capability which cannot run locally is still
covered by the suite. Tagging has the same shape and should reuse it rather
than become the one feature verified only by hand.

This also matters more than usual here. Four of the defects found on
2026-08-11 were invisible to every local gate and caught only by CI. A feature
that **no** automated gate can see is a category worse than that, and DDL puts
tagging in it permanently.

**Rejected: DDL.** It is the more obvious choice and the one #64's "extend
`catalog_metadata.py`, do not create a parallel `uc_tags.py`" note points at —
the COMMENT machinery there is SQL. That note still holds for *structure*
(diff-then-apply, failures never fail ingestion); it should not drag the
transport along with it.

## 3. The gate that must ship with it, and why the #64/#238 split does not remove the need

#238 took ABAC *enforcement*. It did not take the consequence:

> **A governed tag can carry a policy, so a tag write can silently change who
> can read the data.**

That is live today — 63 governed `class.*` keys, ABAC endpoint answering. So
the moment #64 can write `class.email_address` onto a column, #64 can change
access control, whether or not anyone has "implemented ABAC".

**Requirement: no automatic application of AI-drafted tags. Ever.**

The pipeline already has the right shape for this and it must not be
short-circuited:

- `_ai_metadata.pii_flags_json` holds **drafts**. `architecture.md` is explicit
  that nothing in the write path reads `_ai_metadata`, and #208 verified that
  by grep.
- `apply_reviewed_tags(...)` — named in #64's own acceptance criteria as "the
  AI layer's publish target" — must take **reviewed** tags. Not "drafts above a
  confidence threshold". A confidence score is not a review.
- The reviewing step is a human or an explicit promotion record. It is #207's
  shape, not a config flag.

Without this, the chain is: a model guesses a column is an email address → a
governed tag is applied → an ABAC policy masks the column → data disappears
from a downstream consumer, and the cause is three systems away from the
symptom. Every link in that chain now exists.

## 4. Governed vs free-form — #64 must say which

Applying `class.email_address` is not labelling. It is asserting membership of
a controlled vocabulary that policies key on. Applying `team: bronze` is a
label.

**Decision: config-declared tags are free-form; governed keys are only ever
written through `apply_reviewed_tags`.** A YAML file that can silently opt a
table into a policy-bearing key is the same hazard as the above, arriving
through a different door — and config comes from a Volume, which #154
established is a wider trust boundary than the repo.

## 5. What this leaves for implementation

- An injectable tag client (Protocol + real + fake), mirroring `MetadataDrafter`
- Diff-then-apply against a `GET`, so a re-run with unchanged tags is a no-op —
  #64's acceptance criterion, now checkable without `information_schema`
- Failures never fail ingestion, and land in the audit row — same contract as
  `apply_catalog_metadata`
- `apply_reviewed_tags(...)` taking reviewed input only
- Non-UC environments no-op with a warning
- `APPLY TAG` (or its REST equivalent) added to #112's Phase B grant list

## 6. What would change this decision

1. **DDL becomes locally parseable** — e.g. a delta-spark version that accepts
   `SET TAGS` as a no-op. Then the testability argument collapses and DDL's
   consistency with the COMMENT machinery wins.
2. ~~**The REST API turns out not to support column-level assignment.**~~
   **Checked, 2026-08-12 — it does.** See §7. This was the one open risk to the
   decision and it is closed.

## 7. Column-level assignment — verified 2026-08-12

§6 listed this as the one unverified thing that could invalidate the decision.
It does not.

```
$ databricks entity-tag-assignments --help
  Entity tagging is supported on catalogs, schemas, tables (including views),
  COLUMNS, and volumes.

$ databricks entity-tag-assignments list tables  ingredion_en.ingredion_dev.order_001_bronze
[]
$ databricks entity-tag-assignments list columns ingredion_en.ingredion_dev.order_001_bronze.order_id
[]
```

Both read paths work and return an empty list — no tags are assigned anywhere
yet, which is the expected starting state.

Two details the implementation needs, and neither required guessing:

- **Entity types are `tables` and `columns`** (plural), passed as the first
  positional argument. Not `TABLE`/`COLUMN`.
- **A column is addressed by four-part FQN** —
  `catalog.schema.table.column` — not by a table name plus a separate column
  parameter.

The API surface is `create` / `get` / `update` / `delete` / `list`, which is
exactly what diff-then-apply needs: `list` to read current state, `create` and
`update` to converge, and nothing that forces a read-modify-write of the whole
entity.

**Nothing in §2's decision changes.** The one risk that could have sent this
back to DDL is gone, and the idempotency requirement in §5 is now concretely
achievable: diff `list`'s output against the desired set, and issue calls only
for the difference.
