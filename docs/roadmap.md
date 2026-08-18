# Roadmap — rebaseline from code and live issues

Re-audited on 2026-08-18 against committed `dev` @ `9e0dbea` and the **52 issues
open at the start of the audit**. The rebaseline created umbrella #354 and
closed #208, #248, #264, and #324 from verified evidence, leaving 49 open.
Code is the evidence for what exists; an issue is not complete merely because
a commit mentions its number.

This document owns sequence and gates. GitHub issues own acceptance criteria,
and `docs/agent_governance.md` owns approval tiers.

---

## Where the repository stands

- **Bronze is the only implemented pipeline layer.** Its JSON path is deployed
  and mature: governed Delta writes, audit, retry and quarantine, CDF, schema
  drift, catalog metadata, AI metadata, lifecycle machinery, profiling,
  candidate-key detection, and data-contract foundations exist in code.
- **Batch multi-format is in flight.** The format-aware foundation (#302–#309)
  is merged. The current draft registers CSV and Parquet and implements the
  XML path, notebooks, and bundle wiring; XML remains uncommittable until the
  #336/#337 decision records receive named Tier 2 sign-off.
- **Silver is not a pipeline.** `silver_layer/` remains documentation plus an
  archived flattener. The #298 foundations now present in `bronze_ingest`
  should not be mistaken for a working Bronze→Silver execution plane.
- **Gold does not exist.** #228 is therefore a real gate for #210 and #211.
- **Deployment evidence lags code.** Provisioning, grants, environment
  isolation, and several workspace-only acceptance checks remain open.
- **No promotion is implied by a green `dev`.** Every `dev → staging` and
  `staging → main` transition is a separate PR and requires the Project
  Lead's named approval.

### Issue reconciliation result

Four stale issues were closed after their full acceptance criteria were checked:

| Issue | Closure evidence |
| --- | --- | --- |
| #208 | Async AI metadata implementation, job wiring, tests, and two real workspace runs producing 15/15 drafts. |
| #248 | Six real `availableNow` runs plus the merged #294/#295 fixes. |
| #264 | Structural contract implementation, worked source contract, and tests; value ranges remain #109/Silver. |
| #324 | `MetadataStore`, vocabularies, fillability, safe replacement, profiling integration, and tests. |

#256 remains open: structured drift reaches audit rows, but rescued-data
visibility and one real post-fix drift run are still missing.

Partial work stays open with only the unmet criteria retained. In particular,
**#64** still needs governed-tag permissions and workspace proof; #159 has code
and decisions but an undeployed, paused maintenance job; #250 still needs real
business invariants; and #112 still needs Phase B provisioning and isolation
evidence.

---

## Phase 1 — finish batch multi-format Bronze

This is the active delivery chain under #301. Keep each format as a vertical
slice so the registry never advertises a reader that does not exist.

### CSV

1. **#310** — register CSV in the format registry, reader dispatch, option
   dispatch, and tests.
2. **#311** — validate CSV on Databricks, including `/Volumes` discovery,
   `_metadata.file_path`, illegal headers, malformed rows, and DBR rescue
   behavior.

### Parquet

3. **#312** — implement the Parquet batch reader without flattening nested
   structures. Treat standalone `.parquet` files and immediate Parquet
   dataset folders as valid source shapes; ignore marker files.
4. **#313** — register Parquet and prove dispatch, discovery, retry, metadata,
   and folder-as-table behavior.

### XML decisions and implementation

5. **#336 and #337** — obtain Tier 2 sign-off on the proposed XML integrity
   and namespace-identifier records before reader implementation.
6. **#314 and #315** — raise only the local/dev PySpark floor to 4.x and add
   the XML row-tag contract. The explicit `xml_row_tag` field is authoritative;
   a competing generic `rowTag` option is rejected.
7. **#316 and #317** — implement and register the XML reader under the signed
   integrity and identifier policies.
8. **#318** — complete serverless Databricks validation. Force an action
   because `.load()` is lazy; cover malformed documents, namespace collisions,
   nested content, attributes, and `_metadata.file_path`.

### Public wiring

9. **#319** — add the format-neutral API with a compatibility alias; **#320**
   and **#321** wire notebook and bundle parameters; **#322** updates the
   owning documentation. Close #301 only after workspace evidence is recorded.
10. **#323** remains explicitly deferred: this phase is batch-only and does
    not widen Auto Loader beyond JSON.

---

## Phase 2 — establish the platform boundary

Run this chain after multi-format, or sooner if Azure credits or workspace
availability impose a deadline:

```
#112  Phase B grants, Key Vault, staging/prod deploys, isolation proof
  ├── #113  OIDC bundle deployment
  ├── #115  secret scopes
  └── #160  per-environment Volumes
              └── #64 governed tag application → #238 ABAC enforcement
```

All SQL grants, credentials, deploys, resource changes, and promotions follow
their Tier 2/3 gates. #64 cannot be called complete until the required tag
permission and manual integration evidence exist.

---

## Phase 3 — make Bronze operable

1. **#159** — deploy and deliberately activate the maintenance policy. The
   batch audit buffering and streaming-compaction decision exist; operational
   proof does not.
2. **#62 → #61** — build operational dashboards and alerts over the corrected
   audit model, then use that history for volume anomaly baselines.
3. **#250** — obtain business-owned required-column invariants. Remove any
   dead sample override rather than fabricating a source contract. #248 is
   closed from six documented real Auto Loader runs.
4. **#258 → #259 → #260** — resume FinOps only when billing-system access
   exists, then add per-run cost and cost-anomaly signals.
5. **#261** — add the first freshness SLA after the alerting surface exists.

---

## Phase 4 — build the deterministic Bronze→Silver path

Reconcile #205, #298, and #109 before creating more overlapping work. #298's
issue text predates implemented profiling, `MetadataStore`, and candidate-key
detection, so begin from code rather than replaying completed foundation work.

1. Use the now-closed **#324** storage boundary as the common foundation for
   the remaining #298 work.
2. Build **#265** on the now-closed #264 structural contract, without moving
   Silver-owned value/range rules into Bronze.
3. Build the remaining deterministic inference-to-execution boundary under
   **#298**, with **#205** as the medallion delivery parent and **#109** as the
   business-rule quality workstream.
4. Re-home **#255** in Silver per its proposed layer-placement record. Bronze
   retains source versions; Silver owns SCD2 interval bookkeeping.

The canonical Silver output remains atomic, standardized, and lossless.
Dimensional, Data Vault, and other business projections are generated outputs,
not alternate meanings of canonical Silver.

---

## Phase 5 — product and serving layers

The business case **#213** remains the parent for the product outcome, not
evidence that its layers exist.

1. With **#208 closed**, deliver **#206 → #209** within the signed autonomous-
   remediation bounds. Eligible automatic fix classes start empty and expand
   only through named approval.
2. Build **#228** before **#210** or **#211**. Genie and the executive dashboard
   require curated Gold entities and human-authored measures.
3. Keep operational dashboard #62 separate from executive dashboard #211:
   one runs the pipeline; the other explains business outcomes.

---

## Phase 6 — self-service and public surface

Proceed only after one source can traverse the implemented Bronze→Silver path:

```
#266 config design → #267 minimal registration UI
                   └── #268 connector contract and example
                          └── #269 capability matrix and measured benchmarks
```

Park **#65** until a named external-engine consumer requires UniForm. A format
flag without a consumer is compatibility surface with no demonstrated value.

---

## Suggested order

```
1. #301: #310–#322, with #336/#337 signed before XML implementation
2. Resolve #256's rescued-data contract and record one real drift run
3. #112 → #113/#115/#160 → #64/#238
4. #159 → #62 → #61; then #250 and #258–#261
5. #298/#205/#109 → #265/#255
6. #213: #206/#209 and #228 → #210/#211
7. #266 → #267/#268 → #269
8. #65 and #323 stay deferred until their named consumers exist
```

After Phase 1, assemble a promotion sign-off packet with CI results, reviewer
verdict, Databricks evidence, blast radius, rollback notes, and the exact
commits proposed for `dev → staging`. Promotion itself remains a separate,
named Project Lead decision.
