# Roadmap — current sequencing and gates

This is the single source of truth for sequencing. GitHub issues own the acceptance criteria; this document owns order, dependencies, and approval gates.

## Current repo state

- Bronze is the only implemented pipeline layer.
- Batch multi-format is in flight: CSV/Parquet/XML are the active workstream.
- Silver is not yet a working execution layer.
- Gold does not exist.
- Deployment, grants, and environment isolation remain real blockers.
- A green `dev` branch is not a promotion signal.

## Delivery order

### 1) Finish multi-format Bronze

Primary work: #301, #310–#322.

- CSV: register and validate the reader via #310/#311.
- Parquet: implement and register the batch reader via #312/#313.
- XML: sign off on #336/#337 before implementation; then complete #314–#318.
- Keep this batch-only. Do not widen Auto Loader beyond JSON; #323 stays deferred.

### 2) Establish the platform boundary

Primary work: #112, #113, #115, #160, #64, #238.

- Provisioning, Key Vault, secret scopes, deploy readiness, and per-environment Volumes are gates before broader product work.
- #64 cannot be marked complete without governed-tag permissions and workspace proof.

### 3) Make Bronze operationally reliable

Primary work: #159, #62, #61, #250, #258–#261.

- Activate the maintenance policy and prove it in practice.
- Build the operational dashboard and alerting surface over the corrected audit model.
- Use that audit history for volume anomaly baselines.
- Add business invariants and cost/freshness signals only after the pipeline is stable.

### 4) Build the deterministic Bronze→Silver path

Primary work: #205, #298, #109, #265, #255.

- Use the now-closed #324 storage boundary as the common foundation.
- Keep Silver deterministic and lossless; Bronze retains source versions, while Silver owns the standardized transformations and rules.
- Do not move Silver-owned value/range logic into Bronze.

### 5) Product and serving layers

Primary work: #213, #206, #209, #228, #210, #211.

- #228 is the gate for Genie and the executive dashboard.
- Keep operational dashboard #62 separate from executive dashboard #211.
- Autonomous remediation stays within the named, signed guardrails; the eligible fix classes start empty and expand only with approval.

### 6) Self-service and public surface

Primary work: #266–#269.

- Only proceed after one source can traverse the implemented Bronze→Silver path.
- Park #65 and #323 until a real consumer exists.

## Suggested order

1. #301 and #310–#322, with #336/#337 signed before XML implementation
2. Resolve #256’s rescued-data contract and record one real drift run
3. #112 → #113/#115/#160 → #64/#238
4. #159 → #62 → #61, then #250 and #258–#261
5. #298/#205/#109 → #265/#255
6. #213 → #206/#209 and #228 → #210/#211
7. #266 → #267/#268 → #269
8. #65 and #323 stay deferred until a named consumer exists

## Important constraints

- Code wins over issue text when the repo state and the issue description diverge.
- The repo remains Bronze-first; Silver and Gold are not yet working layers.
- Promotion from `dev` to `staging` or `main` remains a separate, named Project Lead decision.

---

This is the master roadmap. Detailed one-session backlog work should live in issue-level tasks, not as a second roadmap file.