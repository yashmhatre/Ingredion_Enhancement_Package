# Buy-vs-build checkpoint — August 2026

Deliberate, documented decisions on significant framework features, for #163.
Each verdict includes the reasoning so that revisits are explicit, not silent.

**Status note (post-August):** Multi-format batch ingestion (CSV, Parquet, XML)
has been implemented. These verdicts remain authoritative for the features
still in backlog, and provide context for why certain architectures were
chosen over platform alternatives.

---

## Verdicts

| Issue | Verdict | One-line reason |
| --- | --- | --- |
| **#109** silver rule engine | **Build** — revisit if Silver ever runs Databricks-only | DQX cannot be constructed without an authenticated workspace, so adopting it makes the entire local suite untestable |
| **#61** volume anomaly detection | **Build** (small) | It is a median over a column this package already owns. DQX has an `anomaly` module, but it inherits the same workspace constraint |
| **#62** ops dashboard + alerts | **Buy** (Lakeview + Databricks SQL alerts) | Already the plan. There is nothing to build but the SQL and the JSON |
| **#64** Unity Catalog TAGS | **Build**, keep `discoverx` in view | Tagging mechanics are ~50 lines against a proven diff-and-apply pattern; classification is the part worth buying, later |
| **#159** lifecycle / OPTIMIZE / VACUUM | **Hybrid** | Rely on predictive optimization where available; ship a maintenance job for retention and quarantine ageing, which nothing else will do |
| **#153** concurrency locking | **Bought already** | `max_concurrent_runs: 1` in the job definition covers the case that occurs |

---

## DQX analysis — why #109 verdict is "build"

**Finding: DQX cannot be instantiated without an authenticated Databricks workspace.**

```python
DQEngine.__init__(self, workspace_client: WorkspaceClient, spark=None, ...)
```

`workspace_client` is required and dereferenced during construction (calls `.clusters`),
before any DataFrame is touched. This makes it unsuitable for a local test suite
(322 tests currently run without a workspace).

**Additional constraints:**
- Requires Python ≥3.10 (this package floors at 3.8)
- Depends on `databricks-sdk~=0.73` (this package deliberately excludes it)
- Adds 5 transitive dependencies to a package whose `install_requires` is currently 1 entry

**Conclusion:** DQX's *design* is worth copying for #109 (severity levels, per-rule result
columns), but the library itself cannot be adopted without removing the core constraint
that keeps Silver testable locally.

---

## Lakeflow Declarative Pipelines (formerly DLT) — verdict unchanged

**Not adopted.** This package's model ("directory defines tables, discovered at runtime")
differs fundamentally from DLT's ("pipeline defines tables"). Three capabilities that
only this package provides:

- Folder-as-table union ingestion
- Per-file archival fallback chain (`processed/` → `quarantine_files/` → leave in place)
- Retry-limit-before-quarantine persisted across runs

**Worth noting:** DLT's automatic table maintenance (#159) exists and works; adopting it
would still leave retry-and-quarantine to build locally.

---

## Verdict details

**#109 — Silver rule engine: BUILD**  
A rolling quality engine for Silver that keeps the suite locally testable (no workspace
required). Design guidance: adopt DQX's severity levels and per-rule result columns.

**#61 — Volume anomaly detection: BUILD (small)**  
A rolling median over `row_count` for the last N runs, window-aggregated over the audit
table this package already owns. Depends on #62 (ops views).

**#62 — Ops dashboard + alerts: BUY (Lakeview + Databricks SQL alerts)**  
Author SQL views over `_ingestion_audit` and check in a `.lvdash.json` dashboard and
alert definitions. Already the plan; no architectural questions remain.

**#64 — Unity Catalog TAGS: BUILD mechanics, defer classification**  
~50 lines to apply tags against `catalog_metadata.py`'s existing pattern. Defer the
decision to *decide what to tag* (PII classification) until the AI metadata layer is
ready. Evaluate `discoverx` at that point as a separate decision.

**#159 — Lifecycle (compaction, VACUUM, quarantine ageing): HYBRID**  
- Bronze compaction: **buy** — Databricks predictive optimization (where enabled)
- VACUUM retention: **build** — floor must exceed CDF consumer lag (30 days recommended)
- Quarantine ageing: **build** — rows that won't pass currently stay forever  
- Audit-table growth: **build** — one row per run can accumulate 1M files/year on streaming

**#153 — Concurrency locking: ALREADY HANDLED**  
`max_concurrent_runs: 1` in the job definition closes the case that occurs (scheduled
run outlasting its interval).

---

## What these decisions unblock

- **#109** may start once `bronze_silver_contract.md` §5 is confirmed
- **#62** may start immediately (author the views)
- **#61** follows #62
- **#64** mechanics may start; classification decision deferred to AI layer

## Reference design

[`adidas/lakehouse-engine`](https://github.com/adidas/lakehouse-engine) solves the same
problem at scale and separates *algorithms* (what), *IO* (where), and *contract* (config).
This package uses the same three-part structure. Worth studying if either layer needs
major refactoring.
