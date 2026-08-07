# Documentation index — who owns what

Documentation in this repo is spread across two directories and several
files that overlap in subject. This index says which document is
**authoritative** for each subject, so a reader who finds two statements
about the same thing knows which one to believe and which one to fix.

The rule everywhere: **the code wins over any document.** Where two
documents disagree, the owner below wins over the other.

## Living documents — kept current

| Subject | Owner | Notes |
| --- | --- | --- |
| What the package does, how to configure it, how to run it | `bronze_layer/README.md` | The primary reference. Anything a user or administrator needs. |
| Provisioning, grants, environment isolation | `bronze_layer/README.md` § "What an administrator must provision" | Must agree with `databricks.yml`'s header, which is the operational source of truth for the target/variable layout. |
| Deployment targets, variables, run-as identities | `databricks.yml` (header comments) | The file *is* the configuration, so its comments cannot drift from what deploys. |
| Job-level operational settings (concurrency, timeouts, retries) | `bronze_layer/resources/bronze_ingest_jobs.yml` | Same reasoning. |
| Design rationale, delivery sequencing, remaining hardening phases | `bronze_layer/docs/architecture.md` | Phased and architectural. The reader-facing counterpart is `bronze_layer/README.md` § "Not yet implemented"; if they disagree, the open GitHub issues are the tiebreak. |
| **What order the remaining work happens in, and why** | `docs/roadmap.md` | Phase plan over the open issues, with the gating relationships between them. The issues own *what* is left; this owns *when* and *why*. Re-audited against the code when it is updated — treat the phase numbering as current only as of the date in its header. |
| First-time Azure / Databricks setup | `azure_setup.md` | Written from an actual walkthrough, including the real error text encountered. |
| Contribution workflow, branch model | `CONTRIBUTING.md` | |
| What changed in a release, and what to do before deploying it | `CHANGELOG.md` | Written per release, not per PR. The migration steps at its top are the part that matters - several changes in a release are silent until something is queried. |
| Performance numbers (archival cost, files-per-folder guidance) | `bronze_layer/docs/testing_directory_ingestion.md` | **Sole owner of the benchmark.** `architecture.md` and the `_archive_files_parallel` docstring both quote it and link back here; neither should carry an independent number. |

## Point-in-time records — not maintained

These are kept because they record what was verified, when, and why —
not because they describe today's code. Each carries its own header
saying so. Do not update them to match new behaviour; supersede them.

| Document | What it records |
| --- | --- |
| `docs/current_behavior.md` | Audit of the package against the README's claims, for #6. At least one section is explicitly superseded (first-load merge, changed by #46). |
| `docs/architecture_review_2026-07.md` | Whole-repository review of `dev` @ `ceeda69`, 2026-07-29. Its findings became issues #145–#164; those issues, not this document, track their state. |
| `docs/architecture_ai_metadata_2026-08.md` | Review of the AI/metadata half of the target architecture against Databricks native AI services (Data Classification, Data Quality Monitoring, AI Functions, Metric Views, Genie Agents), 2026-08-07. **A proposal, not an accepted design** — `bronze_layer/docs/architecture.md` still owns the AI-layer design until this is adopted. Its § 11 lists exactly what would change there if it is. |
| `bronze_layer/docs/testing_json_reader.md` | Manual ADLS validation of `json_reader.py`. Requires a live cluster; not part of the pytest suite. |
| `bronze_layer/docs/testing_end_to_end_deployment.md` | A real deployment run. Names the catalog as it was at the time (`ingredion_en_dev`), which was later renamed to `ingredion_en` — the run is the record, the names are not current. |

## A note on test counts

Test counts are deliberately **not** stated in any living document. They
go stale on every PR that adds a test, and a stale count is worse than no
count: it invites a reader to conclude a suite shrank. `pytest -q` is the
answer. The point-in-time records above may still quote a count — that is
part of what they record.
