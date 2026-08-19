# Documentation index

This repo has a lot of documentation, and not all of it is live guidance. This file is the map: it tells you which document owns a subject and which one to trust when two docs disagree.

The rule is simple: the code wins. If two documents disagree, the owner below wins over the other.

## Living documents

| Subject | Owner | Notes |
| --- | --- | --- |
| package behavior, config, and run steps | `bronze_layer/README.md` | primary reference for users and administrators |
| provisioning, grants, and environment isolation | `bronze_layer/README.md` + `databricks.yml` | the config file and the README must match |
| deployment targets and variable layout | `databricks.yml` | operational source of truth |
| job-level operational settings | `bronze_layer/resources/bronze_ingest_jobs.yml` | same idea as above |
| design rationale and sequencing | `bronze_layer/docs/architecture.md` | implementation view; read `docs/roadmap.md` for the order of work |
| remaining work, phases, and dependencies | `docs/roadmap.md` | phase plan over the open issues |
| first-time Azure and Databricks setup | `azure_setup.md` | based on the real setup steps and error text |
| contribution rules and branch flow | `CONTRIBUTING.md` | keeps the repo workflow consistent |
| release notes and deployment warnings | `CHANGELOG.md` | migration steps matter more than the summary |
| performance numbers | `bronze_layer/docs/testing_directory_ingestion.md` | the benchmark owner |
| AI agent authority and approval rules | `docs/agent_governance.md` | this is the policy document |
| why agent prompt content is not committed here | `docs/private_agent_architecture.md` | explains the private repo + pinned fetch model |
| non-technical overview | `docs/overview.md` | plain-language companion to the technical docs |
| business intake and reconciliation | `docs/business_requirements.md` | owned by `architect` |
| skill config for this repo | `docs/agents/` | `issue-tracker.md`, `triage-labels.md`, and `domain.md` |

## Decision records

`docs/decisions/` holds records that bind once they are signed off. They are not archive material and they are not updated casually.

| Document | What it decides | Status |
| --- | --- | --- |
| `docs/decisions/2026-08_autonomous_remediation.md` | whether the AI layer may write to the pipeline and under what bounds | signed off |
| `docs/decisions/2026-08_ai_genie_architecture.md` | AI architecture choices and scope | signed off |
| `docs/decisions/2026-08_xml_integrity_policy.md` | XML failure and integrity policy | proposed; waiting on Tier 2 sign-off |
| `docs/decisions/2026-08_xml_namespace_identifiers.md` | XML namespace mapping and stability rules | proposed; waiting on Tier 2 sign-off |

## Archive material

These are kept for historical context. They record what was true at the time, not what is current.

| Document | What it records |
| --- | --- |
| `docs/archive/current_behavior.md` | repo state during earlier review work |
| `docs/archive/architecture_review_2026-07.md` | earlier architecture review |
| `docs/architecture_ai_metadata_2026-08.md` | design review that later fed a decision record |
| `bronze_layer/docs/archive/testing_json_reader.md` | manual cluster validation |
| `bronze_layer/docs/archive/testing_end_to_end_deployment.md` | real deployment record |

## Test counts

Do not state a test count in a living doc. It goes stale quickly and becomes misleading. Use `pytest -q` when you need the current answer.
