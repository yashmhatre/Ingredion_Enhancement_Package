# Business requirements register

This folder is the concise, working register for business asks that are being reconciled against the repo's current decisions and roadmap. It is intentionally smaller than the historical single-file register and is meant to be scanned quickly.

## Status meanings

| Status | Meaning |
| --- | --- |
| Proposed | Captured, not yet reconciled |
| Under review | Reconciliation done; waiting on a decision or decomposition |
| Approved → issues filed | Scope is clear and linked to issues |
| Deferred | Real, but not now |
| Rejected | Closed as not going to be built |

## Current register

| ID | Title | Status | Summary |
| --- | --- | --- | --- |
| BR-001 | AI-powered metadata-driven manufacturing intelligence platform | Approved → issues filed | Platform-level ask covering Bronze, Silver, AI monitoring, Genie Spaces, dashboards, and MLflow |
| BR-002 | AI-assisted Silver transformation, constrained to minimal metadata | Under review | A Silver-specific AI ask; reading A is the recommended interpretation |

## Completion gate

A business requirement is considered actionable only when all four are true:

1. The ask is written in a single, named business case.
2. The repo has reconciled it against existing decisions and constraints.
3. The work is split into issue-sized scope rather than one platform-sized statement.
4. Open questions are explicit and not silently filled in.

## Template for each BR

Each BR file should carry the same structure:

- Status
- Problem statement
- Solution as stated
- Reconciliation summary
- Decision summary
- Issues filed
- Open questions
- Approval gate / final read

## How to use this folder

- Start at this file for the status summary.
- Read the BR-specific file for the current scope, decisions, and open questions.
- Use the existing repo docs for the technical decisions that carry weight in this repo:
  - `docs/roadmap.md` for sequencing
  - `docs/buy_vs_build_2026-08.md` for build-vs-buy decisions
  - `docs/bronze_silver_contract.md` for Bronze/Silver contract boundaries
  - `AGENTS.md` for approval and ownership rules

---

## BR summary on one page

### BR-001

- Request: A full AI-enabled manufacturing intelligence platform.
- Current position: Most of this is a restatement of work already in motion or already decided.
- New scope that is genuinely new: Genie Spaces, AI autonomous remediation, and an executive dashboard separate from the existing ops dashboard.
- Key decisions already made:
  - Autonomous remediation is allowed in a bounded Bronze exception.
  - The existing ops dashboard stays as #62.
  - A separate business-facing dashboard is a separate issue.
- Key gate: This is not one feature; it is a platform-level intake item that must be split into issue-sized work.

### BR-002

- Request: "Process bronze data using AI in silver with giving ai minimal metadata."
- Current position: Under review.
- Recommended interpretation: AI drafts metadata-derived transformation/rule specs; Silver's rule engine executes them.
- Not recommended: AI as the live, in-write-path per-row transformer for Silver.
- Open requirement: Yash must confirm whether the intent is advisory rule drafting or a Silver-scoped autonomous transformation path.

