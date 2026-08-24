# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those roles to the
actual label strings used in this repo's issue tracker.

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the corresponding
label string from this table.

Edit the right-hand column to match whatever vocabulary you actually use.

## What exists today

All five labels now exist in the tracker. Reuse them; do not create variants.

## These five are orthogonal to the existing vocabulary

The tracker already carries area, kind, priority, and stage labels
(`bronze-layer`, `bug`, `priority-high`, `stage-1`, …). The triage five describe **readiness**,
not subject — they stack on top of the existing labels rather than replacing them. A fully
specified bronze bug is `bronze-layer` + `bug` + `priority-high` + `ready-for-agent`.

See [`issue-tracker.md`](./issue-tracker.md) for the full existing label vocabulary and the
exact strings (note `priority-high` and `priority:medium` use different separators — match
what's there).

## One label is not a readiness signal

`superseded-pending-verification` means scope may be covered by a native platform feature,
and the issue is **frozen, not closed**, until verified in-workspace. It is not a triage
state. Do not close these, and do not move them to `wontfix` — the verification is the gate.

## Readiness is not authorisation

`ready-for-agent` means the issue is specified well enough for an agent to pick up. It does
**not** grant the agent permission to merge, deploy, or promote. `AGENTS.md`'s approval
rules apply regardless of what a triage label says.
