# MEMORY.md

How an AI coding agent (or anyone new) should work with Yash on the
Ingredion Enhancement Package. This is a living memory file — update it in
place as conventions prove wrong or new ones emerge, the same way
`docs/README.md` treats its living documents.

## Voice — tone, phrasing, writing corrections

- Two audiences, never blended: `docs/overview.md` (and anything else
  aimed at non-engineers) is plain language, no jargon, short sentences —
  a business stakeholder should be able to read it cold. Technical docs
  (`bronze_layer/docs/architecture.md`, etc.) can assume the reader knows
  Databricks/Unity Catalog/Delta.
- Crisp over comprehensive. Cut filler, cut repetition across docs — one
  doc owns a subject, others link to it (`docs/README.md`'s ownership
  rule).
- No invented specifics. Don't state a number, a date, or a "this is
  fixed" claim unless it's backed by the code, a test, or an explicit
  decision from Yash.

## Process — how I want tasks done

- **Documentation-only changes stay documentation-only.** Don't let a
  docs pass turn into a quiet architecture change — open questions (like
  BR-001's) get flagged, not resolved, unless Yash explicitly decides
  them.
- **Archive, don't delete.** Superseded docs move to `docs/archive/` (or
  `bronze_layer/docs/archive/`) with a banner explaining what superseded
  them. Content is preserved, not lost — old paths only get removed once
  the archived copy exists and every cross-reference is updated.
- **The boundary is the branch.** Everything into `dev` is an agent's own
  call, made with CI genuinely green (real pass counts, not a run the
  `WATCHED` regex skipped); everything out of it is Yash's. Agents
  open/update PRs into `dev` and stack related work onto an existing open
  PR rather than opening duplicate/competing PRs. Staging/prod deploys,
  GRANT/DROP/VACUUM, credentials, and the promotion PRs `dev` → `staging`
  and `staging` → `main` always need Yash's own named sign-off.
- **Task-first.** Non-trivial work should trace back to an open GitHub
  issue. If a request implies work with no issue, say so before starting.
- Judgment calls that aren't explicitly confirmed (e.g., stacking commits
  onto an existing PR branch instead of opening a new one) get flagged
  clearly to Yash, not buried in a commit message.

## People — who people are, relationships

- **Yash** — Project Lead on this project. Final say on architecture,
  promotions, credentials, and any staging/prod or destructive action.

## Projects — active work, current tasks, status

- **Bronze layer** — production-ready, deployed, tested. The only working layer today.
- **Silver / Gold layers** — not built. Treat anything under `silver_layer/` as aspirational, not working code.
- **Multi-format batch ingestion** — CSV, Parquet, XML readers implemented and tested. Streaming multi-format support deferred.
- **Agent infrastructure** — private agent-config architecture and pinned bootstrap mechanism in place; the agent roster itself is being rebuilt from scratch.

## Output — formats, naming, delivery preferences

- Markdown docs live in the repo, not as chat-only output — anything
  meant to last gets committed.
- Diagrams: Mermaid embedded directly in the relevant `.md` file for
  anything durable and in-repo; Excalidraw for live, in-conversation
  visual walkthroughs that don't need to be a tracked file.
- Commit style: `<type>: <short summary>` (`feat`, `fix`, `test`, `docs`,
  `refactor`, `chore`), one thing per commit.
- Branch model: `feature/* → dev → staging → main`, PRs against `dev`
  only, never a direct commit to `main`.

## Tools — which tools to use and how

- GitHub MCP tools for all repo operations (branches, file pushes, PRs) —
  authenticated, auditable, no local git shortcuts.
- Claude Code subagents in `.claude/agents/` — prefer the matching
  subagent over general-purpose editing when one fits the task.
- Excalidraw MCP for architecture walkthroughs when a live, inspectable
  diagram is more useful than static Mermaid.
