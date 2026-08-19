# artifacts/

Self-contained HTML documents — runbooks, review pages, validation reports —
that are easier to work through as a rendered page than as Markdown in a
terminal. Open one in a browser; there is no build step and no server.

## Why these are not in `docs/`

`docs/` holds the reasoning: decision records, architecture notes, validation
write-ups. Those are read once and cited afterwards, and Markdown renders them
fine on GitHub.

What lands here is different in kind — a document you *operate*. A staging
runbook is worked through over hours, in order, with a browser open beside a
Databricks workspace, ticking items off as they pass. That wants layout,
state, and tickable checks, none of which Markdown gives you.

Keeping them in the repo rather than only as published pages means they
version with the code they describe. A runbook for `0.6.0` that says
`bronze_ingest-0.6.0` should be recoverable at the commit where that was true.

## Conventions

- **Self-contained.** One file, no external assets. Styles and scripts inline;
  the only permitted remote reference is a Google Fonts stylesheet. A page that
  needs a CDN to render is a page that stops rendering when the CDN moves.
- **Named for what they cover**, plus the version or period they pin:
  `staging_smoke_test_0.6.0.html`, not `runbook.html`. These accumulate.
- **Both themes.** Colours come from CSS custom properties defined for light,
  `prefers-color-scheme: dark`, and an explicit `data-theme` stamp, so the page
  is legible whatever the reader's browser is set to.
- **State is local.** Checkbox progress persists via `localStorage` under a
  key unique to the document. Nothing is sent anywhere, and clearing site data
  resets it.

## Index

| File | Covers | Written |
|---|---|---|
| `staging_smoke_test_0.6.0.html` | Provisioning, deploying and smoke-testing the `0.6.0` release into `ingredion_stg` after the #359 promotion | 2026-08-19 |

Several of these are also published as private Claude artifacts for viewing
away from a checkout. The repo copy is the one under version control; a
published page is a convenience, and can lag.
