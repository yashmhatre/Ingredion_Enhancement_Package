# Contributing to the Ingredion ELT Enhancement Project

Thanks for your interest in contributing! This document covers everything 
you need to get set up locally, find something to work on, and submit 
changes.

---

## Table of Contents

- [Ways to Contribute](#ways-to-contribute)
- [Local Development Setup](#local-development-setup)
- [Finding Something to Work On](#finding-something-to-work-on)
- [Claiming an Issue](#claiming-an-issue)
- [Branch Naming](#branch-naming)
- [Commit Message Guidelines](#commit-message-guidelines)
- [Making Changes](#making-changes)
- [Testing Requirements](#testing-requirements)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Code Review Process](#code-review-process)
- [Questions](#questions)

---

## Ways to Contribute

- Pick up an open [issue](../../issues) labeled `good first issue` or `help wanted`
- Report bugs by opening a new issue (use the **Task** or **Feature Request** template)
- Improve documentation (README, code comments, this file)
- Add or improve test coverage
- Propose new features by opening a **Feature Request** issue before starting work

## Local Development Setup

```bash
# 1. Clone the repo
git clone https://github.com/<your-org>/<repo-name>.git
cd <repo-name>

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run tests to confirm your setup works
pytest tests/
```

> Update these steps if the project uses `poetry`, `conda`, or a different setup process.

## Finding Something to Work On

1. Go to the [Issues](../../issues) tab
2. Filter by label:
   - `good first issue` — small, well-scoped, low context needed
   - `help wanted` — actively looking for contributors
   - Component labels (`bronze-layer`, `schema-validation`, `error-handling`, etc.) if you want to work in a specific area
3. Check the [Project board](../../projects) — issues in the **Ready** column are scoped and available; issues in **Backlog** may still need refinement before starting

## Claiming an Issue

- Comment on the issue (e.g., "I'd like to take this on") before starting work, to avoid duplicate effort
- If it's not assigned to you within a day or two, feel free to self-assign
- If you start an issue but can't finish it, leave a comment so someone else can pick it up

## Branch Naming

Use a short, descriptive branch name prefixed by type:

```
feature/<short-description>     e.g. feature/schema-validation
fix/<short-description>         e.g. fix/null-field-crash
test/<short-description>        e.g. test/malformed-json-coverage
docs/<short-description>        e.g. docs/update-readme
```

## Commit Message Guidelines

Keep commits focused and descriptive:

```
<type>: <short summary>

<optional longer description>
```

Types: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`

Example:
```
feat: add dead-letter queue for malformed JSON records

Routes invalid records to a quarantine folder with error metadata 
instead of failing the full batch load.
```

## Making Changes

1. Create a branch off `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Make your changes, keeping them scoped to the linked issue
3. Follow existing code style/conventions in the repo
4. Add or update tests for any new behavior
5. Update documentation (README, docstrings, comments) if your change affects usage or setup

## Testing Requirements

- All new features or bug fixes should include test coverage
- Run the full test suite before opening a PR:
  ```bash
  pytest tests/
  ```
- All tests must pass locally before requesting review
- If you're adding a new module/loader, add corresponding tests under `tests/`

## Submitting a Pull Request

1. Push your branch:
   ```bash
   git push origin feature/your-feature-name
   ```
2. Open a PR against `main` — the PR template will auto-populate; fill it out completely
3. Link the related issue using `Closes #<issue-number>` in the PR description
4. Ensure CI checks pass (if configured)
5. Request review

## Code Review Process

- At least one approving review is required before merging (adjust based on your team's actual process)
- Address review comments with additional commits rather than force-pushing, unless asked to squash
- Once approved and checks pass, a maintainer will merge the PR

## Questions

- Open a [Discussion](../../discussions) if enabled, or comment directly on the relevant issue
- For anything unclear about expected behavior/schema, ask before implementing — it saves rework on both sides
