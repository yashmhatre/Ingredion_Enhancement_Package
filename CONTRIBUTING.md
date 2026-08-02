# Contributing to the Ingredion Enhancement Project

Thanks for your interest in contributing! This document covers everything
you need to get set up locally, find something to work on, and submit
changes.

---

## Table of Contents

- [Ways to Contribute](#ways-to-contribute)
- [Local Development Setup](#local-development-setup)
- [Finding Something to Work On](#finding-something-to-work-on)
- [Claiming an Issue](#claiming-an-issue)
- [Branches and the promotion flow](#branches-and-the-promotion-flow)
- [Commit Message Guidelines](#commit-message-guidelines)
- [Making Changes](#making-changes)
- [Testing Requirements](#testing-requirements)
- [Documentation Expectations](#documentation-expectations)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Code Review Process](#code-review-process)
- [Questions](#questions)

---

## Ways to Contribute

- Pick up an open [issue](../../issues) labeled `good first issue` or `help wanted`
- Report bugs by opening a new issue (use the **Task** or **Feature Request** template)
- Improve documentation (README, code comments, docs/ folder, this file)
- Add or improve test coverage
- Propose new features by opening a **Feature Request** issue before starting work

## Local Development Setup

### Prerequisites (get these right first)

Five things must be in place before `pytest` will run the Spark-backed
tests. Each one was found the hard way, in this order, and each produces a
failure that looks like something else (#74). The pure-Python tests — config
validation, notebooks, retry — need none of it and run anywhere.

**1. Java 17.** Spark supports 8/11/17 only. A newer JDK fails at
`SparkSession` creation with `ClassNotFoundException: jdk.internal.ref.Cleaner`.
Install Temurin 17 (what CI uses) and set `JAVA_HOME`.

**2. Python 3.11**, in a venv of its own (e.g. `.venv311`) rather than by
replacing an existing interpreter.

Worth correcting the record, because #74 got this wrong and the wrong
diagnosis cost real time: the issue attributes `TypeError: 'JavaPackage'
object is not callable` to Python 3.14. On PySpark 4.1.x that error no
longer occurs — the session builds fine. What *was* still failing looked
like a version problem and was not; see prerequisite 4.

3.11 remains the recommendation because it is what CI runs and what this
setup was verified against end to end. Whether 3.14 also works once
prerequisite 4 is set has not been tested.

**3. `winutils.exe` + `hadoop.dll` (Windows only).** Without `HADOOP_HOME`,
Spark fails during session creation at `Shell.checkHadoopHomeInner` —
so it blocks every Spark test, including ones that never touch a file.

Match the Hadoop version PySpark bundles, not the newest mirror you find:

```powershell
python -c "import pyspark, glob, os; print(glob.glob(os.path.join(os.path.dirname(pyspark.__file__),'jars','hadoop-client-api-*.jar')))"
```

PySpark 4.1.x bundles Hadoop **3.4.x**. The widely-linked `cdarlint/winutils`
mirror stops at 3.3.6; `kontext-tech/winutils` carries `hadoop-3.4.0-win10-x64`,
which works. Both are third-party binaries — Apache publishes no Windows
builds — so this is a judgement call, not a vendor download.

```powershell
# put winutils.exe and hadoop.dll in <dir>\bin, then:
setx HADOOP_HOME C:\Users\<you>\hadoop
```

**4. `PYSPARK_PYTHON` (Windows).** The single most important one, and the
one that looks exactly like a Python-version problem. Without it, Spark
launches its Python workers with whatever `python` it finds rather than the
venv's, and every test needing a worker dies with `SparkException: Python
worker exited unexpectedly (crashed)`. That is the *same* symptom a wrong
Python version produces, which is why #74 originally attributed it to 3.14 —
setting this fixed 94 of 105 failures on 3.11, and the version alone fixed
none of them.

```powershell
setx PYSPARK_PYTHON <repo>\.venv311\Scripts\python.exe
setx PYSPARK_DRIVER_PYTHON <repo>\.venv311\Scripts\python.exe
```

**5. `SPARK_LOCAL_IP` (Windows, Spark 4.x).** Not in the original issue, and
the reason step 3 alone is not enough. With `winutils` in place the Hadoop
error is replaced by:

```
Py4JError: An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext
Caused by: NullPointerException: ... "idWithoutTopologyInfo" is null
```

That is driver host resolution, not Hadoop:

```powershell
setx SPARK_LOCAL_IP 127.0.0.1
setx SPARK_LOCAL_HOSTNAME localhost
```

Both are **environment** settings on purpose. Pinning the driver host in
`conftest.py` would be a Windows-specific workaround that CI does not need
and that could break Linux.

> **Local Spark on Windows is usable but not fully reliable** even with all
> five in place — session startup intermittently fails with the same
> `JavaSparkContext` error. Re-running usually works. The pure-Python suites
> are unaffected, and CI remains the source of truth for Spark-backed tests.

```bash
# 1. Clone the repo
git clone https://github.com/yashmhatre/Ingredion_Enhancement_Package.git
cd Ingredion_Enhancement_Package/bronze_layer

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows

# 3. Install dependencies (pyspark, delta-spark, pytest, and build via the
#    dev extra). None of this is needed to DEPLOY - the bundle builds its
#    wheel with `python -m pip wheel`, so a deploy needs only the Databricks
#    CLI and works the same from a laptop or from Databricks compute. This
#    is for running the tests locally.
pip install -e ".[dev]"

# 4. Run tests to confirm your setup works
pytest
```

If you're setting up the Azure/Databricks environment from scratch
(storage account, Unity Catalog, external volumes), see
[azure_setup.md](../azure_setup.md) at the repo root — a validated,
step-by-step runbook.

### Quality gates

CI runs four static gates in a `quality` job alongside the test suite. Each
one fails the build on a real finding, and each runs locally in seconds —
none of them needs Java, Spark or a Databricks connection.

```bash
pip install ruff mypy bandit pip-audit

ruff format bronze_ingest tests notebooks        # apply formatting
ruff format --check bronze_ingest tests notebooks # what CI checks
ruff check bronze_ingest tests notebooks          # lint
ruff check --fix bronze_ingest tests notebooks    # lint, auto-fixing what is safe
mypy bronze_ingest notebooks                      # types
bandit -r bronze_ingest                           # static security scan
pip-audit --skip-editable                         # dependency CVEs
```

All configuration lives in [`bronze_layer/pyproject.toml`](bronze_layer/pyproject.toml)
— there are no separate `.ruff.toml`/`mypy.ini`/`setup.cfg` files, and
`pytest.ini` was folded in too, so `pytest` picks its settings up from the
same place.

**Run `ruff format` before you commit.** CI checks formatting rather than
applying it, so an unformatted file fails the build instead of being
quietly fixed.

**Suppressions carry a reason.** If a gate flags something that is genuinely
correct, annotate the specific line with the specific code and say why —
`# noqa: BLE001 - a missing retry-state file is the normal first-run case`,
or `# nosec B608 - identifiers are validated at config load (#154)`. Do not
widen the config, lower a severity threshold, or disable a rule globally to
make a single finding go away. A blanket suppression silently covers the
next occurrence too, which is the one nobody looked at.

Two known traps, both hit while setting these up:

- `bandit` anchors a multi-line-string finding on the line where the string
  **opens**. Appending `# nosec` there puts the comment *inside* the string
  — for SQL, that means shipping a comment to the engine. Assign the string
  to a variable and put the marker on the closing `"""`.
- `ruff format` wraps a long trailing comment by parenthesising the value
  next to it, turning `field: Optional[str] = None  # long comment` into
  `field: Optional[str] = (None  # long comment)`. Put long comments *above*
  the line instead.

### Notebooks need tests too

`bronze_layer/notebooks/` holds the **deployed job entrypoints** — the code
the Databricks job actually runs. A change there requires a test, exactly as
a change to the package does.

This is not a style rule. Both known live production defects (#144, #145)
were in that layer, and neither could have been caught by any number of
library tests, because nothing executed those files. `tests/test_notebooks.py`
now does, using the `run_notebook` fixture in `conftest.py`, which supplies
fakes for the three names the Databricks kernel injects (`dbutils`, `spark`,
`display`). It needs no Spark, no Java and no workspace, and the whole file
runs in under a second:

```bash
pytest tests/test_notebooks.py
```

Two contract tests there are worth knowing about before you rename anything:

- **Widget ↔ bundle**, both directions. Every `base_parameters` key in
  `resources/*.yml` must have a matching `dbutils.widgets.*` declaration, and
  every blank-default widget must be supplied by the bundle. A mismatch means
  the configured value is silently ignored and a default nobody chose takes
  effect — which is precisely how a production quality rule came to be inert.
- **Import surface.** Every name a notebook imports from `bronze_ingest` must
  be in `__all__`, since notebooks run against the installed wheel and an
  import error surfaces only after compute has started.

**Do not add a dependency to a notebook without declaring it.** `pandas` was
imported by two notebooks and declared in no extra of `setup.py`; it worked
only because the Databricks runtime happens to ship it. Both now build their
DataFrames with an explicit schema instead, and a test asserts no notebook
imports it.

### Coverage

Coverage is reported on every CI run and posted to the PR comment. It is
deliberately **not** enforced — there is no `--cov-fail-under`, because a
threshold picked before the real number is known is either trivially met or
immediately red. Locally:

```bash
pytest --cov=bronze_ingest --cov-report=term-missing
```

## Finding Something to Work On

1. Go to the [Issues](../../issues) tab
2. Filter by label:
   - `good first issue` — small, well-scoped, low context needed
   - `help wanted` — actively looking for contributors
   - `bug` / `enhancement` / `testing` — component-based filtering
3. Check the [Project board](../../projects) — issues in the **Ready**
   column are scoped and available; issues in **Backlog** may still need
   refinement before starting

## Claiming an Issue

- Comment on the issue (e.g., "I'd like to take this on") before starting work, to avoid duplicate effort
- If it's not assigned to you within a day or two, feel free to self-assign
- If you start an issue but can't finish it, leave a comment so someone else can pick it up

## Branches and the promotion flow

Three long-lived branches, one per deployment environment:

| Branch | Environment | Unity Catalog schema | Deployed as |
|---|---|---|---|
| `dev` | development | `ingredion_en.ingredion_dev` | the deploying user |
| `staging` | pre-production | `ingredion_en.ingredion_stg` | staging service principal |
| `main` | **production** | `ingredion_en.ingredion_prd` | prod service principal |

All three share the `ingredion_en` catalog; the **schema** is the isolation
boundary, so grants are made at schema level rather than catalog level. See
the Deployment section of `bronze_layer/README.md`.

Changes flow one way, and every arrow is a pull request:

```
feature/*  →  dev  →  staging  →  main (prod)
```

**Branch your work off `dev`, and open your PR against `dev`** — not `main`.
`main` is production; nothing lands there except a promotion from `staging`,
or an urgent hotfix (see below).

Use a short, descriptive branch name prefixed by type:

```
feature/<short-description>     e.g. feature/multi-format-reader
fix/<short-description>         e.g. fix/jsonl-discovery
test/<short-description>        e.g. test/json-reader-validation
docs/<short-description>        e.g. docs/update-readme
refactor/<short-description>    e.g. refactor/rename-bronze-layer
```

### Hotfixes

A fix that unblocks production may go straight to `main`. It must then be
**back-merged into `dev` and `staging` immediately**, or those branches
silently carry the bug and will reintroduce it at the next promotion. This
is the known cost of environment branches: they drift whenever `main` moves
independently, and nothing detects that automatically.

## Commit Message Guidelines

Keep commits focused and descriptive:

```
<type>: <short summary>

<optional longer description>
```

Types: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`

Example:
```
feat: retry-limit before quarantining permanently-failing files

Files that fail ingestion (not just the archival move) are now tracked
across runs and quarantined after N consecutive failures, instead of
retrying forever with no signal that a human needs to intervene.
```

## Making Changes

1. Create a branch off `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. **Open or reference a task/issue before starting significant work** —
   this project follows a task-first workflow: draft the issue (context,
   what needs to be done, acceptance criteria), then resolve it
   incrementally, one file/step at a time, validating before moving on.
   For anything beyond a trivial fix, this saves rework and keeps intent
   traceable.
3. Make your changes, keeping them scoped to the linked issue
4. Follow existing code style/conventions in the repo — in particular:
   - Config additions to `IngestionConfig` should be additive with sane
     defaults (never break existing config files)
   - New reader/writer logic should follow the existing dual-environment
     pattern (`dbutils` when available, plain Python fallback otherwise —
     see `directory_ingestion.py`'s `_try_dbutils_ls` / `_try_posix_ls`
     for the pattern to follow)
5. Add or update tests for any new behavior
6. Update documentation (README, docstrings, `docs/` folder) if your
   change affects usage, setup, or discovered a non-obvious gotcha others
   are likely to hit

## Testing Requirements

This project has two layers of testing — know which one your change needs:

**1. Local pytest suite** (`bronze_layer/tests/`)
```bash
cd bronze_layer
pytest
```
Covers config validation, flatten/quality logic, directory ingestion,
file archival, retry-limit behavior, folder-as-table, and the run-level
audit trail. Uses a local, Delta-enabled `SparkSession` — no Databricks
connection needed, and the suite is also environment-aware enough to run
directly on a Databricks cluster if needed (see `conftest.py`'s `spark`
and `json_test_dir` fixtures for how that works).

**2. Real-environment validation** (`bronze_layer/docs/` + associated
notebooks), needed when a change touches:
- Reading behavior against real cloud storage (see
  `notebooks/validate_json_reader.py` and
  [testing_json_reader.md](docs/testing_json_reader.md))
- Deployment or job configuration (`databricks.yml`, notebook entrypoints)
  — see [testing_end_to_end_deployment.md](docs/testing_end_to_end_deployment.md)
  for the kind of validation expected before considering a deployment
  change done

**Before opening a PR:**
- All local pytest tests pass
- If your change affects deployment or real-storage behavior, note what
  real-environment validation (if any) was done in the PR description
- If you hit a genuine environment gotcha (stale config value, path
  mismatch, unexpected Spark behavior) while testing, document it — see
  below

## Documentation Expectations

If you discover something non-obvious while building or testing a
change — a Spark behavior that surprised you, an environment
misconfiguration, a bug in test infrastructure itself — write it down.
The `docs/` folder's testing files are full of exactly this kind of
finding, and they've repeatedly saved the next person from repeating the
same debugging session. A good entry includes: what was expected, what
actually happened, why, and how it was resolved.

## Submitting a Pull Request

1. Push your branch:
   ```bash
   git push origin feature/your-feature-name
   ```
2. Open a PR against **`dev`** — the PR template will auto-populate; fill it out completely.
   (Only promotions and hotfixes target `staging` or `main`.)
3. Link the related issue using `Closes #<issue-number>` in the PR description
4. CI runs automatically via GitHub Actions on every PR
   (`.github/workflows/ci.yml`) — two required checks:
   - **`test`** — the full pytest suite
   - **`wheel`** — the package builds, ships no test/notebook/config files,
     reports a version matching its filename, and imports standalone
   CI runs on PRs into `dev`, `staging` and `main`, so each gate in the
   promotion chain is checked rather than only the last one.
5. Request review — at least 1 approval required before merging into `main`

## Code Review Process

- At least one approving review is required before merging
- Address review comments with additional commits rather than force-pushing, unless asked to squash
- Once approved and CI passes, a maintainer will merge the PR

## Questions

- Open a [Discussion](../../discussions) if enabled, or comment directly on the relevant issue
- For anything unclear about expected behavior/schema, ask before implementing — it saves rework on both sides