#!/usr/bin/env bash
# scripts/verify.sh
#
# The deterministic quality gate. This replaces the `devops-engineer`
# subagent: every check below has an exact exit code, so spending a model
# turn to run it and interpret the output was pure waste.
#
# Three outcomes per gate, never two:
#   PASS     - the tool ran and was happy.
#   FAIL     - the tool ran and was not.
#   NOT RUN  - the tool could not run in this environment.
#
# NOT RUN is deliberately not FAIL. This repo already learned that lesson
# once: #244 removed a CI check that reported a missing credential as a
# bundle failure. A gate that cannot run tells you nothing about the code,
# and reporting it as red trains everyone to ignore red.
#
# Usage:
#   ./scripts/verify.sh            # local-capable gates
#   ./scripts/verify.sh --ci       # also insist the Spark suite ran
#
# Mirrors .github/workflows/ci.yml. If you change a gate there, change it
# here (and vice versa) - two quality gates that disagree are worse than one.

set -uo pipefail   # deliberately NOT -e: every gate must run, then we total up.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BRONZE="${REPO_ROOT}/bronze_layer"
REQUIRE_SPARK=0
[[ "${1:-}" == "--ci" ]] && REQUIRE_SPARK=1

PASSED=(); FAILED=(); SKIPPED=()

run_gate() {
  # run_gate <name> <working-dir> <command...>
  local name="$1" dir="$2"; shift 2
  printf '\n=== %s ===\n' "$name"
  if ( cd "$dir" && "$@" ); then
    PASSED+=("$name")
  else
    FAILED+=("$name")
  fi
}

skip_gate() {
  local name="$1" why="$2"
  printf '\n=== %s ===\nNOT RUN: %s\n' "$name" "$why"
  SKIPPED+=("$name — $why")
}

# --- Gate 0: is the fetched agent config the one agents.lock pins? --------
# Cheap, deterministic, and catches a failure mode nothing else does: the
# agent definitions on disk silently lagging the pinned version, because
# `ls .claude/agents/` still shows a plausible number of files.
printf '\n=== agents.lock freshness ===\n'
STAMP="${REPO_ROOT}/.claude/agents/.fetched"
if [[ ! -f "$STAMP" ]]; then
  skip_gate "agents.lock freshness" "no .claude/agents/.fetched stamp - run scripts/bootstrap_agents.sh"
elif ! diff -q <(grep -E '^(version|sha)=' "${REPO_ROOT}/agents.lock") \
                <(grep -E '^(version|sha)=' "$STAMP") >/dev/null 2>&1; then
  echo "FAIL: .claude/agents/ was fetched at:"
  grep -E '^(version|sha)=' "$STAMP" | sed 's/^/    /'
  echo "      but agents.lock now pins:"
  grep -E '^(version|sha)=' "${REPO_ROOT}/agents.lock" | sed 's/^/    /'
  echo "      Re-run scripts/bootstrap_agents.sh."
  FAILED+=("agents.lock freshness")
else
  echo "PASS: .claude/agents/ matches agents.lock ($(grep '^version=' "$STAMP" | cut -d= -f2))."
  PASSED+=("agents.lock freshness")
fi

# --- Gate 1: the write-lockdown hook's own tests -------------------------
# CI runs these; they are pure-Python and need nothing but an interpreter.
# Pick an interpreter that actually has the dev deps installed. On Windows
# `python3` is frequently a Store alias pointing at a bare interpreter while
# `python` is the 3.11 environment CONTRIBUTING.md tells you to build, so
# "first one on PATH" picks the wrong one and reports a missing pytest as a
# test failure. Probe for pytest rather than trusting the name.
PY=""
for candidate in python python3 py; do
  command -v "$candidate" >/dev/null || continue
  if "$candidate" -c "import pytest" >/dev/null 2>&1; then PY="$candidate"; break; fi
  [[ -z "$PY" ]] && PY_FALLBACK="$candidate"
done
# No interpreter has pytest: keep one for the stdlib-only gates and let the
# pytest gates report NOT RUN with the real reason.
PY_HAS_PYTEST=1
if [[ -z "$PY" ]]; then PY="${PY_FALLBACK:-}"; PY_HAS_PYTEST=0; fi
if [[ -z "$PY" ]]; then
  skip_gate "hook lockdown tests" "no python interpreter on PATH"
else
  run_gate "hook lockdown tests" "$REPO_ROOT" "$PY" scripts/hooks/test_block_orchestrator_writes.py
fi

# --- Gates 2-5: lint / format / types / security -------------------------
# All no-Spark, so they run anywhere the dev extra is installed.
if ! command -v ruff >/dev/null; then
  skip_gate "ruff format" "ruff not installed (pip install -e '.[dev]' in bronze_layer/)"
  skip_gate "ruff lint"   "ruff not installed"
else
  run_gate "ruff format" "$BRONZE" ruff format --check --diff bronze_ingest tests notebooks
  run_gate "ruff lint"   "$BRONZE" ruff check --output-format=concise bronze_ingest tests notebooks
fi

if command -v mypy >/dev/null; then
  run_gate "mypy" "$BRONZE" mypy bronze_ingest notebooks
else
  skip_gate "mypy" "mypy not installed"
fi

if command -v bandit >/dev/null; then
  run_gate "bandit" "$BRONZE" bandit -q -r bronze_ingest
else
  skip_gate "bandit" "bandit not installed"
fi

if command -v pip-audit >/dev/null; then
  run_gate "pip-audit" "$BRONZE" pip-audit --skip-editable
else
  skip_gate "pip-audit" "pip-audit not installed"
fi

# --- Gate 6: notebook contract tests (no Spark, <1s) ---------------------
# The tests that matter most per AGENTS.md - both known live production
# defects in this repo shipped through untested notebooks - and they need
# no JVM at all. These must never be skipped for "Spark isn't set up".
if [[ -z "$PY" ]]; then
  skip_gate "pytest tests/test_notebooks.py" "no python interpreter on PATH"
elif [[ "$PY_HAS_PYTEST" == "0" ]]; then
  skip_gate "pytest tests/test_notebooks.py" "no interpreter on PATH has pytest - run 'pip install -e \".[dev]\"' in bronze_layer/"
else
  run_gate "pytest tests/test_notebooks.py" "$BRONZE" "$PY" -m pytest -q tests/test_notebooks.py
fi

# --- Gate 7: the full Spark suite ----------------------------------------
# On Windows PySpark needs HADOOP_HOME + winutils.exe for local filesystem
# operations; without it the suite errors in a way that says nothing about
# the code. Report that honestly instead of dressing it up as either colour.
SPARK_BLOCKER=""
if [[ -z "$PY" ]]; then
  SPARK_BLOCKER="no python interpreter on PATH"
elif [[ "$PY_HAS_PYTEST" == "0" ]]; then
  SPARK_BLOCKER="no interpreter on PATH has pytest - run 'pip install -e \".[dev]\"' in bronze_layer/"
elif ! command -v java >/dev/null; then
  SPARK_BLOCKER="java not on PATH (PySpark needs a JVM; CONTRIBUTING.md documents Java 17)"
elif [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]] \
     && [[ ! -x "${HADOOP_HOME:-}/bin/winutils.exe" ]]; then
  SPARK_BLOCKER="Windows without HADOOP_HOME/bin/winutils.exe - see CONTRIBUTING.md 'Local Development Setup'"
fi

if [[ -n "$SPARK_BLOCKER" ]]; then
  skip_gate "pytest (full Spark suite)" "$SPARK_BLOCKER"
  if [[ "$REQUIRE_SPARK" == "1" ]]; then
    echo "  --ci was passed, so this NOT RUN is being counted as a failure."
    FAILED+=("pytest (full Spark suite) — required by --ci but could not run")
  else
    echo "  This is the CI-only gate. Local green here is NOT a green suite;"
    echo "  CI is the only place the Spark tests actually prove anything."
  fi
else
  run_gate "pytest (full Spark suite)" "$BRONZE" "$PY" -m pytest -q
fi

# --- Summary --------------------------------------------------------------
printf '\n%s\n' "------------------------------------------------------------"
printf 'PASSED : %d\n' "${#PASSED[@]}"
printf 'FAILED : %d\n' "${#FAILED[@]}"
printf 'NOT RUN: %d\n' "${#SKIPPED[@]}"
for s in "${SKIPPED[@]:-}"; do [[ -n "$s" ]] && printf '  - %s\n' "$s"; done
for f in "${FAILED[@]:-}"; do [[ -n "$f" ]] && printf '  FAILED: %s\n' "$f"; done

if (( ${#FAILED[@]} > 0 )); then
  printf '\nNOT READY. Fix the failures above.\n'
  exit 1
fi
if (( ${#SKIPPED[@]} > 0 )); then
  printf '\nGates passed, but %d could not run here. Do not report this as\n' "${#SKIPPED[@]}"
  printf 'a full green - name the NOT RUN gates when you hand this over.\n'
  exit 0
fi
printf '\nAll gates green.\n'
