#!/usr/bin/env python3
"""Tests for the path-ownership check.

Standalone by design, exactly like scripts/hooks/test_block_orchestrator_writes.py:
the pytest suite lives under bronze_layer/ with `testpaths = ["tests"]` and
would never collect this file. CI runs it directly.

The check's inputs are git state, so the two functions that read git are
stubbed and everything above them is exercised for real. The path-matching
cases matter most: the hook this replaces shipped a version that passed
review and still failed open on every Windows-shaped path.
"""
import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).with_name("check_path_ownership.py")
spec = importlib.util.spec_from_file_location("check_path_ownership", SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

PASS, FAIL = 0, 1


def check(files, message):
    """Run main() with git reads stubbed out, against a dev base."""
    return check_base(files, message, "origin/dev")


def check_base(files, message, base):
    """Run main() with git reads stubbed out, against an explicit base."""
    mod.changed_files = lambda b: files
    mod.commit_messages = lambda b, msg_file: message
    argv = sys.argv
    sys.argv = ["check_path_ownership.py", "--base", base]
    try:
        return mod.main()
    finally:
        sys.argv = argv


CASES = [
    # --- owner_of: which surface a path belongs to ------------------------
    ("bronze_ingest is builder's",
     lambda: mod.owner_of("bronze_layer/bronze_ingest/config.py")[1] == "builder"),
    ("notebooks are notebook-qa's",
     lambda: mod.owner_of("bronze_layer/notebooks/run_ingestion.py")[1] == "notebook-qa"),
    ("the notebook test file is notebook-qa's",
     lambda: mod.owner_of("bronze_layer/tests/test_notebooks.py")[1] == "notebook-qa"),
    ("resources are platform's",
     lambda: mod.owner_of("bronze_layer/resources/bronze_ingest_jobs.yml")[1] == "platform"),
    ("databricks.yml at root is platform's",
     lambda: mod.owner_of("databricks.yml")[1] == "platform"),
    ("Windows separators are handled",
     lambda: mod.owner_of(r"bronze_layer\bronze_ingest\config.py")[1] == "builder"),
    ("docs are unguarded",
     lambda: mod.owner_of("docs/roadmap.md") is None),
    # segment matching, not substring
    ("resources_archive is a different directory",
     lambda: mod.owner_of("bronze_layer/resources_archive/old.yml") is None),
    ("databricks.yml.bak is not databricks.yml",
     lambda: mod.owner_of("databricks.yml.bak") is None),

    # --- rule 1: the commit must declare its role -------------------------
    ("guarded path with a correct trailer passes",
     lambda: check(["bronze_layer/bronze_ingest/config.py"],
                   "feat: x\n\nAgent: builder\n") == PASS),
    ("guarded path with no trailer fails",
     lambda: check(["bronze_layer/bronze_ingest/config.py"], "feat: x\n") == FAIL),
    ("guarded path with the wrong role fails",
     lambda: check(["bronze_layer/bronze_ingest/config.py"],
                   "feat: x\n\nAgent: platform\n") == FAIL),
    ("an orchestration role may never author guarded paths",
     lambda: check(["bronze_layer/bronze_ingest/config.py"],
                   "feat: x\n\nAgent: orchestrator\n") == FAIL),
    ("architect is likewise refused",
     lambda: check(["bronze_layer/notebooks/run_ingestion.py"],
                   "feat: x\n\nAgent: architect\n") == FAIL),

    # --- rule 2: one guarded surface per commit ---------------------------
    ("mixing two guarded surfaces fails",
     lambda: check(["bronze_layer/bronze_ingest/config.py",
                    "bronze_layer/notebooks/run_ingestion.py"],
                   "feat: x\n\nAgent: builder\n") == FAIL),
    ("notebook + its bundle resource is still two surfaces",
     lambda: check(["bronze_layer/notebooks/run_ingestion.py",
                    "bronze_layer/resources/bronze_ingest_jobs.yml"],
                   "feat: x\n\nAgent: notebook-qa\n") == FAIL),

    # --- unguarded changes are none of this check's business --------------
    ("docs-only change passes with no trailer",
     lambda: check(["docs/roadmap.md", "AGENTS.md"], "docs: x\n") == PASS),
    ("scripts-only change passes with no trailer",
     lambda: check(["scripts/verify.sh"], "chore: x\n") == PASS),
    ("a guarded path plus unguarded files is fine when declared",
     lambda: check(["bronze_layer/bronze_ingest/config.py", "docs/roadmap.md"],
                   "feat: x\n\nAgent: builder\n") == PASS),

    # Release bases. A promotion PR spans every role by construction, so the
    # rules that ask "who authored this" are answered by the PRs that built
    # the diff, not by the promotion. Without this the gate fails a release
    # for being a release, and the only way through is an admin override --
    # which defeats the gate everywhere, not just here.
    ("a dev -> staging promotion is exempt",
     lambda: check_base(["bronze_layer/bronze_ingest/config.py",
                         "bronze_layer/notebooks/run_ingestion.py",
                         "bronze_layer/resources/bronze_ingest_jobs.yml"],
                        "release: promote dev to staging\n",
                        "origin/staging") == PASS),
    ("a staging -> main promotion is exempt",
     lambda: check_base(["bronze_layer/bronze_ingest/config.py",
                         "bronze_layer/notebooks/run_ingestion.py"],
                        "release: promote staging to main\n",
                        "origin/main") == PASS),
    ("the exemption reads the branch, not the remote prefix",
     lambda: check_base(["bronze_layer/bronze_ingest/config.py",
                         "bronze_layer/notebooks/run_ingestion.py"],
                        "release: x\n", "staging") == PASS),

    # The exemption must key on the base branch alone. A feature branch whose
    # NAME contains a release word is still authorship and still gets both
    # rules -- otherwise `feat/staging-cleanup` would be a way to land a
    # two-surface change with no trailer.
    ("a feature branch named after a release base is not exempt",
     lambda: check_base(["bronze_layer/bronze_ingest/config.py",
                         "bronze_layer/notebooks/run_ingestion.py"],
                        "feat: x\n", "origin/feat/staging-cleanup") == FAIL),
    ("a two-surface change into dev still fails",
     lambda: check_base(["bronze_layer/bronze_ingest/config.py",
                         "bronze_layer/notebooks/run_ingestion.py"],
                        "feat: x\n\nAgent: builder\n", "origin/dev") == FAIL),
    ("is_release_base ignores a missing base (commit-msg hook)",
     lambda: mod.is_release_base(None) is False),
]


def main():
    failures = []
    for name, fn in CASES:
        try:
            if not fn():
                failures.append(f"  {name}")
        except Exception as exc:  # noqa: BLE001 - a raising case is a failing case
            failures.append(f"  {name}: raised {exc!r}")

    if failures:
        print(f"FAILED {len(failures)}/{len(CASES)}")
        print("\n".join(failures))
        return 1
    print(f"ok - {len(CASES)} path-ownership cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
