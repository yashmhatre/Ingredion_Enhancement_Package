#!/usr/bin/env python3
"""Enforce path ownership at commit time and in CI.

This replaces the PreToolUse write-lockdown hook as the *enforcing* mechanism.
That hook is still worth having, but it can only ever be advisory in practice:
it runs inside one vendor's agent runtime, it matches on an `agent_type` that
only exists there, a session started from the wrong directory silently never
loads it, and Copilot has no equivalent mechanism at all. It was, in fact, not
running at all on the primary dev machine for its entire life -- and nothing
detected that, because a guard that fails open looks exactly like a guard that
found nothing to complain about.

This check has none of those properties. It runs on the commit, so it applies
to Claude, to Copilot, and to a human editing by hand, and it cannot be
defeated by launching an editor from the wrong folder.

Two rules, both deterministic:

1. **A commit touching a guarded path must declare the owning role**, via an
   `Agent: <role>` trailer in the commit message. If the declared role does
   not own the path, the commit fails.

2. **One commit must not touch two different guarded surfaces.** A change
   spanning `bronze_ingest/` and `notebooks/` is two roles' work and gets
   reviewed as two changes -- see docs/agent_governance.md, and #320/#321 for
   why a bundle parameter and its widget ship sequenced rather than together.

Usage:
    check_path_ownership.py --commit-msg .git/COMMIT_EDITMSG   # commit-msg hook
    check_path_ownership.py --base origin/dev                  # CI
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys

# Guarded surface -> the role that owns it. Paths are matched on whole
# segments, so a sibling directory whose name merely starts with a guarded
# one (resources_archive/) is not caught.
OWNERSHIP: list[tuple[str, str]] = [
    ("bronze_layer/bronze_ingest/", "builder"),
    ("bronze_layer/notebooks/", "notebook-qa"),
    ("bronze_layer/tests/test_notebooks.py", "notebook-qa"),
    ("bronze_layer/resources/", "platform"),
    ("databricks.yml", "platform"),
]

# Roles that may never author a change to a guarded path at all. These are the
# orchestration/planning/read-only layer; the whole point of them is that they
# delegate. Mirrors RESTRICTED_AGENTS in scripts/hooks/block_orchestrator_writes.py.
NON_IMPLEMENTING = {"orchestrator", "architect", "reviewer", "scout"}

TRAILER = re.compile(r"^Agent:\s*([A-Za-z0-9_-]+)\s*$", re.M)


def run(*args: str) -> str:
    return subprocess.run(args, capture_output=True, text=True, check=False).stdout


def owner_of(path: str) -> tuple[str, str] | None:
    """Return (guarded prefix, owning role) for a path, or None if unguarded."""
    p = path.replace("\\", "/").lstrip("./")
    for prefix, role in OWNERSHIP:
        if prefix.endswith("/"):
            if p.startswith(prefix) or f"/{prefix}" in f"/{p}":
                return prefix, role
        elif p == prefix or p.endswith("/" + prefix):
            return prefix, role
    return None


def changed_files(base: str | None) -> list[str]:
    if base:
        merge_base = run("git", "merge-base", base, "HEAD").strip() or base
        out = run("git", "diff", "--name-only", merge_base, "HEAD")
    else:
        out = run("git", "diff", "--cached", "--name-only")
    return [line for line in out.splitlines() if line.strip()]


def commit_messages(base: str | None, msg_file: str | None) -> str:
    if msg_file:
        try:
            with open(msg_file, encoding="utf-8") as fh:
                return fh.read()
        except OSError:
            return ""
    if base:
        merge_base = run("git", "merge-base", base, "HEAD").strip() or base
        return run("git", "log", "--format=%B", f"{merge_base}..HEAD")
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit-msg", dest="msg_file")
    ap.add_argument("--base")
    args = ap.parse_args()

    files = changed_files(args.base)
    guarded = {}
    for f in files:
        hit = owner_of(f)
        if hit:
            guarded.setdefault(hit[1], []).append(f)

    if not guarded:
        print("path-ownership: no guarded paths touched.")
        return 0

    message = commit_messages(args.base, args.msg_file)
    declared = TRAILER.findall(message)

    problems: list[str] = []

    # Rule 2: one guarded surface per commit.
    if len(guarded) > 1:
        problems.append(
            "This change touches more than one guarded surface:\n"
            + "\n".join(
                f"    {role:12} <- {', '.join(paths)}" for role, paths in sorted(guarded.items())
            )
            + "\n  These are different roles' work and get reviewed separately.\n"
              "  Split them into sequenced changes (see docs/agent_governance.md)."
        )

    owning_role = next(iter(guarded)) if len(guarded) == 1 else None

    # Rule 1: the commit must declare who owns it.
    if not declared:
        problems.append(
            "No `Agent:` trailer, but this change touches a guarded path owned by "
            f"`{owning_role or 'several roles'}`.\n"
            "  Add a trailer naming the role that authored it, e.g.:\n"
            f"      Agent: {owning_role or 'builder'}"
        )
    else:
        for role in declared:
            if role in NON_IMPLEMENTING:
                problems.append(
                    f"`{role}` may not author changes to guarded paths -- it is part of "
                    "the orchestration/planning layer.\n"
                    "  Implementation goes to builder, notebook-qa, or platform via a "
                    "filed issue."
                )
            elif owning_role and role != owning_role:
                problems.append(
                    f"Declared `Agent: {role}`, but these paths belong to "
                    f"`{owning_role}`:\n"
                    + "\n".join(f"      {p}" for p in guarded[owning_role])
                )

    if problems:
        print("path-ownership: FAILED\n", file=sys.stderr)
        for p in problems:
            print(f"  - {p}\n", file=sys.stderr)
        print("  See docs/agent_governance.md for who owns what.", file=sys.stderr)
        return 1

    print(f"path-ownership: ok - {', '.join(sorted(guarded))} "
          f"({sum(len(v) for v in guarded.values())} file(s)), declared correctly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
