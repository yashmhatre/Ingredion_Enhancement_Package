#!/usr/bin/env python3
"""Tests for the orchestrator write-lockdown PreToolUse hook.

Runs the hook as a subprocess so the real entry point is exercised, not an
imported copy of its internals.

Standalone by design: `python scripts/hooks/test_block_orchestrator_writes.py`.
The pytest suite lives under bronze_layer/ with `testpaths = ["tests"]` and
would never collect this file, and adding a repo-level hook to the bronze
package's suite would put it in the wrong place. CI runs this directly.

These cases exist because the first version of the hook passed review and
still failed open on every Windows-shaped path -- the platform the repo is
actually developed on. Separator and case handling is the whole point.
"""
import json
import subprocess
import sys
from pathlib import Path

HOOK = Path(__file__).with_name("block_orchestrator_writes.py")

WIN_CWD = r"c:\Users\yashm\Downloads\Agentic Metadata Framework\Ingredion_Enhancement_Package"
NIX_CWD = "/home/runner/work/Ingredion_Enhancement_Package"

BLOCK = 2
ALLOW = 0


def run(payload, raw=None):
    stdin = raw if raw is not None else json.dumps(payload)
    proc = subprocess.run(
        [sys.executable, str(HOOK)],
        input=stdin,
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stderr


def edit(path, agent="solution-architect", cwd=WIN_CWD, tool="Edit"):
    return {
        "agent_type": agent,
        "tool_name": tool,
        "cwd": cwd,
        "tool_input": {"file_path": path},
    }


CASES = [
    # (name, payload, expected exit)
    ("relative, forward slashes",
     edit("bronze_layer/bronze_ingest/config.py"), BLOCK),
    ("relative, Windows backslashes",
     edit(r"bronze_layer\bronze_ingest\config.py"), BLOCK),
    ("absolute forward slashes, backslash cwd",
     edit(WIN_CWD.replace("\\", "/") + "/bronze_layer/bronze_ingest/config.py"), BLOCK),
    ("absolute backslashes, backslash cwd",
     edit(WIN_CWD + r"\bronze_layer\bronze_ingest\config.py"), BLOCK),
    ("drive-letter case mismatch",
     edit("C:/Users/yashm/Downloads/Agentic Metadata Framework/"
          "Ingredion_Enhancement_Package/bronze_layer/bronze_ingest/config.py"), BLOCK),
    ("absolute POSIX path, POSIX cwd",
     edit(NIX_CWD + "/bronze_layer/notebooks/run_ingestion.py", cwd=NIX_CWD), BLOCK),
    ("notebooks",
     edit("bronze_layer/notebooks/run_directory_ingestion.py"), BLOCK),
    ("resources",
     edit("bronze_layer/resources/bronze_ingest_jobs.yml"), BLOCK),
    ("databricks.yml at root",
     edit("databricks.yml"), BLOCK),
    ("databricks.yml, absolute",
     edit(WIN_CWD + r"\databricks.yml"), BLOCK),
    ("Write is blocked too",
     edit("bronze_layer/bronze_ingest/audit.py", tool="Write"), BLOCK),
    ("every restricted agent is covered",
     edit("bronze_layer/bronze_ingest/config.py", agent="principal-data-engineer"), BLOCK),
    ("business-analyst covered",
     edit("bronze_layer/bronze_ingest/config.py", agent="business-analyst"), BLOCK),
    ("devops-lead covered",
     edit("bronze_layer/resources/bronze_ingest_jobs.yml", agent="devops-lead"), BLOCK),

    # --- must NOT block ---
    ("data-engineer owns this path",
     edit("bronze_layer/bronze_ingest/config.py", agent="data-engineer"), ALLOW),
    ("main agent has no agent_type",
     {"tool_name": "Edit", "cwd": WIN_CWD,
      "tool_input": {"file_path": "bronze_layer/bronze_ingest/config.py"}}, ALLOW),
    ("docs are allowed",
     edit("docs/roadmap.md"), ALLOW),
    ("business_requirements.md is allowed",
     edit("docs/business_requirements.md", agent="business-analyst"), ALLOW),
    ("non-Edit/Write tool",
     edit("bronze_layer/bronze_ingest/config.py", tool="Read"), ALLOW),
    ("missing file_path",
     {"agent_type": "solution-architect", "tool_name": "Edit",
      "cwd": WIN_CWD, "tool_input": {}}, ALLOW),
    # segment matching, not substring: a sibling directory whose name merely
    # starts with a restricted one must stay writable.
    ("resources_archive is a different directory",
     edit("bronze_layer/resources_archive/old.yml"), ALLOW),
    ("a file named databricks.yml.bak is not databricks.yml",
     edit("databricks.yml.bak"), ALLOW),
]


def main():
    failures = []
    for name, payload, expected in CASES:
        code, stderr = run(payload)
        if code != expected:
            failures.append(
                f"  {name}: expected exit {expected}, got {code}"
                + (f" (stderr: {stderr.strip()[:80]})" if stderr else "")
            )
        elif expected == BLOCK and "BLOCKED" not in stderr:
            failures.append(f"  {name}: blocked but gave no reason on stderr")

    # An unparseable payload is our bug, not the agent's: fail open.
    code, _ = run(None, raw="not json at all")
    if code != ALLOW:
        failures.append(f"  malformed payload: expected exit {ALLOW}, got {code}")

    total = len(CASES) + 1
    if failures:
        print(f"FAILED {len(failures)}/{total}")
        print("\n".join(failures))
        return 1
    print(f"ok - {total} hook cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
