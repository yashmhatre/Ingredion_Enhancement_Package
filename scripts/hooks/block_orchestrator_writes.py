#!/usr/bin/env python3
"""PreToolUse hook: block principal-data-engineer, solution-architect,
business-analyst, and devops-lead from editing implementation code,
notebooks, deploy resources, or databricks.yml directly.

These four agents are the orchestration/planning layer. Implementation
always goes through data-engineer, qa-engineer, or platform-engineer via a
filed issue -- see docs/agent_governance.md and AGENTS.md's agent roster.

Reads the PreToolUse JSON payload from stdin. Exit 0 lets the tool call
proceed; exit 2 blocks it and Claude sees stderr as the reason.

Path handling note: this runs on Windows as well as CI's Linux, and the two
disagree about separators and drive-letter case. `tool_input.file_path` may
arrive absolute or relative, with either separator, and may not share a
separator style with `cwd`. Matching therefore normalises to forward slashes
and compares case-insensitively, and looks for the restricted segments
*anywhere* in the path rather than only at the front -- so an absolute path
is caught even when `cwd` cannot be stripped from it. A guard that fails open
on a path it merely failed to parse is not a guard.
"""
import json
import sys

RESTRICTED_AGENTS = {
    "principal-data-engineer",
    "solution-architect",
    "business-analyst",
    "devops-lead",
}

RESTRICTED_PATH_PREFIXES = (
    "bronze_layer/bronze_ingest/",
    "bronze_layer/notebooks/",
    "bronze_layer/resources/",
)

RESTRICTED_FILENAMES = ("databricks.yml",)


def normalise(path: str) -> str:
    """Forward slashes, no trailing separator, lowercased for comparison."""
    return path.replace("\\", "/").strip().rstrip("/").lower()


def to_relative(file_path: str, cwd: str) -> str:
    """Strip cwd when it genuinely prefixes file_path; otherwise leave it be.

    Returns a normalised path either way. Callers must not assume the result
    is relative -- is_restricted handles both.
    """
    normalised_file = normalise(file_path)
    normalised_cwd = normalise(cwd)
    if normalised_cwd and normalised_file.startswith(normalised_cwd + "/"):
        return normalised_file[len(normalised_cwd) + 1:]
    return normalised_file.lstrip("/")


def is_restricted(path: str) -> bool:
    """True when path names a file one of these agents must not write.

    `path` is expected to be normalise()d. Matching is on whole segments, so
    a directory merely *starting* with a restricted name cannot slip through
    and cannot false-positive either.
    """
    padded = "/" + path.lstrip("/")
    if any(("/" + prefix) in padded for prefix in RESTRICTED_PATH_PREFIXES):
        return True
    return any(padded.endswith("/" + name) for name in RESTRICTED_FILENAMES)


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Fail open only here: a payload we cannot parse is our bug, not the
        # agent's, and blocking every edit on it would be worse than useless.
        return 0

    agent_type = payload.get("agent_type")
    if agent_type not in RESTRICTED_AGENTS:
        return 0

    if payload.get("tool_name") not in ("Edit", "Write"):
        return 0

    tool_input = payload.get("tool_input") or {}
    file_path = tool_input.get("file_path") or ""
    if not file_path:
        return 0

    relative_path = to_relative(file_path, payload.get("cwd") or "")
    if not is_restricted(relative_path):
        return 0

    sys.stderr.write(
        f"BLOCKED: {agent_type} may not {payload['tool_name'].lower()} "
        f"{relative_path or file_path}. This path belongs to "
        "data-engineer, qa-engineer, or platform-engineer. File or "
        "update an issue describing the change instead of editing it "
        "directly -- see docs/agent_governance.md.\n"
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
