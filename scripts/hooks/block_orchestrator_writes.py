#!/usr/bin/env python3
"""PreToolUse hook: block principal-data-engineer, solution-architect,
business-analyst, and devops-lead from editing implementation code,
notebooks, deploy resources, or databricks.yml directly.

These four agents are the orchestration/planning layer. Implementation
always goes through data-engineer, qa-engineer, or platform-engineer via a
filed issue -- see docs/agent_governance.md and AGENTS.md's agent roster.

Reads the PreToolUse JSON payload from stdin. Exit 0 lets the tool call
proceed; exit 2 blocks it and Claude sees stderr as the reason.
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

RESTRICTED_EXACT_PATHS = {
    "databricks.yml",
}


def to_relative(file_path: str, cwd: str) -> str:
    if cwd and file_path.startswith(cwd):
        return file_path[len(cwd):].lstrip("/")
    return file_path.lstrip("/")


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        # Fail open on a payload we can't parse -- don't block on our own bug.
        return 0

    agent_type = payload.get("agent_type")
    if agent_type not in RESTRICTED_AGENTS:
        return 0

    tool_name = payload.get("tool_name")
    if tool_name not in ("Edit", "Write"):
        return 0

    tool_input = payload.get("tool_input") or {}
    file_path = tool_input.get("file_path", "")
    relative_path = to_relative(file_path, payload.get("cwd", ""))

    is_restricted = relative_path in RESTRICTED_EXACT_PATHS or any(
        relative_path.startswith(prefix) for prefix in RESTRICTED_PATH_PREFIXES
    )
    if is_restricted:
        sys.stderr.write(
            f"BLOCKED: {agent_type} may not {tool_name.lower()} "
            f"{relative_path or file_path}. This path belongs to "
            "data-engineer, qa-engineer, or platform-engineer. File or "
            "update an issue describing the change instead of editing it "
            "directly -- see docs/agent_governance.md.\n"
        )
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
