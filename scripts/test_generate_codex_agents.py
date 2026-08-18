#!/usr/bin/env python3
"""Focused tests for the Codex custom-agent renderer."""

from __future__ import annotations

import importlib.util
from pathlib import Path

try:
    import tomllib
except ImportError:  # Python 3.8-3.10: structural assertions still run.
    tomllib = None

SCRIPT = Path(__file__).with_name("generate_codex_agents.py")
SPEC = importlib.util.spec_from_file_location("generate_codex_agents", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_render_produces_valid_required_codex_fields() -> None:
    rendered = MODULE.render(
        {"name": "reviewer", "description": "Read-only adversarial review."},
        "Check the diff and return evidence.",
    )
    assert 'name = "reviewer"' in rendered
    assert 'description = "Read-only adversarial review."' in rendered
    assert 'sandbox_mode = "read-only"' in rendered
    assert 'model_reasoning_effort = "high"' in rendered
    assert "Codex custom subagent" in rendered
    assert "Check the diff" in rendered
    if tomllib is not None:
        assert tomllib.loads(rendered)["name"] == "reviewer"


def test_write_capable_role_inherits_workspace_access() -> None:
    rendered = MODULE.render(
        {"name": "builder", "description": "Implement bronze changes."},
        "Make the requested change.",
    )

    assert 'sandbox_mode = "workspace-write"' in rendered
    if tomllib is not None:
        assert tomllib.loads(rendered)["sandbox_mode"] == "workspace-write"


def test_expected_roster_is_exactly_the_seven_governed_roles() -> None:
    assert MODULE.EXPECTED_ROLES == {
        "architect",
        "builder",
        "notebook-qa",
        "orchestrator",
        "platform",
        "reviewer",
        "scout",
    }


if __name__ == "__main__":
    test_render_produces_valid_required_codex_fields()
    test_write_capable_role_inherits_workspace_access()
    test_expected_roster_is_exactly_the_seven_governed_roles()
    print("Codex agent renderer tests: ok")
