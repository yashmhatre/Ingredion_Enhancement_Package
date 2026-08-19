#!/usr/bin/env python3
"""Focused tests for repository-local third-party skill aliases."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from tempfile import TemporaryDirectory

SCRIPT = Path(__file__).with_name("configure_agent_skills.py")
SPEC = importlib.util.spec_from_file_location("configure_agent_skills", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _install_upstream_skill(skills_dir: Path) -> None:
    skill_dir = skills_dir / "code-review"
    (skill_dir / "agents").mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: code-review\ndescription: Review changes.\n---\n",
        encoding="utf-8",
    )
    (skill_dir / "agents" / "openai.yaml").write_text(
        'interface:\n  display_name: "Code Review"\n'
        '  default_prompt: "Use $code-review to review this branch."\n',
        encoding="utf-8",
    )


def test_alias_is_complete_and_idempotent() -> None:
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        skills_dir = root / ".agents" / "skills"
        claude_skills_dir = root / ".claude" / "skills"
        _install_upstream_skill(skills_dir)
        claude_skills_dir.mkdir(parents=True)
        MODULE._create_directory_link(
            claude_skills_dir / "code-review", skills_dir / "code-review"
        )

        assert MODULE.configure(skills_dir, claude_skills_dir) == 0
        alias = skills_dir / "two-axis-code-review"
        assert not (skills_dir / "code-review").exists()
        assert not os.path.lexists(claude_skills_dir / "code-review")
        claude_alias = claude_skills_dir / "two-axis-code-review"
        assert claude_alias.is_dir()
        assert claude_alias.resolve() == alias.resolve()
        assert "name: two-axis-code-review" in (alias / "SKILL.md").read_text(
            encoding="utf-8"
        )
        interface = (alias / "agents" / "openai.yaml").read_text(encoding="utf-8")
        assert 'display_name: "Two-Axis Code Review"' in interface
        assert "$two-axis-code-review" in interface

        first_pass = {
            path.relative_to(alias): path.read_bytes()
            for path in alias.rglob("*")
            if path.is_file()
        }
        assert MODULE.configure(skills_dir, claude_skills_dir) == 0
        second_pass = {
            path.relative_to(alias): path.read_bytes()
            for path in alias.rglob("*")
            if path.is_file()
        }
        assert second_pass == first_pass


if __name__ == "__main__":
    test_alias_is_complete_and_idempotent()
    print("Agent skill alias tests: ok")
