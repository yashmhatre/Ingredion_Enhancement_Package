#!/usr/bin/env python3
"""Apply repository-local aliases to installed third-party agent skills."""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SKILLS_DIR = REPO_ROOT / ".agents" / "skills"
SOURCE_NAME = "code-review"
ALIAS_NAME = "two-axis-code-review"
ALIAS_DISPLAY_NAME = "Two-Axis Code Review"


def _remove_link(path: Path) -> None:
    """Remove a managed directory link without touching its target."""
    if path.is_symlink():
        path.unlink()
    elif sys.platform == "win32" and (
        os.lstat(path).st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT
    ):
        # Path.is_junction() only exists in Python 3.12. The repository and
        # CI also support 3.11, where installer-created junctions must be
        # identified through their reparse-point attribute instead.
        os.rmdir(path)
    else:
        raise RuntimeError(f"refusing to replace non-link path: {path}")


def _create_directory_link(link: Path, target: Path) -> None:
    """Create the runtime link used by Claude, including on Windows."""
    link.parent.mkdir(parents=True, exist_ok=True)
    try:
        link.symlink_to(target.resolve(), target_is_directory=True)
    except OSError:
        if sys.platform != "win32":
            raise
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target.resolve())],
            check=True,
            capture_output=True,
            text=True,
        )


def configure(skills_dir: Path, claude_skills_dir: Path | None = None) -> int:
    """Apply aliases inside an installed skill pack."""
    source = skills_dir / SOURCE_NAME
    alias = skills_dir / ALIAS_NAME

    if not skills_dir.is_dir():
        print(
            "error: .agents/skills/ is missing; install the pack first with "
            "`npx skills@latest add mattpocock/skills`.",
            file=sys.stderr,
        )
        return 1

    if source.exists() and alias.exists():
        shutil.rmtree(alias)

    if source.exists():
        source.rename(alias)
    elif not alias.exists():
        print(f"error: neither {source} nor {alias} exists.", file=sys.stderr)
        return 1

    skill_file = alias / "SKILL.md"
    content = skill_file.read_text(encoding="utf-8")
    renamed, count = re.subn(
        rf"(?m)^name:\s*{re.escape(SOURCE_NAME)}\s*$",
        f"name: {ALIAS_NAME}",
        content,
        count=1,
    )
    already_renamed = re.search(rf"(?m)^name:\s*{re.escape(ALIAS_NAME)}\s*$", content)
    if count == 0 and not already_renamed:
        print(f"error: {skill_file} has no expected name field.", file=sys.stderr)
        return 1
    if count:
        skill_file.write_text(renamed, encoding="utf-8")

    # The folder and SKILL.md name control invocation. Keep the optional UI
    # metadata aligned too, so pickers do not present a second "Code Review"
    # entry that looks indistinguishable from the built-in command.
    interface_file = alias / "agents" / "openai.yaml"
    if interface_file.exists():
        interface = interface_file.read_text(encoding="utf-8")
        interface = re.sub(
            r'(?m)^(\s*display_name:\s*)["\']Code Review["\']\s*$',
            rf'\1"{ALIAS_DISPLAY_NAME}"',
            interface,
            count=1,
        )
        interface = interface.replace(f"${SOURCE_NAME}", f"${ALIAS_NAME}")
        if interface != interface_file.read_text(encoding="utf-8"):
            interface_file.write_text(interface, encoding="utf-8")

    # The installer also exposes every skill through .claude/skills. Renaming
    # only the vendored directory leaves its old junction dangling and Claude
    # unable to discover the alias, so update that runtime link atomically.
    if claude_skills_dir is not None and claude_skills_dir.is_dir():
        old_link = claude_skills_dir / SOURCE_NAME
        alias_link = claude_skills_dir / ALIAS_NAME
        for path in (old_link, alias_link):
            if os.path.lexists(path):
                _remove_link(path)
        _create_directory_link(alias_link, alias)

    print(f"Configured {ALIAS_NAME}; the built-in /code-review name is now free.")
    return 0


def main() -> int:
    return configure(SKILLS_DIR, REPO_ROOT / ".claude" / "skills")


if __name__ == "__main__":
    sys.exit(main())
