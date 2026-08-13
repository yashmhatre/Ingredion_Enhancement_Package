#!/usr/bin/env python3
"""Validate the frontmatter of every fetched agent definition.

This gate exists because of a failure that cost real time and gave no signal.
Two definitions carried `Read-only: ` inside an unquoted `description:` value.
A colon followed by a space is not legal in a YAML plain scalar -- the parser
reads it as a nested mapping and rejects the whole block with "mapping values
are not allowed here" -- so the runtime skipped both files.

Nothing reported it. Five of seven agents registered, the two broken files sat
on disk looking entirely normal, and the only visible symptom was two names
missing from a list nobody had a reason to count. A definition that fails to
load is indistinguishable from one that was never written.

Checks, per file:
  1. the frontmatter block exists and parses;
  2. `name:` matches the filename, because that string is what the
     write-lockdown hook and the chat-mode generator both key on.

PyYAML is used when importable, for a real parse. When it isn't, the check
falls back to detecting the specific defect class above rather than reporting
NOT RUN -- a plain-scalar colon is worth catching even without a parser.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

AGENTS_DIR = Path(__file__).resolve().parent.parent / ".claude" / "agents"

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

# A plain scalar may not contain ": ". Quoted values may, so only flag unquoted
# ones -- flagging a correctly quoted description would be a false positive.
PLAIN_COLON = re.compile(r'^([A-Za-z_]+):\s+(?![\'"])(.*: .*)$', re.M)


def main() -> int:
    # An unpopulated directory is an environment condition, not a defect. CI
    # never fetches the definitions -- they are proprietary and it has no token
    # -- so failing there would be a permanently red gate that says nothing
    # about the content. Same rule the rest of verify.sh follows: a gate that
    # cannot run reports NOT RUN, never FAIL.
    files = ([p for p in sorted(AGENTS_DIR.glob("*.md")) if p.name != "README.md"]
             if AGENTS_DIR.is_dir() else [])
    if not files:
        print("agent frontmatter: NOT RUN - .claude/agents/ is not populated "
              "(run scripts/bootstrap_agents.sh).")
        return 0

    problems: list[str] = []
    for p in files:
        m = re.match(r"^---\n(.*?)\n---\n", p.read_text(encoding="utf-8"), re.S)
        if not m:
            problems.append(f"{p.name}: no frontmatter block")
            continue
        block = m.group(1)

        if yaml is not None:
            try:
                data = yaml.safe_load(block) or {}
            except yaml.YAMLError as exc:
                first = str(exc).splitlines()[0]
                problems.append(f"{p.name}: frontmatter does not parse -- {first}")
                continue
            name = data.get("name")
        else:
            hit = PLAIN_COLON.search(block)
            if hit:
                problems.append(
                    f"{p.name}: `{hit.group(1)}:` is an unquoted value containing "
                    '": ", which YAML rejects. Use an em dash, or quote the value.'
                )
                continue
            nm = re.search(r"^name:\s*(.+)$", block, re.M)
            name = nm.group(1).strip() if nm else None

        if not name:
            problems.append(f"{p.name}: no `name:` in frontmatter")
        elif name != p.stem:
            problems.append(
                f"{p.name}: `name: {name}` does not match the filename. The hook "
                "and the chat-mode generator both key on this string."
            )

    if problems:
        print("agent frontmatter: FAILED", file=sys.stderr)
        for prob in problems:
            print(f"  - {prob}", file=sys.stderr)
        return 1

    parser = "PyYAML" if yaml is not None else "fallback lint (PyYAML not installed)"
    print(f"agent frontmatter: ok - {len(files)} definition(s) valid, via {parser}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
