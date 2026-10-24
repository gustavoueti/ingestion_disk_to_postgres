#!/usr/bin/env python3
"""PreToolUse hook for agents with a write scope: file edits stay inside the paths allowed.

The same file serves the ingestio_examples hub and every job it generates: the hub copies it
into each job unchanged, and its audit checks that the copies still match.

An agent registers this hook in its frontmatter, followed by the glob patterns of the files
it may write, relative to the project root. A pattern that starts with `!` takes files back
out, and the last pattern that matches a file decides:

    command: python3 "$CLAUDE_PROJECT_DIR/.claude/hooks/restrict_writes.py" \
        'specs/**' '!specs/_template.md'

In a pattern, `*` and `?` stay inside one path segment and `**` spans any number of them.

Write, Edit and NotebookEdit calls outside the patterns, or outside the project, exit with
code 2, which blocks the call and hands stderr to the agent. Any unexpected error blocks too.
Shell commands are not covered: a redirect in Bash can still write anywhere.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

MESSAGE = (
    "Blocked by .claude/hooks/restrict_writes.py: {reason}. This agent writes only to "
    "{allowed}. Report the problem instead: the file belongs to another stage or to the "
    "architecture of the repository."
)


def compile_pattern(pattern: str) -> re.Pattern[str]:
    """Translate a glob pattern into a regular expression over a POSIX relative path."""
    parts: list[str] = []
    position = 0
    while position < len(pattern):
        if pattern.startswith("**/", position):
            parts.append("(?:.*/)?")
            position += 3
        elif pattern.startswith("**", position):
            parts.append(".*")
            position += 2
        elif pattern[position] == "*":
            parts.append("[^/]*")
            position += 1
        elif pattern[position] == "?":
            parts.append("[^/]")
            position += 1
        else:
            parts.append(re.escape(pattern[position]))
            position += 1
    return re.compile("".join(parts) + r"\Z")


def is_allowed(relative: str, patterns: list[str]) -> bool:
    """Apply the patterns in order: the last one that matches decides."""
    allowed = False
    for pattern in patterns:
        negated = pattern.startswith("!")
        if compile_pattern(pattern.removeprefix("!")).match(relative):
            allowed = not negated
    return allowed


def check(event: dict[str, object], patterns: list[str]) -> str | None:
    """Return why the edit is blocked, or None when the file matches the patterns."""
    tool_input = event.get("tool_input")
    target = None
    if isinstance(tool_input, dict):
        target = tool_input.get("file_path") or tool_input.get("notebook_path")
    if not isinstance(target, str) or not target:
        return "the call names no file to check"
    root = Path(os.environ.get("CLAUDE_PROJECT_DIR") or str(event.get("cwd") or ".")).resolve()
    # resolve() folds `..` and symlinks, so `tests/../main.py` is seen as `main.py`.
    path = (root / target).resolve()
    if not path.is_relative_to(root):
        return f"{path} is outside the project"
    relative = path.relative_to(root).as_posix()
    if is_allowed(relative, patterns):
        return None
    return f"{relative} is outside the allowed paths"


def main() -> int:
    """Read the tool call from stdin and exit 2 when the edit must be blocked."""
    patterns = sys.argv[1:]
    try:
        reason = check(json.load(sys.stdin), patterns) if patterns else "no path is allowed"
    except Exception as exc:
        reason = f"the hook could not inspect this call ({type(exc).__name__}: {exc})"
    if reason is None:
        return 0
    print(MESSAGE.format(reason=reason, allowed=" ".join(patterns) or "nothing"), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
