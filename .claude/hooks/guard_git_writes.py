#!/usr/bin/env python3
"""PreToolUse hook: writing to git or GitHub is reserved for the maintainer.

The same file guards the ingestio_examples hub and every job it generates: the hub copies it
into each job unchanged, and its audit checks that the copies still match.

Claude Code sends each tool call as JSON on stdin. For shell commands (Bash, Monitor) the
hook splits the command into simple commands, follows `bash -c`, `eval`, command
substitutions and common wrappers (`timeout`, `xargs`, `env`, ...), and blocks every `git`
or `gh` invocation that is not on the read-only allowlist, whatever `-C <dir>` points it at.
Reads stay allowed in any repository, such as `git -C ../ingestio show v0.5.0:README.md`.
It also blocks agents that ask for worktree isolation, since a worktree creates a branch.

Exit code 2 blocks the call and hands stderr to the agent; exit code 0 leaves the call to
the normal permission flow. An unexpected error blocks too: a guard that fails open is not
a guard.

This is a guardrail, not a sandbox: a program that runs git internally (a Python script, a
Makefile target) is not inspected.
"""

from __future__ import annotations

import json
import re
import shlex
import sys
from pathlib import PurePath
from typing import Any

MESSAGE = (
    "Blocked by .claude/hooks/guard_git_writes.py: {reason}. Only the maintainer writes to "
    "git or GitHub (commit, push, PR, merge, branches, tags, repositories, ...). Finish with a "
    "summary of the changes, a suggested Conventional Commits message, and the commands for the "
    "maintainer to run, the pull request included."
)

# Every git subcommand outside this set is treated as a write. `branch` is checked apart:
# listing branches is a read, creating or deleting one is not. Tags are listed with
# `for-each-ref refs/tags`, since `git tag` also creates them.
GIT_READ_ONLY = frozenset(
    {"blame", "cat-file", "describe", "diff", "for-each-ref", "grep", "help", "log"}
    | {"ls-files", "ls-tree", "merge-base", "name-rev", "rev-list", "rev-parse", "shortlog"}
    | {"show", "status", "version"}
)
# `git grep -O <program>` opens the matching files with that program.
GIT_GREP_UNSAFE_OPTIONS = ("-O", "--open-files-in-pager")
# Options before the subcommand that consume the next argument.
GIT_OPTIONS_WITH_VALUE = frozenset({"-C", "--git-dir", "--work-tree", "--namespace"})
# Options that make git run programs of the caller's choosing, even on a read.
GIT_UNSAFE_OPTIONS = ("-c", "--config-env", "--exec-path=")
# Subcommands that change the repository. Only consulted when `git` shows up in the middle
# of a command (`find -exec git add`), so that a stray word "git" does not block it.
GIT_WRITES = frozenset(
    {"add", "am", "apply", "bisect", "branch", "checkout", "cherry-pick", "clean", "clone"}
    | {"commit", "config", "fetch", "filter-branch", "filter-repo", "gc", "init", "merge"}
    | {"mv", "notes", "prune", "pull", "push", "rebase", "reflog", "remote", "replace"}
    | {"reset", "restore", "revert", "rm", "sparse-checkout", "stash", "submodule", "switch"}
    | {"symbolic-ref", "tag", "update-index", "update-ref", "worktree"}
)

BRANCH_LIST_OPTIONS = frozenset(
    {"-l", "--list", "--contains", "--no-contains", "--merged", "--no-merged", "--points-at"}
)
BRANCH_OPTIONS_WITH_VALUE = (BRANCH_LIST_OPTIONS - {"-l", "--list"}) | {"--sort", "--format"}
BRANCH_WRITE_OPTIONS = frozenset(
    {"--delete", "--move", "--copy", "--force", "--set-upstream-to", "--unset-upstream"}
    | {"--edit-description", "--track", "--no-track", "--create-reflog"}
)
BRANCH_WRITE_FLAGS = frozenset("dDmMcCfut")

# gh commands agents may run, by command group. `None` allows every subcommand of the group.
GH_READ_ONLY: dict[str, frozenset[str] | None] = {
    "auth": frozenset({"status"}),
    "help": None,
    "issue": frozenset({"list", "status", "view"}),
    "pr": frozenset({"checks", "diff", "list", "status", "view"}),
    "release": frozenset({"list", "view"}),
    "repo": frozenset({"list", "view"}),
    "run": frozenset({"list", "view", "watch"}),
    "search": None,
    "status": None,
    "version": None,
    "workflow": frozenset({"list", "view"}),
}
GH_GROUPS = frozenset(GH_READ_ONLY) | {
    "alias",
    "api",
    "attestation",
    "cache",
    "codespace",
    "config",
    "extension",
    "gist",
    "gpg-key",
    "label",
    "org",
    "project",
    "ruleset",
    "secret",
    "ssh-key",
    "variable",
}
GH_API_OPTIONS_WITH_VALUE = frozenset(
    {"-H", "--header", "-q", "--jq", "-t", "--template", "--hostname", "--cache", "-p"}
    | {"--preview"}
)
# Request fields make `gh api` default to POST.
GH_API_FIELD_OPTIONS = ("-f", "-F", "--field", "--raw-field", "--input")

SHELLS = frozenset({"ash", "bash", "dash", "fish", "ksh", "sh", "zsh"})
# Programs whose `-c` argument is a shell command line.
SCRIPT_RUNNERS = SHELLS | {"flock", "runuser", "script", "su"}
# Programs that run the rest of their arguments as a command.
WRAPPERS = frozenset(
    {"builtin", "chrt", "command", "doas", "env", "exec", "flock", "ionice", "nice"}
    | {"noglob", "nohup", "setsid", "stdbuf", "sudo", "taskset", "time", "timeout"}
    | {"unbuffer", "watch", "xargs"}
)
COMMANDS = frozenset({"eval", "gh", "git"}) | SCRIPT_RUNNERS | WRAPPERS
# Shell words that may come before the command they introduce.
KEYWORDS = frozenset({"!", "{", "}", "do", "done", "elif", "else", "esac", "fi", "if", "then"})
LOOP_KEYWORDS = frozenset({"until", "while"})

PUNCTUATION = "();<>|&\n"
ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=")
BACKTICKS = re.compile(r"`([^`]*)`")
# A here-document. `<<<` is a here-string, not a here-document.
HEREDOC = re.compile(
    r"(?<!<)<<(?!<)-?[ \t]*(?P<quote>['\"]?)(?P<escape>\\?)(?P<tag>\w+)(?P=quote)"
    r"(?P<line>[^\n]*)\n(?P<body>.*?)^[ \t]*(?P=tag)[ \t]*$",
    re.DOTALL | re.MULTILINE,
)
MAX_DEPTH = 8


def check_tool(tool_name: str, tool_input: dict[str, Any]) -> str | None:
    """Return why the call is blocked, or None when it may proceed."""
    if tool_name in {"Bash", "Monitor"}:
        command = tool_input.get("command")
        return check_command(command) if isinstance(command, str) else None
    if tool_name == "Agent" and tool_input.get("isolation") == "worktree":
        return "an agent with worktree isolation creates a branch"
    if tool_name == "EnterWorktree":
        return "a worktree creates a branch"
    return None


def check_command(command: str, depth: int = 0) -> str | None:
    """Check every simple command of a shell command line."""
    if depth > MAX_DEPTH:
        return "the command nests too deeply to inspect"
    command = command.replace("\\\n", " ")
    for inner in BACKTICKS.findall(command):
        if reason := check_command(inner, depth + 1):
            return reason
    try:
        tokens = tokenize(command)
    except ValueError:
        # Here-document bodies are text, not shell, and an apostrophe in one breaks the
        # tokenizer. Drop the bodies, but still refuse a git or gh write written in them.
        command, bodies = split_heredocs(command)
        for line in "\n".join(bodies).splitlines():
            if reason := scan_words(line.split(), 0, depth):
                return reason
        try:
            tokens = tokenize(command)
        except ValueError:
            return "the command could not be parsed"

    for token in tokens:
        # A quoted "$(...)" stays inside one token, so open it up and check it on its own.
        if "$(" in token:
            inner = token.replace("$(", " ; ").replace(")", " ; ")
            if reason := check_command(inner, depth + 1):
                return reason
    for words in simple_commands(tokens):
        if reason := check_words(words, depth):
            return reason
    return None


def tokenize(command: str) -> list[str]:
    """Split a command line like a POSIX shell, keeping operators as separate tokens."""
    lexer = shlex.shlex(command, posix=True, punctuation_chars=PUNCTUATION)
    lexer.whitespace = " \t\r"
    lexer.whitespace_split = True
    # shlex would start a comment at a `#` inside a word and hide the rest of the line.
    lexer.commenters = ""
    return list(lexer)


def split_heredocs(command: str) -> tuple[str, list[str]]:
    """Remove here-document bodies, returning the command and the removed bodies."""
    bodies: list[str] = []

    def drop(match: re.Match[str]) -> str:
        body = match["body"]
        quoted = bool(match["quote"] or match["escape"])
        if not quoted and ("$(" in body or "`" in body):
            return match[0]  # an unquoted body runs its command substitutions
        bodies.append(body)
        return f"<<{match['tag']}{match['line']}\n"

    return HEREDOC.sub(drop, command), bodies


def simple_commands(tokens: list[str]) -> list[list[str]]:
    """Group tokens into simple commands, dropping operators and redirection targets."""
    commands: list[list[str]] = [[]]
    is_target = False
    for token in tokens:
        if is_target:
            is_target = False
        elif token and all(char in PUNCTUATION for char in token):
            is_redirection = ("<" in token or ">" in token) and not set("()") & set(token)
            if is_redirection:
                is_target = True
            else:
                commands.append([])
        else:
            commands[-1].append(token)
    return [words for words in commands if words]


def program(word: str) -> str:
    """Name of the program a word runs: `/usr/bin/git` and `$'git'` both run git."""
    return PurePath(word).name.removeprefix("$")


def check_words(words: list[str], depth: int) -> str | None:
    """Check one simple command."""
    start = 0
    while start < len(words) and (
        ASSIGNMENT.match(words[start]) or words[start] in KEYWORDS | LOOP_KEYWORDS
    ):
        start += 1
    if start == len(words):
        return None
    if program(words[start]) in COMMANDS:
        return check_program(words[start:], depth)
    return scan_words(words, start + 1, depth)


def check_program(words: list[str], depth: int) -> str | None:
    """Check a simple command that starts with a program this hook knows."""
    name, args = program(words[0]), words[1:]
    if name == "git":
        return check_git(args)
    if name == "gh":
        return check_gh(args)
    if name == "eval":
        return check_command(" ".join(args), depth + 1)
    if name in SCRIPT_RUNNERS and (script := find_script(name, args)) is not None:
        return check_command(script, depth + 1)
    if name in WRAPPERS:
        for position, word in enumerate(args):
            if program(word) in COMMANDS:
                return check_program(args[position:], depth + 1)
    return None


def scan_words(words: list[str], start: int, depth: int) -> str | None:
    """Look for git, gh or `sh -c` further inside a command, as in `find -exec git add`."""
    for position in range(start, len(words)):
        name, args = program(words[position]), words[position + 1 :]
        if name == "git" and git_subcommand(args)[1] in GIT_WRITES:
            return check_git(args)
        if name == "gh" and args[:1] and args[0] in GH_GROUPS:
            return check_gh(args)
        if name in SCRIPT_RUNNERS and (script := find_script(name, args)) is not None:
            return check_command(script, depth + 1)
    return None


def find_script(name: str, args: list[str]) -> str | None:
    """Return the command line passed with `-c`, as in `bash -lc '...'` or `su -c '...'`."""
    for position, arg in enumerate(args[:-1]):
        if arg in {"-c", "--command"}:
            return args[position + 1]
        if name in SHELLS and re.fullmatch(r"-[a-zA-Z]*c[a-zA-Z]*", arg):
            return args[position + 1]
    for arg in args:
        if arg.startswith("--command="):
            return arg.split("=", 1)[1]
    return None


def git_subcommand(args: list[str]) -> tuple[int, str | None]:
    """Return the position and name of the git subcommand, skipping the options before it.

    The position is past the end when the last option is missing its value.
    """
    position = 0
    while position < len(args):
        arg = args[position]
        if arg in GIT_OPTIONS_WITH_VALUE:
            position += 2
        elif arg.startswith("-"):
            position += 1
        else:
            return position, arg
    return position, None


def check_git(args: list[str]) -> str | None:
    """Allow read-only git commands only."""
    position, subcommand = git_subcommand(args)
    if position > len(args):
        return f"`git {args[-1]}` is missing its value"
    for arg in args[:position]:
        if arg.startswith(GIT_UNSAFE_OPTIONS):
            return f"`git {arg.split('=', 1)[0]}` can make git run arbitrary programs"
    if subcommand == "grep":
        for arg in args[position + 1 :]:
            if arg.startswith(GIT_GREP_UNSAFE_OPTIONS):
                return f"`git grep {arg.split('=', 1)[0]}` runs a program on the matches"
    if subcommand is None or subcommand in GIT_READ_ONLY:
        return None
    if subcommand == "branch":
        return check_git_branch(args[position + 1 :])
    return f"`git {subcommand}` is not on the read-only list"


def check_git_branch(args: list[str]) -> str | None:
    """Allow `git branch` only when it lists branches."""
    listing = False
    names: list[str] = []
    position = 0
    while position < len(args):
        arg = args[position]
        option = arg.split("=", 1)[0]
        if option in BRANCH_WRITE_OPTIONS:
            return f"`git branch {option}` changes branches"
        if arg.startswith("-") and not arg.startswith("--"):
            if written := BRANCH_WRITE_FLAGS & set(arg[1:]):
                return f"`git branch -{''.join(sorted(written))}` changes branches"
            listing = listing or "l" in arg
        elif option in BRANCH_LIST_OPTIONS:
            listing = True
        elif not arg.startswith("-"):
            names.append(arg)
        if arg in BRANCH_OPTIONS_WITH_VALUE:
            position += 1
        position += 1
    if names and not listing:
        return "`git branch <name>` creates a branch"
    return None


def check_gh(args: list[str]) -> str | None:
    """Allow read-only gh commands only."""
    positions = gh_command_positions(args)
    if not positions:
        return None
    group = args[positions[0]]
    if group == "api":
        return check_gh_api(args[positions[0] + 1 :])
    if group not in GH_READ_ONLY:
        return f"`gh {group}` is not on the read-only list"
    allowed = GH_READ_ONLY[group]
    if allowed is None or len(positions) < 2 or args[positions[1]] in allowed:
        return None
    return f"`gh {group} {args[positions[1]]}` is not on the read-only list"


def gh_command_positions(args: list[str]) -> list[int]:
    """Return where the gh command group and its subcommand are, skipping options."""
    positions: list[int] = []
    position = 0
    while position < len(args) and len(positions) < 2:
        arg = args[position]
        if arg in {"-R", "--repo"}:
            position += 1
        elif not arg.startswith("-"):
            positions.append(position)
        position += 1
    return positions


def check_gh_api(args: list[str]) -> str | None:
    """Allow `gh api` GET requests only."""
    method: str | None = None
    has_fields = False
    endpoint: str | None = None
    position = 0
    while position < len(args):
        arg = args[position]
        if arg in {"-X", "--method"}:
            method = args[position + 1] if position + 1 < len(args) else ""
            position += 1
        elif arg.startswith("--method="):
            method = arg.split("=", 1)[1]
        elif arg.startswith("-X"):
            method = arg[2:]
        elif arg.startswith(GH_API_FIELD_OPTIONS):
            has_fields = True
            if arg in GH_API_FIELD_OPTIONS:
                position += 1
        elif arg in GH_API_OPTIONS_WITH_VALUE:
            position += 1
        elif not arg.startswith("-") and endpoint is None:
            endpoint = arg
        position += 1
    if method is not None and method.upper() != "GET":
        return f"`gh api --method {method}` is not a GET request"
    if method is None and has_fields:
        return "`gh api` with request fields defaults to POST"
    if endpoint == "graphql":
        return "`gh api graphql` sends a POST request, which may carry a mutation"
    return None


def main() -> int:
    """Read the tool call from stdin and exit 2 when it must be blocked."""
    try:
        event = json.load(sys.stdin)
        reason = check_tool(event.get("tool_name", ""), event.get("tool_input") or {})
    except Exception as exc:
        reason = f"the hook could not inspect this call ({type(exc).__name__}: {exc})"
    if reason is None:
        return 0
    print(MESSAGE.format(reason=reason), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
