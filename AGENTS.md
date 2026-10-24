# AGENTS.md

Instructions for AI coding agents working on **ingestion_disk_to_postgres**, a proof-of-concept ingestion
job built with [ingestio](https://github.com/gustavoueti/ingestio) `0.5.0`,
installed from PyPI: https://pypi.org/project/ingestio/0.5.0/. Start every task at [docs/index.md](docs/index.md), then read
[.claude/kb/ingestio.md](.claude/kb/ingestio.md).

## Goal

The job ingests CSV files from a public dataset that sit in a local directory on disk into a
table in PostgreSQL. It reads them with `DiskReader` (format `csv`), selecting only the files
above the job's watermark, and loads them with the `AppendOnly` callback through
`PostgresWriter` as an incremental, atomic append. PostgreSQL runs in Docker: `docker compose`
locally, a service container in CI.

The source is local files on disk, from a public dataset: no credentials and no private
third-party data. Which dataset is a decision of the job, taken with its own agents in the
planner's spec.

This job exists for three reasons: to prove that ingestio can be used from the outside, by
someone who only has its public API and its documentation; to reveal design problems of the
library through real use; and to show what the library looks like in practice.

## The library

Everything this job needs to know about ingestio is in
[.claude/kb/ingestio.md](.claude/kb/ingestio.md), generated for version `0.5.0`:
its philosophy and concepts, how to extend it, how a job is structured (`main.py` and
`config.json`), the pieces this job uses, their pitfalls, and the **rules of a valid job**.

- The version in the stamp of that file and the version pinned in `pyproject.toml` are the
  same. If they differ, stop and report it: the guidance is stale.
- Never edit that file. When the job moves to another version of ingestio, the maintainer has
  it regenerated.

## Rules of the job

- **Only public or local sources:** public APIs, files, SQLite, databases in Docker. Never
  credentials, and never real data of third parties.
- **Only valid code**, as the rules of a valid job in the knowledge base define it: the
  public API of ingestio, its documented extension, no workaround.
- **A gap instead of a workaround.** When a process works only with a workaround of the
  library, the job does not accept the workaround: the gap is recorded in
  [LIB_GAPS.md](LIB_GAPS.md) and the maintainer decides what happens to the process.
- **Every change to the job updates its version and [CHANGELOG.md](CHANGELOG.md).** Tags
  `vX.Y.Z` are created by the maintainer.

## Ask the maintainer

When a task needs a definition that only the maintainer can give, ask for it before going
further. Never fill the gap with a guess, a convenient default or the most likely answer. Ask
everything at once, each question with the options you see and the one you recommend. An
agent of the workflow cannot reach the maintainer: it stops and reports the question, and the
main session takes it to the maintainer. In planning, the questions go under **Open
questions** in the spec.

## Constraints

- **Never write to git or GitHub.** Reading history and logs is fine: `git log`, `show`,
  `diff`, `status`, `blame`, `ls-files`, `rev-parse`, `for-each-ref`, and the read commands of
  `gh` such as `gh pr view` or `gh run view`. Anything that changes the index, refs, history,
  a remote or a repository is the maintainer's: add, commit, push, merge, rebase, reset,
  checkout, switch, restore, stash, tag, branches, worktrees, pull requests, releases,
  repositories.
- **Never use worktree isolation.** A worktree creates a branch.
- **Never add a `Co-Authored-By` trailer or an AI-generation footer** to a commit or a pull
  request, whatever the tool.
- **Write only inside this repository.** Never change the ingestio repository, the hub this
  job was generated from, or anything else outside this directory.
- **Never change the version of ingestio** pinned in `pyproject.toml`.
- **Leave the architecture as it is.** These files come from the hub and change only when the
  hub regenerates them: `AGENTS.md`, `CLAUDE.md`, `CONTRIBUTING.md`, `.claude/agents/`,
  `.claude/hooks/`, `.claude/kb/`, `.claude/settings.json`, `.pre-commit-config.yaml`,
  `docs/workflow.md` and `specs/_template.md`.

## Development workflow

Every change to the job goes through five stages, each with its own agent:

planning → tests → documentation → implementation → validation

The spec in `specs/` records where the change stands. Stages, statuses, write scopes and
returns are in [docs/workflow.md](docs/workflow.md); code, tests, commits and pull requests
follow [CONTRIBUTING.md](CONTRIBUTING.md).

## Library gaps

[LIB_GAPS.md](LIB_GAPS.md) records what this job could not do with the library as it is.
Any agent that finds one stops and reports it; the main session records it in the format the
file gives, and the hub consolidates the gaps of every job for the library's maintainers.

## Finishing a task

End with a summary of the changes, a suggested commit message in Conventional Commits form,
and the commands for the maintainer to run, as [CONTRIBUTING.md](CONTRIBUTING.md#finishing-a-change)
shows: the branch, the commit, the push and the pull request, with no co-author trailers.
