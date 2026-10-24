# Contributing to ingestion_disk_to_postgres

This is the prescriptive reference for code, tests, commits and pull requests. How a change
moves from request to merge is described in [docs/workflow.md](docs/workflow.md); what the job
may do with the library is in [.claude/kb/ingestio.md](.claude/kb/ingestio.md), which wins
over this file when the two disagree.

## Setup

```bash
uv sync                                             # environment, with the dev group
uv run pre-commit install --hook-type commit-msg    # once per clone
```

The `commit-msg` hook rejects commit messages that carry a `Co-Authored-By` trailer or an
AI-generation footer.

## Checks

The same commands CI runs:

```bash
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy
uv run --locked pytest -q
```

CI installs with `uv sync --locked`, so a `uv.lock` that is out of date fails the build.
Commit `uv.lock` together with any change to `pyproject.toml`. Until the job holds its first
Python file, CI skips mypy, which refuses to run without one, and accepts that pytest collects
no test.

## Code

### Layout

- `main.py` is the entry point: it places the callbacks and runs the process.
- `config.json` holds the job's configuration: its source, its target and its parameters, as
  the knowledge base describes. Credentials never go in it.
- The job's own callbacks and helpers live in modules next to `main.py`, named after what they
  do, such as `lineage.py`.
- `main.py` runs the process only under `if __name__ == "__main__":`, so tests import its
  pieces without running it.

### Style

- Modules with logic start with `from __future__ import annotations`.
- Every module has a docstring. Docstrings follow the Google convention; a class documents its
  constructor arguments in its own docstring, under `Args:`.
- Comments explain why, not what.
- Required arguments are positional; options are keyword-only, after `*`.
- Validate arguments when the object is built and raise `ValueError` with a lowercase message
  and no trailing period, such as `"a batch needs at least one record"`.
- Prefer immutable structures: tuples, frozen dataclasses.
- Datetimes are timezone-aware (`datetime.now(UTC)`). Paths use `pathlib`.
- Lines are at most 100 characters; `ruff format` decides the rest.

### Types

- Everything is annotated. mypy runs in strict mode over the code and the tests.

### Logging

- Use the standard `logging` module, never `print`: `logger = logging.getLogger(__name__)`.
- Pass arguments `%`-style, so formatting only happens when the record is emitted.
- `main.py` decides what is shown, for example with `ingestio.log_to_console()`.

### Dependencies

- `dependencies` holds ingestio alone, pinned to one version, with the extras the job's
  systems need. Its version is the one the knowledge base was generated for.
- Development tools go into the `dev` dependency group.

## Tests

- Tests are plain pytest functions, without test classes, one file per area, each with a
  module docstring.
- A test name is a sentence about behaviour:
  `test_lineage_stamps_every_record_with_the_run_id`.
- A test that covers an acceptance criterion of a spec is named after it,
  `test_at001_<description>`, and lives in the spec's file, `tests/test_<slug>.py`.
- Tests are fully typed.
- Test doubles live in the test file that uses them, with a leading underscore
  (`_StubReader`). Prefer a real local resource over a mock: a temporary file, a local HTTP
  server, a SQLite database, a database in Docker.
- Use `tmp_path` for files, `caplog` for logs, and `pytest.raises(..., match=...)` for errors.
- Mark tests that touch an external resource (network, Docker, Spark) with
  `@pytest.mark.integration`.

## Versions and the changelog

- The job follows semantic versioning in `project.version` of `pyproject.toml`: a fix bumps the
  patch, a feature the minor, a breaking change of the job's configuration or output the
  major (the minor while the version is `0.x`).
- Every change records its version in [CHANGELOG.md](CHANGELOG.md), under the date of the
  change.
- Tags `vX.Y.Z` are created by the maintainer.

## Commits and pull requests

- Commit messages and pull request titles follow
  [Conventional Commits](https://www.conventionalcommits.org/): `type: subject`, with a
  lowercase subject in the imperative mood and no trailing period. Accepted types: `feat`,
  `fix`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`, `revert`.
- Pull requests are squash-merged, so the PR title becomes the commit message on `main`. CI
  checks the title.
- Branches are named `<type>/<author>/<topic>`, such as `feat/ueti/load_the_todos`.
- No `Co-Authored-By` trailers and no "Generated with" footers, in commits or in pull request
  descriptions.

## Finishing a change

Agents never commit. At the end of a change, the session gives the maintainer a summary, the
suggested commit message and the commands to run, filled in:

```bash
git switch -c <type>/<author>/<topic>
git add <the files of the change>
git commit -m "<type>: <subject>"
git push -u origin HEAD
gh pr create --title "<type>: <subject>" --body "<what changed and why, the spec, the version>"
```
