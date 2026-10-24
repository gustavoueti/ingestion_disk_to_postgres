# ingestio for ingestion_disk_to_postgres

- **ingestio version:** `0.5.0`
- **Package:** https://pypi.org/project/ingestio/0.5.0/
- **Generated:** 2026-10-24, by the ingestio-expert of the ingestio_examples hub, from
  the ingestio repository at tag `v0.5.0`
- **Regenerate it from the hub when the job moves to another version of ingestio.** It
  describes that version only, and nobody edits it by hand. If `pyproject.toml` pins another
  version, stop: this guidance is stale.

The agents of this job work from this file alone: they do not read the ingestio repository or
the hub. When this file and the library disagree, the library at the tag above wins, and the
disagreement is a defect of this file, to report to the maintainer.

How to read it: a path in parentheses, such as `(docs/concepts/callbacks.md)`, names the file of
the library, at tag `v0.5.0`, that a statement comes from. A statement marked *(Spark)*,
*(PostgreSQL)*, *(Docker)* or *(GitHub Actions)* describes that tool, not ingestio: ingestio
only passes the values through to it. Code blocks are illustrations, never files of the job:
the job's files are written by its own agents, from its specs.

## 1. Philosophy and concepts

ingestio describes any data movement process as an ordered sequence of steps. It does not
assume that the data is a table, that there are bronze and silver layers, or that processing
is batch: those are things a job builds with it (README.md). It is an early alpha: the API is
not stable yet (README.md).

**Two concepts.** Everything in ingestio, and everything a job adds to it, is either a
*connector* or a *callback*. Something that fits neither is not a third concept: stop and
report it (docs/philosophy.md).

**Vocabulary** (docs/philosophy.md):

- **Stage**: one callback, the unit of work of a pipeline.
- **Process** (`Process`): the pipeline, an ordered list of steps run together.
- **Step** (`Step`): a named point of the process that holds the callbacks running there.

**Principles** (docs/philosophy.md):

- **Simplicity.** The smallest abstraction that solves the problem: no internal frameworks,
  no open registries, no magic. The one mapping from names to classes is the closed table of
  the connectors the library ships, which its read and write callbacks use (ADR 0015).
- **The user's autonomy.** The library composes; it does not impose. A job writes its own
  callbacks in its own code, with nothing to register. Users do not extend connectors: a
  system the library does not integrate gets a connector contributed to the library.
- **A core without dependencies.** `ingestio` itself has no dependency; an integration that
  needs a third-party package is an optional extra, such as `ingestio[postgres]` (ADR 0002).
  An engine that the runtime provides, pyspark among them, is a documented prerequisite and
  never part of an extra (docs/concepts/connectors.md, section Extras).
- **Streaming is a future direction**, not implemented (ADR 0012).

**What exists at 0.5.0, and what is only a direction:**

| The philosophy states | At 0.5.0 |
|---|---|
| A callback is essential or non-essential, as its author declares | Not implemented: every callback is essential, and none can be declared otherwise (docs/concepts/criticality.md, ADR 0011) |
| A non-essential failure is recorded in the result of the run | There is no run result: `Process.run()` returns the process itself (docs/concepts/criticality.md) |
| Append-only and SCD2 are the library's default callbacks | Shipped as `AppendOnly` and `SCD2`, beside `ReadSource` and `FullLoad` (docs/concepts/callbacks.md) |
| Streaming | Deferred: every reader returns one batch per `read()`, every writer needs the whole batch (ADR 0012) |
| Extras "such as `ingestio[spark]`" | There is no `spark` extra. The extras are `postgres`, `s3`, `gcs`, `databricks` and `bigquery`, and ingestio never installs pyspark (docs/concepts/connectors.md, section Extras; pyproject.toml) |
| A connector decides no format | `JsonLinesWriter` fixes its format, JSON Lines: the one known divergence (docs/concepts/connectors.md, section Known divergence) |

### Connectors

- A connector is an integration with an external system. It knows how to talk to that system
  and nothing else: it decides no write strategy, no format and no architecture
  (docs/philosophy.md, docs/concepts/connectors.md).
- Its capabilities are two protocols, `Reader[T]` with `read() -> T` and `Writer[T]` with
  `write(data) -> None`. They are structural, with no base class, and not checked at runtime:
  the type checker checks them. The payload type is open: a list, a generator, a DataFrame
  (ADR 0003, docs/concepts/connectors.md).
- **The library ships every connector.** A job names its source and its target systems in its
  JSON configuration file, and the library's read and write callbacks build the connector that
  each `system` names, from a closed table that nothing outside the library can add to or
  replace (ADR 0015, docs/concepts/configuration.md).
- A connector takes an ingestion's parameters as constructor arguments named after the
  configuration keys. It never reads a file, an environment variable or a secret store, and
  has no `from_config`. Credentials, SDK clients and per-run values, such as the watermark,
  reach it through the arguments of the callback (ADR 0013, ADR 0015).
- A reader filters inside the source: `DiskReader` selects the files above the watermark
  before Spark loads anything, so Spark never opens a file that was not selected (ADR 0013).
- Every shipped writer has `write(data, *, replace=False)`: the callback that calls it chooses
  between appending and replacing (ADR 0016).
- A writer may create a missing target only in the form its system derives entirely from the
  data. `PostgresWriter` creates nothing, because a PostgreSQL table built from a DataFrame
  needs type mappings that the connector would have to choose: the job creates the table with
  its own DDL (ADR 0014).
- Every shipped connector except `JsonApiReader` and `JsonLinesWriter` is built on Spark: it
  reads into, or writes from, a `pyspark.sql.DataFrame`, expects a Spark session to be running
  already, and never starts one (README.md, docs/concepts/connectors.md).
- The modules import third-party packages only inside the functions that use them, so
  `ingestio` and `ingestio.connector.all` import without pyspark and without any extra
  (docs/concepts/connectors.md, section Extras).

### Callbacks

- A callback is a stage of the pipeline that performs an action: formatting, medallion layers,
  CDC, a write strategy (docs/philosophy.md).
- It subclasses `ingestio.Callback` and overrides any of three moments, `before()`, `run()`
  and `after()`, which do nothing by default (docs/concepts/callbacks.md, ADR 0006).
- Callbacks exchange data through `self.process.state`, a dictionary shared by the whole
  process, instead of passing a payload from one to the next. Its keys are conventions
  between callbacks: nothing checks before the run that a key another callback needs will be
  there (ADR 0004).
- The library ships four callbacks, `ReadSource`, `AppendOnly`, `FullLoad` and `SCD2`. Each
  write strategy is one callback (ADR 0016). They read the job's configuration file when they
  are built and build their connector from it; they never accept a connector instance
  (ADR 0015, docs/concepts/callbacks.md).
- A process uses a shipped callback by placing it in a step; the library never adds one on
  its own (docs/philosophy.md).
- A callback stops part of the run by raising `CancelStepException` or
  `CancelProcessException` (ADR 0007, docs/concepts/callbacks.md).

### Criticality

- **The direction** (docs/philosophy.md, ADR 0011): a stage is essential or non-essential. An
  essential failure fails the pipeline; a non-essential one lets it continue, and is recorded
  in the result of the run, never silent. The author of a callback declares its criticality,
  and without a declaration a callback is essential. Append-only and SCD2 are essential.
- **At 0.5.0** (docs/concepts/criticality.md): not implemented. Every exception other than the
  two cancel exceptions propagates out of `Process.run()`, after the `after` pass of the
  current step, and stops the run. Every callback therefore behaves as essential, there is no
  way to declare one non-essential, and there is no run result to record a tolerated failure.
- The shipped callbacks are essential: every error of `read()` and `write()` propagates out of
  `Process.run()` unchanged (docs/concepts/configuration.md).
- Rule 3 of section 5 says what this means for the job.

### Process and Step

`Process` and `Step` are the engine that composes the two concepts. They are not a third
concept, and nobody extends them (docs/philosophy.md).

- **Building** (docs/concepts/lifecycle.md): `Process(steps, *, state=None, run_id=None,
  log=None)`. `state` is copied into `process.state`; `run_id` is eight hexadecimal
  characters when omitted; `log` is the logger to write to, `ingestio.core` by default. A
  process with no step raises `ValueError("a process needs at least one step")`, and two steps
  with one name raise `ValueError("duplicate step name: '<name>'")` (src/ingestio/core.py).
  `Step(name, callbacks=())` raises `ValueError` for a blank name; a step may hold no
  callback. Building the process binds every callback to it: from then on `callback.process`
  is the process.
- **Running**: `process.run()` runs the steps in list order and returns the process, so the
  state stays reachable: `Process([...]).run().state`. Inside a step the callbacks run in
  three passes, each in list order: every `before`, then every `run`, then every `after`.
  There is no priority and no reordering (ADR 0005, ADR 0006). `process.started_at` holds the
  UTC time the run started.
- **Cancellation and failure** (docs/concepts/lifecycle.md):

  | Raised by a callback | Effect |
  |---|---|
  | `CancelStepException` | the rest of the step is skipped, `run` pass included; the process carries on with the next step |
  | `CancelProcessException` | the process stops cleanly and `process.cancelled` becomes `True` |
  | any other exception | it propagates out of `run()` and the run fails |

  The `after` pass of the current step runs in every case. A `CancelStepException` raised from
  an `after` is not handled and fails the run (ADR 0007).
- **Logging** (docs/concepts/lifecycle.md, ADR 0008): through the standard `logging` module,
  never `print`. A run logs a tree, one record per line, each line starting with the run id:

  ```text
  [a3f7c9] ┌ process started · 2 steps
  [a3f7c9] │ ┌ extract (1/2) · 1 callback
  [a3f7c9] │ │ ✔ ReadSource.run 412ms
  [a3f7c9] │ └ extract done in 413ms
  [a3f7c9] │ ┌ load (2/2) · 2 callbacks
  [a3f7c9] │ │ ⊘ SkipEmptyBatch.run cancelled the step after 1ms: no new file
  [a3f7c9] │ └ load cancelled in 2ms
  [a3f7c9] └ process finished in 432ms
  ```

  Starts, successes and cancellations are `INFO`; a failing moment (`✖ <callback>.<moment>
  failed after <d>: <Type>: <message>`) and the failed closing lines are `ERROR`. A failure
  line holds the type and message of the exception, never a traceback; the exception itself
  leaves `run()` unchanged. `ingestio` registers only a `NullHandler`, so nothing shows until
  the job configures logging, for instance with `log_to_console()`.
- **Behaviour not specified yet** (docs/concepts/lifecycle.md), which the job must not rely on:
  an exception in an `after` stops the remaining `after` calls of the step and replaces any
  exception already propagating; when a `before` fails, every `after` of the step still runs;
  calling `run()` again reuses the state of the previous run; a callback placed in two
  processes stays bound to the one built last.

## 2. Extending the library

At 0.5.0 a job extends ingestio in one way: **callbacks of its own**. It uses everything else
as the library ships it.

**A callback of the job's own** (docs/philosophy.md, docs/concepts/callbacks.md). Subclass
`Callback` in the job's code, override the moments it needs, and read and write
`self.process.state`. Its constructor arguments are the job's to choose. Nothing is
registered and nothing is configured. Its `name`, the class name by default, is what the log
shows. For instance, a stage that ends the load step when the source selected nothing:

```python
from __future__ import annotations

from ingestio import Callback, CancelStepException


class SkipEmptyBatch(Callback):
    """Ends the step when the batch has no row, so that nothing after it in the step runs."""

    def __init__(self, *, key: str = "records") -> None:
        self.key = key

    def run(self) -> None:
        if self.process.state[self.key].isEmpty():  # a Spark DataFrame: this runs a Spark job
            raise CancelStepException("no new file")
```

**A shipped connector inside a callback of the job's own** (docs/concepts/connectors.md,
section Using a connector). For a strategy that the shipped callbacks do not offer, a job's
own callback may build a connector of `ingestio.connector.all` itself and call it, annotating
what it holds with `Reader` or `Writer`. That callback then chooses the strategy itself, with
`write(data)` or `write(data, replace=True)`, and receives its parameters from the job: it
must never hand a source's `path` or `table_name` to a writer (ADR 0013). This job does not
need it: `ReadSource` and `AppendOnly` cover its goal.

```python
from typing import Any

from ingestio import Callback, Writer


class WriteWith(Callback):
    """Writes a value of the state with the writer it was given."""

    def __init__(self, writer: Writer[Any], *, key: str = "records") -> None:
        self.writer = writer
        self.key = key

    def run(self) -> None:
        self.writer.write(self.process.state[self.key])
```

**Controlling the flow** (ADR 0007). `CancelStepException` skips the rest of the step,
`CancelProcessException` stops the process cleanly; the policy is the job's.

**Logging** (docs/concepts/lifecycle.md). The job's own modules log with
`logging.getLogger(__name__)`. `Process(..., log=<logger>)` sends the tree to another logger,
and `log_to_console()` shows it on standard error.

**What a job cannot extend at 0.5.0:**

- **Connectors.** A job cannot add a connector, or name one of its own in the configuration:
  the table of systems is closed, private and not exported, with no mapping argument, no
  import path in the file and no entry point (docs/concepts/configuration.md, ADR 0015). A
  system the library lacks is a library gap. Rule 2 of section 5 speaks of custom connectors
  "through the documented extension": for connectors, none exists at this version.
- **`Process` and `Step`**, which are the engine, not an extension point (docs/philosophy.md).
- **Criticality**, which cannot be declared (docs/concepts/criticality.md).
- **The shipped callbacks and connectors**, whose behaviour comes only from their arguments
  and the keys of the configuration. Their private attributes and modules are not extension
  points (section 5, rules 1 and 5).
- **The reserved names of the configuration**, `source`, `target` and `scd2`
  (docs/concepts/configuration.md).

## 3. The structure of a job

ingestio's minimum job is two files: `main.py`, the entry point, and `config.json`, the job's
configuration. Neither exists until the implementer writes it; this section says what each
one is responsible for and how they connect to the library.

### `config.json`

**Its responsibility.** It names the job's source and target systems with their parameters,
and holds the job's own parameters. It is the only way the library's callbacks reach a
connector: changing the source or the target is an edit of this file (ADR 0015,
docs/concepts/configuration.md).

**Its shape** (docs/concepts/configuration.md):

- The top level is a JSON object. The names `source`, `target` and `scd2` are the library's:
  `ReadSource` reads `source`, `AppendOnly`, `FullLoad` and `SCD2` read `target`, and only
  `SCD2` reads `scd2`. Every other top-level member belongs to the job: every library callback
  ignores it, and the job's own code may read it.
- A section names its system with the key `system`. Every other key of the section is the
  constructor argument of the same name of the connector that the system builds, passed as it
  is, with nothing renamed or converted. A key that the system does not take is an error.
- Each section names its own location: the `target` section's `table_name` is the target's.
- One source and one target per file. A job with two sources uses two files.

For this job, `source` names the `disk` system and `target` the `postgres` system. Its shape,
with the values the spec chooses:

```json
{
  "source": {
    "system": "disk",
    "path": "<absolute path of the directory that holds the CSV files>",
    "format": "csv",
    "watermark_pattern": "<a regular expression with one group, when the file names carry the watermark>",
    "schema": "<the CSV's columns, as a DDL string>",
    "options": {"header": true}
  },
  "target": {"system": "postgres", "table_name": "<schema>.<table>"}
}
```

`watermark_pattern`, `schema` and `options` are optional; section 4 says what each one does.

**What never goes in it** (docs/concepts/configuration.md):

- Credentials and connection settings. `host`, `port`, `database`, `user` and `password` are
  not keys of `postgres`, and a key the system does not take is rejected: they reach
  `AppendOnly` inside a `PostgresConnection`, its `connection` argument.
- Per-run values: the watermark is the `watermark_value` argument of `ReadSource`.
- SDK clients and Spark sessions, which are not JSON.
- A write strategy: `append`, `replace` and `mode` are not keys. The callback the job places
  chooses (ADR 0016).

**Its JSON types** (docs/concepts/configuration.md, section JSON types). `system`, `path`,
`format`, `watermark_pattern`, `schema` and `table_name` are strings; `options` is an object
whose values are strings, numbers or booleans. `null` is never a value: a key that the job
does not set is left out. `NaN`, `Infinity` and `-Infinity` are rejected. A key that appears
twice in an object keeps its last value, silently.

**When it is read.** Each callback reads the file once, when it is built, and builds its
connector then; later changes to the file are not seen. `ReadSource` and `AppendOnly` built
from the same path read it twice. Building a callback touches no Spark session, no database
and no network (docs/concepts/configuration.md). The `config` argument is a `str` or an
`os.PathLike`; a relative path is resolved against the working directory when the callback is
built, so `Path(__file__).with_name("config.json")` names the file wherever the job is run
from (README.md).

**Its errors** (docs/concepts/configuration.md, section Errors). A mistake anywhere in the
file raises when the callback is built, before `Process` exists and before anything runs, and
writes no line of the run's log:

- the callback's own checks raise `ValueError` with a message that starts with the file's
  path, such as `<path>: source.watermark_patern is not a key of system 'disk'`,
  `<path>: source.format is required for system 'disk'` or
  `<path>: target system 'postgres' needs the argument connection`;
- a value that the connector rejects raises the connector's own `ValueError`, unchanged, with
  one note, `while building the source of <path>, system 'disk'`: for instance
  `path must be an absolute local path`;
- a missing extra raises the connector's `ImportError`, which names the extra.

The checks run in a fixed order: the argument and the file, the JSON, the section, `system`,
the keys, the required keys, the types, the callback arguments, and last the connector.

### `main.py`

**Its responsibility.** The entry point: it builds what the configuration cannot hold, defines
or imports the job's own callbacks, places the callbacks in steps, runs the process and
decides what the log shows (README.md, section Structuring a job).

**What belongs in it, for this job:**

- **The Spark session**, created before the process runs, with the PostgreSQL JDBC driver on
  its classpath. No connector creates one (README.md); section 4 says how.
- **The `PostgresConnection`**, built from the environment, never from `config.json`
  (README.md, docs/concepts/configuration.md).
- **The last watermark already ingested**, computed before `ReadSource` is built, because
  `watermark_value` is an argument of the callback and a value that a step of the same process
  computes cannot reach the reader (docs/concepts/configuration.md, section Known
  limitations).
- **The job's own callbacks**, defined there or imported from the job's modules next to it.
- **The process**: its steps, their order, and the callbacks of each.
- **Logging**: `ingestio.log_to_console()` for the tree, and the job's own handlers.

**The guard.** `main.py` runs only under `if __name__ == "__main__":`, so tests import its
pieces without running it (CONTRIBUTING.md of this job). For this job that means more than not
calling `run()` at import: building the callbacks reads `config.json`, building the connection
reads the environment, and building the session starts a JVM. The job keeps all of it in
functions called under the guard, such as one that builds the process from the path of a
configuration and the callbacks' arguments, so that a test runs the same process on its own
configuration, directory and database. (The README's example builds its connection at module
level; this job does not, for that reason.)

```python
from pathlib import Path

from ingestio import AppendOnly, Process, ReadSource, Step
from ingestio.connector.all import PostgresConnection

# SkipEmptyBatch is the job's own callback of section 2, imported from the job's module.


def build_process(
    config: Path, *, connection: PostgresConnection, watermark: str | None
) -> Process:
    """The job's process, for a configuration file and the arguments it cannot hold."""
    return Process([
        Step("extract", [ReadSource(config, watermark_value=watermark)]),
        Step("load", [SkipEmptyBatch(), AppendOnly(config, connection=connection)]),
    ])
```

### How they connect to the library

1. `ReadSource(config, ...)` reads the `source` section and, through the closed table, builds
   `DiskReader(path, format, watermark_pattern=..., schema=..., options=...,
   watermark_value=..., spark=...)` when it is built (docs/concepts/configuration.md).
2. `AppendOnly(config, connection=...)` reads the `target` section and builds
   `PostgresWriter(table_name, connection, options=...)` when it is built. That is when the
   missing `postgres` extra would show, as an `ImportError` (docs/concepts/connectors.md).
3. When the process runs, `ReadSource.run()` calls `read()` and stores the DataFrame it
   returns in `state["records"]`, or in the key given as `into`
   (docs/concepts/callbacks.md).
4. The job's own callbacks between the two read `state["records"]` and may replace it with a
   new DataFrame.
5. `AppendOnly.run()` calls `write(state["records"])`, or the key given as `key`, which
   appends the rows to the table in one transaction (docs/concepts/connectors.md).

**What `main.py` passes as arguments** (docs/concepts/configuration.md, section What does not
belong in the file):

| Callback | Argument | For this job |
|---|---|---|
| `ReadSource` | `watermark_value` | the last watermark already ingested: a `str` with `watermark_pattern`, a timezone-aware `datetime` without it. `None`, or nothing, reads every file |
| `ReadSource` | `spark` | optional: the session to read with. The active session, looked up when `read()` runs, otherwise |
| `ReadSource` | `into` | the key of the state that receives the DataFrame, `"records"` by default |
| `AppendOnly` | `connection` | required: the `PostgresConnection` of the target |
| `AppendOnly` | `key` | the key of the state to write, `"records"` by default |

An argument that is `None` is not given. An argument that the system does not take raises
when the callback is built: `ReadSource` takes no `connection` or `client` for `disk`, and
`AppendOnly` takes no `client` for `postgres`. `AppendOnly` takes no `spark`: the writer saves
the DataFrame through the DataFrame's own session.

**How the data flows** (docs/concepts/callbacks.md, docs/concepts/connectors.md). The state
holds the very DataFrame the reader returned: `AppendOnly` never copies, converts, counts or
checks it, and leaves the state as it found it. A missing key raises `KeyError(key)` when the
process runs, and the writer is not called. The DataFrame is lazy: the CSV files are read when
an action runs on it, such as `isEmpty()` or the write, and every action reads the selected
files again.

### Testing a job

- **Through the public API.** A test imports the job's functions from `main.py` and its
  modules, builds a `Process`, runs it, and checks what is observable: the rows of the target,
  `process.state`, `process.cancelled`, the log and the errors.
- **An initial state.** `Process(steps, state={...}, run_id="<id>")` runs a step on a value
  the test built, such as the load step alone on a DataFrame the test created; a fixed run id
  makes the log lines predictable (docs/concepts/callbacks.md, docs/concepts/lifecycle.md).
- **The configuration.** Tests write each configuration with `json.dumps` to a file under
  `tmp_path` and pass its path, as the library's own tests do
  (specs/config_connectors.md, section Notes for testing). Its `path` is the absolute
  directory, under `tmp_path`, where the test wrote its CSV files.
- **A local source.** CSV files written by the test under `tmp_path`, with modification times
  set by the test with `os.utime(file, ns=(n, n))` when the watermark is the last-modified time
  (specs/disk_connectors.md, section Notes for testing).
- **Spark.** One local session for the whole run, built as the library's own integration tests
  build theirs: `SparkSession.builder.master("local[2]")`, with `spark.jars.packages` naming
  the PostgreSQL JDBC driver, `spark.sql.session.timeZone` set to `UTC` and
  `spark.ui.enabled` set to `false` (tests/test_postgres_connectors.py). Section 4 says why it
  must be the first session of the run.
- **PostgreSQL.** The one in Docker: `docker compose` locally, the service container in CI.
  Each test works in a schema or a table of its own, created and dropped by the test through
  psycopg, as the library's integration tests do (tests/test_postgres_connectors.py).
- **The log.** Each line of the tree is one record whose message is the plain line, such as
  `[run0001] │ │ ✔ AppendOnly.run 15ms` (docs/concepts/lifecycle.md). `caplog` sees the
  `INFO` lines only after `caplog.set_level(logging.INFO, logger="ingestio")`: the library's
  logger has only a `NullHandler`, and its level otherwise comes from the root logger.
- **The errors.** `pytest.raises(ValueError, match=...)` around the building of a callback
  catches the configuration's errors; the errors of `read()` and `write()` leave
  `Process.run()` unchanged.
- **What the library offers for tests:** nothing beyond its public API. There are no fakes,
  fixtures, test sessions or test databases, no run result to inspect, and no way to see
  which files the reader selected.

## 4. What this job uses

### Callbacks

**`ReadSource`** (docs/concepts/callbacks.md, docs/concepts/configuration.md)

```text
ReadSource(config, *, into="records", connection=None, client=None, watermark_value=None, spark=None)
```

- Built: reads the `source` section, checks it, and builds a `DiskReader` from it, which
  validates its arguments. No Spark, no file system access.
- Run: calls `read()` once and stores the DataFrame in `state[into]`. The listing, the
  selection of the files and the errors of the file system happen here (see `disk` below).
- For `disk` it takes `watermark_value` and `spark`, both optional, and rejects `connection`
  and `client`.
- Essential: every error propagates unchanged.

**`AppendOnly`** (docs/concepts/callbacks.md, docs/concepts/connectors.md)

```text
AppendOnly(config, *, key="records", connection=None, client=None)
```

- Built: reads the `target` section and builds a `PostgresWriter`. `connection` is required
  for `postgres`; without psycopg, building fails with
  `ImportError("PostgresWriter needs psycopg: pip install 'ingestio[postgres]'")`, with a note.
- Run: calls `write(state[key])` once, which appends: it inserts every row of the DataFrame
  and leaves the existing rows alone, in one transaction (see `postgres` below).
- It takes no `replace`, `spark` or `watermark_value`: placing `AppendOnly` is the choice of
  the strategy. It calls the writer whatever the value holds, an empty DataFrame included.
- Essential: every error of `write()` propagates unchanged.
- Its limitations: **an append is not idempotent**, so running the same batch twice appends
  it twice; one target per configuration file.

**Not used by this job.** `FullLoad` makes the target hold exactly the batch: on `postgres` it
truncates the table first, which is not an incremental append. `SCD2` keeps the history of a
dimension, which is not the goal (docs/concepts/callbacks.md).

**The job's own callbacks this goal may need.** The library leaves these to the job, and the
spec decides which exist:

- a stage that ends the load step when the batch is empty, before anything reads `_metadata`
  or writes (pitfalls 1 and 2);
- a stage that adds lineage columns to the DataFrame, such as the name or the modification
  time of the file each row came from, from Spark's hidden `_metadata` column, when the job
  keeps its watermark in the target (pitfall 3);
- a stage that renames or casts columns so that the DataFrame's columns are the target's
  (pitfall 10);
- the creation of the target table, unless the job does it outside the process (Prerequisites).

Each of them is essential, as every callback is at 0.5.0.

### Connectors and systems

#### Source: `"system": "disk"`, built as `DiskReader`

```text
from ingestio.connector.all import DiskReader      # not exported by ingestio itself

DiskReader(
    path: str | PathLike[str], format: str,
    *, watermark_pattern: str | None = None,
    watermark_value: str | datetime | None = None,
    schema: StructType | str | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    spark: SparkSession | None = None,
)
DiskReader.read() -> DataFrame
```

It reads the files of one directory through Spark's file data sources, and returns a
`pyspark.sql.DataFrame`. It needs no extra (docs/concepts/connectors.md, section Disk).

**Its keys:**

| Key | Required | What it is |
|---|---|---|
| `path` | yes | the directory that holds the files: an absolute local path. A relative path, an empty one or a URI (`file:/`, `s3://`) raises `ValueError("path must be an absolute local path")`. A trailing slash is dropped |
| `format` | yes | the name of the Spark data source, here `csv`, passed to Spark as it is. `delta` is rejected; an unknown name fails in Spark when the reader reads |
| `watermark_pattern` | no | a regular expression that the whole name of a file matches, with exactly one group that captures the file's watermark. Without it, a file's watermark is its last-modified time |
| `schema` | no | the schema of the files, as a DDL string such as `"id INT, name STRING, measured_at TIMESTAMP"`. Spark infers it when it is left out. A `StructType` cannot be given through the configuration |
| `options` | no | Spark data source options, passed through as they are, with no default of the connector |

The connector never chooses a format, never infers one from file extensions, and transforms
nothing: it adds no column, casts nothing and filters no rows. Spark's CSV data source does the
conversion (docs/concepts/connectors.md).

**How a read selects the files.** Each `read()` goes through these steps, and the selection is
made before Spark loads anything (docs/concepts/connectors.md):

1. **Session:** `spark` when given, otherwise `SparkSession.active()`, looked up at this moment.
   With no active session, PySpark's error propagates.
2. **Listing:** the entries directly inside `path`, once, on the driver, with the standard
   library. A missing `path` raises `FileNotFoundError`, a file raises `NotADirectoryError`,
   and `PermissionError` propagates.
3. **Candidates:** regular files, or links to one, whose name starts with neither `_` nor `.`
   and does not end with `._COPYING_`. Subdirectories are ignored, with everything in them.
4. **The watermark of each candidate**, as below.
5. **Selection:** the candidates whose watermark is strictly greater than `watermark_value`, or
   all of them when it is `None`.
6. **Load:** one load of the selection, with `format`, `schema` and `options` as given. Spark
   receives the list of selected paths, sorted by file name, never the directory.

**The watermark of a file** (docs/concepts/connectors.md):

- *With `watermark_pattern`:* the pattern is matched against the file name with
  `re.fullmatch` (Python's dialect), and the watermark is the text its group captured. A file
  whose name does not match is not read. Watermarks compare as text, code point by code point,
  and are never converted: `"9"` is above `"10"`. `watermark_value` must then be a `str`.
- *Without it:* the watermark is the file's last-modified time, following links, truncated to
  whole milliseconds and compared with `watermark_value` as an instant. `watermark_value` must
  then be a timezone-aware `datetime`.

**The selection is frozen; the contents are not.** Every action on the DataFrame reads the
selected files and no other, so files that appear after `read()` wait for the next run. A
selected file that changes before an action is read as it is then; one that is deleted makes
the action fail, unless the job passes Spark's `ignoreMissingFiles` option
(docs/concepts/connectors.md, section Limitations of the disk connectors).

**The selection is not exposed.** The reader keeps no record of it. A job that advances its
watermark takes it from the DataFrame, through Spark's hidden `_metadata` column, whose fields
`file_name`, `file_path` and `file_modification_time` describe the file of each row
(docs/concepts/connectors.md). `file_modification_time` has millisecond precision, the same as
the reader's, so a job that takes its next watermark from it never selects its last file
again.

**The empty selection.** When nothing is selected, `read()` makes no load and returns an empty
DataFrame: with the columns of `schema` when it is given, with no column at all otherwise. It
is not backed by files, so it has no `_metadata` column (docs/concepts/connectors.md).

**Its errors when it is built** (docs/concepts/connectors.md, section Building the disk
connectors): `path must be an absolute local path`; `format must not be empty`;
`format '<format>' cannot be read file by file` for `delta`; `option '<key>' is set by the
connector` for `path`, `paths`, `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore` and
`modifiedAfter`, in any case; `invalid watermark_pattern: <error>`;
`watermark_pattern must have exactly one group` (a named group counts, `(?:...)` does not);
`watermark_value must be a str with watermark_pattern`;
`watermark_value must be a datetime without watermark_pattern`;
`watermark_value must be timezone-aware`. The connector catches nothing and writes no log line.

**CSV options** *(Spark)*. The `options` object reaches Spark's CSV data source as it is. The
ones a CSV dataset usually needs: `header` (the first line holds the column names), `sep`,
`encoding`, `quote`, `escape`, `multiLine`, `nullValue`, `emptyValue`, `dateFormat`,
`timestampFormat`, `inferSchema`, `enforceSchema`, and `mode` (`PERMISSIVE`, the default, keeps
malformed rows with nulls; `DROPMALFORMED` drops them; `FAILFAST` fails). Inside `options`,
`mode` is Spark's parse mode, not a write mode. A boolean in JSON, such as `true`, reaches
Spark as `"true"` (specs/disk_connectors.md, section Notes for testing).

#### Target: `"system": "postgres"`, built as `PostgresWriter`

```text
from ingestio.connector.all import PostgresConnection, PostgresWriter   # not exported by ingestio

PostgresConnection(
    host: str, database: str, user: str, password: str,
    *, port: int = 5432, sslmode: str | None = None,
)
PostgresWriter(
    table_name: str, connection: PostgresConnection,
    *, options: Mapping[str, str | int | float | bool] | None = None,
)
PostgresWriter.write(data: DataFrame, *, replace: bool = False) -> None
```

**Its keys:** `table_name`, required, and `options`. Its callback argument: `connection`,
required. Its extra: `postgres` (docs/concepts/configuration.md).

**`PostgresConnection`** (docs/concepts/connectors.md). Where and how to reach one database.
It opens nothing, is immutable, and its arguments are readable as attributes of the same
names. Building it raises `ValueError` for an empty `host` (`host must not be empty`), an empty
`database` or `user`, a `port` outside 1 to 65535, or an `sslmode` that is not one of
`disable`, `allow`, `prefer`, `require`, `verify-ca` and `verify-full`. The password is not
validated, and `repr` never shows it. pgJDBC receives the URL
`jdbc:postgresql://<host>:<port>/<database>`, with the user and the password as options;
psycopg receives keyword arguments.

**`table_name`** is `table` or `schema.table`; anything else raises
`ValueError("table_name must be 'table' or 'schema.table'")`. Names are case-sensitive, as
stored in the PostgreSQL catalog, and always quoted: `Sales.Orders` is `"Sales"."Orders"`. A
name without a schema resolves through the connections' `search_path`
(docs/concepts/connectors.md).

**How a write happens** (docs/concepts/connectors.md). Spark's JDBC writer commits one
transaction per partition, so the writer makes the write atomic in three steps:

1. **Setup.** One psycopg connection, on the Spark driver, creates an unlogged staging table,
   `ingestio_staging_<32 hexadecimal digits>`, in the target's schema, with the target's types
   for exactly the DataFrame's columns. A DataFrame column that the target lacks fails here,
   and so does a missing target, with PostgreSQL's error, before any row moves.
2. **Load.** Spark writes the DataFrame into the staging table, in parallel, through pgJDBC.
3. **Apply.** A new psycopg connection runs, in one transaction, `INSERT INTO <target>
   (<columns>) SELECT <columns> FROM <staging>` and the `DROP` of the staging table. Readers
   of the target see none of the new rows until this commits, and then all of them.

- The target's constraints, triggers, identity columns and the defaults of the columns the
  DataFrame lacks apply in step 3, so the target may have more columns than the DataFrame,
  such as an identity key or a load time with a default.
- **Failures:** every error propagates out of `write()`. A failed load or apply leaves the
  target as it was and drops the staging table on a new connection; when that cleanup fails
  too, the original error gets the note `staging table <stg> could not be dropped: ...`. A
  DataFrame with no column raises `ValueError("the DataFrame has no columns")` and does
  nothing else.
- **No DDL on the target:** a missing target fails the setup, and the writer creates nothing
  (ADR 0014; see Prerequisites).
- It evaluates the DataFrame once, through the save, and never counts it. An empty DataFrame
  with columns goes through the same steps and inserts nothing. The connector writes no log
  line.
- **Options:** Spark JDBC write options, such as `batchsize` or `numPartitions`. The writer
  sets `stringtype` to `unspecified`, which lets Spark strings go into typed columns such as
  `uuid`, `jsonb` and enums, and `reWriteBatchedInserts` to `true`; user options replace them.
  `url`, `dbtable`, `query`, `user`, `password` and `driver` are set by the connector, and
  giving one, in any case, raises `option '<key>' is set by the connector`.
- **Privileges:** `CREATE` on the target's schema, for the staging table, and `SELECT` and
  `INSERT` on the target.

#### How the library expects them to run here

- Open-source Spark 3.5 or later, in local mode, with Spark's default file system the local
  one, which is the default outside Hadoop clusters. In local mode the driver's Python process
  and Spark see the same directory, which `DiskReader` requires
  (docs/concepts/connectors.md, section Environments of the disk connectors).
- Java 17 or 21, and the pgJDBC driver on Spark's classpath, "for example through
  `spark.jars.packages=org.postgresql:postgresql:<version>`" (docs/concepts/connectors.md,
  section Supported environments).
- The Spark driver reaches PostgreSQL with psycopg, as Spark's tasks do with pgJDBC: in local
  mode both run on the machine that runs the job.

### Prerequisites

| Prerequisite | Where it comes from | How this job gets it |
|---|---|---|
| ingestio | PyPI, `ingestio[postgres]==0.5.0` | `dependencies` in `pyproject.toml`, the only entry there; `uv.lock` resolves 0.5.0 from PyPI |
| psycopg | the `postgres` extra: `psycopg[binary]>=3.2` (docs/concepts/connectors.md, section Extras) | installed with ingestio; `uv.lock` resolves psycopg 3.3.6 and psycopg-binary 3.3.6 |
| pyspark | never installed by ingestio: a prerequisite that the runtime provides (docs/concepts/connectors.md, section Extras) | the `dev` dependency group of `pyproject.toml`, `pyspark>=3.5`, the floor ingestio documents; `uv.lock` resolves pyspark 4.2.0, with py4j 0.10.9.9 |
| Java 17 or 21 | a JDK, outside Python (docs/concepts/connectors.md) | locally, installed by the developer, on the `PATH` or in `JAVA_HOME`; in CI, `actions/setup-java` with Temurin 17, in `.github/workflows/ci.yml` |
| the pgJDBC driver | `org.postgresql:postgresql`, a Java library on Maven Central, not a Python package and not an extra | the job's Spark session: `spark.jars.packages` on the session's builder |
| PostgreSQL | the Docker image `postgres:17` | locally, a `docker compose` file of the job; in CI, the service container of `.github/workflows/ci.yml` |
| the target table | the job's DDL, never the library | created by the job before the first write, as below |

**psycopg.** `PostgresWriter` imports it when it is built and uses it on the Spark driver for
the setup, the apply and the cleanup (docs/concepts/connectors.md). The job's own code may use
it too, for its DDL or for the query of its watermark: it is not ingestio, so rule 1 of
section 5 does not cover it. It is a dependency of the extra, not of the job, so the job
uses the version the lock resolves and never pins it in `dependencies`.

**pyspark.**

- ingestio documents pyspark 3.5 or later as a prerequisite the runtime provides, and never
  installs it, because installing pyspark over a runtime such as Databricks breaks it
  (docs/concepts/connectors.md; pyproject.toml). This job's runtime is its own environment, so
  pyspark is in the `dev` group: `uv sync`, `uv run` and CI's `uv sync --locked` install that
  group by default. `dependencies` holds ingestio alone.
- pyspark 4.2.0 is published only as a source archive of about 450 MB. The first `uv sync`
  downloads and builds it, and uv caches the result; CI's `setup-uv` cache keeps it between
  runs.
- An environment built without the dev group (`uv sync --no-dev`, `pip install .`) has no
  Spark, and the job cannot run in it.
- `ingestio` imports without pyspark: a missing pyspark shows only when a reader reads or a
  session is built (docs/concepts/connectors.md, section Extras).

**Java.**

- Spark 4.2 needs Java 17 or 21 (docs/concepts/connectors.md; specs/disk_connectors.md,
  section Integration tests). The library's own CI runs its Spark tests on the runner's
  Java 17 (.github/workflows/ci.yml).
- Newer Java releases are not supported: the library's spec records that Spark 4.2 does not
  start on Java 27 (specs/disk_connectors.md, section History). A wrong Java fails when the
  session starts, before any callback runs.

**The pgJDBC driver.**

- The job sets `spark.jars.packages` to `org.postgresql:postgresql:<version>` on the builder,
  before the session starts. Spark downloads it from Maven Central on the first start and
  caches it in the user's home directory *(Spark)*, so the first run needs the network, and so
  does every CI run.
- The version is the job's to pin, in one place of its code that `main.py` and the tests'
  session share. The library's own integration tests use `42.7.4`
  (tests/test_postgres_connectors.py); the latest release on Maven Central when this guidance
  was generated is `42.7.14`. The spec chooses.
- The connector itself sets the driver class, `org.postgresql.Driver`
  (src/ingestio/connector/postgres.py). Without the driver on the classpath, the write fails
  in Spark with `java.lang.ClassNotFoundException: org.postgresql.Driver` *(Spark)*.
- `SparkSession.builder...getOrCreate()` returns the session already running, with its own
  configuration *(Spark)*. A session started earlier in the same process without the package,
  by another fixture for instance, has no driver: the session that carries the driver must be
  the first one of the process.

**PostgreSQL in Docker.**

- In CI, `.github/workflows/ci.yml` runs `postgres:17` as a service container, with
  `POSTGRES_PASSWORD=postgres`, port 5432 published and a `pg_isready` health check, and gives
  the steps libpq's standard variables: `PGHOST=localhost`, `PGPORT=5432`, `PGUSER=postgres`,
  `PGPASSWORD=postgres`, `PGDATABASE=postgres`. The job's code and its tests build the
  `PostgresConnection` from them. If the spec chooses other names, the implementer changes
  that `env` of the workflow with them.
- Locally, the job's `docker compose` file, written by the implementer, runs the same image
  with the same settings and the same published port, so the same variables reach it.
  `docker compose up -d --wait` returns once the health check passes *(Docker)*. The spec
  decides whether the job requires the variables or falls back to the values of its compose
  file.
- The user and the password are the throwaway values of a local container, not credentials of
  a third party. They still never go in `config.json`.

**The target table's DDL: the job's, not the library's.** `PostgresWriter` runs no DDL on the
target: a missing table fails the setup step with PostgreSQL's error, and nothing is created
(docs/concepts/connectors.md; ADR 0014; specs/postgres_connectors.md, decision 29). The job
creates the table before its first write, with the columns and types its spec chooses, and
the schema that holds it, since a connector never creates what holds its target (ADR 0014).

- **Recommended: the job applies its own DDL**, `CREATE TABLE IF NOT EXISTS ...` through
  psycopg, before the load: in a step of its own placed before the load, or in a function
  `main.py` calls before it runs the process. Local runs, tests and CI then share one
  definition.
- An SQL script in the compose file's `/docker-entrypoint-initdb.d` runs only when the
  container starts on an empty data directory *(Docker)*, and CI's service container cannot
  load it: services start before the checkout *(GitHub Actions)*. It can never be the only
  place the table is created.
- The DDL lives in the job's code or files, never in the library's sections of
  `config.json`. The spec decides where, and the tests create the table their own way when
  they work in a schema of their own.

### Known pitfalls

Each with what the job does about it.

1. **An empty selection without `schema` fails the write.** The DataFrame has no column, and
   `PostgresWriter` raises `ValueError("the DataFrame has no columns")`, which fails the run
   (docs/concepts/connectors.md). *The job* ends the load step with its own callback, raising
   `CancelStepException` when the batch is empty, before `AppendOnly` in the same step, or
   gives a `schema`, so that an empty append inserts nothing. A run with no new file is a
   normal run for this job, so the spec says which one it is.
2. **An empty selection has no `_metadata` column**, with or without `schema`
   (docs/concepts/connectors.md). *The job* checks for an empty batch before any of its
   callbacks reads `_metadata`.
3. **An append is not idempotent, and the watermark is the job's.** Running the same files
   twice appends their rows twice (docs/concepts/callbacks.md). The reader decides no strategy:
   whether a run is incremental, from which value, and how the next watermark is computed and
   kept belong to the job (docs/concepts/connectors.md). The append itself is one transaction,
   so the place of the watermark decides whether the job is exactly-once:
   - kept **in the target's own rows**, as a lineage column stamped from `_metadata` and read
     back with `SELECT max(...)` before the next run, it commits in the same transaction as
     the rows: a failed run leaves both as they were, and the next run reads the same files
     again;
   - kept **elsewhere**, in a control table or a file updated after the process, it is a
     second commit: a crash between the two appends the same files again on the next run.

   *The spec* chooses where the watermark lives and states the guarantee that follows. Reading
   the watermark is the job's own code (README.md: "query the warehouse, read a control
   table").
4. **The watermark is computed before the process is built.** `watermark_value` is an
   argument of `ReadSource`, so a step of the same process cannot compute it
   (docs/concepts/configuration.md). *The job* reads it in `main.py` before it builds the
   process.
5. **The selection is not exposed** (docs/concepts/connectors.md). *The job* derives the next
   watermark from the rows it wrote, or from the DataFrame's `_metadata.file_name` or
   `_metadata.file_modification_time`. In Spark, `regexp_extract` uses Java's regular
   expressions, not Python's: "plain groups only" (docs/concepts/connectors.md, section
   Examples). The `watermark_pattern` of `config.json` is matched with Python's `re`, so a
   pattern the job also uses in Spark avoids named groups and other syntax the two dialects do
   not share.
6. **Name watermarks compare as text, on the whole name** (docs/concepts/connectors.md). The
   pattern must match the entire file name, extension included, with exactly one group. The
   captured text must be fixed-width and most significant first, such as `20261021` or a
   zero-padded number: `part_10.csv` comes before `part_9.csv`. A file whose name does not
   match is silently never read, and so are the files of a producer that renames them. In
   JSON, every backslash of the pattern is doubled: `"data_(\\d{8})\\.csv"`. *The spec* uses a
   pattern when the dataset's names carry such a key, and checks every file name against it.
7. **Modification times are fragile watermarks** (docs/concepts/connectors.md). A rename and a
   copy that preserves times keep the modification time, so a file moved in after a newer one
   was read is never read; a checkout or a download gives every file the time it was written
   on that machine; files modified in the same millisecond as the watermark are not read. *The
   spec* prefers a name pattern when the dataset allows it; tests set modification times
   explicitly with `os.utime`.
8. **Late, shared and unfinished files** (docs/concepts/connectors.md). A file that appears
   with a watermark at or below the last one ingested is never read, and so is a file that
   shares the last watermark and arrives after it, because the comparison is strict. A file
   still being written when `read()` lists the directory can be read in part: a producer
   writes under a name starting with `_` or `.`, or ending with `._COPYING_`, and renames it
   when it is complete. *The spec* says how the files reach the directory, and in which order.
9. **`path` must be absolute** (docs/concepts/connectors.md). `config.json` cannot name a
   directory relative to itself or to the repository: a relative path is rejected, because
   the driver's Python process and Spark can resolve it differently. *The tests* write their
   own configuration with a `tmp_path` directory. *The spec* chooses the absolute directory
   the job reads when it runs for real; a job that needs a directory relative to the
   repository records a library gap rather than rewriting the configuration in code.
10. **Column names must be the target's, exactly.** The writer quotes every name as it is, and
    a DataFrame column that the target lacks fails the setup (docs/concepts/connectors.md). A
    CSV header such as `Country Name` is not the column `country_name`. *The job* gives a
    `schema` whose names are the target's, or renames the columns in its own callback before
    the write.
11. **`schema` and the header** *(Spark)*. With a `schema` and `header`, Spark's CSV reader by
    default (`enforceSchema` true) ignores the names in the header and applies the schema by
    position; with `enforceSchema` false it checks the header's names against the schema's and
    fails when they differ. Without a `schema`, the columns take the header's names, and every
    column is a string unless `inferSchema` is set, which costs one more pass over the files.
    *The spec* chooses the schema, its names and these options, knowing pitfall 10.
12. **Types and time zones.** The values go through Spark's CSV parsing, then Spark's JDBC type
    mapping and pgJDBC; the connectors convert nothing (docs/concepts/connectors.md). With
    `stringtype=unspecified`, PostgreSQL converts strings to the staging table's types, and a
    value it cannot convert fails the write, leaving the target unchanged and no staging table
    (specs/postgres_connectors.md, section Integration tests). Timestamps pass through the
    JVM's time zone (docs/concepts/connectors.md): the library's own tests set
    `spark.sql.session.timeZone` to `UTC` (tests/test_postgres_connectors.py). Spark hands
    timestamps to Python as naive local times, and `astimezone` reads them so
    (tests/test_postgres_connectors.py); `DiskReader` rejects a naive `watermark_value`. *The
    job* fixes its session's time zone, prefers `timestamptz` for instants, and makes every
    watermark it passes timezone-aware.
13. **Spark task retries can duplicate rows.** A task re-run after its partition committed
    writes the partition twice into the staging table, and the copy reaches the target
    (docs/concepts/connectors.md). Local mode, `local[N]`, does not retry failed tasks, and
    speculation is off by default *(Spark)*. *The job* keeps a local master and leaves those
    settings alone.
14. **Orphaned staging tables.** When the driver dies, or the cleanup fails, a table named
    `ingestio_staging_<hex>` is left in the target's schema (docs/concepts/connectors.md).
    *The job* leaves them to whoever operates the database; a test that kills nothing can
    check that none is left.
15. **No session, no read.** The connectors never create a session; without one, `read()`
    fails with PySpark's error (docs/concepts/connectors.md). *The job* creates its session in
    `main.py` before `Process.run()`, and the tests in one fixture for the whole run.
16. **Glob characters in file names.** Spark expands `*`, `?`, `[`, `]`, `{`, `}` and `\` in
    the paths it loads, and the reader does not escape them (docs/concepts/connectors.md).
    *The spec* checks the dataset's file names.
17. **Large directories.** The driver lists the whole directory at each `read()`, and Spark
    receives one path per selected file (docs/concepts/connectors.md).
18. **Two consoles.** `log_to_console()` never touches the root logger: a job that also prints
    the root logger on the console sees every line twice (docs/concepts/lifecycle.md). *The
    job* chooses one.
19. **Integration tests that skip themselves.** The library's own PostgreSQL integration tests
    are skipped unless `INGESTIO_POSTGRES_URL` is set, and its CI does not set it
    (tests/test_postgres_connectors.py, .github/workflows/ci.yml): this job's tests are the
    first to run `PostgresWriter` against a real server in CI. *The tests* fail, rather than
    skip, when the PostgreSQL that CI provides is unreachable.

### What the spec decides

This guidance leaves these choices to the job's planner, each with the pitfalls above:

- the public dataset, that its license allows what the job does with it, and how its files
  reach the landing directory;
- where each file's watermark comes from, a pattern on its name or its last-modified time,
  and where the job keeps the last one;
- the CSV options and the `schema`;
- the target table: its schema and name, its columns and types, its DDL and what applies it;
- the absolute path of the landing directory;
- the job's own callbacks, and where each is placed;
- the variables that reach PostgreSQL, and the version of the pgJDBC driver.

## 5. Rules of a valid job

Every process of this job follows these rules. A process that can follow one only with a
workaround of the library is not written with the workaround: the gap is recorded in
`LIB_GAPS.md`, as `AGENTS.md` says. Under each rule, what it means at version
`0.5.0`.

1. **Only the public API of the library, never an internal import.**

   The public API is what `ingestio`, `ingestio.callback.all` and `ingestio.connector.all`
   export, through those import paths, and the documented attributes `Process.state`,
   `Process.run_id`, `Process.cancelled`, `Process.started_at` and `Callback.process`
   (AGENTS.md of the library), plus `Callback.name` and the attributes of
   `PostgresConnection` (docs/concepts/callbacks.md, docs/concepts/connectors.md). The
   connectors are exported by `ingestio.connector.all` only, never by `ingestio`. Everything
   else is internal: `ingestio.basics`, `ingestio.core`, `ingestio.console`,
   `ingestio.callback.core`, `ingestio.callback.io`, `ingestio.callback._configuration`,
   `ingestio.connector.core`, `ingestio.connector.local`, `ingestio.connector.postgres`,
   `ingestio.connector._files` and every other module, and every name that starts with `_`,
   such as `PostgresConnection._jdbc_options`, even when Python lets the job reach it.
   pyspark and psycopg are not ingestio: this rule does not cover them. The full list of
   names is in section 6.

2. **Custom connectors and callbacks only through the documented extension.**

   For callbacks: a subclass of `Callback` in the job's own code, nothing registered
   (docs/philosophy.md, docs/concepts/callbacks.md). For connectors, there is no documented
   extension at 0.5.0: only the shipped systems can be named in `config.json`, and a job
   cannot add one (ADR 0015). What a job's own callback may do is build a shipped connector of
   `ingestio.connector.all` itself (docs/concepts/connectors.md, section Using a connector). A
   process that needs a system the library does not ship is a library gap.

3. **Criticality declared explicitly on every non-essential callback.**

   At 0.5.0 no callback can be declared non-essential: there is no declaration and no run
   result (docs/concepts/criticality.md). Every callback of this job is essential, and its
   failure fails the run. A process that needs a non-essential stage, one whose failure
   should let the run go on and be recorded, cannot be built: it is a library gap. A callback
   that catches its own failure so that the run goes on is a non-essential stage without the
   record the philosophy requires: a workaround, not a solution. Raising
   `CancelStepException` for a condition the spec expects, such as an empty batch, is control
   flow, not criticality.

4. **No connector decides the write strategy.**

   The callback the job places chooses it: `AppendOnly` appends, `FullLoad` replaces, `SCD2`
   keeps a history (ADR 0016). The configuration has no mode, and `append`, `replace` and
   `mode` are not keys (docs/concepts/configuration.md). A job's own callback that builds a
   shipped writer chooses by calling `write(data)` or `write(data, replace=True)` itself. For
   this job the strategy is `AppendOnly`, and no option of the `target` section changes it.

5. **No monkeypatching, no subclassing of internals, no other way around the library.**

   Legitimate at 0.5.0: subclassing `Callback`; the arguments of `Process` (`state`, `run_id`,
   `log`) and of `Step`; the documented arguments of the shipped callbacks (`into`, `key`,
   `connection`, `client`, `watermark_value`, `spark`); the sections and keys of the
   configuration; the two cancel exceptions; `log_to_console`; `Reader` and `Writer` in
   annotations; building a shipped connector inside a callback of the job's own. Working
   around the library: subclassing a shipped callback or connector to reach its private
   attributes, replacing a function of ingestio or of the libraries it calls
   (`SparkSession.active`, `psycopg.connect`) in the job's code, generating the job's
   configuration from code at run time to get around a rule of the configuration (tests
   writing configurations of their own under `tmp_path` is how the library tests itself, and
   is fine), or writing the batch to the target with Spark's own JDBC writer or with psycopg
   instead of `AppendOnly`. The job's own DDL and
   its watermark query are not a way around the library: the library leaves both to the job
   (ADR 0014, README.md).

6. **The library's default callbacks whenever they fit,** append-only and SCD2 among them,
   instead of reimplementing them.

   At 0.5.0 the library ships `ReadSource`, to read the `source` of the configuration into the
   state; `AppendOnly`, to add a batch to the target; `FullLoad`, to make the target hold
   exactly the batch; and `SCD2`, to keep the history of a dimension on `unity_catalog`,
   `postgres` or `bigquery` (docs/concepts/callbacks.md). This job reads with `ReadSource` and
   writes with `AppendOnly`; it never reads the files or appends the rows with code of its
   own.

## 6. Public API at a glance

**`ingestio`** (src/ingestio/__init__.py)

| Name | Signature |
|---|---|
| `Process` | `Process(steps: Iterable[Step], *, state: Mapping[str, Any] \| None = None, run_id: str \| None = None, log: logging.Logger \| None = None)`; `run() -> Process`; attributes `state: dict[str, Any]`, `run_id: str`, `cancelled: bool`, `started_at: datetime \| None` |
| `Step` | `Step(name: str, callbacks: Iterable[Callback] = ())`, immutable; `ValueError` for a blank name |
| `Callback` | subclass it; `before() -> None`, `run() -> None`, `after() -> None`; attributes `process: Process`, `name: str` (a property, the class name by default) |
| `CancelStepException` | `Exception` subclass: skip the rest of the step |
| `CancelProcessException` | `Exception` subclass: stop the process cleanly |
| `ReadSource` | `ReadSource(config: str \| os.PathLike[str], *, into: str = "records", connection: PostgresConnection \| None = None, client: object \| None = None, watermark_value: int \| Decimal \| str \| date \| None = None, spark: SparkSession \| None = None)` |
| `AppendOnly` | `AppendOnly(config: str \| os.PathLike[str], *, key: str = "records", connection: PostgresConnection \| None = None, client: object \| None = None)` |
| `FullLoad` | `FullLoad(config, *, key="records", connection=None, client=None)`, as `AppendOnly` |
| `SCD2` | `SCD2(config, *, key="records", connection=None, client=None)`, as `AppendOnly`; reads the `target` and `scd2` sections |
| `Reader` | `Protocol[T_co]` with `read() -> T_co` |
| `Writer` | `Protocol[T_contra]` with `write(data: T_contra) -> None` |
| `log_to_console` | `log_to_console(*, stream: TextIO \| None = None, level: int = logging.INFO, color: bool \| None = None) -> logging.Handler`; `ValueError("level must be above NOTSET")` |
| `__version__` | `"0.5.0"` |

**`ingestio.callback.all`** (src/ingestio/callback/all.py): `SCD2`, `AppendOnly`, `Callback`,
`CancelProcessException`, `CancelStepException`, `FullLoad`, `ReadSource`, the same objects
as above.

**`ingestio.connector.all`** (src/ingestio/connector/all.py, docs/concepts/connectors.md).
Every reader below except `JsonApiReader` returns a `pyspark.sql.DataFrame`, and every writer
except `JsonLinesWriter` takes one, with `write(data: DataFrame, *, replace: bool = False) ->
None`.

| Name | `system` | Signature |
|---|---|---|
| `Reader`, `Writer` | | the protocols above |
| `JsonApiReader` | `json_api` | `JsonApiReader(url, *, timeout=30.0, headers=None, encoding="utf-8")`; `read()` returns the decoded JSON |
| `JsonLinesWriter` | `json_lines` | `JsonLinesWriter(path: Path \| str, *, encoding: str = "utf-8")`; `write(data: Iterable[Mapping[str, Any]], *, replace: bool = False) -> None` |
| `PostgresConnection` | | `PostgresConnection(host: str, database: str, user: str, password: str, *, port: int = 5432, sslmode: str \| None = None)`; attributes of the same names |
| `PostgresReader` | `postgres` | `PostgresReader(table_name: str, connection: PostgresConnection, *, watermark_column: str \| None = None, watermark_value: int \| Decimal \| str \| date \| None = None, options: Mapping[str, str \| int \| float \| bool] \| None = None, spark: SparkSession \| None = None)` |
| `PostgresWriter` | `postgres` | `PostgresWriter(table_name: str, connection: PostgresConnection, *, options: Mapping[str, str \| int \| float \| bool] \| None = None)` |
| `DiskReader` | `disk` | `DiskReader(path: str \| PathLike[str], format: str, *, watermark_pattern: str \| None = None, watermark_value: str \| datetime \| None = None, schema: StructType \| str \| None = None, options: Mapping[...] \| None = None, spark: SparkSession \| None = None)` |
| `DiskWriter` | `disk` | `DiskWriter(path: str \| PathLike[str], format: str, *, options: Mapping[...] \| None = None)` |
| `S3Reader` | `s3` | `S3Reader(path: str, format: str, *, watermark_pattern=None, watermark_value: str \| datetime \| None = None, schema=None, options=None, client: S3Client \| None = None, spark=None)` |
| `S3Writer` | `s3` | `S3Writer(path: str, format: str, *, options=None)` |
| `GcsReader` | `gcs` | `GcsReader(path: str, format: str, *, watermark_pattern=None, watermark_value: str \| datetime \| None = None, schema=None, options=None, client: google.cloud.storage.Client \| None = None, spark=None)` |
| `GcsWriter` | `gcs` | `GcsWriter(path: str, format: str, *, options=None)` |
| `VolumeReader` | `volume` | `VolumeReader(volume: str, path: str, format: str, *, watermark_pattern=None, watermark_value: str \| datetime \| None = None, schema=None, options=None, client: WorkspaceClient \| None = None, spark=None)` |
| `VolumeWriter` | `volume` | `VolumeWriter(volume: str, path: str, format: str, *, options=None)` |
| `UnityCatalogReader` | `unity_catalog` | `UnityCatalogReader(table_name: str, *, watermark_column=None, watermark_value: int \| Decimal \| str \| date \| None = None, options=None, spark=None)` |
| `UnityCatalogWriter` | `unity_catalog` | `UnityCatalogWriter(table_name: str, *, options=None)` |
| `BigQueryReader` | `bigquery` | `BigQueryReader(table_name: str, *, watermark_column=None, watermark_value: int \| Decimal \| str \| date \| None = None, billing_project=None, options=None, client: bigquery.Client \| None = None, spark=None)` |
| `BigQueryWriter` | `bigquery` | `BigQueryWriter(table_name: str, *, billing_project=None, options=None, client: bigquery.Client \| None = None)` |

`Mapping[...]` is `Mapping[str, str | int | float | bool]` throughout. Building any connector
validates its arguments and touches neither Spark, nor the network, nor the file system.

## 7. Sources

Every file of the ingestio repository read to write this guidance, at tag `v0.5.0`:

- `README.md`, `AGENTS.md`, `CHANGELOG.md`, `pyproject.toml`, `.python-version`,
  `.github/workflows/ci.yml`, and the entries of `uv.lock` for pyspark, psycopg and py4j
- `docs/philosophy.md`, `docs/index.md`, `docs/backlog.md`
- `docs/concepts/callbacks.md`, `docs/concepts/configuration.md`,
  `docs/concepts/connectors.md`, `docs/concepts/criticality.md`, `docs/concepts/lifecycle.md`
- `docs/decisions/0002-zero-dependency-core.md`, `0003-connectors-as-protocols.md`,
  `0004-shared-process-state.md`, `0005-declaration-order.md`, `0006-three-pass-step.md`,
  `0007-cancellation-exceptions.md`, `0008-stdlib-logging-run-id.md`,
  `0009-typed-package.md`, `0011-callbacks-as-stages-with-criticality.md`,
  `0012-streaming-deferred.md`, `0013-connector-configuration-convention.md`,
  `0014-create-a-missing-target-only-when-derived-from-the-data.md`,
  `0015-choose-connectors-from-the-job-configuration.md`,
  `0016-make-each-write-strategy-one-callback.md`,
  `0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md`
- `specs/disk_connectors.md` (summary, motivation, fit, environments, limitations, notes for
  testing, integration tests, open questions, history), `specs/postgres_connectors.md`
  (environments, limitations, notes for testing, integration tests, decisions 28 to 30),
  `specs/append_only_callback.md` (empty batch, errors, limitations, notes for testing),
  `specs/config_connectors.md` (limitations, notes for testing)
- `src/ingestio/__init__.py`, `src/ingestio/basics.py`, `src/ingestio/core.py`,
  `src/ingestio/console.py`, `src/ingestio/callback/__init__.py`,
  `src/ingestio/callback/all.py`, `src/ingestio/callback/core.py`,
  `src/ingestio/callback/io.py`, `src/ingestio/callback/_configuration.py`,
  `src/ingestio/connector/__init__.py`, `src/ingestio/connector/all.py`,
  `src/ingestio/connector/core.py`, `src/ingestio/connector/local.py`,
  `src/ingestio/connector/_files.py`, `src/ingestio/connector/postgres.py`
- `tests/conftest.py`, and the header and integration part of
  `tests/test_postgres_connectors.py`; `tests/` searched for how the library runs its Spark
  and PostgreSQL tests
- the list of files of the tag, `git ls-tree -r --name-only v0.5.0`

Outside the repository: the release metadata of ingestio 0.5.0 and pyspark 4.2.0 on PyPI, the
versions of `org.postgresql:postgresql` on Maven Central, the tags of the Docker image
`postgres`, and the inputs of `actions/setup-java` v6.
