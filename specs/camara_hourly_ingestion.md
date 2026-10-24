# Spec: Hourly CSV files of the Câmara dos Deputados, from a landing directory to PostgreSQL

- **Status:** validated
- **Slug:** `camara_hourly_ingestion`. The spec is `specs/camara_hourly_ingestion.md` and its tests are `tests/test_camara_hourly_ingestion.py`.
- **Version:** `0.3.0`, the layout of D15 (`0.2.0` was the first process)
- **Related:** `LIB_GAPS.md`: GAP-001 (a landing laid out in year/month/day folders cannot be
  read), from **Library gaps** below. Open questions Q1 to Q8 below, answered. The maintainer's
  definitions of 2026-10-24 for the layout of `0.3.0`: D15.

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

The job's first process. A preparation script of the job's own, `scripts/prepare_landing.py`,
downloads the yearly CSV files of the *Votações* dataset of the Câmara dos Deputados and lays
out, as a prerequisite, a flat landing directory with one CSV file per hour that has data, named
after that hour (`votacoes_YYYYMMDDHH.csv`), for two years of history. The ingestio process,
`src/main.py` with its configuration `src/config.json`, then reads with `ReadSource`, on the
`disk` system, only the files above the watermark kept in the target's own rows, stamps every
row with the name of its file, and appends the batch with `AppendOnly` to `camara.votacoes` in
PostgreSQL, in one transaction. Everything runs in two `docker compose` services, `spark` (the
job's image, where the landing directory is mounted and the script and the job run) and
`postgres`, and the README documents how to bring them up and run the job.

**Revision `0.3.0`.** On 2026-10-24 the maintainer asked for a change of structure: the job's
code moves to `src/` (`main.py`, `config.json` and `warehouse.py`), the job's own callbacks are
defined in `src/main.py` and `callbacks.py` no longer exists, and the preparation script moves
to `scripts/`. The commands become `docker compose exec spark python src/main.py` and
`docker compose exec spark python scripts/prepare_landing.py ...`. The process, its steps and
callbacks, the contents of the configuration, the table, the landing, the watermark, the log and
the exit codes are those of `0.2.0`: only where the files live and how they are invoked change
(D15).

This spec is written for the options recommended under **Open questions**. The parts that
depend on an answer are marked with the question, such as *(Q2)*; a different answer sends the
spec back to planning for those parts before approval.

## Motivation

The maintainer's request, 2026-10-24: two Docker containers orchestrated by `docker compose`,
with the way to bring the environment up and run the job documented in the README; container 1
with Spark and a mounted directory of CSV files of a public dataset, the Câmara dos Deputados –
Dados Abertos, prepared as a prerequisite, with the time of the data in each file name, one file
per hour, two years of history at first; container 2 with PostgreSQL, where the job creates the
table and loads the data; a job built with ingestio, as `main.py` and `config.json`.

The maintainer's second request, 2026-10-24, for `0.3.0`: "eu queria uma mudanca de estrutura.
Main, config e callbacks ficam dentro de uma pasta src", and then "inclusive, nao precisa existir
um arquivo chamado callback, quero que ele esteja dentro da main": a change of structure, with
the main module, the configuration and the callbacks inside a folder `src`, and no file of
callbacks, the callbacks living in the main module. The maintainer's answers to the questions
this raised are D15.

What it shows about ingestio:

- the library's minimum job, `main.py` and `config.json`, running a complete incremental load
  from files on disk to PostgreSQL with the shipped `ReadSource` and `AppendOnly`;
- that the minimum job does not depend on where its two files sit: `src/main.py` finds
  `src/config.json` as `Path(__file__).with_name("config.json")`, wherever it is run from, and
  defines the job's own callbacks itself, as the knowledge base allows;
- a watermark taken from the file names, selected by `DiskReader` before Spark loads anything,
  and kept in the target's own rows through Spark's `_metadata` column, so that the rows and the
  watermark commit in one transaction;
- the atomic append of `PostgresWriter`, the target's DDL owned by the job (ADR 0014), and the
  empty batch handled as control flow with `CancelStepException`;
- a real public dataset whose CSV quirks (the `;` separator, every field quoted, line breaks
  inside values) reach Spark through `options` that the connector passes on untouched.

What it reveals: the year/month/day layout the maintainer suggested cannot be read by a valid
job (GAP-001), and the job's own code needs the target's table name and the source's watermark
pattern, which only `config.json` holds (see **Decisions**, D13).

## Source and target

- **Source:** the `disk` system. The flat landing directory `/data/landing/votacoes` inside the
  `spark` service, a bind mount of the directory `data/landing/votacoes` of the repository's
  working copy on the host. Its files are local: the preparation script writes them from the
  public bulk files of the Câmara dos Deputados,
  `https://dadosabertos.camara.leg.br/arquivos/votacoes/csv/votacoes-{year}.csv`, one per
  calendar year, updated daily by the Câmara *(Q2)*.
  - **Public, with no credentials:** the portal needs no registration or identification to
    download. It states no formal license; its FAQ says the data is free and that there is no
    restriction ("Não há qualquer restrição") on its use, commercial use included *(Q8)*.
  - **Never committed:** each environment downloads the files when the preparation script
    runs. The repository holds no row of the dataset; tests use synthetic rows they write
    themselves.
- **Target:** the `postgres` system, the table `camara.votacoes` of the database `postgres` in
  the `postgres` service, a throwaway local container of the image `postgres:17`. In tests, the
  same image: the compose service locally, the service container in CI, with a schema of each
  test's own.
- **Data:** one record is one voting held by an organ of the Chamber, with its aggregated
  result: how many voted yes, no and otherwise, whether it was approved, and its description.
  A row of 2024, in the landing format:

  ```text
  "id";"uri";"data";"data_hora_registro";"id_orgao";"uri_orgao";"sigla_orgao";"id_evento";"uri_evento";"aprovacao";"votos_sim";"votos_nao";"votos_outros";"descricao";"ultima_abertura_votacao_data_hora_registro";"ultima_abertura_votacao_descricao";"ultima_apresentacao_proposicao_data_hora_registro";"ultima_apresentacao_proposicao_descricao";"ultima_apresentacao_proposicao_id_proposicao";"ultima_apresentacao_proposicao_uri_proposicao"
  "2355879-47";"https://dadosabertos.camara.leg.br/api/v2/votacoes/2355879-47";"2024-02-06";"2024-02-06T18:35:59";"100001";"https://dadosabertos.camara.leg.br/api/v2/orgaos/100001";"SECAP(SGM)";"0";"";"1";"0";"0";"0";"Realizar o encaminhamento do PL-1825/2023 à CCJC ...";"";"";"";"";"0";""
  ```

  It lands in `votacoes_2024020618.csv`, and in the target with `data_hora_registro`
  `2024-02-06 21:35:59+00`, the instant of 18:35:59 in Brasília *(Q6)*.
- **No credentials, no real data of third parties.** The dataset is published by its owner, the
  Chamber itself, for free reuse. It holds the aggregated results of public votings: no
  individual vote, no citizen's personal data, no CPF. Its free text sometimes names a deputy
  acting in office (for instance "pelo Deputado <name> (PL/MG)"), which is an official act of a
  public office, published by the Chamber. The datasets with one row per deputy's vote
  (`votacoesVotos`) or with more personal data and volume (`proposicoes`) are not used. The
  PostgreSQL user and password, `postgres`, exist only inside the throwaway container.

## Fit with ingestio

### The process

`src/main.py` builds, under its guard, one process of two steps:

```text
Step("extract", [ReadSource(config, watermark_value=watermark)])
Step("load",    [SkipEmptyBatch(), StampSourceFile(), AppendOnly(config, connection=connection)])
```

Before it builds the process, the job's own code, outside the process: builds the
`PostgresConnection` from the environment, starts the Spark session, applies the target's DDL,
and reads the last watermark from the target. Pitfall 4 of the knowledge base requires the
watermark before `ReadSource` is built, and the watermark query requires the table, so the DDL
runs before both.

- **Shipped callbacks:**
  - `ReadSource`, in `extract`: reads the `source` section, a `disk` system, and stores the
    DataFrame of the selected files in `state["records"]`. It is the library's way to read a
    configured source (rule 6). It receives `watermark_value`, a `str` (the pattern's group) or
    nothing on the first run, and no `spark`: the active session is looked up when it reads.
  - `AppendOnly`, last in `load`: appends `state["records"]` to the `target`, a `postgres`
    system, in one transaction. The goal is an incremental append: `FullLoad` would truncate,
    `SCD2` keeps a dimension's history.
- **Own callbacks** (subclasses of `ingestio.Callback`, defined in `src/main.py` itself, D15;
  both essential, as every callback is at 0.5.0):
  - `SkipEmptyBatch`, first in `load`: when `state["records"]` has no row (`isEmpty()`), raises
    `CancelStepException("no new row")`, so that nothing after it in the step runs. It must run
    before `StampSourceFile`, because an empty selection has no `_metadata` column (pitfall 2).
    Raising it for an expected condition is control flow, not criticality (rule 3).
  - `StampSourceFile`, second in `load`: replaces `state["records"]` with the same DataFrame
    plus the column `source_file`, the value of `_metadata.file_name`, the name of the file each
    row came from. It is the lineage column that keeps the watermark in the target (pitfall 3).
- **Connectors:** `disk` (built by `ReadSource` as `DiskReader`) and `postgres` (built by
  `AppendOnly` as `PostgresWriter`), named in `config.json`. No callback of the job builds a
  connector itself.
- **Outside the process, the job's own code, which the library leaves to the job:**
  - the `PostgresConnection`, from libpq's variables;
  - the Spark session, with the pgJDBC driver;
  - the target's DDL, through psycopg (ADR 0014), in `src/warehouse.py`;
  - the watermark query, through psycopg (README of the library: "query the warehouse"), in
    `src/warehouse.py`;
  - the preparation script, `scripts/prepare_landing.py`, which is not part of the ingestio
    job and imports nothing of ingestio or Spark: it is the producer of the landing directory,
    the prerequisite the maintainer asked for. ingestio 0.5.0 has no reader of CSV over HTTP (`JsonApiReader` decodes JSON), and
    this process does not need one: it reads the local files.

### Rules of a valid job

1. **Only the public API.** The job imports `Process`, `Step`, `Callback`,
   `CancelStepException`, `ReadSource`, `AppendOnly` and `log_to_console` from `ingestio`, and
   `PostgresConnection` from `ingestio.connector.all`. It reads `Process.state`,
   `Callback.process` and the documented attributes of `PostgresConnection` (`host`, `port`,
   `database`, `user`, `password`) to open its own psycopg connections. No internal module, no
   name starting with `_`. pyspark and psycopg are used directly, which the rule does not cover.
2. **Extension only through the documented way.** The two own callbacks subclass `Callback` in
   `src/main.py`, the entry point, which section 3 of the knowledge base allows ("defined there
   or imported from the job's modules next to it"), and are registered nowhere. The systems are
   the shipped `disk` and `postgres`; the job adds no connector.
3. **Criticality.** Every callback is essential and fails the run when it fails; no callback
   catches its own failure. No stage needs to be non-essential. `SkipEmptyBatch` cancels the
   step for an expected condition, the empty batch.
4. **No connector decides the strategy.** `AppendOnly` chooses the append. The `target` section
   holds `system` and `table_name` only: no `options`, no mode.
5. **No way around the library.** No monkeypatching and no subclass of a shipped callback or
   connector. `src/config.json` is a static file of the repository with an absolute `path`;
   nothing generates or rewrites it at run time. The job's code reads two of its values,
   `target.table_name` and `source.watermark_pattern`, for its DDL and its watermark query,
   and never writes it (D13). The batch reaches the target only through `AppendOnly`; the job's
   psycopg connections run only the DDL and the watermark query, which the library leaves to
   the job. Tests write configurations of their own under `tmp_path`, as the library's tests do.
6. **The default callbacks where they fit.** `ReadSource` reads and `AppendOnly` appends; the
   job never reads the landing files or appends the rows with code of its own. The preparation
   script reads the Câmara's files and writes the landing before the process runs; it never
   reads the landing to load it and never touches the target.

The layout the maintainer suggested, year/month/day folders, meets rules 5 and 6 only with a
workaround: it is not planned, and the gap is described under **Library gaps** (Q1).

### The layout of `0.3.0` against the knowledge base and CONTRIBUTING.md

The layout of D15 moves files and changes how they are invoked; it adds no import of ingestio,
no extension and no way around the library, so each rule above holds as it did in `0.2.0`.

- **The knowledge base, section 3.** `main.py` "defines or imports the job's own callbacks", and
  they are "defined there or imported from the job's modules next to it": defining
  `SkipEmptyBatch` and `StampSourceFile` in `src/main.py` is the first of the two. The knowledge
  base names no directory for the two files of the minimum job.
- **The configuration's location.** `src/main.py` names its configuration
  `Path(__file__).with_name("config.json")`, which is `src/config.json` wherever the job is run
  from (section 3, When it is read). The landing's `path` inside it stays absolute (pitfall 9):
  nothing in the configuration is relative to `src/`.
- **The guard.** `src/main.py` keeps everything that reads the configuration, the environment or
  starts a JVM in functions called under `if __name__ == "__main__":`. Defining the two classes
  in it builds nothing: they are built by `build_process`. Importing `main` still reads no file,
  touches no database and starts no session.
- **CONTRIBUTING.md, Layout.** "`main.py` is the entry point" and "`config.json` holds the job's
  configuration" hold for `src/main.py` and `src/config.json`; neither sentence names a
  directory. "The job's own callbacks and helpers live in modules next to `main.py`, named after
  what they do": `warehouse.py`, the helper, is a module next to `src/main.py`, named after what
  it does. The two callbacks are defined in `main.py` itself, which that sentence, read
  literally, does not foresee. The knowledge base allows it, and CONTRIBUTING.md says that the
  knowledge base "wins over this file when the two disagree", so the layout follows the rules of
  both. The wording is reported to the main session, since CONTRIBUTING.md is the hub's.
- **`scripts/prepare_landing.py`** is not part of the ingestio job: it imports nothing of
  ingestio and runs outside the process. The layout rules of CONTRIBUTING.md cover the job's
  `main.py`, `config.json`, callbacks and helpers, and say nothing against a script in a
  directory of its own.

## Scope

In scope:

- The layout of the repository and of the `spark` image (**Configuration**, The layout of the
  job):
  - `src/main.py`, the entry point, which defines the job's own callbacks `SkipEmptyBatch` and
    `StampSourceFile`; `src/config.json`, the job's configuration; and `src/warehouse.py`, next
    to them: the values the job reads from its configuration (D13), the DDL and the watermark
    query.
  - `scripts/prepare_landing.py`, the preparation script, which is not part of the ingestio
    job: it downloads the yearly files of the years a window of hours touches, splits their
    rows into one file per hour, and writes them to the landing directory atomically, in
    ascending order, never rewriting a file already there.
- `compose.yaml` at the repository root, with two services:
  - `postgres`: the image, environment, published port and health check of the service
    container of `.github/workflows/ci.yml`;
  - `spark`: the job's image, built from a `Dockerfile` at the repository root, with the
    repository's `data` directory mounted at `/data`, libpq's variables pointing at `postgres`,
    and `/app` as its working directory. It stays up, and the preparation script and the job
    run inside it with `docker compose exec`.
- The `Dockerfile` of the `spark` image: Java 17, Python 3.12, uv, and the job's environment
  from `uv sync --locked`, whose pyspark 4.2.0 brings Spark *(Q7)*; `src/` and `scripts/`
  copied to `/app/src` and `/app/scripts`; and a `.dockerignore` that keeps `data/`, `.venv/`,
  `.git/`, the tool caches and every `__pycache__` directory, at any depth, out of the build
  context.
- The target table `camara.votacoes`, its schema `camara`, and their DDL, applied by the job.
- `.gitignore`: the contents of `data/` ignored, with a tracked `data/.gitkeep` so that the
  directory exists before Docker mounts it.
- Where the tests import the job's modules from: `pythonpath` of `[tool.pytest.ini_options]` in
  `pyproject.toml`, `["src", "scripts"]` (D16).
- The tests of the acceptance criteria, on the host and in CI.
- Documentation: the README section that brings the environment up and runs the job, with the
  commands of AT-022 in that order, what each one does and what it shows; the dataset, its
  terms, and that the data is never committed; the landing layout and file format; what a
  second run shows; that `docker compose down --volumes` discards the database and keeps the
  landing; how to run the tests on the host; the gap of the nested layout; where the job's files
  are, `src/` and `scripts/`. A page in `docs/` for the process, `docs/index.md`, and the
  `0.2.0` and `0.3.0` entries of `CHANGELOG.md`.
- The job's version `0.3.0` in `pyproject.toml`.

**What the `0.3.0` revision changes**, against `0.2.0`; everything else stays as it is:

- `main.py`, `config.json` and `warehouse.py` move from the root to `src/`, with the same
  behaviour; `SkipEmptyBatch` and `StampSourceFile` move from `callbacks.py` into `src/main.py`,
  and `callbacks.py` is removed;
- `prepare_landing.py` moves from the root to `scripts/`, with the same behaviour;
- the root keeps none of `main.py`, `config.json`, `warehouse.py`, `callbacks.py` and
  `prepare_landing.py`;
- the `Dockerfile` copies `src/` and `scripts/` into `/app` instead of the five files of the
  root; `.dockerignore` keeps every `__pycache__` out, now that modules live in subdirectories;
  `compose.yaml` sets the `spark` service's working directory to `/app`;
- pytest's `pythonpath` becomes `["src", "scripts"]`, and its comment says so (D16);
- the acceptance criteria: the job's configuration is `src/config.json`, in "the job runs" and
  in AT-015; AT-012 and AT-016 to AT-020 name `scripts/prepare_landing.py`; AT-021 checks the
  working directory `/app`; AT-022 holds the new commands and none of `0.2.0`; AT-023 to AT-028
  are new;
- the README's commands, the page of the process, `docs/index.md` and the `0.3.0` entry of
  `CHANGELOG.md`;
- the version, `0.3.0`.

Out of scope:

- Any change of behaviour in `0.3.0`: the process, its steps and callbacks, the contents of the
  configuration, the table, the landing, the watermark, the log and the exit codes are those of
  `0.2.0`.
- Making the job a Python package or an installable project: no `__init__.py` in `src/` or
  `scripts/`, no `python -m`, no entry point in `pyproject.toml`. The modules are run as
  scripts, `python src/main.py`, and imported by their names.
- Moving `tests/`, `data/`, `compose.yaml`, the `Dockerfile`, `pyproject.toml`, `uv.lock` or
  `.python-version`: they stay at the root.
- The year/month/day layout of the landing directory (GAP-001, Q1).
- Downloading the dataset inside an ingestio process: the preparation script does it, outside
  the process.
- Other datasets of the Câmara, and rows of a yearly file outside the years the window
  touches (see Behaviour, the preparation script).
- Late data: rows that the Câmara adds to an hour after the hour was landed. A landed file is
  never rewritten.
- Scheduling the job, or running two runs of it at once.
- A CI job that builds the image or runs the compose environment end to end. The compose file
  (AT-021) and the `Dockerfile` (AT-028) are checked statically; the end-to-end run is the
  README's, run by hand.
- Removing orphaned `ingestio_staging_*` tables (pitfall 14): left to whoever runs the database.
- Spark outside local mode.

## Configuration

### The layout of the job

The job's files, in the repository and in the `spark` image, whose working directory `/app`
mirrors the repository (D15):

| In the repository | In the `spark` image | What it is |
|---|---|---|
| `src/main.py` | `/app/src/main.py` | the entry point; it defines the job's own callbacks, `SkipEmptyBatch` and `StampSourceFile` |
| `src/config.json` | `/app/src/config.json` | the job's configuration, which `src/main.py` names `Path(__file__).with_name("config.json")` |
| `src/warehouse.py` | `/app/src/warehouse.py` | the job's helper: the values it reads from the configuration (D13), the DDL and the watermark query |
| `scripts/prepare_landing.py` | `/app/scripts/prepare_landing.py` | the preparation script, not part of the ingestio job |
| `pyproject.toml`, `uv.lock`, `.python-version` | `/app/` | the job's environment, which the image builds with `uv sync --locked` |
| `data/` | `/data`, a bind mount | the landing, never committed |
| `tests/`, `docs/`, `specs/` | not copied | |

No `callbacks.py` exists, and the root of the repository holds none of `main.py`,
`config.json`, `warehouse.py`, `callbacks.py` and `prepare_landing.py`. `src/` and `scripts/`
are plain directories, not packages: `python src/main.py` puts `src/` first on `sys.path`, so
`src/main.py` imports `warehouse` by its name, and the tests import `main`, `warehouse` and
`prepare_landing` by their names (D16).

### `src/config.json`

```json
{
  "source": {
    "system": "disk",
    "path": "/data/landing/votacoes",
    "format": "csv",
    "watermark_pattern": "votacoes_(\\d{10})\\.csv",
    "schema": "id STRING, uri STRING, data DATE, data_hora_registro TIMESTAMP, id_orgao BIGINT, uri_orgao STRING, sigla_orgao STRING, id_evento BIGINT, uri_evento STRING, aprovacao INT, votos_sim INT, votos_nao INT, votos_outros INT, descricao STRING, ultima_abertura_votacao_data_hora_registro TIMESTAMP, ultima_abertura_votacao_descricao STRING, ultima_apresentacao_proposicao_data_hora_registro TIMESTAMP, ultima_apresentacao_proposicao_descricao STRING, ultima_apresentacao_proposicao_id_proposicao BIGINT, ultima_apresentacao_proposicao_uri_proposicao STRING",
    "options": {
      "header": true,
      "sep": ";",
      "quote": "\"",
      "escape": "\"",
      "multiLine": true,
      "encoding": "UTF-8",
      "enforceSchema": false,
      "mode": "FAILFAST",
      "timeZone": "America/Sao_Paulo"
    }
  },
  "target": {"system": "postgres", "table_name": "camara.votacoes"}
}
```

- `path`: the landing directory as the `spark` container sees it, absolute (pitfall 9). The job
  runs where that path exists, the `spark` service; tests use configurations of their own whose
  `path` is under `tmp_path`.
- `watermark_pattern`: the whole file name, with one group capturing the hour, ten digits,
  most significant first (pitfall 6). It uses only syntax that Python's and Java's regular
  expressions share. It matches `votacoes_2024020618.csv` and captures `2024020618`; it matches
  neither `.votacoes_2024020618.csv.tmp`, the preparation script's temporary name, nor
  `votacoes_2024-02-06T18.csv`. No file name has a glob character (pitfall 16).
- `schema`: the twenty columns of the landing files, named as the target's columns
  (pitfall 10), with types whose nulls and values map to the target's types without a cast:
  `INT` to `integer`, `BIGINT` to `bigint`, `DATE` to `date`, `TIMESTAMP` to `timestamptz`,
  `STRING` to `text`. With a schema, the empty selection has these columns.
- `options`, each for Spark's CSV data source:
  - `header`, `sep`, `quote`, `escape`: the landing format below: a header line, `;`, every
    field in `"`, a `"` inside a value doubled;
  - `multiLine`: values contain line breaks;
  - `encoding`: UTF-8, without a BOM (the preparation script removes the Câmara's);
  - `enforceSchema` `false`: Spark checks each file's header against the names of the schema
    and fails on a difference, instead of applying the schema by position and warning once per
    file (pitfall 11);
  - `mode` `FAILFAST`: a record with another number of fields, or a value that does not parse as
    its type, fails the run instead of becoming nulls;
  - `timeZone` `America/Sao_Paulo`: the timestamps have no offset and are Brasília times, so
    Spark reads them as instants of that zone *(Q6)*. The session's own time zone stays `UTC`.
  - `dateFormat` and `timestampFormat` are left to Spark's defaults, `yyyy-MM-dd` and an
    ISO 8601 format that reads `2024-02-06T18:35:59`.
- `target.table_name`: `camara.votacoes`, in the form `schema.table`. No `options`.
- The job adds no top-level member of its own.

### Arguments that do not belong in `config.json`

| Callback | Argument | Value |
|---|---|---|
| `ReadSource` | `watermark_value` | the watermark read from the target, a `str` such as `"2025123123"`; not given on the first run |
| `AppendOnly` | `connection` | the `PostgresConnection` built from the environment |

### The environment

| Variable | Required | In the `spark` service | In CI and for tests on the host |
|---|---|---|---|
| `PGHOST` | yes | `postgres` | `localhost` |
| `PGPORT` | no, `5432` when unset | `5432` | `5432` |
| `PGUSER` | yes | `postgres` | `postgres` |
| `PGPASSWORD` | yes | `postgres` | `postgres` |
| `PGDATABASE` | yes | `postgres` | `postgres` |

The job requires the variables and has no fallback (D8). CI already sets them, with these names,
in `.github/workflows/ci.yml`.

### `compose.yaml`

- `postgres`: image `postgres:17`, `POSTGRES_PASSWORD=postgres`, port `5432` published on
  `5432`, health check `pg_isready -U postgres` with an interval of 5 s, a timeout of 5 s and 10
  retries: the settings of the CI service container. No volume of its own: its data lives as
  long as its container.
- `spark`: built from the repository's `Dockerfile`; the repository's `data` directory mounted
  at `/data`; `depends_on` `postgres` with the condition `service_healthy`; the environment of
  the table above; it stays up (it runs no job on start), with `/app` as its working directory
  (`working_dir`), the directory of the image that mirrors the repository, so that
  `docker compose exec spark python src/main.py` runs the job and
  `docker compose exec spark python scripts/prepare_landing.py ...` the preparation script. It
  runs as a non-root user with uid and gid 1000, so that the files it writes under `data/`
  belong on the host to the usual first user. It publishes no port.

### The `Dockerfile`

The image of D11, with `/app` as its `WORKDIR`. In `/app` it copies `.python-version`,
`pyproject.toml` and `uv.lock` and runs `uv sync --locked --no-install-project`, as in `0.2.0`;
then, in place of the five files of the root that `0.2.0` copied, it copies the build context's
`src/` to `/app/src/` and its `scripts/` to `/app/scripts/`, owned by uid and gid 1000. It copies
nothing else of the job.

### The landing directory and its files

`/data/landing/votacoes` in the `spark` service (`data/landing/votacoes` on the host), flat:
the files sit directly inside it, with no subdirectory *(Q1)*. One file per hour that has at
least one row *(Q5)*:

- **Name:** `votacoes_<YYYYMMDDHH>.csv`, where `YYYYMMDDHH` is the hour of the
  `dataHoraRegistro` of every row in the file, as the dataset gives it (Brasília time, without
  an offset), zero-padded: `votacoes_2024020618.csv` holds the rows registered from
  `2024-02-06T18:00:00` to `2024-02-06T18:59:59`.
- **Encoding:** UTF-8, without a BOM.
- **Header:** one line with the twenty column names of the target, in the order of the table
  below, each in `"`, separated by `;`.
- **Records:** the file's rows in the order of the yearly file they come from, every field in
  `"`, a `"` inside a value doubled, fields separated by `;`, each record ended by `\n`. The
  values are the Câmara's, unchanged, line breaks inside a value included.

The Câmara's header, which the preparation script requires, and the names it becomes:

| Câmara's column | Landing and target column | Spark type | PostgreSQL type |
|---|---|---|---|
| `id` | `id` | `STRING` | `text PRIMARY KEY` |
| `uri` | `uri` | `STRING` | `text` |
| `data` | `data` | `DATE` | `date` |
| `dataHoraRegistro` | `data_hora_registro` | `TIMESTAMP` | `timestamptz` |
| `idOrgao` | `id_orgao` | `BIGINT` | `bigint` |
| `uriOrgao` | `uri_orgao` | `STRING` | `text` |
| `siglaOrgao` | `sigla_orgao` | `STRING` | `text` |
| `idEvento` | `id_evento` | `BIGINT` | `bigint` |
| `uriEvento` | `uri_evento` | `STRING` | `text` |
| `aprovacao` | `aprovacao` | `INT` | `integer` |
| `votosSim` | `votos_sim` | `INT` | `integer` |
| `votosNao` | `votos_nao` | `INT` | `integer` |
| `votosOutros` | `votos_outros` | `INT` | `integer` |
| `descricao` | `descricao` | `STRING` | `text` |
| `ultimaAberturaVotacao_dataHoraRegistro` | `ultima_abertura_votacao_data_hora_registro` | `TIMESTAMP` | `timestamptz` |
| `ultimaAberturaVotacao_descricao` | `ultima_abertura_votacao_descricao` | `STRING` | `text` |
| `ultimaApresentacaoProposicao_dataHoraRegistro` | `ultima_apresentacao_proposicao_data_hora_registro` | `TIMESTAMP` | `timestamptz` |
| `ultimaApresentacaoProposicao_descricao` | `ultima_apresentacao_proposicao_descricao` | `STRING` | `text` |
| `ultimaApresentacaoProposicao_idProposicao` | `ultima_apresentacao_proposicao_id_proposicao` | `BIGINT` | `bigint` |
| `ultimaApresentacaoProposicao_uriProposicao` | `ultima_apresentacao_proposicao_uri_proposicao` | `STRING` | `text` |

### The target table

Applied by the job before every run, through psycopg, in one transaction, with the schema and
the table taken from `target.table_name` (quoted as identifiers), so that a test's own
`table_name` gets the same definition in its own schema:

```sql
CREATE SCHEMA IF NOT EXISTS camara;

CREATE TABLE IF NOT EXISTS camara.votacoes (
    id                                                text PRIMARY KEY,
    uri                                               text,
    data                                              date,
    data_hora_registro                                timestamptz,
    id_orgao                                          bigint,
    uri_orgao                                         text,
    sigla_orgao                                       text,
    id_evento                                         bigint,
    uri_evento                                        text,
    aprovacao                                         integer,
    votos_sim                                         integer,
    votos_nao                                         integer,
    votos_outros                                      integer,
    descricao                                         text,
    ultima_abertura_votacao_data_hora_registro        timestamptz,
    ultima_abertura_votacao_descricao                 text,
    ultima_apresentacao_proposicao_data_hora_registro timestamptz,
    ultima_apresentacao_proposicao_descricao          text,
    ultima_apresentacao_proposicao_id_proposicao      bigint,
    ultima_apresentacao_proposicao_uri_proposicao     text,
    source_file                                       text COLLATE "C" NOT NULL,
    loaded_at                                         timestamptz NOT NULL DEFAULT now()
);
```

- `source_file` is stamped by `StampSourceFile`; its `"C"` collation orders the names code
  point by code point, as `DiskReader` compares watermarks.
- `loaded_at` is not a column of the DataFrame: PostgreSQL fills it when the append commits, the
  same value for every row of one run.
- The primary key on `id` makes a second copy of a row fail the append, and the whole run with
  it, instead of duplicating the row.

### The job's entry points

The module `main` is `src/main.py` and the module `prepare_landing` is
`scripts/prepare_landing.py`. The tests import them by those names (D16) and call the job
through these names, which keep the names and signatures of `0.2.0`; `src/main.py` runs only
`main()`, under its guard:

```text
main.PGJDBC_VERSION = "42.7.14"
main.SkipEmptyBatch                       # a subclass of ingestio.Callback, defined in main
main.StampSourceFile                      # a subclass of ingestio.Callback, defined in main
main.build_spark_session(*, master: str = "local[*]") -> SparkSession
main.connection_from_env(environ: Mapping[str, str]) -> PostgresConnection
main.build_process(config: str | os.PathLike[str], *, connection: PostgresConnection, watermark: str | None) -> Process
main.run_job(config: str | os.PathLike[str], *, connection: PostgresConnection) -> Process
main.main() -> None
prepare_landing.main(argv: Sequence[str] | None = None) -> int
```

- `build_spark_session`: `SparkSession.builder` with `master`, `spark.jars.packages` set to
  `org.postgresql:postgresql:<PGJDBC_VERSION>`, `spark.sql.session.timeZone` set to `UTC` and
  `spark.ui.enabled` set to `false`, then `getOrCreate()`. The tests build their one session of
  the run with it, `master="local[2]"`, as the first session of the run.
- `build_process`: the process of **The process** above, for a configuration file and the
  arguments it cannot hold.
- `run_job`: reads `target.table_name` and `source.watermark_pattern` from `config`, applies the
  DDL, reads the watermark and logs it, builds the process with `build_process`, runs it and
  returns it. It uses the active Spark session.
- `main`: configures the console, builds the connection from `os.environ`, starts the session
  with `build_spark_session()`, calls `run_job` on `src/config.json`, the `config.json` next to
  `main.py` (`Path(__file__).with_name("config.json")`), and stops the session at the end, also
  when the run fails.
- `SkipEmptyBatch` and `StampSourceFile` are defined in `src/main.py` itself, not imported into
  it, and `build_process` builds each with no argument. The reading of `target.table_name` and
  `source.watermark_pattern`, the DDL and the watermark query live in `src/warehouse.py`, next
  to `main.py`; the names of its functions are the implementer's.

### The preparation script

```text
python scripts/prepare_landing.py --landing DIR --start YYYY-MM-DDTHH --end YYYY-MM-DDTHH [--base-url URL]
```

It is run from the directory that holds `scripts/`: `/app` in the `spark` service, the
repository's root on the host. It needs only the standard library.

| Argument | Required | What it is |
|---|---|---|
| `--landing` | yes | the landing directory to write; created when missing |
| `--start`, `--end` | yes | the first and the last hour of the window, both included, in Brasília time as the dataset gives it, such as `2024-01-01T00` |
| `--base-url` | no | where the yearly files are, an `https:` or a `file:` URL; `https://dadosabertos.camara.leg.br/arquivos/votacoes/csv` by default. The file of a year is `<base-url>/votacoes-<year>.csv` |

It exits with `0` when it succeeds, `1` when a source cannot be fetched or breaks the format it
expects, and `2` for a usage error, such as a missing argument or a `--start` after `--end`.
It logs through `logging`, never `print`.

## Behaviour

### A run of the job

1. `main()` calls `ingestio.log_to_console()`, and shows the job's own log lines on the same
   console, so that each line appears once (pitfall 18).
2. It builds the `PostgresConnection` from `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and
   `PGDATABASE`. A missing or empty required variable raises
   `ValueError("<NAME> is not set")`, and a `PGPORT` that is not an integer
   `ValueError("PGPORT must be an integer")`, before Spark starts and before anything touches
   the database. The error propagates out of `main()`, and Python exits with status 1.
3. It starts the Spark session, in local mode, with the pgJDBC driver, which Spark downloads
   from Maven Central on the first start in a container and keeps in the container's home.
4. `run_job` applies the DDL: the schema and the table are created when missing, and an existing
   table and its rows are left as they are.
5. It reads the watermark: `SELECT max(source_file)` of the target. With no row, there is none,
   and it logs `watermark: none (every file is read)`. Otherwise it matches the name with the
   `watermark_pattern` of `config.json`, through Python's `re.fullmatch`, takes its group, such
   as `2025123123`, and logs `watermark: 2025123123`. A stored name that does not match the
   pattern raises `ValueError`. Both lines are `INFO`, through `logging`.
6. It builds the process and runs it:
   - `extract`: `ReadSource` lists `/data/landing/votacoes`, selects the files whose captured
     hour is strictly above the watermark, or all of them without one, and stores the lazy
     DataFrame. Hidden files (the preparation script's temporary files among them),
     subdirectories, and names the pattern does not match are never read.
   - `load`: `SkipEmptyBatch` checks for a row, `StampSourceFile` adds `source_file`, and
     `AppendOnly` appends the batch, in one transaction.
7. The tree of the run, for a run that appends:

   ```text
   [<run id>] ┌ process started · 2 steps
   [<run id>] │ ┌ extract (1/2) · 1 callback
   [<run id>] │ │ ✔ ReadSource.run <d>
   [<run id>] │ └ extract done in <d>
   [<run id>] │ ┌ load (2/2) · 3 callbacks
   [<run id>] │ │ ✔ SkipEmptyBatch.run <d>
   [<run id>] │ │ ✔ StampSourceFile.run <d>
   [<run id>] │ │ ✔ AppendOnly.run <d>
   [<run id>] │ └ load done in <d>
   [<run id>] └ process finished in <d>
   ```

   Spark's own output, such as the driver's download on the first start, appears beside it; the
   job does not silence Spark.
8. `main()` returns and Python exits with status 0.

**The values.** Spark parses each field with the schema's type; an empty field is null, whatever
its type, and reaches the target as `NULL`. Text, line breaks and `;` inside a value included,
reaches the target as it is in the file. Dates are `date`. Timestamps are read as Brasília
instants and stored as `timestamptz`, so `2024-02-06T18:35:59` is `2024-02-06 21:35:59+00`,
whatever the time zone of the machine that runs the job *(Q6)*.

**Nothing new.** When no file is above the watermark, or the landing directory is empty, the
DataFrame has the schema's columns and no row. `SkipEmptyBatch` raises
`CancelStepException("no new row")`; the log shows
`⊘ SkipEmptyBatch.run cancelled the step after <d>: no new row` and `└ load cancelled in <d>`,
the process finishes, `process.cancelled` stays `False`, the target is unchanged, and `main()`
exits with status 0. A run with nothing new is a normal run.

**Failures.** Every failure propagates out of `run_job` and `main()`, after the tree's `✖` line;
Python exits with status 1. In every case below the target holds what it held before the run,
and no staging table is left:

- a selected file whose header is not the schema's names, a record with another number of
  fields, or a value that does not parse as its type: Spark's error, in `SkipEmptyBatch.run` or
  `AppendOnly.run`, whichever action reads the file first;
- a row whose `id` is already in the target, or appears twice in the batch: PostgreSQL's unique
  violation, in `AppendOnly.run`;
- PostgreSQL unreachable: psycopg's error, when the DDL is applied, before the process exists;
- the landing directory missing: `FileNotFoundError` in `ReadSource.run`, after the DDL created
  the table. The preparation script creates the directory.

**The watermark and its guarantee.** The watermark lives in the target's own rows, as
`source_file`, and commits in the same transaction as them (pitfall 3). A failed run leaves both
as they were, and the next run selects the same files again: each landed file's rows reach the
target exactly once, provided that:

- files reach the landing in ascending order of hour, and never at or below an hour already
  landed (the preparation script guarantees both);
- a landed file is not changed or removed afterwards (one removed between the read and the write
  fails the run);
- runs of the job do not overlap. When two do, both read the same watermark, and the primary key
  makes the second commit fail instead of duplicating rows.

A batch that holds only files without rows would not advance the watermark; the preparation
script writes no such file *(Q5)*.

**`docker compose down --volumes`** discards the database with its container; the landing stays
on the host. The next run finds an empty target, no watermark, and loads every landed file
again: the target and the landing agree again.

### The preparation script

1. It validates its arguments, then fetches the yearly file of every calendar year from the
   year of `--start` to the year of `--end`, and only those, from `--base-url`, reading each as
   UTF-8 with its BOM removed, as a CSV with `;`, `"` and line breaks inside values, whatever its
   line endings.
2. It checks, before it writes anything, that:
   - each file's header is exactly the twenty names of the Câmara's columns, in order;
     otherwise `votacoes-<year>.csv: unexpected header`;
   - each `dataHoraRegistro` is empty or a valid `YYYY-MM-DDTHH:MM:SS`; otherwise
     `votacoes-<year>.csv: invalid dataHoraRegistro '<value>' for id '<id>'`;
   - no `id` appears twice across the files it read; otherwise
     `id '<id>' appears more than once`.

   A file that cannot be fetched fails with `cannot fetch <url>: <reason>`. Each failure is
   logged at `ERROR`, exits with status 1, and leaves the landing directory as it was.
3. It puts each row in the hour of its `dataHoraRegistro`. Rows without one are left out
   *(Q4)*, and so are rows registered outside the window, such as rows of the 2024 file
   registered in 2026. Rows registered inside the window but published in the file of a year
   the window does not touch are not read.
4. It removes the temporary files an interrupted run left in the landing directory, then
   writes, in ascending order of hour, the file of each hour of the window that has at least one
   row *(Q5)*, in the landing format. Each file is written under a temporary name that starts
   with `.`, `.votacoes_<YYYYMMDDHH>.csv.tmp`, and renamed to its name when it is complete, so
   that a run of the job never sees a partial file and never sees an hour before an earlier one
   (pitfall 8). A file whose name already exists is left as it is, whatever it holds, and
   counted as skipped.
5. It logs one `INFO` line at the end, with the counts of the run, such as
   `files written: 4, files skipped: 0, rows written: 5, rows without dataHoraRegistro: 1, rows outside the window: 1`.

A later window adds later hours: run with `--start 2026-01-01T00 --end 2026-01-31T23` after the
two years, it adds the hours of January 2026, and the next run of the job appends them alone
*(Q3)*. Rows the Câmara adds later to an hour already landed are not landed.

### The two years

The landing holds the hours from `2024-01-01T00` to `2025-12-31T23`, from the files of 2024 and
2025 *(Q3)*: about 1,400 files, and somewhat fewer rows than the 24,200 of the two yearly files,
since the rows registered outside the window or without a time are left out (counts of the
dataset on 2026-10-24). The README's incremental step adds January 2026.

## Acceptance criteria

Each criterion is testable on its own, from outside the job's code. Its tests are named after
it: `AT-001` is covered by `test_at001_<description>` in `tests/test_camara_hourly_ingestion.py`.

"The job runs" means `main.run_job(config, connection=...)`, of `src/main.py`, with a
configuration that is the job's `src/config.json` with its own `source.path` (a directory under
`tmp_path`) and its own `target.table_name` (a schema of the test's own), and the connection to
the PostgreSQL of the environment. "A landing file" is a file the test writes in the landing format of
**Configuration**, with synthetic values.

### The process

- **AT-001**: Given a database without the target's schema and a landing directory with
  `votacoes_2024020618.csv` (two rows), `votacoes_2024020619.csv` (one row) and
  `votacoes_2025010110.csv` (three rows), when the job runs, then the target holds the six rows,
  each once, each with `source_file` the name of the file it came from, and with one same
  non-null `loaded_at` for all of them.
- **AT-002**: Given a landing file with a row whose twenty fields are all filled, and a row whose
  every field but `id` and `data_hora_registro` is empty, when the job runs, then the first row
  is in the target with each value of its type: `id`, `uri`, `sigla_orgao` and the texts as
  written; `data` `2024-02-06` as a date; `id_orgao`, `id_evento` and
  `ultima_apresentacao_proposicao_id_proposicao` as bigints; `aprovacao`, `votos_sim`,
  `votos_nao` and `votos_outros` as integers; `data_hora_registro` `2024-02-06T18:35:59` as the
  instant `2024-02-06 21:35:59+00`, and the other two timestamps likewise; and the second row's
  empty fields are `NULL`.
- **AT-003**: Given a landing file whose `descricao` values hold a line break, a `;`, and a `"`
  (doubled in the file), when the job runs, then each `descricao` in the target is the value
  with its line break, `;` and single `"`, and the target holds exactly the file's number of
  rows.
- **AT-004**: Given that an earlier run of the job loaded `votacoes_2024020618.csv` and
  `votacoes_2024020619.csv`, and that `votacoes_2024020620.csv` and `votacoes_2025010110.csv`
  were then added to the landing, when the job runs again, then the target holds the rows of
  the two new files, appended once, and the rows of the earlier run unchanged, with their
  `loaded_at`; the new rows' `loaded_at` is later than theirs.
- **AT-005**: Given an empty target, when the job runs, then it logs at `INFO`
  `watermark: none (every file is read)`; and given a target whose newest `source_file` is
  `votacoes_2024020619.csv`, when the job runs, then it logs `watermark: 2024020619`.
- **AT-006**: Given a target that holds the rows of every file in the landing, or an empty
  landing directory, when the job runs, then it ends without an exception, the log holds
  `⊘ SkipEmptyBatch.run cancelled the step after <d>: no new row` and `└ load cancelled in <d>`
  and no `AppendOnly.run` line, the returned process has `cancelled` `False`, and the target is
  unchanged.
- **AT-007**: Given a landing with the new file `votacoes_2024020618.csv` beside
  `.votacoes_2024020619.csv.tmp` and `votacoes_2024-02-06T20.csv`, both landing files with rows,
  when the job runs, then the target holds only the rows of `votacoes_2024020618.csv`.
- **AT-008**: Given an empty target and two new landing files, one of which is broken in one of
  these ways: a `votos_sim` of `abc`; a record with twenty-one fields; a header with the
  Câmara's names (`"id";"uri";"data";"dataHoraRegistro";...`) instead of the target's; when the
  job runs, then an exception propagates out of `run_job`, the target holds no row, and no table
  whose name starts with `ingestio_staging_` is left in the target's schema.
- **AT-009**: Given that a run failed as in AT-008, when the broken file is corrected and the job
  runs again, then the target holds the rows of both files, each once.
- **AT-010**: Given a database without the target's schema and an empty landing directory, when
  the job runs, then the schema and the table exist with exactly the columns, types,
  nullability, the primary key on `id`, the `"C"` collation of `source_file` and the `now()`
  default of `loaded_at` of the DDL in **Configuration**; and given that table already holding
  rows, when the job runs with nothing new, then its definition and its rows are unchanged.
- **AT-011**: Given a target that holds a row with `id` `X`, and a new landing file holding
  another row with `id` `X`, when the job runs, then an exception propagates out of `run_job`
  and the target is unchanged.
- **AT-012**: Given synthetic yearly files of 2024 and 2025 in the Câmara's format (with a BOM,
  mixed `\r\n` and `\n` record ends, line breaks inside values), when the preparation script,
  `prepare_landing.main` of `scripts/prepare_landing.py`, lays out the landing for
  `2024-01-01T00` to `2025-12-31T23` from them and the job then runs on that landing, then the target holds every row registered in the window, each once, with its values,
  and no other row.

### The connection

- **AT-013**: Given an environment with `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and
  `PGDATABASE`, when `main.connection_from_env` builds the connection, then its `host`, `port`
  (an `int`), `user`, `password` and `database` are those values; and given the same
  environment without `PGPORT`, then its `port` is `5432`.
- **AT-014**: Given an environment where one of `PGHOST`, `PGUSER`, `PGPASSWORD` and
  `PGDATABASE` is missing, or empty, when `main.connection_from_env` builds the connection, then
  it raises `ValueError` with the message `<NAME> is not set`; and given a `PGPORT` of `abc`,
  then it raises `ValueError` with the message `PGPORT must be an integer`.

### The configuration

- **AT-015**: Given the job's configuration, `src/config.json`, then its `source` is the `disk`
  system with `path` `/data/landing/votacoes`, `format` `csv`, the `schema` and the `options` of
  **Configuration**, and a `watermark_pattern` that `re.fullmatch`es `votacoes_2024020618.csv`
  capturing `2024020618` and matches neither `.votacoes_2024020618.csv.tmp` nor
  `votacoes_2024-02-06T18.csv`; its `target` is the `postgres` system with `table_name`
  `camara.votacoes`; and `ReadSource` and `AppendOnly`, given a `PostgresConnection`, are built
  from it without an error.

### The preparation script

In these criteria "`prepare_landing.py` runs" means `prepare_landing.main(argv)`, of
`scripts/prepare_landing.py`, with the arguments given; the yearly files are synthetic, written
by the test in the Câmara's format into a directory that `--base-url` names as a `file:` URL.

- **AT-016**: Given yearly files of 2024 and 2025 whose rows are registered in four distinct
  hours of the window, two rows in one of them, when `prepare_landing.py` runs with
  `--start 2024-01-01T00 --end 2025-12-31T23`, then it exits with 0 and the landing directory
  holds exactly four files, one per hour, named `votacoes_<YYYYMMDDHH>.csv` after it; each in
  the landing format (UTF-8 without a BOM, the target's header, every field quoted, `;`, records
  ended by `\n`), holding exactly the rows of its hour, in the order of the yearly file, with
  their values unchanged, line breaks inside values included.
- **AT-017**: Given yearly files with one row whose `dataHoraRegistro` is empty and one row
  registered after `--end`, when `prepare_landing.py` runs, then neither row is in any file, no
  file exists for an hour without rows, and it logs
  `files written: <n>, files skipped: 0, rows written: <r>, rows without dataHoraRegistro: 1, rows outside the window: 1`
  with the counts of the run.
- **AT-018**: Given a landing directory that already holds the file of one hour of the window,
  with contents other than the source's, and `.votacoes_2024020618.csv.tmp`, left by an
  interrupted run, when `prepare_landing.py` runs, then that file keeps its bytes and its
  modification time, the files of the other hours are written, no file whose name starts with
  `.` is left, and its last line counts one file skipped.
- **AT-019**: Given a source broken in one of these ways: a header other than the Câmara's
  twenty names; a `dataHoraRegistro` of `2024-02-30T10:00:00`; an `id` in both years' files; a
  year whose file is missing; when `prepare_landing.py` runs, then it exits with 1, it logs at
  `ERROR` the message of **Behaviour** for that case, and the landing directory holds what it
  held before.
- **AT-020**: Given a directory that holds only the yearly file of 2026, when
  `prepare_landing.py` runs with `--start 2026-01-01T00 --end 2026-01-31T23`, then it exits
  with 0 and writes the hours of January 2026 from that file alone.

### The environment and the README

- **AT-021**: Given the repository, when `docker compose config --format json` runs at its root,
  then it describes exactly two services, `postgres` and `spark`; `postgres` has the image
  `postgres:17`, `POSTGRES_PASSWORD` `postgres`, the port 5432 published on 5432, and a health
  check that runs `pg_isready -U postgres` every 5 s with a timeout of 5 s and 10 retries;
  `spark` is built from the repository's root, has the working directory `/app`, mounts the
  repository's `data` directory at `/data`, depends on `postgres` with the condition
  `service_healthy`, and has `PGHOST` `postgres`, `PGPORT` `5432`, `PGUSER` `postgres`,
  `PGPASSWORD` `postgres` and `PGDATABASE` `postgres` in its environment.
- **AT-022**: Given `README.md`, then it holds these commands, each verbatim, in this order:

  ```bash
  docker compose up --build --wait
  docker compose exec spark python scripts/prepare_landing.py --landing /data/landing/votacoes --start 2024-01-01T00 --end 2025-12-31T23
  docker compose exec spark python src/main.py
  docker compose exec postgres psql -U postgres -c "SELECT count(*), max(source_file) FROM camara.votacoes"
  docker compose exec spark python scripts/prepare_landing.py --landing /data/landing/votacoes --start 2026-01-01T00 --end 2026-01-31T23
  docker compose exec spark python src/main.py
  docker compose down --volumes
  ```

  and it holds neither `python main.py` nor `python prepare_landing.py`, the commands of
  `0.2.0`.

### The layout of the job

- **AT-023**: Given the repository, then `src/` holds `main.py`, `config.json` and
  `warehouse.py`, and `scripts/` holds `prepare_landing.py`; and the modules that the tests
  import as `main`, `warehouse` and `prepare_landing` are the files `src/main.py`,
  `src/warehouse.py` and `scripts/prepare_landing.py`.
- **AT-024**: Given the repository, then no file named `callbacks.py` exists anywhere in it,
  outside its hidden directories; and its root holds none of `main.py`, `config.json`,
  `warehouse.py`, `callbacks.py` and `prepare_landing.py`.
- **AT-025**: Given the module `main`, then `main.SkipEmptyBatch` and `main.StampSourceFile` are
  classes that subclass `ingestio.Callback` and whose `__module__` is `"main"`: they are defined
  in `src/main.py`, not imported into it.
- **AT-026**: Given an environment without `PGHOST`, when `python src/main.py` runs with the
  repository's root as its working directory, then it exits with status 1 and its standard
  error holds `ValueError: PGHOST is not set`.
- **AT-027**: Given a directory that holds a yearly file of 2024 in the Câmara's format, with
  one row registered at `2024-02-06T18:35:59`, when
  `python scripts/prepare_landing.py --landing <a new directory> --start 2024-02-06T18 --end 2024-02-06T18 --base-url <that directory, as a file: URL>`
  runs with the repository's root as its working directory, then it exits with status 0 and
  the landing directory holds exactly `votacoes_2024020618.csv`.
- **AT-028**: Given the repository's `Dockerfile`, then its last `WORKDIR` is `/app`; it copies
  the build context's directory `src` to `/app/src` and its directory `scripts` to
  `/app/scripts`; and none of its `COPY` instructions copies `main.py`, `config.json`,
  `warehouse.py`, `callbacks.py` or `prepare_landing.py` from the root of the build context.

### Notes for testing

- AT-001 to AT-012 need Spark and PostgreSQL, AT-021 the Docker CLI: they are integration tests.
  AT-013 to AT-020 and AT-022 to AT-028 need neither; AT-015 builds the callbacks, which touches
  nothing, and AT-026's child process imports pyspark but starts no session.
- The tests import `main` (and, through it, `warehouse`) from `src/` and `prepare_landing` from
  `scripts/`, by those names: pytest's `pythonpath` is `["src", "scripts"]` (D16). The job's
  configuration is `src/config.json`.
- The session of the run is `main.build_spark_session(master="local[2]")`, built first.
- The tests reach PostgreSQL through the five variables, `PGHOST=localhost` on the host and in
  CI, and fail, rather than skip, when it is unreachable (pitfall 19).
- Each test takes the `schema` and the `options` from the job's `src/config.json`, so that it
  runs the job's real configuration, and writes its copy with its own `path` and `table_name`
  with `json.dumps` under `tmp_path`.
- The synthetic rows invent their ids, texts and organs; no row of the dataset is used.
- The AT-022 test reads `README.md`, finds each command after the previous one, and checks that
  neither command of `0.2.0` appears anywhere in it.
- AT-023 compares resolved paths: `Path(main.__file__).resolve()` with `src/main.py` of the
  repository's root, and likewise for `warehouse` and `prepare_landing`.
- AT-024 walks the repository from its root and skips every directory whose name starts with
  `.` (`.venv`, `.git`, `.claude` and the tool caches among them) and `data/`.
- AT-026 and AT-027 run the commands as subprocesses, with `sys.executable` in place of
  `python` and the repository's root, laid out as `/app` is in the image, as the working
  directory. AT-026 removes `PGHOST` from the child's environment, since CI sets it. Neither
  sets `PYTHONPATH`: the child finds `warehouse` because `python src/main.py` puts `src/` first
  on `sys.path`, as it does in the container. AT-027's yearly file is synthetic, as in AT-016
  to AT-020.
- AT-028 reads the `Dockerfile` as text: it joins the lines that end with `\`, reads the
  `WORKDIR` and `COPY` instructions in order, skips the `COPY --from=...` instructions (the uv
  binary) and the options of `COPY` such as `--chown`, resolves a relative destination against
  the `WORKDIR` before it, and ignores trailing slashes and a leading `./`.
- Together, AT-021, AT-026, AT-027 and AT-028 check that the commands of AT-022 run in the
  `spark` service from `/app`, without building the image: the service's working directory is
  `/app`, the image holds the repository's `src/` and `scripts/` under it, and the commands work
  from a directory laid out that way.
- The end-to-end run in compose, with the real dataset, is not an automated test: it needs the
  image build, the Câmara and Maven Central. It is the README's, run by hand.

## Library gaps

The `0.3.0` revision runs into no gap: it moves files and changes how they are invoked, with
the public API and the documented extension of `0.2.0`. The gap of `0.2.0`:

### GAP-001: `DiskReader` reads one directory only, so a landing laid out in year/month/day folders cannot be read

- **ingestio version:** 0.5.0
- **Recorded:** 2026-10-24
- **Spec:** specs/camara_hourly_ingestion.md
- **Status:** open

**What the job needed to do.** The maintainer suggested a landing laid out as
`/data/landing/votacoes/YYYY/MM/DD/`, with one file per hour in each day's folder, read by one
process that selects, across the whole tree, the files above the watermark. At 0.5.0
`DiskReader` lists only the entries directly inside `path` and ignores subdirectories with
everything in them; it rejects Spark's `recursiveFileLookup` and `pathGlobFilter` with
`option '<key>' is set by the connector`; and a configuration holds one source with one
absolute `path`. The process would need one of these workarounds:

- generate a configuration per day folder at run time and build a `ReadSource` for each;
- write a callback of the job's own that walks the tree, chooses the day folders above the
  watermark, builds a `DiskReader` for each with a `path` computed in code, and unions their
  DataFrames;
- commit one configuration per day folder, 731 for two years and one more each day, each run
  as a process of its own with a watermark of its own, chosen by code of the job.

**Why it points to a gap.** The first breaks rule 5: generating the configuration from code at
run time to get around the rule that a source has one `path`. The second breaks rule 6: it
reimplements, in the job's code, what `ReadSource` and the reader's selection do, the listing
and the selection before Spark loads anything, and builds the reader's `path` at run time. The
third is the second with the code moved into files. The library offers no valid way: the
connector closes the Spark options that would read a tree, and connectors cannot be extended.

**Expected behaviour of the library.** The `disk` system, and the readers of files that share
its selection (`s3`, `gcs`, `volume`), offer a documented way to read a directory tree: for
instance a key that lists every regular file under `path` at any depth, applying the candidate
rules (names starting with `_` or `.`, `._COPYING_`) to every file and folder name, matching
`watermark_pattern` against the file's name or its path relative to `path`, and handing Spark
the sorted list of the selected files, so that the selection still happens before Spark loads
anything.

## Decisions

- **D1. A flat landing directory** *(Q1)*. The files sit directly in `/data/landing/votacoes`,
  each name carrying the full hour. It is the layout `DiskReader` reads, and it needs no
  workaround. The nested layout is GAP-001.
- **D2. The Votações dataset** *(Q2)*. Its `dataHoraRegistro` has second precision and is the
  moment a voting was registered: a natural time for an hourly landing. Its counts, dates, ids
  and timestamps exercise the CSV parsing more than Eventos would. Its edges, the empty
  `dataHoraRegistro` and the rows filed under another year than the one they were registered
  in, are handled by the preparation script, outside the process.
- **D3. The preparation script is the producer of the landing, outside ingestio.** Python's
  standard library only (`urllib.request`, `csv`): it needs neither Spark nor ingestio, and its
  output is the documented landing format. It removes the Câmara's BOM, renames the header to
  the target's names, quotes every field and ends every record with `\n`, and keeps every value
  as it is. A clean, checked header lets Spark verify each file against the schema
  (`enforceSchema` `false`) instead of mapping by position. It reads every year the window
  touches into memory (about 6 MB a year) and checks them before it writes, so that a broken
  source writes nothing. It writes in ascending order, atomically, and never rewrites, so the
  job may run while it writes. It is run through compose, in the `spark` service, which mounts
  the landing directory.
- **D4. The watermark comes from the file names, and lives in the target's rows.** A name
  pattern rather than the modification time (pitfall 7): the hour is in the name, fixed-width
  and most significant first. The last one is `max(source_file)` of the target, so it commits
  with the rows, and a failed run reads the same files again: exactly once per file, under the
  conditions of **Behaviour**.
- **D5. A typed schema read by Spark, with Spark's CSV options in `config.json`.** Each Spark
  type maps to its PostgreSQL type, nulls included, without relying on
  `stringtype=unspecified` for typed columns. `FAILFAST` and `enforceSchema` `false` make a
  malformed file fail the run rather than load wrong rows. Timestamps are read in
  `America/Sao_Paulo` through the CSV `timeZone` option and stored as `timestamptz` *(Q6)*:
  instants, as pitfall 12 prefers, and independent of the JVM's time zone.
- **D6. The job applies its DDL, before every run.** `CREATE SCHEMA IF NOT EXISTS` and
  `CREATE TABLE IF NOT EXISTS`, through psycopg, from the job's code, before the watermark
  query, which needs the table: one definition for the compose environment, the tests and CI.
  No `/docker-entrypoint-initdb.d` script: CI's service container could not load it. The primary
  key on `id` guards against appending a row twice; `source_file` uses the `"C"` collation;
  `loaded_at` is filled by PostgreSQL.
- **D7. Two own callbacks:** `SkipEmptyBatch`, because a run with nothing new is normal and
  `StampSourceFile` cannot read `_metadata` on an empty selection; and `StampSourceFile`, for the
  watermark. No renaming callback: the schema already names the target's columns.
- **D8. The PostgreSQL variables are required, except `PGPORT`.** The host differs between the
  `spark` service (`postgres`) and the host and CI (`localhost`), so a fallback would be wrong
  in one of them. `PGPORT` defaults to libpq's and `PostgresConnection`'s `5432`.
- **D9. pgJDBC `42.7.14`**, the latest release on Maven Central when the knowledge base was
  generated, in the 42.7 line the library's own tests use (`42.7.4`); set once, in
  `main.PGJDBC_VERSION`, shared by the job and the tests. The job's session uses `local[*]`,
  the tests' `local[2]`; both `UTC` and without the UI. Local mode does not retry tasks
  (pitfall 13).
- **D10. Compose: two services, the `spark` one long-running.** The maintainer asked for two
  containers: `docker compose up --build --wait` brings both up, and the preparation script and
  the job run inside `spark` with `docker compose exec`, where the landing directory is mounted
  and Spark runs in local mode, as `DiskReader` requires. The landing is a bind mount of the
  repository's `data/` at `/data`, so it can be seen on the host; the container runs as uid
  1000 and `data/.gitkeep` exists, so that Docker does not create the directory as root. The
  `postgres` service keeps the CI service container's settings and no volume.
- **D11. The `spark` image** *(Q7)*: Java 17 from Eclipse Temurin, the Java CI uses; Python 3.12
  as `.python-version` gives it, managed by uv; the job's environment from `uv sync --locked`,
  so the container runs the very ingestio 0.5.0, pyspark 4.2.0 and psycopg 3.3.6 of `uv.lock`
  that the tests run. pyspark is in the `dev` group, so the image installs the default groups.
  The first build downloads and builds pyspark's source archive of about 450 MB and takes
  minutes; the README says so. The exact base tag is the implementer's, pinned. Its working
  directory, `/app`, mirrors the repository: `/app/src` and `/app/scripts`, copied from the
  build context, beside the `pyproject.toml`, `uv.lock` and `.python-version` that `uv sync`
  reads (D15).
- **D12. Tests run on the host and in CI**, from the repository's root, as the knowledge base
  describes, not in the containers. The compose file is checked with `docker compose config`
  and the `Dockerfile` as text; CI's runner has the Docker CLI, so `.github/workflows/ci.yml`
  needs no change for this spec: its commands run from the root, and pytest reads its
  `pythonpath` from `pyproject.toml`.
- **D13. The job reads two values of its configuration, `src/config.json`:**
  `target.table_name`, for the DDL and the watermark query, and `source.watermark_pattern`, to
  turn the stored file name into the watermark. Reading them keeps each named once; the job
  never writes the file. The reading lives in `src/warehouse.py`.
- **D14. The versions.** `0.2.0`: the job's first process is a feature, which bumps the minor
  of `0.1.0`. `0.3.0`: the layout of D15 changes the way the job is run, the commands of the
  README among them, since `python main.py` and `python prepare_landing.py` no longer exist; it
  is a breaking change, which bumps the minor while the version is `0.x` (CONTRIBUTING.md,
  Versions and the changelog; the maintainer's answer 5 of 2026-10-24, D15).
- **D15. The layout of `0.3.0`**, from the maintainer's request and answers of 2026-10-24. The
  request: "eu queria uma mudanca de estrutura. Main, config e callbacks ficam dentro de uma
  pasta src", and then "inclusive, nao precisa existir um arquivo chamado callback, quero que
  ele esteja dentro da main". The answers:
  1. `src/` holds `main.py`, `config.json` and `warehouse.py`, the helper with the values of the
     configuration, the DDL and the watermark query, next to `main.py`.
  2. `callbacks.py` no longer exists: the job's own callbacks, `SkipEmptyBatch` and
     `StampSourceFile`, are defined in `src/main.py`.
  3. `prepare_landing.py` moves to a folder `scripts/`, as `scripts/prepare_landing.py`: it is
     not part of the ingestio job.
  4. The commands become `docker compose exec spark python src/main.py` and
     `docker compose exec spark python scripts/prepare_landing.py --landing ...`. The container
     mirrors the repository: its working directory is `/app`, with `/app/src` and
     `/app/scripts`.
  5. The version is `0.3.0` (D14).

  The behaviour is the one of `0.2.0`: the process, its steps and callbacks, the contents of the
  configuration, the table, the landing, the watermark, the log and the exit codes. The layout
  meets the knowledge base and CONTRIBUTING.md as **Fit with ingestio** shows.
- **D16. The tests import the job's modules by their names, from `src/` and `scripts/`.**
  `pythonpath` of `[tool.pytest.ini_options]` in `pyproject.toml`, `["."]` in `0.2.0`, becomes
  `["src", "scripts"]`, and its comment says that the job's modules are imported from those two
  directories. The implementer changes it, with the move of the files: the tester writes only
  `tests/`, and the implementer is the one stage of docs/workflow.md that writes
  `pyproject.toml`, where that page names `project.version` and the ingestio extras; the import
  roots follow the layout the implementer creates. mypy keeps running over `.`; the implementer confirms with `uv run --locked mypy` that it
  resolves `main`, `warehouse` and `prepare_landing`, and changes `[tool.mypy]` only if it does
  not.

## Open questions

Every question has the options the planner sees and the one it recommends. The spec above is
written for the recommended options.

**Answered on 2026-10-24.** The maintainer delegated the decisions of this change to the main
session ("Vc tem a minnha carta branca pra ir tocando"), which took the recommended option of
every question: Q1 (a), Q2 (a), Q3 (a), Q4 (a), Q5 (a), Q6 (a), Q7 (a), Q8 (a). The maintainer
reviews them when back; a different answer returns the spec to planning.

**The `0.3.0` revision raises no question.** The maintainer gave the definitions on 2026-10-24,
recorded in D15 and D14. The maintainer's sixth answer, an approval of the spec provided it
follows them, is the main session's to record.

- **Q1. The layout of the landing directory.** The suggested year/month/day folders cannot be
  read by a valid job (GAP-001).
  - **(a) Recommended:** a flat landing directory, one file per hour named
    `votacoes_YYYYMMDDHH.csv`; GAP-001 is recorded for the nested layout.
  - (b) The nested tree for people to browse, plus the flat directory the job reads, holding a
    symbolic link to each file of the tree (`DiskReader` follows links). It bends no rule of the
    library, but keeps a second layout only to hide the gap, and GAP-001 is recorded anyway.
  - (c) The nested tree only: this process waits for the library to read a tree.
- **Q2. The dataset.**
  - **(a) Recommended: Votações**, about 10,000 to 14,000 rows a year, bucketed by
    `dataHoraRegistro` (D2).
  - (b) Eventos, about 2,400 to 3,200 rows a year, bucketed by `dataHoraInicio`, never empty;
    its file holds scheduled and cancelled events. The header, schema, table and file names of
    this spec would change.
- **Q3. The two years.**
  - **(a) Recommended:** the calendar years 2024 and 2025, `2024-01-01T00` to `2025-12-31T23`,
    whose files no longer grow, leaving January 2026 for the README's incremental step.
  - (b) The 24 months up to yesterday, `2024-10-24T00` to `2026-10-23T23`: the latest hours may
    still be incomplete when they are landed, since a landed hour is never rewritten, and the
    incremental step can add only the days that pass after it.
- **Q4. Rows without `dataHoraRegistro`** (67 in the 2025 file).
  - **(a) Recommended:** left out of the landing and counted in the script's last line: the
    file name carries an hour, and these rows have none.
  - (b) Put in the hour `00` of their `data`, which gives them an hour they do not have.
- **Q5. Hours without rows.**
  - **(a) Recommended:** no file: about 1,400 files for the two years.
  - (b) A file with only the header for every hour of the window, 17,544 files: each run lists
    them all, and since the watermark comes from rows, the empty hours after the last row are
    selected again at every run until a later hour with rows is loaded.
- **Q6. The timestamps**, which the dataset gives without an offset, in Brasília time by every
  indication.
  - **(a) Recommended:** `timestamptz`, read as `America/Sao_Paulo`: instants, as the knowledge
    base prefers, independent of the time zone of the machine; `psql` shows them in the
    server's zone, UTC, so `18:35:59` shows as `21:35:59+00`.
  - (b) `timestamp`, the wall-clock time as published, with no zone assumed: the Spark session
    and the JVM must then share one time zone (`UTC`, set for the JVM as well), or the values
    shift by the machine's offset.
- **Q7. The Spark image.**
  - **(a) Recommended:** the job's own image: Temurin Java 17, Python 3.12 and `uv sync
    --locked`, whose pyspark 4.2.0 brings Spark: the container runs the Spark the tests and CI
    run.
  - (b) An official `apache/spark` image with the job installed on it: its Spark and Python
    versions are the image's, not those of `uv.lock` and `.python-version`, so the job would run
    on a Spark its tests never ran on, beside the pyspark the job's environment installs.
- **Q8. The terms of the dataset.** The portal states no formal license; its FAQ says the data
  is free, needs no registration, and has no restriction on commercial use. The repository
  never commits the data.
  - **(a) Recommended:** that basis is enough for this job.
  - (b) A formal license is required, and the job takes another public dataset.

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-24 | planning | draft | First spec, for 0.2.0: a preparation script lays out a flat landing of hourly CSV files from the Câmara's Votações (2024 to 2025), and the job appends the files above its watermark to `camara.votacoes`, with the watermark kept in the table's own `source_file` column. Two compose services, documented in the README; 22 acceptance criteria; one library gap proposed (GAP-001). |
| 2026-10-24 | approval | approved | Approved by the main session under the maintainer's explicit delegation ("carta branca pra ir tocando"), with the recommended option of Q1 to Q8. GAP-001 recorded in `LIB_GAPS.md`. |
| 2026-10-24 | tests | tested | 42 tests in `tests/test_camara_hourly_ingestion.py`, with the shared Spark, PostgreSQL and schema fixtures in `tests/conftest.py`, cover AT-001 to AT-022: 19 integration tests on Spark and PostgreSQL, one on the Docker CLI, 22 that need neither. They fail only because `main.py`, `prepare_landing.py`, `config.json`, `compose.yaml` and the README commands do not exist yet (23 failed, 19 errors). |
| 2026-10-24 | documentation | documented | README run section with the seven AT-022 commands in order, a new page `docs/camara_hourly_ingestion.md`, the page and the spec in `docs/index.md`, and the `0.2.0` entry of `CHANGELOG.md`. |
| 2026-10-24 | implementation | implemented | `main.py` with `callbacks.py` (`SkipEmptyBatch`, `StampSourceFile`) and `warehouse.py` (config values, DDL, watermark query); `config.json`; `prepare_landing.py`; `compose.yaml`, `Dockerfile` and `.dockerignore` for the `spark` image; `data/.gitkeep` and `.gitignore`; version `0.2.0`. All 42 tests, ruff and mypy pass, and the image builds. |
| 2026-10-24 | validation | validated | Passed all nine checks: 42 tests, ruff, mypy and `uv lock --check`; code matches the documentation and the six rules of a valid job; ingestio 0.5.0 pinned and locked from PyPI; job at 0.2.0 in `pyproject.toml` and `CHANGELOG.md`. Non-blocking notes: the spec's status in `docs/index.md`, and the field-count error of the preparation script missing from the docs' errors table. |
| 2026-10-24 | planning | draft | Revised in place for 0.3.0, the maintainer's layout of D15: `src/` holds `main.py`, `config.json` and `warehouse.py`; `SkipEmptyBatch` and `StampSourceFile` are defined in `src/main.py` and `callbacks.py` goes; the preparation script moves to `scripts/prepare_landing.py`; the image works from `/app`, and the commands become `python src/main.py` and `python scripts/prepare_landing.py`. Behaviour unchanged. AT-012 and AT-015 to AT-022 updated, AT-023 to AT-028 new for the layout; D14 covers both versions, D15 records the answers, D16 gives pytest's `pythonpath` to the implementer. No new gap or question. |
| 2026-10-24 | approval | approved | Approved by the maintainer in advance, explicitly, provided the spec follows the answers of D15 ("Aprovo com estas respostas"); the revision follows them. |
| 2026-10-24 | tests | tested | Tests revised for 0.3.0, 55 in all: "the job runs" uses `src/config.json` and the `main` of `src/main.py`, the preparation script's criteria the `prepare_landing` of `scripts/prepare_landing.py`; AT-021 and AT-022 updated, AT-023 to AT-028 new. Against 0.2.0 they fail only because the layout is missing (25 failed, 19 errors, 11 passed: AT-013 and AT-014); on a simulated 0.3.0 layout in a scratch copy all 55 pass. |
| 2026-10-24 | documentation | documented | Documented 0.3.0, a layout change with the behaviour unchanged: the README's run section with the seven AT-022 commands (`scripts/prepare_landing.py`, `src/main.py`) and a section on the layout of `src/` and `scripts/`; every path and command of `docs/camara_hourly_ingestion.md`, with a layout section; the page's coverage in `docs/index.md`; the breaking `v0.3.0` entry of `CHANGELOG.md`. |
| 2026-10-24 | implementation | implemented | `src/main.py` (with `SkipEmptyBatch` and `StampSourceFile` defined in it), `src/config.json` and `src/warehouse.py`; `scripts/prepare_landing.py`; the five root files and `callbacks.py` removed; version `0.3.0` and pytest's `pythonpath = ["src", "scripts"]` in `pyproject.toml`, `uv.lock` updated; `working_dir: /app` in `compose.yaml`; the Dockerfile copies `src/` and `scripts/` into `/app`; `.dockerignore` excludes `**/__pycache__`. All 55 tests, ruff, mypy and `uv lock --check` pass, and the image builds with the files in place. |
| 2026-10-24 | validation | documented | Passed all nine checks (55 tests, ruff, mypy, `uv lock --check`, code against docs, rules of a valid job, pins, sources, write scopes), with one note the change itself introduced: the comment over the local import of `pyspark.sql.functions` in `StampSourceFile.run`, true in `callbacks.py`, is false in `src/main.py`, which imports pyspark at module level. The main session returned the change to the implementer to fix it before closing. |
| 2026-10-24 | implementation | implemented | Fixed the returned note: `src/main.py` imports `SparkSession` and `functions` from `pyspark.sql` at the top, and `StampSourceFile.run` loses its local import and the stale comment; behaviour identical. 55 tests, ruff, mypy and `uv lock --check` pass, and the image rebuilt. |
| 2026-10-24 | validation | validated | Re-validated after the returned fix, nothing else changed: all nine checks pass for 0.3.0 (55 tests, ruff, format, mypy, `uv lock --check`; the moved files identical to 0.2.0 apart from the callbacks inlined in `src/main.py`; the image mirrors the repository under `/app`; no stale reference to the removed files; architecture files and the ingestio pin unchanged). Notes for the hub: the Layout wording of CONTRIBUTING.md, which does not foresee callbacks defined in `main.py`, and the implementer's scope in the table of docs/workflow.md, which does not name pytest's settings, `compose.yaml`, `Dockerfile` or `.dockerignore`. |
