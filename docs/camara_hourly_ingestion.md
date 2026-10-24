# Hourly CSV files of the Câmara dos Deputados, from a landing directory to PostgreSQL

The job's first process, version `0.2.0`. Spec:
[camara_hourly_ingestion](../specs/camara_hourly_ingestion.md). The README has the commands that
bring the environment up and run it; this page describes what the process ingests, how it is
configured, what it writes, and how it fails.

## What it does

A preparation script of the job's own lays out a flat landing directory with one CSV file per
hour of the *Votações* dataset of the Câmara dos Deputados. The ingestio process then reads
only the files above its watermark with `ReadSource` (the `disk` system), stamps every row with
the name of its file, and appends the batch with `AppendOnly` (the `postgres` system) to the
table `camara.votacoes`, in one transaction. A run with nothing new is a normal run and changes
nothing.

```text
Câmara's yearly files --prepare_landing.py--> landing directory --main.py--> camara.votacoes
```

## The dataset

- **Source:** the yearly bulk files of *Votações* of the Câmara dos Deputados – Dados Abertos,
  `https://dadosabertos.camara.leg.br/arquivos/votacoes/csv/votacoes-{year}.csv`, updated daily
  by the Câmara.
- **A record** is one voting held by an organ of the Chamber, with its aggregated result: how
  many voted yes, no and otherwise, whether it was approved, and its description. There is no
  individual vote and no personal data of citizens; the free text sometimes names a deputy
  acting in office.
- **Terms:** the portal needs no registration or credentials. It states no formal license; its
  FAQ says the data is free and that there is no restriction on its use, commercial use
  included.
- **The data is never committed.** Each environment downloads it when the preparation script
  runs; the repository holds no row of the dataset. The contents of `data/` are ignored by git,
  and tests use synthetic rows.

## Prerequisites

| Prerequisite | For |
|---|---|
| Docker with `docker compose` | the environment: the `postgres` and `spark` services |
| Java 17 or 21, and `uv sync` | running the tests on the host |
| Network access | the Câmara's portal (preparation script) and Maven Central (pgJDBC driver) |

- The first `docker compose up --build --wait` builds the `spark` image. It installs the job's
  environment from `uv.lock`, whose pyspark 4.2.0 is published as a source archive of about
  450 MB: the first build downloads and builds it and takes minutes.
- The first job run in the container downloads the pgJDBC driver from Maven Central and keeps
  it in the container's home. A container created anew downloads it again.
- The target table and its schema are created by the job itself, with its own DDL, before every
  run. Nothing creates them at container start.

## The environment

`compose.yaml` describes two services.

| Service | What it is |
|---|---|
| `postgres` | PostgreSQL 17 (`postgres:17`), user, password and database `postgres`, port 5432 published on 5432, with a health check (`pg_isready -U postgres`). It has no volume of its own: its data lives as long as its container. |
| `spark` | The job's image (Java 17, Python 3.12, the environment of `uv.lock`). It waits for `postgres` to be healthy, mounts the repository's `data` directory at `/data`, runs as uid and gid 1000, and stays up. The preparation script and the job run inside it with `docker compose exec`. Spark runs in local mode. |

The `spark` service reaches the database through libpq's variables: `PGHOST=postgres`,
`PGPORT=5432`, `PGUSER`, `PGPASSWORD` and `PGDATABASE` all `postgres`. The user and password
are the throwaway values of a local container.

## The landing directory

`/data/landing/votacoes` in the `spark` service, `data/landing/votacoes` on the host. It is
flat: the files sit directly inside it, with no subdirectory. A layout of year/month/day folders
cannot be read by the library at 0.5.0; see [the gap](#the-nested-layout-gap).

One file per hour that has at least one row:

- **Name:** `votacoes_<YYYYMMDDHH>.csv`, the hour of the `dataHoraRegistro` of every row in the
  file, as the dataset gives it (Brasília time, no offset), zero-padded.
  `votacoes_2024020618.csv` holds the rows registered from `2024-02-06T18:00:00` to
  `2024-02-06T18:59:59`. Hours without rows have no file.
- **Encoding:** UTF-8, without a BOM.
- **Header:** one line with the twenty column names of the table, each in `"`, separated by `;`.
- **Records:** the rows in the order of the yearly file, every field in `"`, a `"` inside a
  value doubled, fields separated by `;`, each record ended by `\n`. The values are the
  Câmara's, unchanged, line breaks inside a value included.

The names carry the hour, fixed-width and most significant first, so they order as the hours do.

## The preparation script

```text
python prepare_landing.py --landing DIR --start YYYY-MM-DDTHH --end YYYY-MM-DDTHH [--base-url URL]
```

| Argument | Required | What it is |
|---|---|---|
| `--landing` | yes | the landing directory to write; created when missing |
| `--start`, `--end` | yes | the first and the last hour of the window, both included, in Brasília time as the dataset gives it, such as `2024-01-01T00` |
| `--base-url` | no | where the yearly files are, an `https:` or a `file:` URL; the Câmara's portal by default. The file of a year is `<base-url>/votacoes-<year>.csv` |

It is not an ingestio process and uses only Python's standard library. What it does:

1. Fetches the yearly file of every calendar year from the year of `--start` to the year of
   `--end`, and only those.
2. Checks, before it writes anything, that each header is exactly the Câmara's twenty column
   names, that each `dataHoraRegistro` is empty or a valid `YYYY-MM-DDTHH:MM:SS`, and that no
   `id` appears twice across the files.
3. Puts each row in the hour of its `dataHoraRegistro`. Rows without one, and rows registered
   outside the window, are left out.
4. Removes the temporary files an interrupted run left, then writes the file of each hour of
   the window that has rows, in ascending order of hour. Each file is written under a temporary
   name that starts with `.` and renamed when complete, so that a run of the job never sees a
   partial file or an hour before an earlier one. A file whose name already exists is left as
   it is and counted as skipped.
5. Logs one `INFO` line with the counts, such as
   `files written: 4, files skipped: 0, rows written: 5, rows without dataHoraRegistro: 1, rows outside the window: 1`.

A later window adds later hours: after the two years, a window for January 2026 adds those
hours, and the next run of the job appends them alone. Rows the Câmara adds later to an hour
already landed are not landed.

**Exit status:** `0` on success; `1` when a source cannot be fetched or breaks the format it
expects (the message is logged at `ERROR`, and the landing directory stays as it was); `2` for a
usage error, such as a missing argument or a `--start` after `--end`.

| Error logged | When |
|---|---|
| `cannot fetch <url>: <reason>` | a yearly file cannot be fetched |
| `votacoes-<year>.csv: unexpected header` | the header is not the Câmara's |
| `votacoes-<year>.csv: invalid dataHoraRegistro '<value>' for id '<id>'` | a time that is not valid |
| `votacoes-<year>.csv: record <n> has <k> fields` | a record that does not have twenty fields |
| `id '<id>' appears more than once` | an `id` repeated across the files |

The two years of history are the hours from `2024-01-01T00` to `2025-12-31T23`: about 1,400
files, and somewhat fewer rows than the 24,200 of the two yearly files (counts of the dataset on
2026-10-24).

## The process

`main.py` runs one process of two steps:

| Step | Callbacks |
|---|---|
| `extract` | `ReadSource` reads the `source` section of `config.json` and stores the DataFrame of the selected files in the state |
| `load` | `SkipEmptyBatch` ends the step when the batch has no row; `StampSourceFile` adds the column `source_file`, the name of the file each row came from; `AppendOnly` appends the batch to the `target` section's table, in one transaction |

Around the process, the job's own code, before it is built:

1. Builds the PostgreSQL connection from the environment.
2. Starts the Spark session in local mode, with the pgJDBC driver.
3. Applies the DDL of the target.
4. Reads the watermark from the target.

### `config.json`

| Key | Meaning |
|---|---|
| `source.system` | `disk`: the files of one local directory |
| `source.path` | `/data/landing/votacoes`, absolute: the landing directory as the `spark` service sees it |
| `source.format` | `csv` |
| `source.watermark_pattern` | `votacoes_(\\d{10})\\.csv`: matches the whole file name and captures the hour. Files whose names do not match are never read |
| `source.schema` | the twenty columns of the landing files, named as the table's columns, as a DDL string |
| `source.options` | Spark's CSV options, below |
| `target.system` | `postgres` |
| `target.table_name` | `camara.votacoes` |

The CSV options: `header` (`true`), `sep` (`;`), `quote` and `escape` (`"`), `multiLine`
(`true`, values contain line breaks), `encoding` (`UTF-8`), `enforceSchema` (`false`: Spark
checks each file's header against the schema's names and fails on a difference), `mode`
(`FAILFAST`: a malformed record or a value that does not parse fails the run instead of becoming
nulls) and `timeZone` (`America/Sao_Paulo`: the timestamps have no offset and are Brasília
times). Credentials and connection settings are never in the file, and neither is the watermark:
they reach the callbacks as arguments.

### Environment variables

| Variable | Required | Meaning |
|---|---|---|
| `PGHOST` | yes | the PostgreSQL host: `postgres` in the `spark` service, `localhost` on the host |
| `PGPORT` | no | the port; `5432` when unset |
| `PGUSER` | yes | the user |
| `PGPASSWORD` | yes | the password |
| `PGDATABASE` | yes | the database |

There is no fallback. A missing or empty required variable raises `ValueError("<NAME> is not
set")`, and a `PGPORT` that is not an integer raises `ValueError("PGPORT must be an integer")`,
before Spark starts and before anything touches the database.

### The target table

`camara.votacoes`, created by the job before every run with `CREATE SCHEMA IF NOT EXISTS` and
`CREATE TABLE IF NOT EXISTS`: an existing table and its rows are left as they are.

| Column | Type |
|---|---|
| `id` | `text PRIMARY KEY` |
| `uri`, `uri_orgao`, `sigla_orgao`, `uri_evento`, `descricao`, `ultima_abertura_votacao_descricao`, `ultima_apresentacao_proposicao_descricao`, `ultima_apresentacao_proposicao_uri_proposicao` | `text` |
| `data` | `date` |
| `data_hora_registro`, `ultima_abertura_votacao_data_hora_registro`, `ultima_apresentacao_proposicao_data_hora_registro` | `timestamptz` |
| `id_orgao`, `id_evento`, `ultima_apresentacao_proposicao_id_proposicao` | `bigint` |
| `aprovacao`, `votos_sim`, `votos_nao`, `votos_outros` | `integer` |
| `source_file` | `text COLLATE "C" NOT NULL`: the name of the landing file the row came from |
| `loaded_at` | `timestamptz NOT NULL DEFAULT now()`: filled by PostgreSQL when the append commits, the same for every row of one run |

The columns of the Câmara's files become `snake_case` (for instance `dataHoraRegistro` becomes
`data_hora_registro`), and the preparation script writes the header with the table's names.

**Values.** An empty field is `NULL`, whatever its type. Text, with line breaks and `;` inside,
reaches the table as it is in the file. Timestamps are read as Brasília instants and stored as
`timestamptz`, so `2024-02-06T18:35:59` is stored as `2024-02-06 21:35:59+00`, whatever the time
zone of the machine; `psql` shows it in the server's zone, UTC.

## A run

1. The job logs the tree of the run and its own lines on the console.
2. It builds the connection, starts Spark and applies the DDL.
3. It reads the watermark: the highest `source_file` of the table. With no row it logs
   `watermark: none (every file is read)`; otherwise it extracts the hour from the name and logs,
   for instance, `watermark: 2025123123`. A stored name that does not match the pattern raises
   `ValueError`.
4. `ReadSource` lists the landing directory and selects the files whose hour is strictly above
   the watermark, or all of them without one. Hidden files (the preparation script's temporary
   files among them), subdirectories, and names the pattern does not match are never read.
5. The load step stamps and appends the batch.

The log of a run that appends:

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

Spark's own output, such as the driver's download on the first start, appears beside it.

**A second run** with no new file in the landing directory reads the watermark, selects nothing,
and logs `⊘ SkipEmptyBatch.run cancelled the step after <d>: no new row` and
`└ load cancelled in <d>`. The process finishes, nothing is appended, and the exit status is 0.
After the landing is extended with a later window, a run appends the rows of the new files alone.

### Failures

Every failure propagates after the `✖` line of the tree, and Python exits with status 1. In every
case the table holds what it held before the run, and no `ingestio_staging_*` table is left.

| Failure | Where |
|---|---|
| a selected file whose header is not the schema's names, a record with another number of fields, or a value that does not parse as its type | Spark's error, in `SkipEmptyBatch.run` or `AppendOnly.run` |
| a row whose `id` is already in the table, or appears twice in the batch | PostgreSQL's unique violation, in `AppendOnly.run` |
| PostgreSQL unreachable | psycopg's error, when the DDL is applied, before the process exists |
| the landing directory missing | `FileNotFoundError` in `ReadSource.run`, after the DDL created the table |

### The watermark and its guarantee

The watermark lives in the table's own rows, as `source_file`, and commits in the same
transaction as them. A failed run leaves both as they were, and the next run selects the same
files again: the rows of each landed file reach the table exactly once, provided that:

- files reach the landing in ascending order of hour, never at or below an hour already landed
  (the preparation script guarantees both);
- a landed file is not changed or removed afterwards;
- runs of the job do not overlap. When two do, both read the same watermark, and the primary
  key makes the second commit fail instead of duplicating rows.

### Starting over

`docker compose down --volumes` discards the database with its container; the landing stays on
the host. The next run finds an empty table, no watermark, and loads every landed file again.

## Running the tests

The tests run on the host and in CI, not in the containers. They need Java 17 or 21, the
environment of `uv sync`, and the PostgreSQL that `docker compose` starts, reached through
libpq's variables (`PGHOST=localhost`, `PGPORT=5432`, `PGUSER`, `PGPASSWORD` and `PGDATABASE`
`postgres`). They fail, rather than skip, when it is unreachable. The Docker CLI is needed for
the test of the compose file. The first Spark session downloads the pgJDBC driver. The tests use
synthetic rows and configurations of their own.

## The nested layout gap

The year/month/day layout (`/data/landing/votacoes/YYYY/MM/DD/`) cannot be read by a valid job:
`DiskReader` lists only the entries directly inside `path` and ignores subdirectories, and the
configuration holds one source with one absolute `path`. The job therefore uses the flat layout.
The gap is recorded as GAP-001 in [LIB_GAPS.md](../LIB_GAPS.md).
