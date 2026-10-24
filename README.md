# ingestion_disk_to_postgres

Loads the CSV files of a public dataset from a local directory into a PostgreSQL table, appending only the files above its watermark.

A proof-of-concept job built with [ingestio](https://github.com/gustavoueti/ingestio)
[`0.5.0`](https://pypi.org/project/ingestio/0.5.0/).

## Goal

The job ingests CSV files from a public dataset that sit in a local directory on disk into a
table in PostgreSQL. It reads them with `DiskReader` (format `csv`), selecting only the files
above the job's watermark, and loads them with the `AppendOnly` callback through
`PostgresWriter` as an incremental, atomic append. PostgreSQL runs in Docker: `docker compose`
locally, a service container in CI.

The source is local files on disk, from a public dataset: no credentials and no private
third-party data. Which dataset is a decision of the job, taken with its own agents in the
planner's spec.

## Setup

```bash
uv sync
uv run pre-commit install --hook-type commit-msg
```

## What the job does

It loads the *Votações* dataset of the Câmara dos Deputados – Dados Abertos (the votings held by
the organs of the Chamber, with their aggregated results) into the table `camara.votacoes` in
PostgreSQL. A preparation script downloads the Câmara's yearly CSV files and lays out a landing
directory with one file per hour, named `votacoes_YYYYMMDDHH.csv`; two years of history at
first. The ingestio process reads only the files above its watermark, stamps each row with the
name of its file, and appends the batch in one transaction. The watermark is the highest
`source_file` in the table itself, so it commits with the rows. Details are in
[docs/camara_hourly_ingestion.md](docs/camara_hourly_ingestion.md).

## Prerequisites

- Docker with `docker compose`.
- The first `docker compose up --build --wait` builds pyspark's source archive of about 450 MB
  and takes minutes.
- The first job run in the container downloads the pgJDBC driver from Maven Central, so it
  needs the network, as does the preparation script, for the Câmara's portal.

## The dataset

The yearly files at `https://dadosabertos.camara.leg.br/arquivos/votacoes/csv/`. The portal
needs no registration or credentials. It states no formal license; its FAQ says the data is
free and that there is no restriction on its use, commercial use included. The repository
never commits the data: each environment downloads it, and `data/` is ignored by git.

## The landing directory

Inside the `spark` service it is `/data/landing/votacoes`, on the host
`data/landing/votacoes`. It is flat, with one file per hour that has rows, named
`votacoes_YYYYMMDDHH.csv` after the hour of the rows' `dataHoraRegistro` (Brasília time). Each
file is UTF-8 without a BOM, with a header of the table's column names, `;` as separator, every
field in `"` and records ended by `\n`.

## Run it

```bash
docker compose up --build --wait
docker compose exec spark python prepare_landing.py --landing /data/landing/votacoes --start 2024-01-01T00 --end 2025-12-31T23
docker compose exec spark python main.py
docker compose exec postgres psql -U postgres -c "SELECT count(*), max(source_file) FROM camara.votacoes"
docker compose exec spark python prepare_landing.py --landing /data/landing/votacoes --start 2026-01-01T00 --end 2026-01-31T23
docker compose exec spark python main.py
docker compose down --volumes
```

1. **Bring the environment up.** Builds the `spark` image and starts the `spark` and `postgres`
   services, returning once PostgreSQL is healthy. It shows the build and the containers
   reaching `Healthy`.
2. **Land two years.** Downloads the 2024 and 2025 files and writes one file per hour from
   2024-01-01 to 2025-12-31 into the landing directory, about 1,400 files. It logs a line such
   as `files written: <n>, files skipped: 0, rows written: <r>, ...`.
3. **Run the job.** Creates the schema and the table, reads the watermark (it logs
   `watermark: none (every file is read)`), and appends every landed file. It shows the tree of
   the run, with `AppendOnly.run` done.
4. **Look at the table.** Shows the number of rows and the highest `source_file`, the
   watermark of the next run, which is the last file of 2025 that has rows.
5. **Land January 2026.** Adds the hours of January 2026; the files already there are left as
   they are.
6. **Run the job again.** It logs the watermark from step 4 and appends only the files of
   January 2026. Run it once more with nothing new and it shows
   `⊘ SkipEmptyBatch.run cancelled the step after <d>: no new row`, appends nothing and exits
   with status 0.
7. **Tear it down.** Discards the database with its container; the landing stays on the host.
   The next run finds an empty table, no watermark, and loads every landed file again.

Timestamps are stored as `timestamptz`, and `psql` shows them in UTC: `2024-02-06T18:35:59`
in Brasília is `2024-02-06 21:35:59+00`.

## Running the tests

The tests run on the host, not in the containers. They need Java 17 or 21, `uv sync`, and the
PostgreSQL that `docker compose` starts (`docker compose up --wait postgres`), reached through
libpq's variables: `PGHOST=localhost`, `PGPORT=5432`, `PGUSER=postgres`, `PGPASSWORD=postgres`
and `PGDATABASE=postgres`. They fail, rather than skip, when PostgreSQL is unreachable.

```bash
uv run --locked pytest -q
```

## Known gap

A landing laid out in year/month/day folders cannot be read by `DiskReader` at ingestio 0.5.0,
and the job accepts no workaround, so the landing is flat. It is recorded as GAP-001 in
[LIB_GAPS.md](LIB_GAPS.md).

## Documentation

See [docs/index.md](docs/index.md).
