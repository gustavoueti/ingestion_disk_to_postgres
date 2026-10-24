# Library gaps

What this job could not do with ingestio as it is. A process that works only with a
workaround of the library is not accepted with the workaround: the gap is recorded here and
the maintainer decides what happens to the process. The ingestio_examples hub consolidates
the gaps of every job for the library's maintainers.

Any agent that finds a gap stops and reports it; the main session records it below, from the
template, with the next number. An entry is never deleted: when the library closes the gap,
its status changes.

<!--
## GAP-NNN: <the gap, in one line>

- **ingestio version:** X.Y.Z
- **Recorded:** YYYY-MM-DD
- **Spec:** specs/<slug>.md
- **Status:** open | closed in ingestio X.Y.Z | withdrawn: <why>

**What the job needed to do.** The workaround the process would have needed, concretely.

**Why it points to a gap.** Which rule of a valid job, or which principle of the library,
the workaround would break, and why the library offers no valid way.

**Expected behaviour of the library.** What the library should offer so that the job does it
with the public API alone.
-->

## GAP-001: `DiskReader` reads one directory only, so a landing laid out in year/month/day folders cannot be read

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
