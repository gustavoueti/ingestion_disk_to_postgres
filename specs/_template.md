# Spec: <title>

- **Status:** draft
- **Slug:** `<slug>`. The spec is `specs/<slug>.md` and its tests are `tests/test_<slug>.py`.
- **Version:** the version this change gives the job, such as `0.2.0`
- **Related:** other specs, entries of `LIB_GAPS.md`

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

What the process does, in two or three sentences.

## Motivation

Why the job needs it, and what it shows about ingestio.

## Source and target

- **Source:** the system, where the data comes from, and why it is public or local.
- **Target:** the system and where the data goes.
- **Data:** what one record looks like. No credentials, no real data of third parties.

## Fit with ingestio

How the process uses the library, as `.claude/kb/ingestio.md` describes it:

- **Shipped callbacks:** the callbacks of the library the process places, and why each fits.
- **Own callbacks:** the callbacks the job writes, what each does, and its criticality.
- **Connectors:** the systems named in `config.json`, and any shipped connector a callback of
  the job builds itself.
- **Rules of a valid job:** each rule, and how the process meets it.

A process that meets a rule only with a workaround stops here: describe the gap under
**Library gaps**.

## Scope

In scope:

-

Out of scope:

-

## Configuration

The sections and keys of `config.json`, and the arguments that do not belong in it.

## Behaviour

How the process behaves when it runs: what it reads, what it writes, its log, its errors and
its edge cases, such as an empty source.

## Acceptance criteria

Each criterion is testable on its own, from outside the job's code. Its tests are named after
it: `AT-001` is covered by `test_at001_<description>` in `tests/test_<slug>.py`.

- **AT-001**: Given ..., when ..., then ....

## Library gaps

Each gap the process runs into, in the format of `LIB_GAPS.md`, or "None".

## Decisions

The design decisions this spec takes.

## Open questions

Questions for the maintainer. All of them are answered before the spec is approved.

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| YYYY-MM-DD | planning | draft | |
