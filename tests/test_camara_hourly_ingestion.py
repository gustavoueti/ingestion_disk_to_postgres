"""Tests of the Câmara's hourly ingestion, the spec `specs/camara_hourly_ingestion.md`.

Each test is named after the acceptance criterion it covers. "The job runs" is, as the spec
defines it, `main.run_job` on a copy of the job's `config.json`, written under `tmp_path` with a
landing directory and a schema of the test's own, and the connection to the PostgreSQL of the
environment.

Every row is synthetic: the tests invent the ids, the texts, the organs and the URIs, under
`example.org`, and write the landing files and the Câmara's yearly files themselves. No row of
the dataset is used.

AT-001 to AT-012 need Spark and PostgreSQL, and AT-021 the Docker CLI: they are integration
tests, which fail rather than skip when what they need is missing.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, LiteralString

import psycopg
import pytest
from ingestio import AppendOnly, Process, ReadSource
from ingestio.connector.all import PostgresConnection
from psycopg import sql
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
JOB_CONFIG = ROOT / "config.json"

# The twenty columns: the Câmara's name, the landing's and the target's name, and the target's
# type as information_schema names it (Configuration, The landing directory and its files).
COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("id", "id", "text"),
    ("uri", "uri", "text"),
    ("data", "data", "date"),
    ("dataHoraRegistro", "data_hora_registro", "timestamp with time zone"),
    ("idOrgao", "id_orgao", "bigint"),
    ("uriOrgao", "uri_orgao", "text"),
    ("siglaOrgao", "sigla_orgao", "text"),
    ("idEvento", "id_evento", "bigint"),
    ("uriEvento", "uri_evento", "text"),
    ("aprovacao", "aprovacao", "integer"),
    ("votosSim", "votos_sim", "integer"),
    ("votosNao", "votos_nao", "integer"),
    ("votosOutros", "votos_outros", "integer"),
    ("descricao", "descricao", "text"),
    (
        "ultimaAberturaVotacao_dataHoraRegistro",
        "ultima_abertura_votacao_data_hora_registro",
        "timestamp with time zone",
    ),
    ("ultimaAberturaVotacao_descricao", "ultima_abertura_votacao_descricao", "text"),
    (
        "ultimaApresentacaoProposicao_dataHoraRegistro",
        "ultima_apresentacao_proposicao_data_hora_registro",
        "timestamp with time zone",
    ),
    ("ultimaApresentacaoProposicao_descricao", "ultima_apresentacao_proposicao_descricao", "text"),
    (
        "ultimaApresentacaoProposicao_idProposicao",
        "ultima_apresentacao_proposicao_id_proposicao",
        "bigint",
    ),
    (
        "ultimaApresentacaoProposicao_uriProposicao",
        "ultima_apresentacao_proposicao_uri_proposicao",
        "text",
    ),
)
CAMARA_COLUMNS = tuple(camara for camara, _, _ in COLUMNS)
TARGET_COLUMNS = tuple(target for _, target, _ in COLUMNS)
TYPES = {target: kind for _, target, kind in COLUMNS}

# The Python type psycopg returns for each PostgreSQL type of the target.
PYTHON_TYPES: dict[str, type[object]] = {
    "text": str,
    "date": date,
    "timestamp with time zone": datetime,
    "bigint": int,
    "integer": int,
}

# The dataset's times are Brasília's, which has kept UTC-3 all year since 2019.
BRASILIA = timezone(timedelta(hours=-3))

# The `source.schema` and `source.options` of Configuration.
SCHEMA = (
    "id STRING, uri STRING, data DATE, data_hora_registro TIMESTAMP, id_orgao BIGINT, "
    "uri_orgao STRING, sigla_orgao STRING, id_evento BIGINT, uri_evento STRING, aprovacao INT, "
    "votos_sim INT, votos_nao INT, votos_outros INT, descricao STRING, "
    "ultima_abertura_votacao_data_hora_registro TIMESTAMP, "
    "ultima_abertura_votacao_descricao STRING, "
    "ultima_apresentacao_proposicao_data_hora_registro TIMESTAMP, "
    "ultima_apresentacao_proposicao_descricao STRING, "
    "ultima_apresentacao_proposicao_id_proposicao BIGINT, "
    "ultima_apresentacao_proposicao_uri_proposicao STRING"
)
OPTIONS: dict[str, object] = {
    "header": True,
    "sep": ";",
    "quote": '"',
    "escape": '"',
    "multiLine": True,
    "encoding": "UTF-8",
    "enforceSchema": False,
    "mode": "FAILFAST",
    "timeZone": "America/Sao_Paulo",
}

# The definition of the target in Configuration, as information_schema.columns gives it:
# name, type, nullability, collation and default.
TARGET_DEFINITION: list[tuple[str, str, str, str | None, str | None]] = [
    *((column, kind, "NO" if column == "id" else "YES", None, None) for _, column, kind in COLUMNS),
    ("source_file", "text", "NO", "C", None),
    ("loaded_at", "timestamp with time zone", "NO", None, "now()"),
]

# The commands of AT-022, in their order.
README_COMMANDS = (
    "docker compose up --build --wait",
    "docker compose exec spark python prepare_landing.py --landing /data/landing/votacoes "
    "--start 2024-01-01T00 --end 2025-12-31T23",
    "docker compose exec spark python main.py",
    'docker compose exec postgres psql -U postgres -c "SELECT count(*), max(source_file) '
    'FROM camara.votacoes"',
    "docker compose exec spark python prepare_landing.py --landing /data/landing/votacoes "
    "--start 2026-01-01T00 --end 2026-01-31T23",
    "docker compose exec spark python main.py",
    "docker compose down --volumes",
)

# The ways a landing file is broken in AT-008 and AT-009.
LANDING_BREAKAGES = ("votos_sim_abc", "twenty_one_fields", "camara_header")

# The ways a source is broken in AT-019.
SOURCE_BREAKAGES = ("unexpected_header", "invalid_registration", "repeated_id", "missing_year")


# --- Synthetic rows and files ---------------------------------------------------------------


def _row(id_: str, registered: str, **values: str) -> dict[str, str]:
    """A synthetic record with every field filled, by target column name.

    Args:
        id_: its `id`.
        registered: its `data_hora_registro`, in Brasília time, such as `2024-02-06T18:35:59`.
        **values: fields, by target column name, that replace the defaults.
    """
    unknown = values.keys() - set(TARGET_COLUMNS)
    if unknown:
        raise ValueError(f"not columns of the target: {sorted(unknown)}")
    row = {
        "id": id_,
        "uri": f"https://example.org/votacoes/{id_}",
        "data": registered[:10],
        "data_hora_registro": registered,
        "id_orgao": "900001",
        "uri_orgao": "https://example.org/orgaos/900001",
        "sigla_orgao": "ORGTESTE",
        "id_evento": "800001",
        "uri_evento": "https://example.org/eventos/800001",
        "aprovacao": "1",
        "votos_sim": "10",
        "votos_nao": "2",
        "votos_outros": "1",
        "descricao": f"Votação sintética {id_}",
        "ultima_abertura_votacao_data_hora_registro": registered,
        "ultima_abertura_votacao_descricao": "Abertura sintética",
        "ultima_apresentacao_proposicao_data_hora_registro": registered,
        "ultima_apresentacao_proposicao_descricao": "Apresentação sintética",
        "ultima_apresentacao_proposicao_id_proposicao": "700001",
        "ultima_apresentacao_proposicao_uri_proposicao": "https://example.org/proposicoes/700001",
    }
    return row | values


def _record(fields: Sequence[str], end: str = "\n") -> str:
    """One CSV record: every field in `"`, a `"` inside a value doubled, fields separated by `;`."""
    return ";".join('"' + field.replace('"', '""') + '"' for field in fields) + end


def _fields(row: Mapping[str, str]) -> list[str]:
    """The fields of a record, in the order of the columns."""
    return [row[column] for column in TARGET_COLUMNS]


def _landing_bytes(
    rows: Sequence[Mapping[str, str]], *, header: Sequence[str] = TARGET_COLUMNS
) -> bytes:
    """A file in the landing format: UTF-8 without a BOM, the header, the records, `\\n` ends."""
    text = _record(header) + "".join(_record(_fields(row)) for row in rows)
    return text.encode("utf-8")


def _write_landing_file(
    landing: Path,
    name: str,
    rows: Sequence[Mapping[str, str]],
    *,
    header: Sequence[str] = TARGET_COLUMNS,
) -> None:
    """Writes a landing file of these rows."""
    (landing / name).write_bytes(_landing_bytes(rows, header=header))


def _write_broken_landing_file(
    landing: Path, name: str, row: Mapping[str, str], breakage: str
) -> None:
    """Writes a landing file of one row, broken in one of the ways of AT-008."""
    if breakage == "votos_sim_abc":
        _write_landing_file(landing, name, [{**row, "votos_sim": "abc"}])
    elif breakage == "twenty_one_fields":
        text = _record(TARGET_COLUMNS) + _record([*_fields(row), "um campo a mais"])
        (landing / name).write_bytes(text.encode("utf-8"))
    elif breakage == "camara_header":
        _write_landing_file(landing, name, [row], header=CAMARA_COLUMNS)
    else:
        raise AssertionError(f"unknown breakage {breakage!r}")


def _landing_name(registered: str) -> str:
    """The landing file of an hour: `2024-02-06T18:35:59` lands in `votacoes_2024020618.csv`."""
    return f"votacoes_{datetime.fromisoformat(registered).strftime('%Y%m%d%H')}.csv"


def _landing_files(rows: Sequence[Mapping[str, str]]) -> dict[str, bytes]:
    """The landing files these rows make: one per hour, each with its rows in their order."""
    hours: dict[str, list[Mapping[str, str]]] = {}
    for row in rows:
        hours.setdefault(_landing_name(row["data_hora_registro"]), []).append(row)
    return {name: _landing_bytes(hour) for name, hour in hours.items()}


def _contents(directory: Path) -> dict[str, bytes]:
    """Every entry of a directory, by name, with its bytes."""
    return {path.name: path.read_bytes() for path in directory.iterdir()}


def _write_yearly_file(
    sources: Path,
    year: int,
    rows: Sequence[Mapping[str, str]],
    *,
    header: Sequence[str] = CAMARA_COLUMNS,
) -> None:
    """Writes `votacoes-<year>.csv` in the Câmara's format.

    UTF-8 with a BOM, the Câmara's header, every field in `"`, fields separated by `;`, and
    records ended by `\\r\\n` and `\\n` in turn.
    """
    ends = ("\r\n", "\n")
    records = "".join(_record(_fields(row), ends[i % 2]) for i, row in enumerate(rows))
    text = "\ufeff" + _record(header, "\r\n") + records
    (sources / f"votacoes-{year}.csv").write_bytes(text.encode("utf-8"))


def _write_broken_sources(sources: Path, breakage: str) -> str:
    """Writes the yearly files of 2024 and 2025, broken in one of the ways of AT-019.

    Returns the regular expression of the message the script must log at `ERROR`.
    """
    header = CAMARA_COLUMNS
    rows_2024 = [_row("9001901-1", "2024-03-01T10:00:00"), _row("9001902-1", "2024-03-01T11:00:00")]
    rows_2025 = [_row("9001903-1", "2025-03-01T10:00:00")]
    if breakage == "unexpected_header":
        header = tuple("dataHora" if name == "dataHoraRegistro" else name for name in header)
        expected = re.escape("votacoes-2024.csv: unexpected header")
    elif breakage == "invalid_registration":
        rows_2024[1] = _row("9001902-1", "2024-02-30T10:00:00")
        expected = re.escape(
            "votacoes-2024.csv: invalid dataHoraRegistro '2024-02-30T10:00:00' for id '9001902-1'"
        )
    elif breakage == "repeated_id":
        rows_2025.append(_row("9001901-1", "2025-03-01T11:00:00"))
        expected = re.escape("id '9001901-1' appears more than once")
    elif breakage == "missing_year":
        expected = re.escape(f"cannot fetch {sources.as_uri()}/votacoes-2025.csv: ") + ".+"
    else:
        raise AssertionError(f"unknown breakage {breakage!r}")
    _write_yearly_file(sources, 2024, rows_2024, header=header)
    if breakage != "missing_year":
        _write_yearly_file(sources, 2025, rows_2025)
    return expected


def _typed(column: str, value: str) -> object:
    """The value the target holds for a field: its column's type, or `None` when it is empty."""
    if value == "":
        return None
    kind = TYPES[column]
    if kind == "date":
        return date.fromisoformat(value)
    if kind == "timestamp with time zone":
        return datetime.fromisoformat(value).replace(tzinfo=BRASILIA)
    if kind in {"bigint", "integer"}:
        return int(value)
    return value


def _stored(row: Mapping[str, str]) -> dict[str, object]:
    """The row of the target that a landed record becomes, with the file it lands in."""
    values: dict[str, object] = {column: _typed(column, row[column]) for column in TARGET_COLUMNS}
    return values | {"source_file": _landing_name(row["data_hora_registro"])}


# --- The job, PostgreSQL and the log --------------------------------------------------------


def _run_job(config: Path, connection: PostgresConnection) -> Process:
    """Runs the job as the criteria define it."""
    import main

    return main.run_job(config, connection=connection)


def _assert_run_fails(config: Path, connection: PostgresConnection) -> None:
    """Runs the job and checks that an exception propagates out of `run_job`."""
    # The spec names no type: the error is Spark's or PostgreSQL's, propagated unchanged.
    with pytest.raises(Exception) as failure:
        _run_job(config, connection)
    # A mistake of the code that runs the job is not the failure the criteria expect.
    assert not isinstance(failure.value, ImportError | NameError | AttributeError | TypeError), (
        repr(failure.value)
    )


def _prepare(landing: Path, sources: Path, start: str, end: str) -> int:
    """Runs the preparation script on the yearly files of `sources`, named as a `file:` URL."""
    import prepare_landing

    return prepare_landing.main(
        [
            "--landing",
            str(landing),
            "--start",
            start,
            "--end",
            end,
            "--base-url",
            sources.as_uri(),
        ]
    )


def _fetch(
    query: LiteralString | sql.Composed, params: Sequence[object] | None = None
) -> list[dict[str, Any]]:
    """Runs a query on the PostgreSQL of the environment, reached through libpq's variables."""
    with psycopg.connect(row_factory=dict_row) as database:
        return database.execute(query, params).fetchall()


def _select(schema: str, columns: Sequence[str]) -> list[dict[str, Any]]:
    """These columns of the rows of the target, ordered by `id`."""
    query = sql.SQL("SELECT {columns} FROM {table} ORDER BY id").format(
        columns=sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        table=sql.Identifier(schema, "votacoes"),
    )
    return _fetch(query)


def _snapshot(schema: str) -> list[dict[str, Any]]:
    """Every column of every row of the target, ordered by `id`."""
    return _select(schema, (*TARGET_COLUMNS, "source_file", "loaded_at"))


def _sources_of(schema: str) -> list[tuple[str, str]]:
    """The `id` and the `source_file` of every row of the target, ordered by `id`."""
    return [(row["id"], row["source_file"]) for row in _select(schema, ("id", "source_file"))]


def _schema_exists(schema: str) -> bool:
    found = _fetch("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", [schema])
    return bool(found)


def _definition(schema: str) -> dict[str, Any]:
    """The target's columns, its primary key, and the oid that names this very table."""
    columns = _fetch(
        "SELECT column_name, data_type, is_nullable, collation_name, column_default"
        " FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = 'votacoes'"
        " ORDER BY ordinal_position",
        [schema],
    )
    primary_key = _fetch(
        "SELECT kcu.column_name"
        " FROM information_schema.table_constraints AS tc"
        " JOIN information_schema.key_column_usage AS kcu"
        " ON kcu.constraint_schema = tc.constraint_schema"
        " AND kcu.constraint_name = tc.constraint_name"
        " WHERE tc.table_schema = %s AND tc.table_name = 'votacoes'"
        " AND tc.constraint_type = 'PRIMARY KEY'"
        " ORDER BY kcu.ordinal_position",
        [schema],
    )
    oid = _fetch(
        "SELECT c.oid FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace"
        " WHERE n.nspname = %s AND c.relname = 'votacoes'",
        [schema],
    )
    return {
        "columns": [tuple(column.values()) for column in columns],
        "primary_key": [column["column_name"] for column in primary_key],
        "oid": [row["oid"] for row in oid],
    }


def _staging_tables(schema: str) -> list[str]:
    """The staging tables of `PostgresWriter` left in a schema."""
    tables = _fetch("SELECT tablename FROM pg_tables WHERE schemaname = %s", [schema])
    return [row["tablename"] for row in tables if row["tablename"].startswith("ingestio_staging_")]


def _capture_info(caplog: pytest.LogCaptureFixture) -> None:
    """Makes `caplog` see the `INFO` records of the job and of ingestio's tree."""
    # py4j, Spark's bridge, logs at INFO from threads of its own: none of its records is the job's.
    # It comes first because set_level also sets the level of caplog's handler: INFO, last.
    caplog.set_level(logging.WARNING, logger="py4j")
    caplog.set_level(logging.INFO, logger="ingestio")
    caplog.set_level(logging.INFO)


def _logged(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    """The level and the message of every record captured."""
    return [(record.levelno, record.getMessage()) for record in caplog.records]


def _assert_load_step_cancelled(caplog: pytest.LogCaptureFixture) -> None:
    """Checks the tree of a run with nothing new: the load step cancelled, nothing appended."""
    messages = caplog.messages
    assert any(
        re.search(r"⊘ SkipEmptyBatch\.run cancelled the step after .+: no new row$", message)
        for message in messages
    ), messages
    assert any(re.search(r"└ load cancelled in .+$", message) for message in messages), messages
    assert not any("AppendOnly.run" in message for message in messages), messages


# --- Fixtures of this spec ------------------------------------------------------------------


@pytest.fixture
def landing(tmp_path: Path) -> Path:
    """The landing directory of the test, empty."""
    directory = tmp_path / "landing"
    directory.mkdir()
    return directory


@pytest.fixture
def sources(tmp_path: Path) -> Path:
    """The directory where the test writes the Câmara's yearly files, empty."""
    directory = tmp_path / "sources"
    directory.mkdir()
    return directory


@pytest.fixture
def job_config(tmp_path: Path, landing: Path, target_schema: str) -> Path:
    """The job's `config.json`, with the test's landing directory and a table of its own schema."""
    config = json.loads(JOB_CONFIG.read_text(encoding="utf-8"))
    config["source"]["path"] = str(landing)
    config["target"]["table_name"] = f"{target_schema}.votacoes"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


# --- The process ----------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at001_the_first_run_appends_every_landed_row_once_with_its_file_and_one_load_time(
    landing: Path, job_config: Path, target_schema: str, connection: PostgresConnection
) -> None:
    files = {
        "votacoes_2024020618.csv": [
            _row("9000101-1", "2024-02-06T18:05:00"),
            _row("9000102-1", "2024-02-06T18:40:10"),
        ],
        "votacoes_2024020619.csv": [_row("9000103-1", "2024-02-06T19:00:00")],
        "votacoes_2025010110.csv": [
            _row("9000104-1", "2025-01-01T10:15:00"),
            _row("9000105-1", "2025-01-01T10:30:00"),
            _row("9000106-1", "2025-01-01T10:59:59"),
        ],
    }
    for name, rows in files.items():
        _write_landing_file(landing, name, rows)
    assert not _schema_exists(target_schema)

    _run_job(job_config, connection)

    stored = _select(target_schema, ("id", "source_file", "loaded_at"))
    assert [(row["id"], row["source_file"]) for row in stored] == sorted(
        (row["id"], name) for name, rows in files.items() for row in rows
    )
    loaded_at = {row["loaded_at"] for row in stored}
    assert len(loaded_at) == 1
    assert None not in loaded_at


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at002_each_value_reaches_the_target_with_its_type_and_empty_fields_are_null(
    landing: Path, job_config: Path, target_schema: str, connection: PostgresConnection
) -> None:
    full = _row(
        "9000201-1",
        "2024-02-06T18:35:59",
        uri="https://example.org/votacoes/9000201-1",
        data="2024-02-06",
        id_orgao="4294967301",
        uri_orgao="https://example.org/orgaos/4294967301",
        sigla_orgao="ORG(TESTE)",
        id_evento="4294967302",
        uri_evento="https://example.org/eventos/4294967302",
        aprovacao="1",
        votos_sim="312",
        votos_nao="47",
        votos_outros="3",
        descricao="Aprovado o requerimento sintético.",
        ultima_abertura_votacao_data_hora_registro="2024-02-06T18:30:00",
        ultima_abertura_votacao_descricao="Abertura da votação sintética",
        ultima_apresentacao_proposicao_data_hora_registro="2024-02-05T10:15:00",
        ultima_apresentacao_proposicao_descricao="Apresentação da proposição sintética",
        ultima_apresentacao_proposicao_id_proposicao="4294967303",
        ultima_apresentacao_proposicao_uri_proposicao="https://example.org/proposicoes/4294967303",
    )
    sparse = dict.fromkeys(TARGET_COLUMNS, "") | {
        "id": "9000202-1",
        "data_hora_registro": "2024-02-06T18:50:00",
    }
    _write_landing_file(landing, "votacoes_2024020618.csv", [full, sparse])

    _run_job(job_config, connection)

    first, second = _select(target_schema, TARGET_COLUMNS)
    assert first == {
        "id": "9000201-1",
        "uri": "https://example.org/votacoes/9000201-1",
        "data": date(2024, 2, 6),
        "data_hora_registro": datetime(2024, 2, 6, 21, 35, 59, tzinfo=UTC),
        "id_orgao": 4294967301,
        "uri_orgao": "https://example.org/orgaos/4294967301",
        "sigla_orgao": "ORG(TESTE)",
        "id_evento": 4294967302,
        "uri_evento": "https://example.org/eventos/4294967302",
        "aprovacao": 1,
        "votos_sim": 312,
        "votos_nao": 47,
        "votos_outros": 3,
        "descricao": "Aprovado o requerimento sintético.",
        "ultima_abertura_votacao_data_hora_registro": datetime(2024, 2, 6, 21, 30, tzinfo=UTC),
        "ultima_abertura_votacao_descricao": "Abertura da votação sintética",
        "ultima_apresentacao_proposicao_data_hora_registro": datetime(
            2024, 2, 5, 13, 15, tzinfo=UTC
        ),
        "ultima_apresentacao_proposicao_descricao": "Apresentação da proposição sintética",
        "ultima_apresentacao_proposicao_id_proposicao": 4294967303,
        "ultima_apresentacao_proposicao_uri_proposicao": (
            "https://example.org/proposicoes/4294967303"
        ),
    }
    assert {column: type(value) for column, value in first.items()} == {
        column: PYTHON_TYPES[TYPES[column]] for column in TARGET_COLUMNS
    }
    assert second == dict.fromkeys(TARGET_COLUMNS) | {
        "id": "9000202-1",
        "data_hora_registro": datetime(2024, 2, 6, 21, 50, tzinfo=UTC),
    }


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at003_line_breaks_semicolons_and_quotes_inside_a_value_reach_the_target_unchanged(
    landing: Path, job_config: Path, target_schema: str, connection: PostgresConnection
) -> None:
    descriptions = (
        "Primeira linha\nsegunda linha",
        "Item um; item dois",
        'Aprovado o "requerimento" sintético',
        'Tudo junto:\n"aspas"; ponto e vírgula\ne outra linha',
    )
    rows = [
        _row(f"9000301-{number}", f"2024-02-06T18:{10 + number:02d}:00", descricao=description)
        for number, description in enumerate(descriptions, start=1)
    ]
    _write_landing_file(landing, "votacoes_2024020618.csv", rows)

    _run_job(job_config, connection)

    assert _select(target_schema, ("id", "descricao")) == [
        {"id": row["id"], "descricao": row["descricao"]} for row in rows
    ]


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at004_a_second_run_appends_only_the_new_files_and_keeps_the_earlier_rows(
    landing: Path, job_config: Path, target_schema: str, connection: PostgresConnection
) -> None:
    _write_landing_file(
        landing,
        "votacoes_2024020618.csv",
        [_row("9000401-1", "2024-02-06T18:01:00"), _row("9000402-1", "2024-02-06T18:02:00")],
    )
    _write_landing_file(
        landing, "votacoes_2024020619.csv", [_row("9000403-1", "2024-02-06T19:03:00")]
    )
    _run_job(job_config, connection)
    earlier = _snapshot(target_schema)
    _write_landing_file(
        landing, "votacoes_2024020620.csv", [_row("9000404-1", "2024-02-06T20:04:00")]
    )
    _write_landing_file(
        landing,
        "votacoes_2025010110.csv",
        [_row("9000405-1", "2025-01-01T10:05:00"), _row("9000406-1", "2025-01-01T10:06:00")],
    )

    _run_job(job_config, connection)

    stored = _snapshot(target_schema)
    earlier_ids = {row["id"] for row in earlier}
    assert [row for row in stored if row["id"] in earlier_ids] == earlier
    new = [row for row in stored if row["id"] not in earlier_ids]
    assert [(row["id"], row["source_file"]) for row in new] == [
        ("9000404-1", "votacoes_2024020620.csv"),
        ("9000405-1", "votacoes_2025010110.csv"),
        ("9000406-1", "votacoes_2025010110.csv"),
    ]
    assert min(row["loaded_at"] for row in new) > max(row["loaded_at"] for row in earlier)


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at005_a_run_on_an_empty_target_logs_that_every_file_is_read(
    landing: Path,
    job_config: Path,
    connection: PostgresConnection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _write_landing_file(
        landing, "votacoes_2024020618.csv", [_row("9000501-1", "2024-02-06T18:00:00")]
    )
    _capture_info(caplog)

    _run_job(job_config, connection)

    assert (logging.INFO, "watermark: none (every file is read)") in _logged(caplog)


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at005_a_run_logs_the_hour_of_the_newest_file_in_the_target_as_its_watermark(
    landing: Path,
    job_config: Path,
    connection: PostgresConnection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _write_landing_file(
        landing, "votacoes_2024020618.csv", [_row("9000502-1", "2024-02-06T18:00:00")]
    )
    _write_landing_file(
        landing, "votacoes_2024020619.csv", [_row("9000503-1", "2024-02-06T19:00:00")]
    )
    _run_job(job_config, connection)
    _capture_info(caplog)
    caplog.clear()

    _run_job(job_config, connection)

    assert (logging.INFO, "watermark: 2024020619") in _logged(caplog)


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at006_a_run_with_no_file_above_the_watermark_cancels_the_load_step(
    landing: Path,
    job_config: Path,
    target_schema: str,
    connection: PostgresConnection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _write_landing_file(
        landing, "votacoes_2024020618.csv", [_row("9000601-1", "2024-02-06T18:00:00")]
    )
    _run_job(job_config, connection)
    before = _snapshot(target_schema)
    _capture_info(caplog)
    caplog.clear()

    process = _run_job(job_config, connection)

    _assert_load_step_cancelled(caplog)
    assert process.cancelled is False
    assert _snapshot(target_schema) == before


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at006_a_run_on_an_empty_landing_directory_cancels_the_load_step(
    job_config: Path,
    target_schema: str,
    connection: PostgresConnection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _capture_info(caplog)

    process = _run_job(job_config, connection)

    _assert_load_step_cancelled(caplog)
    assert process.cancelled is False
    assert _snapshot(target_schema) == []


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at007_hidden_files_and_names_off_the_pattern_are_never_read(
    landing: Path, job_config: Path, target_schema: str, connection: PostgresConnection
) -> None:
    _write_landing_file(
        landing, "votacoes_2024020618.csv", [_row("9000701-1", "2024-02-06T18:00:00")]
    )
    _write_landing_file(
        landing, ".votacoes_2024020619.csv.tmp", [_row("9000702-1", "2024-02-06T19:00:00")]
    )
    _write_landing_file(
        landing, "votacoes_2024-02-06T20.csv", [_row("9000703-1", "2024-02-06T20:00:00")]
    )

    _run_job(job_config, connection)

    assert _sources_of(target_schema) == [("9000701-1", "votacoes_2024020618.csv")]


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
@pytest.mark.parametrize("breakage", LANDING_BREAKAGES)
def test_at008_a_broken_file_fails_the_run_and_leaves_the_target_without_rows(
    breakage: str,
    landing: Path,
    job_config: Path,
    target_schema: str,
    connection: PostgresConnection,
) -> None:
    _write_landing_file(
        landing, "votacoes_2024020618.csv", [_row("9000801-1", "2024-02-06T18:00:00")]
    )
    _write_broken_landing_file(
        landing, "votacoes_2024020619.csv", _row("9000802-1", "2024-02-06T19:00:00"), breakage
    )

    _assert_run_fails(job_config, connection)

    assert _snapshot(target_schema) == []
    assert _staging_tables(target_schema) == []


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
@pytest.mark.parametrize("breakage", LANDING_BREAKAGES)
def test_at009_once_the_broken_file_is_corrected_the_next_run_loads_both_files(
    breakage: str,
    landing: Path,
    job_config: Path,
    target_schema: str,
    connection: PostgresConnection,
) -> None:
    good = _row("9000901-1", "2024-02-06T18:00:00")
    corrected = _row("9000902-1", "2024-02-06T19:00:00")
    _write_landing_file(landing, "votacoes_2024020618.csv", [good])
    _write_broken_landing_file(landing, "votacoes_2024020619.csv", corrected, breakage)
    _assert_run_fails(job_config, connection)
    _write_landing_file(landing, "votacoes_2024020619.csv", [corrected])

    _run_job(job_config, connection)

    assert _sources_of(target_schema) == [
        ("9000901-1", "votacoes_2024020618.csv"),
        ("9000902-1", "votacoes_2024020619.csv"),
    ]


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at010_the_first_run_creates_the_schema_and_the_table_of_the_spec(
    job_config: Path, target_schema: str, connection: PostgresConnection
) -> None:
    assert not _schema_exists(target_schema)

    _run_job(job_config, connection)

    assert _schema_exists(target_schema)
    definition = _definition(target_schema)
    assert definition["columns"] == TARGET_DEFINITION
    assert definition["primary_key"] == ["id"]


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at010_a_run_with_nothing_new_leaves_the_existing_table_and_its_rows_as_they_are(
    landing: Path, job_config: Path, target_schema: str, connection: PostgresConnection
) -> None:
    _write_landing_file(
        landing,
        "votacoes_2024020618.csv",
        [_row("9001001-1", "2024-02-06T18:00:00"), _row("9001002-1", "2024-02-06T18:30:00")],
    )
    _run_job(job_config, connection)
    definition = _definition(target_schema)
    rows = _snapshot(target_schema)
    assert len(rows) == 2

    _run_job(job_config, connection)

    assert _definition(target_schema) == definition
    assert _snapshot(target_schema) == rows


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at011_a_row_whose_id_is_already_in_the_target_fails_the_run_and_changes_nothing(
    landing: Path, job_config: Path, target_schema: str, connection: PostgresConnection
) -> None:
    _write_landing_file(
        landing, "votacoes_2024020618.csv", [_row("9001101-1", "2024-02-06T18:00:00")]
    )
    _run_job(job_config, connection)
    before = _snapshot(target_schema)
    _write_landing_file(
        landing,
        "votacoes_2024020619.csv",
        [
            _row("9001102-1", "2024-02-06T19:00:00"),
            _row("9001101-1", "2024-02-06T19:30:00", descricao="Outra votação com o mesmo id"),
        ],
    )

    _assert_run_fails(job_config, connection)

    assert _snapshot(target_schema) == before
    assert _staging_tables(target_schema) == []


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at012_the_landing_prepared_from_the_yearly_files_loads_every_row_of_the_window_once(
    landing: Path,
    sources: Path,
    job_config: Path,
    target_schema: str,
    connection: PostgresConnection,
) -> None:
    in_2024 = [
        _row("9001201-1", "2024-01-01T00:00:00", descricao="A primeira hora da janela"),
        _row("9001202-1", "2023-12-31T23:59:59", descricao="Registrada antes da janela"),
        _row(
            "9001203-1",
            "2024-07-15T14:20:00",
            descricao='Linha um\nlinha dois; com "aspas"',
            uri_evento="",
            votos_outros="",
        ),
        _row("9001204-1", "2024-07-15T14:05:00", ultima_abertura_votacao_descricao=""),
        _row("9001205-1", "", data="2024-03-01", descricao="Sem dataHoraRegistro"),
    ]
    in_2025 = [
        _row("9001206-1", "2025-12-31T23:59:59", descricao="A última hora da janela"),
        _row("9001207-1", "2026-01-01T00:00:00", descricao="Registrada depois da janela"),
        _row("9001208-1", "2024-12-31T23:10:00", descricao="De 2024, no arquivo de 2025"),
    ]
    _write_yearly_file(sources, 2024, in_2024)
    _write_yearly_file(sources, 2025, in_2025)
    assert _prepare(landing, sources, "2024-01-01T00", "2025-12-31T23") == 0

    _run_job(job_config, connection)

    in_the_window = {"9001201-1", "9001203-1", "9001204-1", "9001206-1", "9001208-1"}
    assert _select(target_schema, (*TARGET_COLUMNS, "source_file")) == [
        _stored(row)
        for row in sorted(in_2024 + in_2025, key=lambda row: row["id"])
        if row["id"] in in_the_window
    ]


# --- The connection -------------------------------------------------------------------------


ENVIRONMENT = {
    "PGHOST": "db.example.org",
    "PGPORT": "6543",
    "PGUSER": "loader",
    "PGPASSWORD": "throwaway",
    "PGDATABASE": "warehouse",
}


def test_at013_the_connection_takes_every_setting_from_the_environment() -> None:
    import main

    built = main.connection_from_env(ENVIRONMENT)

    assert (built.host, built.port, built.user, built.password, built.database) == (
        "db.example.org",
        6543,
        "loader",
        "throwaway",
        "warehouse",
    )
    assert type(built.port) is int


def test_at013_the_port_is_5432_when_pgport_is_unset() -> None:
    import main

    environment = {name: value for name, value in ENVIRONMENT.items() if name != "PGPORT"}

    assert main.connection_from_env(environment).port == 5432


@pytest.mark.parametrize("value", [None, ""], ids=["missing", "empty"])
@pytest.mark.parametrize("name", ["PGHOST", "PGUSER", "PGPASSWORD", "PGDATABASE"])
def test_at014_a_missing_or_empty_required_variable_is_an_error(
    name: str, value: str | None
) -> None:
    import main

    environment = {key: setting for key, setting in ENVIRONMENT.items() if key != name}
    if value is not None:
        environment[name] = value

    with pytest.raises(ValueError, match=rf"^{name} is not set$"):
        main.connection_from_env(environment)


def test_at014_a_pgport_that_is_not_an_integer_is_an_error() -> None:
    import main

    with pytest.raises(ValueError, match=r"^PGPORT must be an integer$"):
        main.connection_from_env(ENVIRONMENT | {"PGPORT": "abc"})


# --- The configuration ----------------------------------------------------------------------


def test_at015_the_job_configuration_reads_the_landing_directory_into_camara_votacoes() -> None:
    config = json.loads(JOB_CONFIG.read_text(encoding="utf-8"))

    assert set(config) == {"source", "target"}
    source = config["source"]
    assert {key: value for key, value in source.items() if key != "watermark_pattern"} == {
        "system": "disk",
        "path": "/data/landing/votacoes",
        "format": "csv",
        "schema": SCHEMA,
        "options": OPTIONS,
    }
    pattern = source["watermark_pattern"]
    match = re.fullmatch(pattern, "votacoes_2024020618.csv")
    assert match is not None
    assert match.groups() == ("2024020618",)
    assert re.fullmatch(pattern, ".votacoes_2024020618.csv.tmp") is None
    assert re.fullmatch(pattern, "votacoes_2024-02-06T18.csv") is None
    assert config["target"] == {"system": "postgres", "table_name": "camara.votacoes"}


def test_at015_the_library_callbacks_are_built_from_the_job_configuration() -> None:
    connection = PostgresConnection("localhost", "postgres", "postgres", "postgres")

    ReadSource(JOB_CONFIG)
    ReadSource(JOB_CONFIG, watermark_value="2025123123")
    AppendOnly(JOB_CONFIG, connection=connection)


# --- The preparation script -----------------------------------------------------------------


def test_at016_the_script_writes_one_landing_file_per_hour_with_the_rows_of_that_hour(
    tmp_path: Path, sources: Path
) -> None:
    landing = tmp_path / "landing"  # not created: the script creates it when it is missing
    in_2024 = [
        _row("9001601-1", "2024-02-06T18:40:00", descricao='Com "aspas"; e ponto e vírgula'),
        _row("9001602-1", "2024-03-10T09:00:00", descricao="Linha um\nlinha dois"),
        _row("9001603-1", "2024-02-06T18:05:00", votos_outros="", uri_evento=""),
    ]
    in_2025 = [
        _row("9001604-1", "2025-06-01T12:30:00", descricao="Três\nlinhas\nde texto"),
        _row("9001605-1", "2025-11-30T23:59:59"),
    ]
    _write_yearly_file(sources, 2024, in_2024)
    _write_yearly_file(sources, 2025, in_2025)

    assert _prepare(landing, sources, "2024-01-01T00", "2025-12-31T23") == 0

    expected = _landing_files(in_2024 + in_2025)
    assert sorted(expected) == [
        "votacoes_2024020618.csv",
        "votacoes_2024031009.csv",
        "votacoes_2025060112.csv",
        "votacoes_2025113023.csv",
    ]
    assert _contents(landing) == expected


def test_at017_rows_without_a_registration_time_or_outside_the_window_are_left_out_and_counted(
    landing: Path, sources: Path, caplog: pytest.LogCaptureFixture
) -> None:
    in_2024 = [
        _row("9001701-1", "2024-05-05T05:05:05"),
        _row("9001702-1", "", data="2024-05-06"),
        _row("9001703-1", "2024-05-05T05:55:00"),
    ]
    in_2025 = [
        _row("9001704-1", "2025-12-31T23:00:00"),
        _row("9001705-1", "2026-01-01T00:00:01"),
    ]
    _write_yearly_file(sources, 2024, in_2024)
    _write_yearly_file(sources, 2025, in_2025)
    _capture_info(caplog)

    assert _prepare(landing, sources, "2024-01-01T00", "2025-12-31T23") == 0

    assert _contents(landing) == _landing_files([in_2024[0], in_2024[2], in_2025[0]])
    assert (
        logging.INFO,
        "files written: 2, files skipped: 0, rows written: 3, "
        "rows without dataHoraRegistro: 1, rows outside the window: 1",
    ) in _logged(caplog)


def test_at018_a_landed_file_is_never_rewritten_and_leftover_temporary_files_are_removed(
    landing: Path, sources: Path, caplog: pytest.LogCaptureFixture
) -> None:
    landed = landing / "votacoes_2024020619.csv"
    landed.write_bytes(b"what an earlier run wrote, not the source's rows\n")
    landed_at = 1_700_000_000_123_000_000
    os.utime(landed, ns=(landed_at, landed_at))
    (landing / ".votacoes_2024020618.csv.tmp").write_bytes(b'"id";"uri"\n"9001801-1";"htt')
    rows = [
        _row("9001801-1", "2024-02-06T18:10:00"),
        _row("9001802-1", "2024-02-06T19:10:00"),
        _row("9001803-1", "2024-02-06T20:10:00"),
        _row("9001804-1", "2024-02-06T21:10:00"),
    ]
    _write_yearly_file(sources, 2024, rows)
    _capture_info(caplog)

    # The window leaves out the hour of the temporary file, so that no file written by this run
    # replaces it: only its removal makes it go away.
    assert _prepare(landing, sources, "2024-02-06T19", "2024-02-06T21") == 0

    assert landed.read_bytes() == b"what an earlier run wrote, not the source's rows\n"
    assert landed.stat().st_mtime_ns == landed_at
    assert _contents(landing) == _landing_files(rows[2:]) | {landed.name: landed.read_bytes()}
    assert not [name for name in _contents(landing) if name.startswith(".")]
    last = caplog.records[-1]
    assert last.levelno == logging.INFO
    assert re.fullmatch(
        r"files written: 2, files skipped: 1, rows written: \d+, "
        r"rows without dataHoraRegistro: 0, rows outside the window: 1",
        last.getMessage(),
    ), last.getMessage()


@pytest.mark.parametrize("breakage", SOURCE_BREAKAGES)
def test_at019_a_broken_source_fails_the_script_and_leaves_the_landing_as_it_was(
    breakage: str, landing: Path, sources: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (landing / "votacoes_2024010100.csv").write_bytes(b"landed by an earlier run\n")
    (landing / ".votacoes_2024010101.csv.tmp").write_bytes(b'"id";"uri"\n"90019')
    before = _contents(landing)
    expected = _write_broken_sources(sources, breakage)
    _capture_info(caplog)

    assert _prepare(landing, sources, "2024-01-01T00", "2025-12-31T23") == 1

    errors = [message for level, message in _logged(caplog) if level == logging.ERROR]
    assert any(re.fullmatch(expected, message, re.DOTALL) for message in errors), errors
    assert _contents(landing) == before


def test_at020_a_later_window_is_written_from_the_file_of_its_own_year_alone(
    landing: Path, sources: Path
) -> None:
    in_2026 = [
        _row("9002001-1", "2025-12-31T23:59:00", descricao="Registrada antes da janela"),
        _row("9002002-1", "2026-01-01T00:00:00"),
        _row("9002003-1", "2026-01-15T10:30:00"),
        _row("9002004-1", "2026-01-15T10:45:00"),
        _row("9002005-1", "2026-01-31T23:59:59"),
        _row("9002006-1", "2026-02-01T00:00:00", descricao="Registrada depois da janela"),
    ]
    _write_yearly_file(sources, 2026, in_2026)

    assert _prepare(landing, sources, "2026-01-01T00", "2026-01-31T23") == 0

    assert _contents(landing) == _landing_files(in_2026[1:5])


# --- The environment and the README ---------------------------------------------------------


def _health_command(test: Sequence[str]) -> str:
    """The command of a health check, whether compose holds it as `CMD-SHELL` or as `CMD`."""
    kind, *command = test
    return command[0] if kind == "CMD-SHELL" else " ".join(command)


@pytest.mark.integration
def test_at021_the_compose_file_describes_the_postgres_and_the_spark_services() -> None:
    result = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    services = json.loads(result.stdout)["services"]
    assert set(services) == {"postgres", "spark"}

    postgres = services["postgres"]
    assert postgres["image"] == "postgres:17"
    assert postgres["environment"]["POSTGRES_PASSWORD"] == "postgres"
    assert [(port["target"], str(port["published"])) for port in postgres["ports"]] == [
        (5432, "5432")
    ]
    health = postgres["healthcheck"]
    assert _health_command(health["test"]) == "pg_isready -U postgres"
    assert (health["interval"], health["timeout"], health["retries"]) == ("5s", "5s", 10)

    spark = services["spark"]
    assert Path(spark["build"]["context"]).resolve() == ROOT
    assert [
        (volume["type"], Path(volume["source"]).resolve(), volume["target"])
        for volume in spark["volumes"]
        if volume["target"] == "/data"
    ] == [("bind", ROOT / "data", "/data")]
    assert spark["depends_on"]["postgres"]["condition"] == "service_healthy"
    variables = ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE")
    assert {name: spark["environment"].get(name) for name in variables} == {
        "PGHOST": "postgres",
        "PGPORT": "5432",
        "PGUSER": "postgres",
        "PGPASSWORD": "postgres",
        "PGDATABASE": "postgres",
    }


def test_at022_the_readme_holds_the_commands_of_the_environment_and_the_job_in_order() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    position = 0
    for command in README_COMMANDS:
        found = readme.find(command, position)
        assert found >= 0, f"README.md lacks, after the commands before it: {command}"
        position = found + len(command)
