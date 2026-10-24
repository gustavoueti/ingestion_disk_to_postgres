"""The job's own SQL: the DDL of the target and the query of the watermark.

The library leaves both to the job (the writer creates nothing, and the reader keeps no
watermark). They use psycopg, which is not ingestio.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

import psycopg
from ingestio.connector.all import PostgresConnection
from psycopg import sql

logger = logging.getLogger(__name__)

# The columns of the landing files and of the target, in the order of the schema of config.json.
_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
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
)
"""


def read_config(config: str | os.PathLike[str]) -> dict[str, dict[str, object]]:
    """Reads the job's configuration file, which the job never writes."""
    loaded: dict[str, dict[str, object]] = json.loads(Path(config).read_text(encoding="utf-8"))
    return loaded


def _open(connection: PostgresConnection) -> psycopg.Connection:
    return psycopg.connect(
        host=connection.host,
        port=connection.port,
        dbname=connection.database,
        user=connection.user,
        password=connection.password,
    )


def _identifier(table_name: str) -> sql.Identifier:
    return sql.Identifier(*table_name.split("."))


def apply_ddl(connection: PostgresConnection, table_name: str) -> None:
    """Creates the schema and the table when they are missing, in one transaction.

    An existing table and its rows are left as they are.

    Args:
        connection: where to reach the database.
        table_name: `table` or `schema.table`, as the `target` section of the configuration.
    """
    with _open(connection) as database:
        if "." in table_name:
            schema = table_name.split(".")[0]
            database.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
        database.execute(sql.SQL(_DDL).format(table=_identifier(table_name)))


def read_watermark(connection: PostgresConnection, table_name: str, pattern: str) -> str | None:
    """The hour of the newest file in the target, or `None` when the target has no row.

    Args:
        connection: where to reach the database.
        table_name: the target, as in the configuration.
        pattern: the `watermark_pattern` of the source, with one group.

    Raises:
        ValueError: a stored file name does not match the pattern.
    """
    with _open(connection) as database:
        row = database.execute(
            sql.SQL("SELECT max(source_file) FROM {}").format(_identifier(table_name))
        ).fetchone()
    newest = row[0] if row else None
    if newest is None:
        logger.info("watermark: none (every file is read)")
        return None
    match = re.fullmatch(pattern, newest)
    if match is None:
        raise ValueError(f"source_file '{newest}' does not match the watermark pattern")
    watermark = match.group(1)
    logger.info("watermark: %s", watermark)
    return watermark
