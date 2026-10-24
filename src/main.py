"""The entry point of the job: the Câmara's hourly votings, from the landing to PostgreSQL."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from pathlib import Path

from ingestio import (
    AppendOnly,
    Callback,
    CancelStepException,
    Process,
    ReadSource,
    Step,
    log_to_console,
)
from ingestio.connector.all import PostgresConnection
from pyspark.sql import SparkSession, functions

import warehouse

# One place for the version of the driver: the job and its tests share it.
PGJDBC_VERSION = "42.7.14"

_CONFIG = Path(__file__).with_name("config.json")


class SkipEmptyBatch(Callback):
    """Ends the load step when the batch has no row, so that nothing after it in the step runs.

    A run with nothing new is a normal run. The check comes before `StampSourceFile` because an
    empty selection has no `_metadata` column.

    Args:
        key: the key of the state that holds the DataFrame.
    """

    def __init__(self, *, key: str = "records") -> None:
        self.key = key

    def run(self) -> None:
        if self.process.state[self.key].isEmpty():  # a Spark action: it reads the files
            raise CancelStepException("no new row")


class StampSourceFile(Callback):
    """Adds the column `source_file`: the name of the file each row came from.

    The column is the job's watermark, kept in the target's own rows, so it commits with them.

    Args:
        key: the key of the state that holds the DataFrame.
        column: the name of the column to add.
    """

    def __init__(self, *, key: str = "records", column: str = "source_file") -> None:
        self.key = key
        self.column = column

    def run(self) -> None:
        records = self.process.state[self.key]
        self.process.state[self.key] = records.withColumn(
            self.column, functions.col("_metadata.file_name")
        )


def build_spark_session(*, master: str = "local[*]") -> SparkSession:
    """Starts the Spark session of the job, in local mode, with the pgJDBC driver.

    Args:
        master: Spark's master.
    """
    return (
        SparkSession.builder.master(master)
        .appName("camara_hourly_ingestion")
        .config("spark.jars.packages", f"org.postgresql:postgresql:{PGJDBC_VERSION}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def connection_from_env(environ: Mapping[str, str]) -> PostgresConnection:
    """Builds the PostgreSQL connection from libpq's variables.

    Args:
        environ: the environment, such as `os.environ`.

    Raises:
        ValueError: a required variable is missing or empty, or `PGPORT` is not an integer.
    """
    values: dict[str, str] = {}
    for name in ("PGHOST", "PGUSER", "PGPASSWORD", "PGDATABASE"):
        value = environ.get(name)
        if not value:
            raise ValueError(f"{name} is not set")
        values[name] = value
    port = environ.get("PGPORT") or "5432"
    try:
        number = int(port)
    except ValueError:
        raise ValueError("PGPORT must be an integer") from None
    return PostgresConnection(
        values["PGHOST"],
        values["PGDATABASE"],
        values["PGUSER"],
        values["PGPASSWORD"],
        port=number,
    )


def build_process(
    config: str | os.PathLike[str], *, connection: PostgresConnection, watermark: str | None
) -> Process:
    """The job's process, for a configuration file and the arguments it cannot hold.

    Args:
        config: the path of the configuration file.
        connection: the connection of the target.
        watermark: the hour of the newest file already loaded, or `None` to read every file.
    """
    return Process(
        [
            Step("extract", [ReadSource(config, watermark_value=watermark)]),
            Step(
                "load",
                [
                    SkipEmptyBatch(),
                    StampSourceFile(),
                    AppendOnly(config, connection=connection),
                ],
            ),
        ]
    )


def run_job(config: str | os.PathLike[str], *, connection: PostgresConnection) -> Process:
    """Applies the DDL, reads the watermark, and runs the process on the active Spark session.

    Args:
        config: the path of the configuration file.
        connection: the connection of the target.
    """
    settings = warehouse.read_config(config)
    table_name = str(settings["target"]["table_name"])
    pattern = str(settings["source"]["watermark_pattern"])
    warehouse.apply_ddl(connection, table_name)
    watermark = warehouse.read_watermark(connection, table_name, pattern)
    return build_process(config, connection=connection, watermark=watermark).run()


def main() -> None:
    """Runs the job once, as the `spark` service does."""
    handler = log_to_console()
    # The job's own lines go to the same console as the tree, so each appears once.
    own = logging.getLogger(warehouse.__name__)
    own.setLevel(logging.INFO)
    own.addHandler(handler)
    connection = connection_from_env(os.environ)
    session = build_spark_session()
    try:
        run_job(_CONFIG, connection=connection)
    finally:
        session.stop()


if __name__ == "__main__":
    main()
