"""Fixtures shared by the job's tests: the Spark session of the run and the PostgreSQL to load.

The integration tests reach PostgreSQL through libpq's variables, `PGHOST`, `PGPORT`, `PGUSER`,
`PGPASSWORD` and `PGDATABASE`: those of the `postgres` service of `docker compose` locally, and
those of `.github/workflows/ci.yml`, for its service container, in CI. They fail, rather than
skip, when a variable is missing or the server is unreachable.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import psycopg
import pytest
from ingestio.connector.all import PostgresConnection
from psycopg import sql
from pyspark.sql import SparkSession

_REQUIRED_VARIABLES = ("PGHOST", "PGUSER", "PGPASSWORD", "PGDATABASE")


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """The one Spark session of the run: the job's own, in local mode, with the pgJDBC driver.

    It must be the first session of the process, so that the driver is on its classpath.
    """
    # Imported here, not at the top, so that the tests that need no Spark do not need the job.
    import main

    session = main.build_spark_session(master="local[2]")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def connection() -> PostgresConnection:
    """The connection to the PostgreSQL of the environment, checked to be reachable."""
    missing = [name for name in _REQUIRED_VARIABLES if not os.environ.get(name)]
    if missing:
        pytest.fail(
            f"{', '.join(missing)} not set: the integration tests need the PostgreSQL of "
            "docker compose or of CI, reached through libpq's variables",
            pytrace=False,
        )
    # libpq reads the same variables. An unreachable server fails here, and every test with it.
    with psycopg.connect() as probe:
        probe.execute("SELECT 1")
    return PostgresConnection(
        os.environ["PGHOST"],
        os.environ["PGDATABASE"],
        os.environ["PGUSER"],
        os.environ["PGPASSWORD"],
        port=int(os.environ.get("PGPORT") or "5432"),
    )


@pytest.fixture
def target_schema(connection: PostgresConnection) -> Iterator[str]:
    """The name of a schema of the test's own, which does not exist yet.

    The job creates it; the fixture drops it after the test, with everything in it.
    """
    name = f"test_{uuid.uuid4().hex[:12]}"
    yield name
    with psycopg.connect(autocommit=True) as admin:
        admin.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)))
