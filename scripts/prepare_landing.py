"""Lays out the landing directory: one CSV file per hour of the Câmara's Votações dataset.

It is the producer of the landing, a prerequisite of the ingestio process and not a part of it.
It downloads the yearly files of the years a window of hours touches, checks them, splits their
rows by the hour of their `dataHoraRegistro` and writes one file per hour, atomically and in
ascending order, never rewriting a file that is already there. It uses only the standard library.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import sys
import urllib.request
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://dadosabertos.camara.leg.br/arquivos/votacoes/csv"

# The Câmara's header, and the names it becomes in the landing and in the target.
CAMARA_COLUMNS = (
    "id",
    "uri",
    "data",
    "dataHoraRegistro",
    "idOrgao",
    "uriOrgao",
    "siglaOrgao",
    "idEvento",
    "uriEvento",
    "aprovacao",
    "votosSim",
    "votosNao",
    "votosOutros",
    "descricao",
    "ultimaAberturaVotacao_dataHoraRegistro",
    "ultimaAberturaVotacao_descricao",
    "ultimaApresentacaoProposicao_dataHoraRegistro",
    "ultimaApresentacaoProposicao_descricao",
    "ultimaApresentacaoProposicao_idProposicao",
    "ultimaApresentacaoProposicao_uriProposicao",
)
LANDING_COLUMNS = (
    "id",
    "uri",
    "data",
    "data_hora_registro",
    "id_orgao",
    "uri_orgao",
    "sigla_orgao",
    "id_evento",
    "uri_evento",
    "aprovacao",
    "votos_sim",
    "votos_nao",
    "votos_outros",
    "descricao",
    "ultima_abertura_votacao_data_hora_registro",
    "ultima_abertura_votacao_descricao",
    "ultima_apresentacao_proposicao_data_hora_registro",
    "ultima_apresentacao_proposicao_descricao",
    "ultima_apresentacao_proposicao_id_proposicao",
    "ultima_apresentacao_proposicao_uri_proposicao",
)
_REGISTERED = 3  # the index of dataHoraRegistro
_HOUR_FORMAT = "%Y-%m-%dT%H"
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
_TEMPORARY = re.compile(r"\.votacoes_\d{10}\.csv\.tmp")


class SourceError(Exception):
    """A source cannot be fetched or breaks the format the script expects."""


def _hour(value: str) -> datetime:
    """Parses `YYYY-MM-DDTHH` for the arguments: Brasília time, with no offset."""
    try:
        return datetime.strptime(value, _HOUR_FORMAT)  # noqa: DTZ007
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not YYYY-MM-DDTHH") from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="prepare_landing.py",
        description="Lays out the landing directory of the Câmara's Votações, one file per hour.",
    )
    parser.add_argument("--landing", required=True, type=Path, help="the landing directory")
    parser.add_argument("--start", required=True, type=_hour, help="first hour, YYYY-MM-DDTHH")
    parser.add_argument("--end", required=True, type=_hour, help="last hour, YYYY-MM-DDTHH")
    parser.add_argument(
        "--base-url", default=DEFAULT_BASE_URL, help="where the votacoes-<year>.csv files are"
    )
    return parser


def _fetch(url: str) -> str:
    """The text of a yearly file: UTF-8, its BOM removed."""
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            data: bytes = response.read()
        return data.decode("utf-8-sig")
    except (OSError, ValueError) as error:
        reason = getattr(error, "reason", error)
        raise SourceError(f"cannot fetch {url}: {reason}") from error


def _valid_timestamp(value: str) -> bool:
    if not _TIMESTAMP.fullmatch(value):
        return False
    try:
        datetime.strptime(value, _TIMESTAMP_FORMAT)  # noqa: DTZ007
    except ValueError:
        return False
    return True


def _parse(name: str, text: str) -> list[list[str]]:
    """The rows of a yearly file, checked: its header, its field counts and its times."""
    # newline="" keeps the line breaks inside values as they are.
    records = list(csv.reader(io.StringIO(text, newline=""), delimiter=";", quotechar='"'))
    if not records or tuple(records[0]) != CAMARA_COLUMNS:
        raise SourceError(f"{name}: unexpected header")
    rows = records[1:]
    for number, row in enumerate(rows, start=2):
        if len(row) != len(CAMARA_COLUMNS):
            raise SourceError(f"{name}: record {number} has {len(row)} fields")
        registered = row[_REGISTERED]
        if registered and not _valid_timestamp(registered):
            raise SourceError(f"{name}: invalid dataHoraRegistro '{registered}' for id '{row[0]}'")
    return rows


def _record(fields: Sequence[str]) -> str:
    """One record: every field in `"`, a `"` inside a value doubled, fields joined by `;`."""
    return ";".join('"' + field.replace('"', '""') + '"' for field in fields) + "\n"


def _write(landing: Path, name: str, rows: Sequence[Sequence[str]]) -> None:
    """Writes a file under a hidden temporary name and renames it when it is complete."""
    temporary = landing / f".{name}.tmp"
    text = _record(LANDING_COLUMNS) + "".join(_record(row) for row in rows)
    temporary.write_bytes(text.encode("utf-8"))
    temporary.replace(landing / name)


def prepare(landing: Path, start: datetime, end: datetime, base_url: str) -> None:
    """Fetches, checks and writes; logs the counts of the run.

    Args:
        landing: the landing directory, created when missing.
        start: the first hour of the window.
        end: the last hour of the window, included.
        base_url: where the yearly files are.

    Raises:
        SourceError: a source cannot be fetched or breaks the format.
    """
    base = base_url.rstrip("/")
    fetched = [
        (f"votacoes-{year}.csv", _fetch(f"{base}/votacoes-{year}.csv"))
        for year in range(start.year, end.year + 1)
    ]
    parsed = [_parse(name, text) for name, text in fetched]

    seen: set[str] = set()
    for rows in parsed:
        for row in rows:
            if row[0] in seen:
                raise SourceError(f"id '{row[0]}' appears more than once")
            seen.add(row[0])

    hours: dict[str, list[list[str]]] = {}
    without = outside = 0
    for rows in parsed:
        for row in rows:
            if not row[_REGISTERED]:
                without += 1
                continue
            registered = datetime.strptime(row[_REGISTERED][:13], _HOUR_FORMAT)  # noqa: DTZ007
            if not start <= registered <= end:
                outside += 1
                continue
            hours.setdefault(registered.strftime("%Y%m%d%H"), []).append(row)

    # Nothing is touched before the sources have been checked.
    landing.mkdir(parents=True, exist_ok=True)
    for leftover in landing.iterdir():
        if _TEMPORARY.fullmatch(leftover.name):
            leftover.unlink()

    written = skipped = rows_written = 0
    for hour in sorted(hours):
        name = f"votacoes_{hour}.csv"
        if (landing / name).exists():
            skipped += 1
            continue
        _write(landing, name, hours[hour])
        written += 1
        rows_written += len(hours[hour])
    logger.info(
        "files written: %d, files skipped: %d, rows written: %d, "
        "rows without dataHoraRegistro: %d, rows outside the window: %d",
        written,
        skipped,
        rows_written,
        without,
        outside,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Runs the script and returns its exit status: 0, 1 for a source error, 2 for usage.

    Args:
        argv: the arguments; `sys.argv[1:]` when omitted.
    """
    try:
        arguments = _parser().parse_args(argv)
    except SystemExit as usage:
        return 0 if usage.code in (0, None) else 2
    if arguments.start > arguments.end:
        logger.error("--start must not be after --end")
        return 2
    try:
        prepare(arguments.landing, arguments.start, arguments.end, arguments.base_url)
    except SourceError as error:
        logger.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(main())
