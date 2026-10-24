"""The job's own callbacks: the stages of the load step that the library does not ship."""

from __future__ import annotations

from ingestio import Callback, CancelStepException


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
        # Imported here so that importing the module needs no Spark, as ingestio's own modules do.
        from pyspark.sql import functions

        records = self.process.state[self.key]
        self.process.state[self.key] = records.withColumn(
            self.column, functions.col("_metadata.file_name")
        )
