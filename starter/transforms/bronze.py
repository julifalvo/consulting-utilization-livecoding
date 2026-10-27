"""Stage 1 - Bronze (suggested time: 15 min).

Run the tests for this stage with:  uv run pytest --impl starter -k bronze
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from utilization.schemas import CORRUPT_RECORD, TIMESHEET_RAW  # noqa: F401  (you will need them)


def files_to_ingest(available: list[str], already_ingested: set[str]) -> list[str]:
    """Return the files (full paths) whose *file name* is not in ``already_ingested``, sorted.

    Re-running the job must not ingest the same export twice.
    """
    raise NotImplementedError("TODO: stage 1")


def read_timesheets(spark: SparkSession, files: list[str]) -> DataFrame:
    """Read the NDJSON exports.

    Requirements:
      * use the explicit schema ``TIMESHEET_RAW`` (no inference)
      * a malformed line must not fail the read: keep it in ``CORRUPT_RECORD``
      * add ``_source_file`` (file name only, e.g. ``timesheets_2026-09-07.json``)
      * add ``_ingested_at`` (current timestamp)

    Hint: file-based sources expose a hidden ``_metadata`` struct column.
    """
    raise NotImplementedError("TODO: stage 1")


def read_reference(spark: SparkSession, path: str) -> DataFrame:
    """Read a reference CSV (header row, every column as string) and add ``_ingested_at``."""
    raise NotImplementedError("TODO: stage 1")
