"""Stage 1 - Bronze: land raw data as-is, plus lineage. No business logic here."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utilization.schemas import CORRUPT_RECORD, TIMESHEET_RAW


def files_to_ingest(available: list[str], already_ingested: set[str]) -> list[str]:
    """Idempotency at file level: a file whose name is already in bronze is never read again."""
    return sorted(p for p in available if os.path.basename(p) not in already_ingested)


def read_timesheets(spark: SparkSession, files: list[str]) -> DataFrame:
    """Read NDJSON exports with an explicit schema; keep malformed lines instead of failing.

    On Databricks this would be Auto Loader (``cloudFiles``), which tracks processed files
    in its checkpoint and gives the same exactly-once guarantee as ``files_to_ingest``.
    """
    return (
        spark.read.schema(TIMESHEET_RAW)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .json(files)
        .withColumn("_source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def read_reference(spark: SparkSession, path: str) -> DataFrame:
    """Reference CSVs are full snapshots: read every column as a string, type it in silver."""
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(path)
        .withColumn("_ingested_at", F.current_timestamp())
    )
