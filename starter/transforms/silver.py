"""Stage 2 - Silver (suggested time: 30 min).

Run the tests for this stage with:  uv run pytest --impl starter -k silver
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from utilization.schemas import CORRUPT_RECORD, TIMESHEET_FIELDS  # noqa: F401

VALID_STATUSES = ["SUBMITTED", "APPROVED", "REJECTED"]
MAX_HOURS_PER_ENTRY = 24.0

# Columns of silver.time_entries, in this order.
SILVER_COLUMNS = [
    "entry_id",
    "employee_id",
    "engagement_code",
    "work_date",
    "hours",
    "status",
    "activity_type",
    "is_deleted",
    "updated_at",
    "_source_file",
    "_ingested_at",
]


# --- timesheets --------------------------------------------------------------------------


def parse_time_entries(bronze: DataFrame) -> DataFrame:
    """Cast bronze strings to types: ``work_date`` date, ``hours`` double, ``updated_at``
    timestamp, ``is_deleted`` boolean (missing -> False). Trim/upper-case the codes.

    Look at the raw files before you write this: what do ``hours`` and ``updated_at`` look like?
    """
    raise NotImplementedError("TODO: stage 2")


def latest_versions(entries: DataFrame) -> DataFrame:
    """One row per ``entry_id``: the most recent version."""
    raise NotImplementedError("TODO: stage 2")


def clean_time_entries(
    bronze: DataFrame, employees: DataFrame, engagements: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Return ``(valid, quarantine)``.

    * ``valid``: one row per entry with ``SILVER_COLUMNS``.
    * ``quarantine``: columns ``reason, entry_id, _source_file, raw, _ingested_at,
      quarantine_id``. Every bronze row that does not make it to silver must be here.

    Reasons the tests expect: ``malformed_json``, ``missing_key``, ``invalid_updated_at``,
    ``invalid_work_date``, ``invalid_hours``, ``hours_out_of_range`` (valid is 0 < h <= 24),
    ``invalid_status``, ``unknown_employee``, ``unknown_engagement``.
    A tombstone (``is_deleted``) only needs a key and a timestamp.

    Think about it: should you validate before or after deduplicating? Why?
    """
    raise NotImplementedError("TODO: stage 2")


def merge_time_entries(target: DeltaTable, updates: DataFrame) -> None:
    """Upsert ``updates`` into ``target`` on ``entry_id``.

    * running it twice with the same input must change nothing
    * an older version must never overwrite a newer one
    * a newer tombstone deletes the row; a tombstone for an unknown row inserts nothing
    """
    raise NotImplementedError("TODO: stage 2")


# --- reference data ----------------------------------------------------------------------
# These four are short; they are provided so you can spend the time on timesheets.


def employees_history(raw: DataFrame) -> DataFrame:
    from utilization.transforms.silver import employees_history

    return employees_history(raw)


def engagements(raw: DataFrame) -> DataFrame:
    from utilization.transforms.silver import engagements

    return engagements(raw)


def rate_cards(raw: DataFrame) -> DataFrame:
    from utilization.transforms.silver import rate_cards

    return rate_cards(raw)


def fx_daily(raw: DataFrame, until: str) -> DataFrame:
    """Dense daily FX calendar ``(rate_date, currency, usd_per_unit)`` from the first published
    date to ``until``. Days without a published rate (weekends, holidays) use the last known
    rate. Treasury does not publish USD: add it as 1.0 for every day.
    """
    raise NotImplementedError("TODO: stage 2 (bonus)")
