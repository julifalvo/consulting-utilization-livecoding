"""Stage 2 - Silver: typed, deduplicated, validated. One row per timesheet entry."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from utilization.schemas import CORRUPT_RECORD, TIMESHEET_FIELDS

VALID_STATUSES = ["SUBMITTED", "APPROVED", "REJECTED"]
MAX_HOURS_PER_ENTRY = 24.0

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
    """Cast raw strings to proper types. Unparseable values become NULL and fail validation."""
    return (
        bronze.withColumn("entry_id", F.trim("entry_id"))
        .withColumn("employee_id", F.upper(F.trim("employee_id")))
        .withColumn("engagement_code", F.upper(F.trim("engagement_code")))
        .withColumn("status", F.upper(F.trim("status")))
        # Spark 4 / Databricks run with ANSI mode on: a bad value in to_date() or cast() fails
        # the whole job. try_* variants return NULL, which validation turns into a reason.
        .withColumn("work_date", F.try_to_date("work_date", "yyyy-MM-dd"))
        # "7,5" from localized UIs -> 7.5.
        .withColumn("hours", F.regexp_replace("hours", ",", ".").try_cast("double"))
        # Parse the offset so ordering is by real instant, not by the string ("+02:00" vs "Z").
        .withColumn(
            "updated_at", F.try_to_timestamp("updated_at", F.lit("yyyy-MM-dd'T'HH:mm:ssXXX"))
        )
        .withColumn("is_deleted", F.coalesce("is_deleted", F.lit(False)))
    )


def latest_versions(entries: DataFrame) -> DataFrame:
    """Keep the most recent version of each entry: re-sent rows and corrections collapse."""
    w = Window.partitionBy("entry_id").orderBy(
        F.col("updated_at").desc_nulls_last(), F.col("_source_file").desc()
    )
    return entries.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _rejection_reason(known_employee: Column, known_engagement: Column) -> Column:
    tombstone = F.col("is_deleted")
    return (
        F.when(F.col("entry_id").isNull() | F.col("employee_id").isNull(), "missing_key")
        .when(F.col("updated_at").isNull(), "invalid_updated_at")
        # A tombstone only needs a key: its payload is irrelevant once it is deleted.
        .when(tombstone, None)
        .when(F.col("work_date").isNull(), "invalid_work_date")
        .when(F.col("hours").isNull(), "invalid_hours")
        .when((F.col("hours") <= 0) | (F.col("hours") > MAX_HOURS_PER_ENTRY), "hours_out_of_range")
        .when(~F.col("status").isin(VALID_STATUSES), "invalid_status")
        .when(~known_employee, "unknown_employee")
        .when(~known_engagement, "unknown_engagement")
    )


def clean_time_entries(
    bronze: DataFrame, employees: DataFrame, engagements: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Return ``(valid, quarantine)``. Nothing is silently dropped: every bronze row either
    reaches silver or lands in quarantine with a reason.

    Validation runs *before* deduplication, so the latest *valid* version wins. That makes a
    full replay of bronze and a week-by-week incremental run converge to the same silver.
    """
    corrupt = bronze.filter(F.col(CORRUPT_RECORD).isNotNull())
    parsed = parse_time_entries(bronze.filter(F.col(CORRUPT_RECORD).isNull()))

    emp_ids = employees.select("employee_id", F.lit(True).alias("_known_emp")).distinct()
    eng_ids = engagements.select("engagement_code", F.lit(True).alias("_known_eng"))
    checked = (
        parsed.join(F.broadcast(emp_ids), "employee_id", "left")
        .join(F.broadcast(eng_ids), "engagement_code", "left")
        .withColumn(
            "reason",
            _rejection_reason(
                F.coalesce("_known_emp", F.lit(False)), F.coalesce("_known_eng", F.lit(False))
            ),
        )
    )

    valid = latest_versions(checked.filter(F.col("reason").isNull())).select(*SILVER_COLUMNS)
    invalid = checked.filter(F.col("reason").isNotNull()).select(
        "reason",
        "entry_id",
        "_source_file",
        F.to_json(F.struct(*TIMESHEET_FIELDS)).alias("raw"),
        "_ingested_at",
    )
    malformed = corrupt.select(
        F.lit("malformed_json").alias("reason"),
        F.lit(None).cast("string").alias("entry_id"),
        "_source_file",
        F.col(CORRUPT_RECORD).alias("raw"),
        "_ingested_at",
    )
    quarantine = invalid.unionByName(malformed).withColumn(
        "quarantine_id", F.sha2(F.concat_ws("|", "_source_file", "reason", "raw"), 256)
    )
    return valid, quarantine


def merge_time_entries(target: DeltaTable, updates: DataFrame) -> None:
    """Idempotent upsert. Only a strictly newer version changes a row, so re-running a batch
    (or replaying all of bronze) is a no-op. Tombstones delete; unseen tombstones are ignored."""
    newer = "s.updated_at > t.updated_at"
    (
        target.alias("t")
        .merge(updates.alias("s"), "t.entry_id = s.entry_id")
        .whenMatchedDelete(condition=f"{newer} AND s.is_deleted")
        .whenMatchedUpdateAll(condition=newer)
        .whenNotMatchedInsertAll(condition="NOT s.is_deleted")
        .execute()
    )


# --- reference data ----------------------------------------------------------------------


def employees_history(raw: DataFrame) -> DataFrame:
    """SCD2: one row per (employee, level/office version). ``valid_to`` is exclusive; NULL = current."""
    return raw.select(
        F.upper(F.trim("employee_id")).alias("employee_id"),
        "full_name",
        "email",  # PII: stays in silver (restricted schema), never reaches gold
        "office",
        "level",
        F.to_date("valid_from").alias("valid_from"),
        F.to_date(F.nullif(F.col("valid_to"), F.lit(""))).alias("valid_to"),
    )


def engagements(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.upper(F.trim("engagement_code")).alias("engagement_code"),
        "client_name",
        F.upper("billing_type").alias("billing_type"),
        F.upper("currency").alias("currency"),
    )


def rate_cards(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.upper(F.trim("engagement_code")).alias("engagement_code"),
        "level",
        F.col("bill_rate").cast("double").alias("bill_rate"),
        F.upper("currency").alias("currency"),
    )


def fx_daily(raw: DataFrame, until: str) -> DataFrame:
    """Dense daily FX calendar: weekends and holidays carry the last published rate forward.
    USD is not published by treasury and is added as 1.0."""
    rates = raw.select(
        F.to_date("rate_date").alias("rate_date"),
        F.upper("currency").alias("currency"),
        F.col("usd_per_unit").cast("double").alias("usd_per_unit"),
    )
    calendar = rates.agg(F.min("rate_date").alias("start")).select(
        F.explode(F.sequence("start", F.to_date(F.lit(until)))).alias("rate_date")
    )
    grid = rates.select("currency").distinct().crossJoin(calendar)
    w = (
        Window.partitionBy("currency")
        .orderBy("rate_date")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    filled = (
        grid.join(rates, ["currency", "rate_date"], "left")
        .withColumn("usd_per_unit", F.last("usd_per_unit", ignorenulls=True).over(w))
        .select("rate_date", "currency", "usd_per_unit")
    )
    usd = calendar.select(
        "rate_date", F.lit("USD").alias("currency"), F.lit(1.0).alias("usd_per_unit")
    )
    return filled.unionByName(usd)
