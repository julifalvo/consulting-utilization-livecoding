"""Stage 3 - Gold: business-ready, PII-free datasets for staffing and finance."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

HOURS_PER_DAY = 8.0
BILLABLE_TYPES = ["TM", "FIXED"]


def _as_of(entries: DataFrame, employees: DataFrame) -> DataFrame:
    """Point-in-time join: the level/office an employee had *on the day the work was done*.
    Half-open intervals [valid_from, valid_to) guarantee at most one match per entry."""
    e, h = entries.alias("e"), employees.alias("h")
    on = (
        (F.col("e.employee_id") == F.col("h.employee_id"))
        & (F.col("e.work_date") >= F.col("h.valid_from"))
        & (F.col("h.valid_to").isNull() | (F.col("e.work_date") < F.col("h.valid_to")))
    )
    return e.join(h, on, "left").select("e.*", "h.office", "h.level")


def fct_time_entries(
    entries: DataFrame,
    employees: DataFrame,
    engagements: DataFrame,
    rate_cards: DataFrame,
    fx_daily: DataFrame,
) -> DataFrame:
    """One row per approved/submitted entry, priced in USD at the rate valid that day."""
    priced = (
        _as_of(entries.filter(F.col("status") != "REJECTED"), employees)
        .join(engagements, "engagement_code", "left")
        .join(rate_cards.drop("currency"), ["engagement_code", "level"], "left")
        .join(
            fx_daily.withColumnRenamed("rate_date", "work_date"),
            ["currency", "work_date"],
            "left",
        )
    )
    is_billable = F.col("billing_type").isin(BILLABLE_TYPES)
    return priced.select(
        "entry_id",
        "employee_id",
        "office",
        "level",
        "engagement_code",
        "client_name",
        "billing_type",
        "work_date",
        F.to_date(F.date_trunc("week", "work_date")).alias("week_start"),
        "hours",
        "status",
        is_billable.alias("is_billable"),
        "bill_rate",
        "currency",
        "usd_per_unit",
        F.when(is_billable, F.round(F.col("hours") * F.col("bill_rate") * F.col("usd_per_unit"), 2))
        .otherwise(F.lit(0.0))
        .alias("billable_value_usd"),
    )


def consultant_utilization_weekly(
    fct: DataFrame, employees: DataFrame, period_start: str, period_end: str
) -> DataFrame:
    """Billable hours / capacity per consultant and week.

    Built from a *capacity spine* (every active employee x working day), not from the
    timesheets: a consultant who logged nothing is exactly who staffing needs to see.
    """
    days = (
        fct.sparkSession.range(1)
        .select(
            F.explode(
                F.sequence(F.to_date(F.lit(period_start)), F.to_date(F.lit(period_end)))
            ).alias("day")
        )
        .filter(~F.dayofweek("day").isin(1, 7))  # Sunday=1, Saturday=7
    )
    active = days.join(
        employees,
        (F.col("day") >= F.col("valid_from"))
        & (F.col("valid_to").isNull() | (F.col("day") < F.col("valid_to"))),
    )
    capacity = active.groupBy(
        "employee_id", F.to_date(F.date_trunc("week", "day")).alias("week_start")
    ).agg(
        (F.count("*") * HOURS_PER_DAY).alias("capacity_hours"),
        F.max_by("office", "day").alias("office"),  # end-of-week attributes
        F.max_by("level", "day").alias("level"),
    )
    logged = fct.groupBy("employee_id", "week_start").agg(
        F.sum("hours").alias("total_hours"),
        F.sum(F.when(F.col("is_billable"), F.col("hours")).otherwise(0.0)).alias("billable_hours"),
    )
    return (
        capacity.join(logged, ["employee_id", "week_start"], "left")
        .fillna(0.0, subset=["total_hours", "billable_hours"])
        .withColumn("utilization", F.round(F.col("billable_hours") / F.col("capacity_hours"), 4))
        .withColumn("is_bench", F.col("billable_hours") == 0)
        .select(
            "week_start",
            "employee_id",
            "office",
            "level",
            "capacity_hours",
            "total_hours",
            "billable_hours",
            "utilization",
            "is_bench",
        )
    )


def office_utilization_weekly(utilization: DataFrame) -> DataFrame:
    """Ratio of sums, not average of ratios: a 2-day new joiner must not weigh like a full week."""
    return (
        utilization.groupBy("office", "week_start")
        .agg(
            F.countDistinct("employee_id").alias("headcount"),
            F.sum("capacity_hours").alias("capacity_hours"),
            F.sum("billable_hours").alias("billable_hours"),
            F.sum(F.col("is_bench").cast("int")).alias("bench_count"),
        )
        .withColumn("utilization", F.round(F.col("billable_hours") / F.col("capacity_hours"), 4))
    )


def engagement_summary(fct: DataFrame) -> DataFrame:
    return fct.groupBy("engagement_code", "client_name", "billing_type").agg(
        F.countDistinct("employee_id").alias("consultants"),
        F.sum("hours").alias("hours"),
        F.round(F.sum("billable_value_usd"), 2).alias("billable_value_usd"),
        F.sum(
            F.when(F.col("is_billable") & F.col("bill_rate").isNull(), F.col("hours")).otherwise(
                0.0
            )
        ).alias("unpriced_hours"),
        F.min("work_date").alias("first_work_date"),
        F.max("work_date").alias("last_work_date"),
    )
