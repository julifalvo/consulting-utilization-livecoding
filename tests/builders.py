"""Tiny DataFrame builders: tests describe only the columns they care about."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utilization.schemas import TIMESHEET_RAW

BRONZE_DEFAULTS = {
    "entry_id": "TE-1",
    "employee_id": "E001",
    "engagement_code": "ENG-001",
    "work_date": "2026-09-07",
    "hours": "8",
    "status": "APPROVED",
    "updated_at": "2026-09-07T18:00:00Z",
    "is_deleted": None,
    "activity_type": None,
    "_corrupt_record": None,
}


def bronze(
    spark: SparkSession, *rows: dict, source_file: str = "timesheets_2026-09-14.json"
) -> DataFrame:
    full = [{**BRONZE_DEFAULTS, **r} for r in rows]
    names = [f.name for f in TIMESHEET_RAW.fields]
    return (
        spark.createDataFrame([tuple(r[n] for n in names) for r in full], TIMESHEET_RAW)
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def employees(spark: SparkSession, *rows: tuple) -> DataFrame:
    """rows: (employee_id, office, level, valid_from, valid_to | None)"""
    return spark.createDataFrame(
        [
            (eid, f"Name {eid}", f"{eid.lower()}@example.com", office, level, _d(vf), _d(vt))
            for eid, office, level, vf, vt in rows
        ],
        "employee_id string, full_name string, email string, office string, level string, "
        "valid_from date, valid_to date",
    )


def engagements(spark: SparkSession, *rows: tuple) -> DataFrame:
    """rows: (engagement_code, client_name, billing_type, currency)"""
    return spark.createDataFrame(
        list(rows),
        "engagement_code string, client_name string, billing_type string, currency string",
    )


def rate_cards(spark: SparkSession, *rows: tuple) -> DataFrame:
    """rows: (engagement_code, level, bill_rate, currency)"""
    return spark.createDataFrame(
        list(rows), "engagement_code string, level string, bill_rate double, currency string"
    )


def fx_daily(spark: SparkSession, *rows: tuple) -> DataFrame:
    """rows: (rate_date, currency, usd_per_unit)"""
    return spark.createDataFrame(
        [(_d(d), c, r) for d, c, r in rows], "rate_date date, currency string, usd_per_unit double"
    )


def silver_entries(spark: SparkSession, *rows: dict) -> DataFrame:
    defaults = {
        "entry_id": "TE-1",
        "employee_id": "E001",
        "engagement_code": "ENG-001",
        "work_date": "2026-09-07",
        "hours": 8.0,
        "status": "APPROVED",
        "activity_type": None,
        "is_deleted": False,
        "updated_at": "2026-09-07T18:00:00Z",
        "_source_file": "f.json",
    }
    full = [{**defaults, **r} for r in rows]
    df = spark.createDataFrame(
        [
            (
                r["entry_id"],
                r["employee_id"],
                r["engagement_code"],
                _d(r["work_date"]),
                float(r["hours"]),
                r["status"],
                r["activity_type"],
                r["is_deleted"],
                dt.datetime.fromisoformat(r["updated_at"]),
                r["_source_file"],
            )
            for r in full
        ],
        "entry_id string, employee_id string, engagement_code string, work_date date, hours double, "
        "status string, activity_type string, is_deleted boolean, updated_at timestamp, "
        "_source_file string",
    )
    return df.withColumn("_ingested_at", F.current_timestamp())


def _d(value: str | None) -> dt.date | None:
    return dt.date.fromisoformat(value) if value else None
