"""Orchestration of the stages: I/O around the pure transforms. Each stage is re-runnable."""

from __future__ import annotations

import glob
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utilization import quality
from utilization.lakehouse import Lakehouse
from utilization.schemas import REFERENCE_FILES
from utilization.transforms import bronze, gold, silver

log = logging.getLogger("utilization")


def run_bronze(spark: SparkSession, lh: Lakehouse, landing: str) -> dict[str, int]:
    available = glob.glob(os.path.join(landing, "timesheets", "*.json"))
    ingested: set[str] = set()
    if lh.exists(spark, "bronze", "timesheets"):
        rows = lh.read(spark, "bronze", "timesheets").select("_source_file").distinct().collect()
        ingested = {r[0] for r in rows}
    new_files = bronze.files_to_ingest(available, ingested)
    if new_files:
        lh.write(bronze.read_timesheets(spark, new_files), "bronze", "timesheets", mode="append")
    log.info("bronze: %d new file(s), %d already ingested", len(new_files), len(ingested))

    for table, file_name in REFERENCE_FILES.items():
        path = os.path.join(landing, "reference", file_name)
        lh.write(bronze.read_reference(spark, path), "bronze", table)
    return {"new_files": len(new_files), "skipped_files": len(available) - len(new_files)}


def run_silver(spark: SparkSession, lh: Lakehouse) -> dict[str, int]:
    def raw(table: str):
        return lh.read(spark, "bronze", table)

    employees = silver.employees_history(raw("employees"))
    engagements = silver.engagements(raw("engagements"))
    lh.write(employees, "silver", "employees")
    lh.write(engagements, "silver", "engagements")
    lh.write(silver.rate_cards(raw("rate_cards")), "silver", "rate_cards")

    valid, quarantine = silver.clean_time_entries(raw("timesheets"), employees, engagements)
    silver.merge_time_entries(lh.ensure(valid, "silver", "time_entries"), valid)
    (
        lh.ensure(quarantine, "silver", "time_entries_quarantine")
        .alias("t")
        .merge(quarantine.alias("s"), "t.quarantine_id = s.quarantine_id")
        .whenNotMatchedInsertAll()
        .execute()
    )

    entries = lh.read(spark, "silver", "time_entries")
    until = entries.agg(F.max("work_date")).first()[0]
    lh.write(silver.fx_daily(raw("fx_rates"), str(until)), "silver", "fx_daily")

    counts = {
        "time_entries": entries.count(),
        "quarantine": lh.read(spark, "silver", "time_entries_quarantine").count(),
    }
    log.info("silver: %s", counts)
    return counts


def run_gold(spark: SparkSession, lh: Lakehouse) -> dict[str, int]:
    def s(table: str):
        return lh.read(spark, "silver", table)

    entries, employees = s("time_entries"), s("employees")
    fct = gold.fct_time_entries(
        entries, employees, s("engagements"), s("rate_cards"), s("fx_daily")
    )
    lh.write(fct, "gold", "fct_time_entries")
    fct = lh.read(spark, "gold", "fct_time_entries")

    start, end = fct.agg(F.min("week_start"), F.max("work_date")).first()
    util = gold.consultant_utilization_weekly(fct, employees, str(start), str(end))
    lh.write(util, "gold", "consultant_utilization_weekly")
    util = lh.read(spark, "gold", "consultant_utilization_weekly")
    lh.write(gold.office_utilization_weekly(util), "gold", "office_utilization_weekly")
    lh.write(gold.engagement_summary(fct), "gold", "engagement_summary")

    counts = {"fct_time_entries": fct.count(), "consultant_weeks": util.count()}
    log.info("gold: %s", counts)
    return counts


def run_quality(spark: SparkSession, lh: Lakehouse) -> list[quality.CheckResult]:
    results = quality.run_checks(
        lh.read(spark, "silver", "time_entries"),
        lh.read(spark, "silver", "employees"),
        lh.read(spark, "gold", "fct_time_entries"),
        lh.read(spark, "gold", "consultant_utilization_weekly"),
    )
    for r in results:
        status = "PASS" if r.passed else r.severity.upper()
        log.info("[%s] %s (%d) %s", status, r.name, r.failing_rows, r.detail)
    return results


def run_all(spark: SparkSession, lh: Lakehouse, landing: str) -> list[quality.CheckResult]:
    run_bronze(spark, lh, landing)
    run_silver(spark, lh)
    run_gold(spark, lh)
    return run_quality(spark, lh)
