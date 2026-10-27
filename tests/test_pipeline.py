"""End-to-end over the real synthetic exports: planted defects, idempotency, replay == incremental."""

import shutil

import pytest
from pyspark.sql import functions as F

from utilization import datagen, pipeline, quality
from utilization.lakehouse import Lakehouse

pytestmark = [pytest.mark.spark, pytest.mark.solution_only]


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    out = tmp_path_factory.mktemp("raw")
    datagen.generate(out)
    return out


@pytest.fixture(scope="module")
def full_run(spark, landing, tmp_path_factory):
    lh = Lakehouse(root=str(tmp_path_factory.mktemp("lakehouse")))
    results = pipeline.run_all(spark, lh, str(landing))
    return lh, results


def _snapshot(spark, lh, layer, table, key):
    df = lh.read(spark, layer, table)
    return sorted(tuple(r) for r in df.drop("_ingested_at").orderBy(key).collect())


def test_pipeline_passes_all_error_checks(full_run):
    _, results = full_run
    assert quality.failed_errors(results) == []
    warned = {r.name for r in results if not r.passed}
    assert warned == {"gold.fct_time_entries: billable entries have a rate card"}


def test_pipeline_catches_every_planted_defect(spark, full_run):
    lh, _ = full_run
    reasons = {
        r.reason
        for r in lh.read(spark, "silver", "time_entries_quarantine").select("reason").collect()
    }
    assert {
        "malformed_json",
        "unknown_engagement",
        "unknown_employee",
        "hours_out_of_range",
    } <= reasons

    entries = lh.read(spark, "silver", "time_entries")
    assert entries.filter("entry_id = 'TE-900001'").first().hours == 7.5  # timezone trap
    assert entries.filter("is_deleted").count() == 0
    assert entries.groupBy("entry_id").count().filter("count > 1").count() == 0

    util = lh.read(spark, "gold", "consultant_utilization_weekly")
    bench = util.filter("employee_id = 'E029' AND week_start >= '2026-09-14'")
    assert bench.count() == 2 and bench.filter("is_bench").count() == 2
    new_joiner = util.filter("employee_id = 'E031'").orderBy("week_start").first()
    assert new_joiner.capacity_hours == 24


def test_pipeline_rerun_is_a_no_op(spark, landing, full_run):
    lh, _ = full_run
    before = _snapshot(spark, lh, "silver", "time_entries", "entry_id")
    stats = pipeline.run_bronze(spark, lh, str(landing))
    pipeline.run_silver(spark, lh)

    assert stats == {"new_files": 0, "skipped_files": 4}
    assert _snapshot(spark, lh, "silver", "time_entries", "entry_id") == before


def test_pipeline_incremental_weekly_runs_match_full_replay(spark, landing, full_run, tmp_path):
    lh_full, _ = full_run
    lh_inc = Lakehouse(root=str(tmp_path / "lakehouse"))
    weekly = tmp_path / "weekly"
    shutil.copytree(landing / "reference", weekly / "reference")
    (weekly / "timesheets").mkdir()
    for export in sorted((landing / "timesheets").glob("*.json")):
        shutil.copy(export, weekly / "timesheets" / export.name)
        pipeline.run_bronze(spark, lh_inc, str(weekly))
        pipeline.run_silver(spark, lh_inc)

    cols = ["entry_id", "hours", "status", "engagement_code", "updated_at"]
    full = lh_full.read(spark, "silver", "time_entries").select(*cols).orderBy("entry_id")
    inc = lh_inc.read(spark, "silver", "time_entries").select(*cols).orderBy("entry_id")
    assert full.collect() == inc.collect()


def test_pipeline_gold_reconciles_to_silver(spark, full_run):
    lh, _ = full_run
    silver = lh.read(spark, "silver", "time_entries").filter("status != 'REJECTED'")
    fct = lh.read(spark, "gold", "fct_time_entries")
    assert fct.count() == silver.count()
    total = fct.agg(F.sum("billable_value_usd")).first()[0]
    by_engagement = (
        lh.read(spark, "gold", "engagement_summary").agg(F.sum("billable_value_usd")).first()[0]
    )
    assert by_engagement == pytest.approx(total, abs=1.0)
