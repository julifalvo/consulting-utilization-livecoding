"""Data-quality gates. ``error`` makes the job's quality task fail (alerting on-call and
blocking downstream tasks); ``warn`` is logged.

On Databricks the same rules map to Lakeflow Declarative Pipelines (DLT) expectations
(``expect_or_fail`` / ``expect``); they are plain PySpark here so they run anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: str  # "error" | "warn"
    failing_rows: int
    detail: str = ""

    @property
    def passed(self) -> bool:
        return self.failing_rows == 0


def _duplicates(df: DataFrame, keys: list[str]) -> int:
    return df.groupBy(*keys).count().filter("count > 1").count()


def scd2_overlaps(employees: DataFrame) -> int:
    a, b = employees.alias("a"), employees.alias("b")
    far = F.to_date(F.lit("9999-12-31"))
    overlap = (
        (F.col("a.employee_id") == F.col("b.employee_id"))
        & (F.col("a.valid_from") < F.col("b.valid_from"))
        & (F.col("b.valid_from") < F.coalesce(F.col("a.valid_to"), far))
    )
    return a.join(b, overlap).count()


def run_checks(
    silver_entries: DataFrame, employees: DataFrame, fct: DataFrame, utilization: DataFrame
) -> list[CheckResult]:
    silver_hours = (
        silver_entries.filter("status != 'REJECTED'").agg(F.sum("hours")).first()[0] or 0.0
    )
    gold_hours = fct.agg(F.sum("hours")).first()[0] or 0.0
    return [
        CheckResult(
            "silver.time_entries: entry_id unique",
            "error",
            _duplicates(silver_entries, ["entry_id"]),
        ),
        CheckResult(
            "silver.employees: no overlapping SCD2 versions", "error", scd2_overlaps(employees)
        ),
        CheckResult(
            "gold.fct_time_entries: every entry matched an employee version",
            "error",
            fct.filter(F.col("level").isNull()).count(),
        ),
        CheckResult(
            "gold.fct_time_entries: every entry has an FX rate",
            "error",
            fct.filter(F.col("usd_per_unit").isNull()).count(),
        ),
        CheckResult(
            # Catches join fan-out (e.g. overlapping SCD2 rows) and silent drops alike.
            "reconciliation: gold hours == silver non-rejected hours",
            "error",
            0 if abs(silver_hours - gold_hours) < 1e-6 else 1,
            f"silver={silver_hours:.2f} gold={gold_hours:.2f}",
        ),
        CheckResult(
            "gold.fct_time_entries: billable entries have a rate card",
            "warn",
            fct.filter(F.col("is_billable") & F.col("bill_rate").isNull()).count(),
            "unpriced hours are excluded from billable value until finance adds the rate",
        ),
        CheckResult(
            "gold.consultant_utilization_weekly: utilization <= 125%",
            "warn",
            utilization.filter(F.col("utilization") > 1.25).count(),
        ),
    ]


def failed_errors(results: list[CheckResult]) -> list[CheckResult]:
    return [r for r in results if r.severity == "error" and not r.passed]
