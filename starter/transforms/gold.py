"""Stage 3 - Gold (suggested time: 25 min).

Run the tests for this stage with:  uv run pytest --impl starter -k gold
"""

from __future__ import annotations

from pyspark.sql import DataFrame

HOURS_PER_DAY = 8.0
BILLABLE_TYPES = ["TM", "FIXED"]


def fct_time_entries(
    entries: DataFrame,
    employees: DataFrame,
    engagements: DataFrame,
    rate_cards: DataFrame,
    fx_daily: DataFrame,
) -> DataFrame:
    """One row per non-rejected entry with columns:

    entry_id, employee_id, office, level, engagement_code, client_name, billing_type,
    work_date, week_start (Monday), hours, status, is_billable, bill_rate, currency,
    usd_per_unit, billable_value_usd

    * ``office``/``level`` are the ones the employee had **on work_date** (``employees`` is
      SCD2 with ``[valid_from, valid_to)``, ``valid_to`` NULL = current)
    * ``bill_rate`` comes from the engagement's rate card for that level
    * ``billable_value_usd = hours * bill_rate * usd_per_unit`` for TM/FIXED, 0 for INTERNAL
    * no PII in gold
    """
    raise NotImplementedError("TODO: stage 3")


def consultant_utilization_weekly(
    fct: DataFrame, employees: DataFrame, period_start: str, period_end: str
) -> DataFrame:
    """One row per (week_start, employee_id) with columns:

    week_start, employee_id, office, level, capacity_hours, total_hours, billable_hours,
    utilization, is_bench

    * capacity = 8h x working days (Mon-Fri) the employee was employed that week
    * utilization = billable_hours / capacity_hours, rounded to 4 decimals
    * an active consultant who logged nothing still appears (that is the bench report)
    """
    raise NotImplementedError("TODO: stage 3")


def office_utilization_weekly(utilization: DataFrame) -> DataFrame:
    """(office, week_start, headcount, capacity_hours, billable_hours, bench_count, utilization)"""
    raise NotImplementedError("TODO: stage 3")


def engagement_summary(fct: DataFrame) -> DataFrame:
    """(engagement_code, client_name, billing_type, consultants, hours, billable_value_usd,
    unpriced_hours, first_work_date, last_work_date)"""
    raise NotImplementedError("TODO: stage 3 (bonus)")
