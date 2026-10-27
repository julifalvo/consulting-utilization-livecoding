import pytest

from tests import builders as b

pytestmark = pytest.mark.spark


@pytest.fixture
def world(spark):
    employees = b.employees(
        spark,
        ("E001", "Madrid", "Consultant", "2025-01-01", "2026-09-14"),
        ("E001", "Madrid", "Manager", "2026-09-14", None),  # promoted on Monday 14th
        ("E002", "London", "Analyst", "2025-01-01", None),  # on the bench
        ("E003", "London", "Analyst", "2026-09-16", None),  # joins on Wednesday 16th
    )
    engagements = b.engagements(
        spark,
        ("ENG-001", "Northwind Bank", "TM", "EUR"),
        ("INT-001", "Internal - Training", "INTERNAL", "USD"),
    )
    rate_cards = b.rate_cards(
        spark,
        ("ENG-001", "Consultant", 200.0, "EUR"),
        ("ENG-001", "Manager", 300.0, "EUR"),
    )
    fx = b.fx_daily(
        spark,
        *[(f"2026-09-{d:02d}", "EUR", 1.1) for d in range(7, 21)],
        *[(f"2026-09-{d:02d}", "USD", 1.0) for d in range(7, 21)],
    )
    return employees, engagements, rate_cards, fx


def _fct(impl, spark, world, *entries):
    employees, engagements, rate_cards, fx = world
    return impl.gold.fct_time_entries(
        b.silver_entries(spark, *entries), employees, engagements, rate_cards, fx
    )


def test_gold_fct_uses_level_and_rate_valid_on_work_date(impl, spark, world):
    fct = _fct(
        impl,
        spark,
        world,
        {"entry_id": "before", "work_date": "2026-09-11", "hours": 8},  # Friday, still Consultant
        {"entry_id": "after", "work_date": "2026-09-14", "hours": 8},  # Monday, Manager
    )
    rows = {r.entry_id: r for r in fct.collect()}

    assert fct.count() == 2, "the as-of join must not fan out"
    assert (rows["before"].level, rows["before"].bill_rate) == ("Consultant", 200.0)
    assert (rows["after"].level, rows["after"].bill_rate) == ("Manager", 300.0)
    assert rows["after"].billable_value_usd == pytest.approx(8 * 300 * 1.1)
    assert str(rows["after"].week_start) == "2026-09-14"


def test_gold_fct_excludes_rejected_prices_internal_at_zero_and_has_no_pii(impl, spark, world):
    fct = _fct(
        impl,
        spark,
        world,
        {"entry_id": "rejected", "status": "REJECTED"},
        {"entry_id": "training", "engagement_code": "INT-001", "hours": 2},
    )
    rows = {r.entry_id: r for r in fct.collect()}

    assert set(rows) == {"training"}
    assert rows["training"].is_billable is False
    assert rows["training"].billable_value_usd == 0.0
    assert not {"email", "full_name"} & set(fct.columns)


def test_gold_utilization_has_bench_partial_weeks_and_correct_ratio(impl, spark, world):
    employees = world[0]
    fct = _fct(
        impl,
        spark,
        world,
        *[
            {"entry_id": f"e{d}", "work_date": f"2026-09-{d:02d}", "hours": 8}
            for d in range(14, 18)
        ],
        {"entry_id": "t", "work_date": "2026-09-18", "engagement_code": "INT-001", "hours": 8},
    )
    util = impl.gold.consultant_utilization_weekly(fct, employees, "2026-09-14", "2026-09-18")
    rows = {r.employee_id: r for r in util.collect()}

    assert rows["E001"].capacity_hours == 40
    assert rows["E001"].total_hours == 40 and rows["E001"].billable_hours == 32
    assert rows["E001"].utilization == pytest.approx(0.8)
    assert rows["E001"].level == "Manager"
    assert rows["E002"].billable_hours == 0 and rows["E002"].is_bench is True
    assert rows["E003"].capacity_hours == 24, "joined on Wednesday: 3 working days"


def test_gold_office_utilization_is_a_ratio_of_sums(impl, spark, world):
    util = spark.createDataFrame(
        [
            ("2026-09-14", "A", "London", "Analyst", 40.0, 40.0, 40.0, 1.0, False),
            ("2026-09-14", "B", "London", "Analyst", 8.0, 0.0, 0.0, 0.0, True),
        ],
        "week_start string, employee_id string, office string, level string, capacity_hours double, "
        "total_hours double, billable_hours double, utilization double, is_bench boolean",
    )
    row = impl.gold.office_utilization_weekly(util).first()

    assert row.headcount == 2 and row.bench_count == 1
    assert row.utilization == pytest.approx(40 / 48, abs=1e-4)  # not (1.0 + 0.0) / 2


def test_gold_engagement_summary_reports_unpriced_hours(impl, spark, world):
    employees, engagements, _, fx = world
    no_manager_rate = b.rate_cards(spark, ("ENG-001", "Consultant", 200.0, "EUR"))
    fct = impl.gold.fct_time_entries(
        b.silver_entries(
            spark,
            {"entry_id": "priced", "work_date": "2026-09-11", "hours": 8},
            {"entry_id": "unpriced", "work_date": "2026-09-14", "hours": 5},
        ),
        employees,
        engagements,
        no_manager_rate,
        fx,
    )
    row = impl.gold.engagement_summary(fct).first()

    assert row.hours == 13 and row.unpriced_hours == 5 and row.consultants == 1
    assert row.billable_value_usd == pytest.approx(8 * 200 * 1.1)
