import datetime as dt

import pytest
from delta.tables import DeltaTable

from tests import builders as b

pytestmark = pytest.mark.spark


@pytest.fixture
def refs(spark):
    employees = b.employees(spark, ("E001", "London", "Analyst", "2025-01-01", None))
    engagements = b.engagements(spark, ("ENG-001", "Northwind Bank", "TM", "USD"))
    return employees, engagements


def _by_id(df):
    return {r.entry_id: r for r in df.collect()}


def test_silver_parse_handles_comma_decimals_offsets_and_missing_flags(impl, spark):
    raw = b.bronze(
        spark,
        {"entry_id": "TE-1", "hours": "7,5", "updated_at": "2026-09-11T01:00:00+02:00"},
        {
            "entry_id": "TE-2",
            "hours": "8",
            "updated_at": "2026-09-10T23:30:00Z",
            "is_deleted": True,
        },
    )
    rows = _by_id(impl.silver.parse_time_entries(raw))

    assert rows["TE-1"].hours == 7.5
    assert rows["TE-1"].work_date == dt.date(2026, 9, 7)
    assert rows["TE-1"].updated_at.replace(tzinfo=None) == dt.datetime(2026, 9, 10, 23, 0)
    assert rows["TE-1"].is_deleted is False
    assert rows["TE-2"].is_deleted is True


def test_silver_latest_version_is_by_instant_not_by_string(impl, spark):
    # "2026-09-11T01:00+02:00" sorts after "2026-09-10T23:30Z" as text, but happened before it.
    raw = b.bronze(
        spark,
        {"entry_id": "TE-1", "hours": "9,5", "updated_at": "2026-09-11T01:00:00+02:00"},
        {"entry_id": "TE-1", "hours": "7.5", "updated_at": "2026-09-10T23:30:00Z"},
    )
    latest = impl.silver.latest_versions(impl.silver.parse_time_entries(raw)).collect()

    assert len(latest) == 1
    assert latest[0].hours == 7.5


def test_silver_clean_collapses_resent_duplicates(impl, spark, refs):
    row = {"entry_id": "TE-1"}
    raw = b.bronze(spark, row, source_file="timesheets_2026-09-07.json").unionByName(
        b.bronze(spark, row, source_file="timesheets_2026-09-14.json")
    )
    valid, quarantine = impl.silver.clean_time_entries(raw, *refs)

    assert valid.count() == 1
    assert quarantine.count() == 0
    assert valid.columns == impl.silver.SILVER_COLUMNS


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        ({"entry_id": None}, "missing_key"),
        ({"updated_at": "yesterday"}, "invalid_updated_at"),
        ({"work_date": "2026-13-01"}, "invalid_work_date"),
        ({"hours": "eight"}, "invalid_hours"),
        ({"hours": "-4"}, "hours_out_of_range"),
        ({"hours": "0"}, "hours_out_of_range"),
        ({"hours": "25"}, "hours_out_of_range"),
        ({"status": "PENDING"}, "invalid_status"),
        ({"employee_id": "C900"}, "unknown_employee"),
        ({"engagement_code": "ENG-0O7"}, "unknown_engagement"),
    ],
)
def test_silver_clean_quarantines_invalid_rows_with_a_reason(impl, spark, refs, row, reason):
    valid, quarantine = impl.silver.clean_time_entries(b.bronze(spark, row), *refs)

    assert valid.count() == 0
    assert [r.reason for r in quarantine.collect()] == [reason]
    q = quarantine.first()
    assert q.raw and q._source_file and q.quarantine_id


def test_silver_clean_quarantines_malformed_json(impl, spark, refs):
    raw = b.bronze(spark, {"entry_id": None, "_corrupt_record": '{"entry_id":"TE-'})
    valid, quarantine = impl.silver.clean_time_entries(raw, *refs)

    assert valid.count() == 0
    assert quarantine.first().reason == "malformed_json"
    assert quarantine.first().raw == '{"entry_id":"TE-'


def test_silver_clean_accepts_tombstones_without_payload(impl, spark, refs):
    raw = b.bronze(spark, {"is_deleted": True, "hours": None, "engagement_code": None})
    valid, quarantine = impl.silver.clean_time_entries(raw, *refs)

    assert quarantine.count() == 0
    assert valid.first().is_deleted is True


def test_silver_clean_keeps_last_valid_version_when_correction_is_invalid(impl, spark, refs):
    raw = b.bronze(
        spark,
        {"hours": "8", "updated_at": "2026-09-07T18:00:00Z"},
        {"hours": "80", "updated_at": "2026-09-08T09:00:00Z"},
    )
    valid, quarantine = impl.silver.clean_time_entries(raw, *refs)

    assert [r.hours for r in valid.collect()] == [8.0]
    assert [r.reason for r in quarantine.collect()] == ["hours_out_of_range"]


def _merge(impl, spark, path, *rows):
    updates = b.silver_entries(spark, *rows)
    if not DeltaTable.isDeltaTable(spark, str(path)):
        updates.limit(0).write.format("delta").save(str(path))
    impl.silver.merge_time_entries(DeltaTable.forPath(spark, str(path)), updates)
    return _by_id(spark.read.format("delta").load(str(path)))


def test_silver_merge_is_idempotent_and_never_goes_back_in_time(impl, spark, tmp_path):
    path = tmp_path / "time_entries"
    v1 = {"entry_id": "TE-1", "hours": 8, "updated_at": "2026-09-07T18:00:00+00:00"}
    v2 = {**v1, "hours": 6, "updated_at": "2026-09-08T09:00:00+00:00"}

    assert _merge(impl, spark, path, v1)["TE-1"].hours == 8
    assert _merge(impl, spark, path, v2)["TE-1"].hours == 6
    assert _merge(impl, spark, path, v2)["TE-1"].hours == 6, "replaying the same batch is a no-op"
    assert _merge(impl, spark, path, v1)["TE-1"].hours == 6, "an older version must not win"
    assert len(_merge(impl, spark, path, v1, {**v1, "entry_id": "TE-2"})) == 2


def test_silver_merge_applies_tombstones(impl, spark, tmp_path):
    path = tmp_path / "time_entries"
    live = {"entry_id": "TE-1", "updated_at": "2026-09-07T18:00:00+00:00"}
    gone = {**live, "is_deleted": True, "updated_at": "2026-09-15T08:00:00+00:00"}
    never_seen = {**gone, "entry_id": "TE-404"}

    _merge(impl, spark, path, live)
    assert _merge(impl, spark, path, gone, never_seen) == {}


def test_silver_fx_daily_fills_weekends_holidays_and_usd(impl, spark):
    raw = spark.createDataFrame(
        [
            ("2026-08-28", "GBP", "1.27"),  # Friday
            ("2026-09-01", "GBP", "1.28"),  # Tuesday: Monday 31st was a UK bank holiday
        ],
        "rate_date string, currency string, usd_per_unit string",
    )
    fx = {
        (r.currency, r.rate_date.isoformat()): r.usd_per_unit
        for r in impl.silver.fx_daily(raw, "2026-09-02").collect()
    }

    assert fx[("GBP", "2026-08-30")] == 1.27  # Sunday
    assert fx[("GBP", "2026-08-31")] == 1.27  # holiday
    assert fx[("GBP", "2026-09-02")] == 1.28  # after the last published date, up to `until`
    assert fx[("USD", "2026-08-29")] == 1.0
    assert len(fx) == 2 * 6  # dense: 6 days x 2 currencies
