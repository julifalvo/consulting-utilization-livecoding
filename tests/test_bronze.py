import json

import pytest

from utilization.schemas import CORRUPT_RECORD


def test_bronze_files_to_ingest_skips_files_already_landed(impl):
    available = ["/landing/timesheets_2026-09-14.json", "/landing/timesheets_2026-09-07.json"]
    assert impl.bronze.files_to_ingest(available, {"timesheets_2026-09-07.json"}) == [
        "/landing/timesheets_2026-09-14.json"
    ]
    assert impl.bronze.files_to_ingest(available, set()) == sorted(available)


@pytest.mark.spark
def test_bronze_read_timesheets_keeps_bad_lines_and_lineage(impl, spark, tmp_path):
    good = {
        "entry_id": "TE-1",
        "employee_id": "E001",
        "hours": 8,
        "updated_at": "2026-09-07T18:00:00Z",
    }
    comma = {**good, "entry_id": "TE-2", "hours": "7,5"}
    path = tmp_path / "timesheets_2026-09-07.json"
    path.write_text(f"{json.dumps(good)}\n{json.dumps(comma)}\n{json.dumps(good)[:20]}\n")

    df = impl.bronze.read_timesheets(spark, [str(path)]).cache()

    assert df.count() == 3, "a malformed line must be kept, not dropped or fatal"
    assert df.filter(df[CORRUPT_RECORD].isNotNull()).count() == 1
    assert {r.hours for r in df.collect() if r.entry_id} == {"8", "7,5"}
    assert {r._source_file for r in df.collect()} == {"timesheets_2026-09-07.json"}
    assert "_ingested_at" in df.columns


@pytest.mark.spark
def test_bronze_read_timesheets_tolerates_schema_drift(impl, spark, tmp_path):
    old = tmp_path / "timesheets_2026-09-07.json"
    new = tmp_path / "timesheets_2026-09-28.json"
    old.write_text(json.dumps({"entry_id": "TE-1"}) + "\n")
    new.write_text(
        json.dumps({"entry_id": "TE-2", "is_deleted": True, "activity_type": "travel"}) + "\n"
    )

    rows = {
        r.entry_id: r for r in impl.bronze.read_timesheets(spark, [str(old), str(new)]).collect()
    }

    assert rows["TE-1"].is_deleted is None and rows["TE-1"].activity_type is None
    assert rows["TE-2"].is_deleted is True and rows["TE-2"].activity_type == "travel"


@pytest.mark.spark
def test_bronze_read_reference_reads_strings_with_header(impl, spark, tmp_path):
    path = tmp_path / "rate_cards.csv"
    path.write_text("engagement_code,level,bill_rate,currency\nENG-001,Analyst,150.0,USD\n")

    df = impl.bronze.read_reference(spark, str(path))

    assert dict(df.dtypes)["bill_rate"] == "string"
    assert "_ingested_at" in df.columns
    assert df.first().engagement_code == "ENG-001"
