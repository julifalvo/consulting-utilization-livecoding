"""The app needs no Spark: gold fixtures are written with delta-rs, so these run anywhere."""

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from dashboard import sql_guard
from dashboard.ask import SqlAnswer
from dashboard.main import create_app


@pytest.fixture
def gold(tmp_path):
    from deltalake import write_deltalake

    weeks = pd.to_datetime(["2026-09-14", "2026-09-21"]).date
    write_deltalake(
        tmp_path / "office_utilization_weekly",
        pd.DataFrame(
            {
                "office": ["London", "Madrid", "London"],
                "week_start": [weeks[0], weeks[0], weeks[1]],
                "headcount": [2, 1, 2],
                "capacity_hours": [80.0, 40.0, 80.0],
                "billable_hours": [40.0, 36.0, 72.0],
                "bench_count": [1, 0, 0],
                "utilization": [0.5, 0.9, 0.9],
            }
        ),
    )
    write_deltalake(
        tmp_path / "consultant_utilization_weekly",
        pd.DataFrame(
            {
                "week_start": [weeks[1], weeks[1], weeks[1]],
                "employee_id": ["E001", "E002", "E003"],
                "office": ["London", "London", "Madrid"],
                "level": ["Analyst", "Manager", "Consultant"],
                "capacity_hours": [40.0, 40.0, 40.0],
                "total_hours": [40.0, 8.0, 0.0],
                "billable_hours": [36.0, 8.0, 0.0],
                "utilization": [0.9, 0.2, 0.0],
                "is_bench": [False, False, True],
            }
        ),
    )
    write_deltalake(
        tmp_path / "engagement_summary",
        pd.DataFrame(
            {
                "engagement_code": ["ENG-001"],
                "client_name": ["Northwind Bank"],
                "billing_type": ["TM"],
                "consultants": [2],
                "hours": [44.0],
                "billable_value_usd": [9900.0],
                "unpriced_hours": [0.0],
                "first_work_date": [weeks[0]],
                "last_work_date": [weeks[1]],
            }
        ),
    )
    write_deltalake(
        tmp_path / "fct_time_entries",
        pd.DataFrame({"entry_id": ["TE-1"], "employee_id": ["E001"], "hours": [8.0]}),
    )
    return str(tmp_path)


def _client(gold, generator=None):
    return TestClient(create_app(gold, sql_generator=generator))


def test_app_serves_latest_week_by_default(gold):
    with _client(gold) as client:
        assert client.get("/api/weeks").json() == ["2026-09-21", "2026-09-14"]
        offices = client.get("/api/offices").json()
        assert [o["office"] for o in offices] == ["London"]
        assert client.get("/api/offices?week=2026-09-14").json()[0]["office"] == "Madrid"
        assert client.get("/").status_code == 200


def test_app_bench_report_filters_by_threshold_and_office(gold):
    with _client(gold) as client:
        low = client.get("/api/consultants?below=0.5").json()
        assert [c["employee_id"] for c in low] == ["E003", "E002"]
        madrid = client.get("/api/consultants?below=0.5&office=Madrid").json()
        assert [c["employee_id"] for c in madrid] == ["E003"]


def test_app_ask_runs_generated_sql_in_the_sandbox(gold):
    def fake(question, schema):
        assert "consultant_utilization_weekly(" in schema
        return SqlAnswer(
            "SELECT employee_id FROM consultant_utilization_weekly WHERE is_bench", "bench people"
        )

    with _client(gold, fake) as client:
        body = client.post("/api/ask", json={"question": "who is on the bench?"}).json()
    assert body["rows"] == [{"employee_id": "E003"}]
    assert body["sql"].startswith("SELECT")


def test_app_ask_rejects_unsafe_sql(gold):
    def evil(question, schema):
        return SqlAnswer("DROP TABLE fct_time_entries", "oops")

    with _client(gold, evil) as client:
        res = client.post("/api/ask", json={"question": "delete everything"})
    assert res.status_code == 422
    assert "only SELECT" in res.json()["detail"]


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM t",
        "SELECT 1; DROP TABLE t",
        "COPY t TO 'out.csv'",
        "ATTACH 'x.db'",
        "PRAGMA version",
        "not sql at all",
    ],
)
def test_sql_guard_blocks_everything_but_a_single_select(sql):
    with pytest.raises(sql_guard.UnsafeSQL):
        sql_guard.ensure_select(sql)


def test_sql_guard_sandbox_has_no_file_access():
    con = sql_guard.sandbox({"t": pd.DataFrame({"x": [1, 2]})})
    assert sql_guard.run_select(con, "SELECT sum(x) AS s FROM t")["s"][0] == 3
    with pytest.raises(Exception, match=r"(?i)external|permission|disabled"):
        sql_guard.run_select(con, "SELECT * FROM read_csv('pyproject.toml')")
