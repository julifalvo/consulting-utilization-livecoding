"""Staffing dashboard: a lightweight FastAPI app over the gold tables.

Two ways to load gold, picked by environment:

* local / container: Delta files read with delta-rs (no Spark, no JVM, starts in a second)
* Databricks App: ``DATABRICKS_WAREHOUSE_ID`` is injected by the app resource, and the
  tables are read from Unity Catalog through that SQL warehouse as the app's service principal

    uv run uvicorn dashboard.main:app --app-dir app --reload
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from dashboard import sql_guard
from dashboard.ask import ClaudeSqlGenerator, SqlGenerator, describe

GOLD_TABLES = [
    "office_utilization_weekly",
    "consultant_utilization_weekly",
    "engagement_summary",
    "fct_time_entries",
]
STATIC = Path(__file__).parent / "static"


DATE_COLUMNS = ("week_start", "work_date", "first_work_date", "last_work_date")


def _read_delta(root: str, name: str) -> pd.DataFrame:
    from deltalake import DeltaTable

    return DeltaTable(os.path.join(root, name)).to_pandas()


def _read_warehouse(name: str) -> pd.DataFrame:
    from databricks import sql
    from databricks.sdk.core import Config

    cfg = Config()  # picks up the app's service-principal credentials
    catalog = os.getenv("UTIL_CATALOG", "prod")
    with (
        sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
            credentials_provider=lambda: cfg.authenticate,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(f"SELECT * FROM {catalog}.gold.{name}")
        return cur.fetchall_arrow().to_pandas()


def load_gold(root: str) -> dict[str, pd.DataFrame]:
    on_databricks = bool(os.getenv("DATABRICKS_WAREHOUSE_ID"))
    tables = {}
    for name in GOLD_TABLES:
        df = _read_warehouse(name) if on_databricks else _read_delta(root, name)
        for col in df.columns:
            if col in DATE_COLUMNS:
                df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")
        tables[name] = df
    return tables


def _records(df: pd.DataFrame) -> list[dict]:
    return df.astype(object).where(df.notna(), None).to_dict(orient="records")


class Question(BaseModel):
    question: str = Field(min_length=3, max_length=500)


def create_app(gold_root: str | None = None, sql_generator: SqlGenerator | None = None) -> FastAPI:
    root = gold_root or os.getenv("UTIL_GOLD", "lakehouse/gold")
    state: dict = {}

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        state["tables"] = load_gold(root)
        state["schema"] = describe(state["tables"])
        yield

    api = FastAPI(title="Consultant utilization", lifespan=lifespan)

    def table(name: str) -> pd.DataFrame:
        return state["tables"][name]

    @api.get("/api/health")
    def health() -> dict:
        return {"status": "ok", "tables": {n: len(df) for n, df in state["tables"].items()}}

    @api.get("/api/weeks")
    def weeks() -> list[str]:
        return sorted(table("office_utilization_weekly")["week_start"].unique(), reverse=True)

    @api.get("/api/offices")
    def offices(week: str | None = None) -> list[dict]:
        df = table("office_utilization_weekly")
        week = week or df["week_start"].max()
        return _records(df[df["week_start"] == week].sort_values("utilization", ascending=False))

    @api.get("/api/consultants")
    def consultants(
        week: str | None = None,
        office: str | None = None,
        below: float = Query(1.0, ge=0, description="only utilization below this ratio"),
    ) -> list[dict]:
        df = table("consultant_utilization_weekly")
        week = week or df["week_start"].max()
        df = df[(df["week_start"] == week) & (df["utilization"] < below)]
        if office:
            df = df[df["office"] == office]
        return _records(df.sort_values(["utilization", "employee_id"]))

    @api.get("/api/engagements")
    def engagements() -> list[dict]:
        return _records(
            table("engagement_summary").sort_values("billable_value_usd", ascending=False)
        )

    @api.post("/api/ask")
    def ask(body: Question) -> dict:
        generator = sql_generator
        if generator is None:
            try:
                generator = ClaudeSqlGenerator()
            except Exception as exc:  # SDK missing or no credentials configured
                raise HTTPException(503, f"AI querying is not configured: {exc}") from exc
        try:
            answer = generator(body.question, state["schema"])
        except Exception as exc:
            raise HTTPException(502, f"could not generate SQL: {exc}") from exc
        if not answer.sql:
            return {"sql": "", "explanation": answer.explanation, "columns": [], "rows": []}
        try:
            con = sql_guard.sandbox(state["tables"])
            result = sql_guard.run_select(con, answer.sql)
        except sql_guard.UnsafeSQL as exc:
            raise HTTPException(422, f"rejected SQL: {exc}") from exc
        except Exception as exc:  # the SQL was safe but wrong: show it so the user can see why
            raise HTTPException(422, f"SQL failed: {exc} -- {answer.sql}") from exc
        return {
            "sql": answer.sql,
            "explanation": answer.explanation,
            "columns": list(result.columns),
            "rows": _records(result),
        }

    @api.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC / "index.html")

    return api


app = create_app()
