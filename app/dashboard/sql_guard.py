"""Guardrails for model-written SQL. Defense in depth, cheapest check first:

1. exactly one statement, and it must be a SELECT (parsed by DuckDB, not regex)
2. executed on a connection with no file/network access and a locked configuration
3. wrapped in an outer LIMIT so a cartesian product cannot exhaust the app's memory
"""

from __future__ import annotations

import duckdb
import pandas as pd

MAX_ROWS = 200


class UnsafeSQL(ValueError):
    pass


def ensure_select(sql: str) -> str:
    sql = sql.strip().rstrip(";")
    try:
        statements = duckdb.extract_statements(sql)
    except duckdb.Error as exc:
        raise UnsafeSQL(f"not valid SQL: {exc}") from exc
    if len(statements) != 1:
        raise UnsafeSQL("exactly one statement is allowed")
    if statements[0].type != duckdb.StatementType.SELECT:
        raise UnsafeSQL(f"only SELECT is allowed, got {statements[0].type.name}")
    return sql


def sandbox(tables: dict[str, pd.DataFrame]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"enable_external_access": False})
    for name, df in tables.items():
        con.register(name, df)
    con.execute("SET lock_configuration = true")
    return con


def run_select(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    safe = ensure_select(sql)
    return con.execute(f"SELECT * FROM ({safe}) AS q LIMIT {MAX_ROWS}").df()
