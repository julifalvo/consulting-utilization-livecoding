"""Natural language -> SQL over the gold tables, with Claude.

The model only *writes* SQL; it never sees data rows and never executes anything. The SQL is
validated and run by sql_guard.py, and both the SQL and the result go back to the user, so
every answer is auditable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

import pandas as pd

MODEL = "claude-opus-5-5"

SYSTEM = """You write DuckDB SQL for a consulting firm's staffing and finance analysts.

Rules:
- Use only the tables and columns listed in the schema. Never invent columns.
- Exactly one SELECT statement. No DDL, DML, PRAGMA or file functions.
- utilization is a ratio (0.85 = 85%). Weeks start on Monday (week_start).
- Aggregate utilization as SUM(billable_hours) / SUM(capacity_hours), never AVG(utilization).
- If the question cannot be answered from these tables, return sql "" and say why."""

ANSWER_SCHEMA = {
    "type": "object",
    "properties": {
        "sql": {"type": "string"},
        "explanation": {"type": "string"},
    },
    "required": ["sql", "explanation"],
    "additionalProperties": False,
}


@dataclass(frozen=True)
class SqlAnswer:
    sql: str
    explanation: str


class SqlGenerator(Protocol):
    def __call__(self, question: str, schema: str) -> SqlAnswer: ...


def describe(tables: dict[str, pd.DataFrame]) -> str:
    lines = []
    for name, df in sorted(tables.items()):
        cols = ", ".join(f"{c} {t}" for c, t in df.dtypes.astype(str).items())
        lines.append(f"{name}({cols})")
    return "\n".join(lines)


class ClaudeSqlGenerator:
    def __init__(self) -> None:
        import anthropic  # imported lazily: the app runs without the SDK or a key

        self.client = anthropic.Anthropic()

    def __call__(self, question: str, schema: str) -> SqlAnswer:
        response = self.client.beta.messages.create(
            model=MODEL,
            max_tokens=16000,
            # Re-run on Anthropic's recommended fallback model if a safety classifier declines.
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            output_config={
                "effort": "medium",
                "format": {"type": "json_schema", "schema": ANSWER_SCHEMA},
            },
            system=SYSTEM,
            messages=[
                {"role": "user", "content": f"Schema:\n{schema}\n\nQuestion: {question}"},
            ],
        )
        if response.stop_reason == "refusal":
            raise RuntimeError("the model declined to answer this question")
        if response.stop_reason == "max_tokens":
            raise RuntimeError("the model ran out of output tokens")
        text = next(b.text for b in response.content if b.type == "text")
        return SqlAnswer(**json.loads(text))
