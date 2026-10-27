"""Shared fixtures. The same tests grade both implementations:

uv run pytest                  # reference solution (src/utilization/transforms)
uv run pytest --impl starter   # your attempt (starter/transforms)
"""

from __future__ import annotations

import importlib
import os
import shutil
import time
from types import SimpleNamespace

import pytest

# PySpark returns naive datetimes in the *process* timezone; pin it so assertions are stable.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--impl",
        choices=["solution", "starter"],
        default=os.getenv("UTIL_IMPL", "solution"),
        help="which transforms package the tests import",
    )


def _spark_unavailable() -> str | None:
    if shutil.which("java") is None and not os.getenv("JAVA_HOME"):
        return "Java 17+ not found"
    if os.name == "nt" and not os.getenv("HADOOP_HOME"):
        return "Spark on Windows needs winutils; run `docker compose run --rm dev pytest`"
    return None


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    starter = config.getoption("--impl") == "starter"
    no_spark = _spark_unavailable()
    for item in items:
        if starter and "solution_only" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="not part of the exercise"))
        if no_spark and "spark" in item.keywords:
            item.add_marker(pytest.mark.skip(reason=no_spark))


@pytest.fixture(scope="session")
def impl(request: pytest.FixtureRequest) -> SimpleNamespace:
    base = (
        "starter.transforms"
        if request.config.getoption("--impl") == "starter"
        else "utilization.transforms"
    )
    return SimpleNamespace(
        **{
            stage: importlib.import_module(f"{base}.{stage}")
            for stage in ("bronze", "silver", "gold")
        }
    )


@pytest.fixture(scope="session")
def spark():
    from utilization.spark import get_spark

    session = get_spark("utilization-tests")
    yield session
    session.stop()
