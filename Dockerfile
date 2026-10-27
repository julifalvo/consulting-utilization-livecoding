# Dev container: Linux + Java 17 + uv, so PySpark/Delta behave exactly as on CI and Databricks.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless make \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /usr/local/bin/uv

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    PATH="/opt/venv/bin:$PATH" \
    TZ=UTC

WORKDIR /workspace
COPY pyproject.toml uv.lock README.md ./
RUN mkdir -p src/utilization && touch src/utilization/__init__.py \
    && uv sync --frozen --all-extras --no-install-project

COPY . .
RUN uv sync --frozen --all-extras \
    # Warm the Ivy cache with the Delta jars so the first Spark start needs no network.
    && python -c "from utilization.spark import get_spark; get_spark().stop()"

CMD ["bash"]
