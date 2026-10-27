"""Entry point used locally (``uv run utilization ...``) and by the Databricks job
(``python_wheel_task`` with ``entry_point: utilization``)."""

from __future__ import annotations

import argparse
import logging
import sys

from utilization import datagen, pipeline, quality
from utilization.lakehouse import Lakehouse

STAGES = ["bronze", "silver", "gold", "quality", "all"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="utilization")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="write the synthetic raw landing zone")
    gen.add_argument("--out", default="data/raw")
    gen.add_argument("--seed", type=int, default=42)

    run = sub.add_parser("run", help="run one stage or the whole pipeline")
    run.add_argument("--stage", choices=STAGES, default="all")
    run.add_argument("--landing", default="data/raw", help="raw files (a UC volume on Databricks)")
    run.add_argument("--lakehouse", default=None, help="local Delta root (local mode)")
    run.add_argument("--catalog", default=None, help="Unity Catalog catalog (Databricks mode)")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    if args.command == "generate":
        for name, rows in datagen.generate(args.out, args.seed).items():
            print(f"{name}: {rows} lines")
        return 0

    from utilization.spark import get_spark

    spark = get_spark()
    lh = Lakehouse.from_env(root=args.lakehouse, catalog=args.catalog)
    if args.stage == "bronze":
        pipeline.run_bronze(spark, lh, args.landing)
    elif args.stage == "silver":
        pipeline.run_silver(spark, lh)
    elif args.stage == "gold":
        pipeline.run_gold(spark, lh)
    else:
        results = (
            pipeline.run_quality(spark, lh)
            if args.stage == "quality"
            else pipeline.run_all(spark, lh, args.landing)
        )
        failed = quality.failed_errors(results)
        if failed:
            print(f"{len(failed)} data-quality error(s): " + "; ".join(r.name for r in failed))
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
