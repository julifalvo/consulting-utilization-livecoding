"""Thin storage layer: the same pipeline writes to local Delta paths or Unity Catalog tables.

Transforms never touch storage; only this module and pipeline.py do. That is what keeps the
transforms unit-testable with tiny in-memory DataFrames.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class Lakehouse:
    root: str | None = None  # local mode: <root>/<layer>/<table>
    catalog: str | None = None  # Unity Catalog mode: <catalog>.<layer>.<table>

    @classmethod
    def from_env(cls, root: str | None = None, catalog: str | None = None) -> Lakehouse:
        catalog = catalog or os.getenv("UTIL_CATALOG")
        if catalog:
            return cls(catalog=catalog)
        return cls(root=root or os.getenv("UTIL_LAKEHOUSE", "lakehouse"))

    def name(self, layer: str, table: str) -> str:
        if self.catalog:
            return f"{self.catalog}.{layer}.{table}"
        return os.path.abspath(os.path.join(self.root or "lakehouse", layer, table))

    def exists(self, spark: SparkSession, layer: str, table: str) -> bool:
        if self.catalog:
            return spark.catalog.tableExists(self.name(layer, table))
        return DeltaTable.isDeltaTable(spark, self.name(layer, table))

    def read(self, spark: SparkSession, layer: str, table: str) -> DataFrame:
        if self.catalog:
            return spark.read.table(self.name(layer, table))
        return spark.read.format("delta").load(self.name(layer, table))

    def delta(self, spark: SparkSession, layer: str, table: str) -> DeltaTable:
        if self.catalog:
            return DeltaTable.forName(spark, self.name(layer, table))
        return DeltaTable.forPath(spark, self.name(layer, table))

    def write(self, df: DataFrame, layer: str, table: str, mode: str = "overwrite") -> None:
        writer = df.write.format("delta").mode(mode)
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        else:
            writer = writer.option("mergeSchema", "true")
        if self.catalog:
            writer.saveAsTable(self.name(layer, table))
        else:
            writer.save(self.name(layer, table))

    def ensure(self, df: DataFrame, layer: str, table: str) -> DeltaTable:
        """Create an empty table with ``df``'s schema if missing, then return it for MERGE."""
        spark = df.sparkSession
        if not self.exists(spark, layer, table):
            self.write(df.limit(0), layer, table)
        return self.delta(spark, layer, table)
