# ADR 0001: Pure transforms, thin I/O layer

**Status:** accepted

## Context

The pipeline must run in three places: a laptop during an interview, GitHub Actions, and
Databricks with Unity Catalog. Tests must be fast and must not depend on a workspace.

## Decision

* `transforms/` contains only functions `DataFrame(s) -> DataFrame(s)`. They never read,
  write, or know table names. The single exception is `merge_time_entries`, which receives
  a `DeltaTable` handle so the upsert semantics can be tested.
* `lakehouse.py` is the only module that knows whether a table is a local Delta path or a
  Unity Catalog name (`--catalog`).
* `pipeline.py` wires them together; `cli.py` is the entry point for both local runs and
  the Databricks `python_wheel_task`.

## Consequences

* Every business rule is unit-tested with a few hand-built rows (`tests/builders.py`).
* The starter kit can swap in an alternative `transforms` package and reuse every test.
* One indirection to learn (`Lakehouse`), which is cheap.
