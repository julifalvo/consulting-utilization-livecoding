# Solution walkthrough

How the reference solution in `src/utilization/` solves the exercise, in the order you would
say it out loud during the interview. Spoilers below.

## Stage 0: what is wrong with the data

Spend five minutes in a notebook (`notebooks/01_explore.py`) or with `head`/`jq`
before writing a transform. Everything below is planted on purpose by `datagen.py`:

| # | Defect | Where | How it surfaces | Handled in |
|---|--------|-------|-----------------|------------|
| 1 | Re-sent rows: export windows overlap | 12 rows per export | same `entry_id` + same `updated_at` in two files | silver `latest_versions` |
| 2 | Late corrections | 15 per export | same `entry_id`, later `updated_at`, different hours | silver `latest_versions` + MERGE condition |
| 3 | `"7,5"` hours from localized UIs (Madrid, Buenos Aires) | ~35% of their rows | `hours` is sometimes a number, sometimes a string | silver `parse_time_entries` |
| 4 | Mixed UTC offsets in `updated_at` | every row | `+02:00`, `-04:00`, `Z` | silver: parse to an instant |
| 5 | **Timezone trap**: `TE-900001` has two versions where text order != time order | export 2 | `"2026-09-11T01:00+02:00"` sorts after `"2026-09-10T23:30Z"` but happened before it | silver: order by the parsed timestamp |
| 6 | Impossible hours: `-4`, `0`, `25` | 3 per export | range check | quarantine `hours_out_of_range` |
| 7 | Unknown engagement `ENG-0O7` (letter O) | export 2 | anti-join with engagements | quarantine `unknown_engagement` |
| 8 | Contractor `C900` unknown to HR | export 3 | anti-join with employees | quarantine `unknown_employee` |
| 9 | Truncated JSON line | export 3 | `_corrupt_record` not null | quarantine `malformed_json` |
| 10 | Schema drift: `is_deleted` appears in export 3 | export 3 | soft deletes arrive as tombstones | explicit schema + MERGE delete |
| 11 | Schema drift: `activity_type` appears in export 4 | export 4 | new optional column | explicit schema (nullable) |
| 12 | Promotions mid-period (E003, E010, E021) | HR history | 2 rows per employee in `employees.csv` | gold point-in-time join |
| 13 | New joiner on a Wednesday (E031), leaver on a Friday (E032) | HR history | `valid_from` / `valid_to` | gold capacity spine |
| 14 | Consultant on the bench (E029 stops logging; E028 only does internal work) | timesheets | **absent rows** | gold capacity spine |
| 15 | No FX on weekends; no GBP on 2026-08-31 (UK bank holiday); no USD rows at all | FX | gaps | silver `fx_daily` forward fill |
| 16 | Missing rate card: ENG-005 has no Principal rate | rate cards | NULL `bill_rate` | quality warning + `unpriced_hours` |
| 17 | PII: `full_name`, `email` | HR | must not reach gold | gold column selection + UC grants |

Result on the sample: 880 raw lines -> 777 silver entries, 15 quarantined, 762 in gold
(15 rejected), reconciliation exact (4,518 h silver = 4,518 h gold).

## Stage 1: bronze

`transforms/bronze.py`

* **Explicit schema, every field a string.** Inference would scan everything and pick
  `bigint` for `hours` until the first `"7,5"` shows up. Bronze stores what arrived; types are
  enforced in silver where a bad value can be quarantined instead of failing the read.
* **`PERMISSIVE` + `columnNameOfCorruptRecord`**: a truncated line becomes a row with
  `_corrupt_record` set. `FAILFAST` would stop the whole firm's reporting for one bad line;
  `DROPMALFORMED` would lose it silently.
* **Lineage**: `_source_file` (from the hidden `_metadata` column) and `_ingested_at`.
* **Idempotency at file level**: `files_to_ingest` skips any file name already in bronze.
  On Databricks this is exactly what **Auto Loader** (`cloudFiles`) gives you through its
  checkpoint, plus schema evolution (`cloudFiles.schemaEvolutionMode`) and rescued data.

## Stage 2: silver

`transforms/silver.py`

1. **Parse** with `try_*` functions. Spark 4 and Databricks run with **ANSI mode on**:
   `to_date("2026-13-01")` raises and kills the job; `try_to_date` returns NULL and the
   validation step gives it a reason. (This bit the reference solution during development:
   the test for `invalid_work_date` caught it.)
2. **Validate, then deduplicate.** Every row gets a `reason` (or NULL). Valid rows are
   deduplicated with `row_number()` over `entry_id` ordered by the *parsed* `updated_at`.
   Validating first means the latest *valid* version wins, so a full replay of bronze and a
   week-by-week incremental run produce the same silver. The end-to-end test
   `test_pipeline_incremental_weekly_runs_match_full_replay` proves it.
3. **Quarantine, never drop.** Invalid rows go to `silver.time_entries_quarantine` with the
   raw payload and a deterministic `quarantine_id` (hash of file + reason + payload), so the
   insert-only MERGE into quarantine is idempotent too.
4. **MERGE into silver**, keyed on `entry_id`:

   ```python
   .whenMatchedDelete(condition="s.updated_at > t.updated_at AND s.is_deleted")
   .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
   .whenNotMatchedInsertAll(condition="NOT s.is_deleted")
   ```

   The strict `>` is what makes re-runs a no-op and stops an older version from overwriting
   a newer one. A tombstone for a row we never had inserts nothing.
5. **Reference data**: employees stay SCD2 with half-open `[valid_from, valid_to)` intervals;
   FX becomes a dense daily calendar with `last(..., ignorenulls=True)` over a window, plus
   USD = 1.0. A dense calendar turns the as-of FX lookup into a plain equi-join.

## Stage 3: gold

`transforms/gold.py`

* **Point-in-time join** to the employee history: `work_date >= valid_from AND (valid_to IS
  NULL OR work_date < valid_to)`. E003's work on Friday 11th is priced at Consultant, on
  Monday 14th at Manager. Half-open intervals guarantee one match; the quality check
  `no overlapping SCD2 versions` protects that assumption, and the reconciliation check
  catches any fan-out.
* **Capacity spine.** Utilization is built from *every active employee x working day*,
  left-joined to the hours, not from the timesheets. Starting from the timesheets is the
  classic bug: E029 logged nothing, so E029 would not exist, and the bench report (the
  whole point for staffing) would be empty. The spine also gives E031 24h of capacity in
  the week they joined on Wednesday.
* **Ratio of sums**, not average of ratios, for office utilization: a two-day new joiner
  must not weigh like a full-time week.
* **No PII in gold**: names and emails stay in silver, which analysts and the app cannot read
  (Terraform grants).
* Gold is a full rebuild: four weeks of data is milliseconds. At firm scale, rebuild only
  the affected `week_start` partitions with `replaceWhere`, driven by the weeks touched in
  the latest silver changes (Change Data Feed).

## Data quality gates

`quality.py` runs after gold. Errors fail the job's last task (and alert); warnings are logged.

| Check | Severity | Why |
|-------|----------|-----|
| `entry_id` unique in silver | error | the MERGE key |
| No overlapping SCD2 versions | error | otherwise the point-in-time join fans out |
| Every entry matched an employee version | error | otherwise office/level is NULL |
| Every entry has an FX rate | error | otherwise value is NULL |
| Gold hours == silver non-rejected hours | error | catches fan-out *and* silent drops |
| Billable entries have a rate card | warn | ENG-005 has no Principal rate: finance must add it |
| Utilization <= 125% | warn | plausible but worth a look |

## Stage 4: shipping it

* **Tests**: pure transforms + tiny hand-built DataFrames (`tests/builders.py`), one end-to-end
  test over the real sample, idempotency and replay-equals-incremental tests.
* **Databricks**: `databricks.yml` + `resources/` deploy a wheel, a 4-task serverless job and
  the app. Terraform (`infra/terraform`) owns schemas, the landing volume, grants and the
  SQL warehouse. See [ADR 0002](adr/0002-terraform-for-platform-bundles-for-workloads.md).
* **CI**: lint, the whole test suite with Java 17, the starter still collects, `terraform
  validate`, the bundle checked against the CLI's JSON schema, and the wheel builds.
  **CD** deploys the bundle on merge to `main` with GitHub OIDC, no stored token.
* **Consumption**: the FastAPI app (`app/`) reads gold through a SQL warehouse as its own
  service principal: utilization by office, bench report, and "ask the data" (Claude writes
  SQL; the app only runs a single validated `SELECT` in a sandbox).
