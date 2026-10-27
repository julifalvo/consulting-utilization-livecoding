# Follow-up questions (and strong answers)

The questions an interviewer asks once the code runs. Practise answering out loud in English,
in two or three sentences, then go deeper only if they ask.

## Pipelines and data modelling

**"The volume grows 1000x. What breaks first?"**
Reprocessing all of bronze in silver. I'd make silver incremental: read only new bronze rows
with Delta Change Data Feed or a streaming read with `trigger(availableNow=True)`, and keep
the same MERGE. Gold would rebuild only the weeks touched (`replaceWhere` on `week_start`)
instead of everything. Then I'd look at file layout: liquid clustering on `entry_id` for the
MERGE target and on `week_start` for gold.

**"Why MERGE and not overwrite?"**
Overwrite is fine for gold, which is a pure function of silver. Silver is the system of
record for "latest version of each entry", and the source only sends changes, so I need an
upsert with a version guard (`s.updated_at > t.updated_at`) so replays and out-of-order
files are harmless.

**"What if two exports arrive out of order?"**
Nothing breaks: the MERGE only applies a strictly newer `updated_at`, so an older file
landing later is a no-op for rows that already have a newer version.

**"A correction arrives for a week that is already closed and invoiced. What happens?"**
The pipeline will change history, which is right for staffing but maybe wrong for finance.
I'd ask finance. A common answer is to keep gold "as known now" and publish a period-close
snapshot (a Delta table version or a `closed_periods` table) that invoicing reads from.

**"Why validate before deduplicating?"**
So the latest *valid* version wins. If I deduplicate first, an invalid correction would hide
the previous valid version, and the result would depend on whether I process weekly or
replay everything. The incremental-equals-replay test proves the two converge.

**"How do you know you didn't lose rows?"**
Every bronze row ends up in silver or in quarantine with a reason, and gold hours reconcile
exactly to silver non-rejected hours. That reconciliation is a failing check, not a dashboard.

**"Why not just use the timesheets to compute utilization?"**
Because the people with zero hours disappear, and they are exactly who staffing needs to
see. Utilization starts from a capacity spine: every active employee x working day.

## Databricks

**"How would this run on Databricks?"**
Exports land in a Unity Catalog volume. Bronze would use Auto Loader for file tracking and
schema evolution. The job is a Databricks Asset Bundle: a wheel, four tasks on serverless
compute, a schedule, failure alerts, and dev/prod targets. Tables are Unity Catalog managed
Delta tables in `bronze`/`silver`/`gold` schemas.

**"Why not Lakeflow Declarative Pipelines (DLT)?"**
It would fit well: streaming tables, `APPLY CHANGES` for the upsert, expectations for the
quality rules. I used plain PySpark + a Job here because it runs locally, is easy to unit
test, and is explicit about every step in an interview. In production I'd seriously
consider moving silver to `APPLY CHANGES INTO` with `SEQUENCE BY updated_at`.

**"Unity Catalog: who can see what?"**
Silver has PII (names, emails), so only the pipeline's service principal and engineers can
read it. Gold is PII-free and readable by analysts and by the app's service principal.
All of that is Terraform. For finer control: column masks on `email`, row filters by office.

**"How do you handle schema drift?"**
Bronze reads with an explicit schema that includes the new optional columns, so old files
get NULLs. With Auto Loader I'd use `addNewColumns` and alert on rescued data. A *breaking*
change (a type change, a renamed key) should fail loudly and be handled in a PR, not
absorbed silently.

**"Data lifecycle?"**
Bronze keeps raw data for reprocessing; I'd set a retention period agreed with legal
(e.g. 13 months), then `VACUUM` with the default 7-day window. Quarantine is reviewed weekly
and expires. PII: right-to-erasure requests delete from silver by `employee_id`; gold has
none. Delta history gives time travel for audits and `RESTORE` for bad deploys.

## Engineering practice

**"How is this tested?"**
Transforms are pure functions from DataFrames to DataFrames, tested with tiny hand-built
inputs, one behaviour per test. On top of that, one end-to-end test over the sample, plus
idempotency and replay tests. CI runs everything on every PR with a real local Spark.

**"Walk me through your CI/CD."**
On a PR: lint, tests, `terraform validate`, the bundle checked against the CLI's JSON
schema, and the wheel build. On merge to `main`: `databricks bundle deploy -t prod` using
GitHub OIDC (no long-lived token), behind a GitHub environment with required reviewers.
Terraform runs separately with `plan` on PR and `apply` after review, because platform
changes have a bigger blast radius.

**"Why Terraform *and* Asset Bundles?"**
Different lifecycles and owners. Terraform holds slow-moving, shared platform objects:
schemas, volumes, grants, warehouses. Bundles hold the workload that changes every week:
the job, the code, the app. Mixing them means a job tweak needs a platform review, or a
grant can be dropped by a job deploy. See ADR 0002.

**"How would you roll back a bad deploy?"**
Code: revert the PR, CD redeploys the previous bundle. Data: Delta `RESTORE TABLE ... TO
VERSION AS OF n` on the affected tables, then re-run from bronze. The pipeline is
idempotent, so re-running is safe.

## AI and stakeholders

**"How did you use AI in this exercise?"**
To go faster on the parts I can verify immediately: boilerplate, PySpark API details, test
fixtures. Not to decide semantics. Example: an assistant will happily suggest `to_date`,
which fails under ANSI mode; the test caught it. I keep everything an assistant writes
behind a test.

**"Is the 'ask the data' feature safe?"**
The model never sees rows and never executes anything: it returns SQL. The app parses it
with DuckDB, accepts exactly one `SELECT`, runs it on an in-memory connection with no file
or network access and a locked configuration, and caps the rows. The SQL is shown to the
user so every answer is auditable.

**"A staffing manager says the numbers are wrong. What do you do?"**
Ask for one concrete example: person, week, the number they expected. Trace it through
gold -> silver -> bronze -> the raw file. Usually it's a definition gap (e.g. they count
internal work as utilized), and the fix is to agree on the definition and document it in
the dataset, not to patch the number.
