## What and why

<!-- One paragraph: the problem, and the change that fixes it. Link the ticket. -->

## How it was verified

- [ ] `uv run pytest` green locally (or in the dev container)
- [ ] New/changed transform has a test with a tiny hand-built DataFrame
- [ ] Re-running the job is still a no-op (idempotency)
- [ ] `terraform plan` / `databricks bundle validate` reviewed, if infra changed

## Data impact

<!-- Which tables change, is a backfill needed, does any consumer (app, dashboards) break? -->

## Rollback

<!-- Revert the PR? Delta RESTORE to version N? Re-run from bronze? -->
