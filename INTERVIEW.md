# Live coding: firm-wide consultant utilization

> **Format:** 90 minutes, pair-programming style. You share your screen, think out loud, and
> may use documentation and AI assistants as you would at work; be ready to explain every
> line you keep. The interviewer plays the stakeholder: ask them anything.

## Context

You are a data engineer at a global consulting firm. Every Monday the time-tracking SaaS
drops an export of all timesheet entries created or changed since the previous export into
the landing zone. HR, finance and treasury publish reference snapshots alongside it.

Staffing managers decide every Monday morning who gets assigned to new client work. Today
they do it from a spreadsheet someone rebuilds by hand. Finance wants the same numbers to
track the billable value of each engagement.

**Your job: build the firm-wide dataset that answers "how utilized was each consultant and
each office last week, and who is on the bench?"**

```
data/raw/
├── timesheets/timesheets_<export_date>.json   newline-delimited JSON, one file per week
└── reference/
    ├── employees.csv     HR history: employee_id, full_name, email, office, level, valid_from, valid_to
    ├── engagements.csv   engagement_code, client_name, billing_type (TM | FIXED | INTERNAL), currency
    ├── rate_cards.csv    engagement_code, level, bill_rate (in the engagement's currency)
    └── fx_rates.csv      rate_date, currency, usd_per_unit (published on business days)
```

Definitions agreed with the business:

* **Capacity** = 8 hours x working days (Mon-Fri) the person was employed that week.
* **Billable hours** = hours on `TM` or `FIXED` engagements. `INTERNAL` work is not billable.
* **Utilization** = billable hours / capacity.
* **Billable value (USD)** = hours x the rate card for the consultant's level x FX rate of the day.
* Rejected timesheet entries do not count.

**The data is real-world messy.** Spend the first minutes looking at it. Nobody will hand
you the list of problems.

## Stages

You are not expected to finish everything. Depth and reasoning beat coverage.

| # | Stage | Time | What "done" looks like |
|---|-------|------|------------------------|
| 0 | Explore | 5 min | You can name at least three problems in the raw data |
| 1 | **Bronze** | 15 min | Exports land in a table with lineage; a bad line does not kill the job; re-running does not duplicate a file |
| 2 | **Silver** | 30 min | One row per timesheet entry, typed, latest version, bad rows quarantined with a reason, idempotent upsert |
| 3 | **Gold** | 25 min | `fct_time_entries` priced in USD; weekly utilization per consultant and per office, bench included |
| 4 | Ship it | 15 min | Discussion (or code, if time allows): tests, CI/CD, how it runs on Databricks, how the business consumes it |

The function signatures are in `starter/transforms/`. The tests in `tests/` grade them:

```bash
uv run pytest --impl starter -k bronze     # stage 1
uv run pytest --impl starter -k silver     # stage 2
uv run pytest --impl starter -k gold       # stage 3
```

> On Windows, Spark needs the dev container:
> `docker compose run --rm dev pytest --impl starter -k bronze`

## What the interviewer is looking for

| Area | Strong signal |
|------|---------------|
| Data intuition | Looks at the data before coding; spots the traps; asks what the business means by a number |
| Correctness | Explicit schemas, real timestamps, point-in-time joins, no silent row loss, reconciliation |
| Engineering | Pure, testable transforms; small functions; tests with tiny hand-built DataFrames; idempotency |
| Platform | Knows how this maps to Databricks: Auto Loader, Delta MERGE, Unity Catalog, Jobs, Asset Bundles |
| Communication | States assumptions, explains trade-offs, says "I'd confirm this with finance" when it matters |
| AI fluency | Uses an assistant to go faster, and checks its output; explains when it was wrong |

Follow-up questions the interviewer may ask are in
[docs/follow-up-questions.md](docs/follow-up-questions.md). Do not open
[docs/solution-walkthrough.md](docs/solution-walkthrough.md) until you have tried.
