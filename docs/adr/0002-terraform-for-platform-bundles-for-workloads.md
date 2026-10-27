# ADR 0002: Terraform for the platform, Asset Bundles for the workload

**Status:** accepted

## Context

Both Terraform (Databricks provider) and Databricks Asset Bundles can create jobs, and
Terraform can do everything a bundle does. We need one clear rule for where a resource lives.

## Decision

| Terraform (`infra/terraform`) | Asset Bundle (`databricks.yml`, `resources/`) |
|---|---|
| Schemas, volumes | Wheel build and upload |
| Grants (who can read silver vs gold) | Job, tasks, schedule, alerts |
| SQL warehouse | Databricks App |
| Changes rarely; reviewed by platform owners | Changes every sprint; deployed by CI on merge |

The bundle consumes Terraform outputs (`warehouse_id`) as bundle variables.

## Consequences

* A job change never risks revoking a grant, and a grant change never redeploys code.
* Developers get isolated copies with `databricks bundle deploy -t dev` (`mode: development`
  prefixes names and pauses schedules) without Terraform state access.
* Two tools to learn; the split is documented here and in the README.
