locals {
  layers = {
    raw    = "Landing volumes for files exactly as received"
    bronze = "Raw records with lineage columns, append-only"
    silver = "Typed, deduplicated, validated. Contains PII (employee names and emails)"
    gold   = "Business-ready, PII-free datasets for staffing and finance"
  }
  tags = {
    domain      = "staffing"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "databricks_schema" "layer" {
  for_each     = local.layers
  catalog_name = var.catalog
  name         = each.key
  comment      = each.value
  properties   = local.tags
}

resource "databricks_volume" "landing" {
  catalog_name = var.catalog
  schema_name  = databricks_schema.layer["raw"].name
  name         = "timesheets_landing"
  volume_type  = "MANAGED"
  comment      = "Weekly NDJSON exports from the time-tracking SaaS"
}

# Additive grant: the catalog is shared with other teams, so never manage all of its grants.
resource "databricks_grant" "catalog_use" {
  for_each   = toset([var.pipeline_principal, var.app_principal, var.engineers_group, var.analysts_group])
  catalog    = var.catalog
  principal  = each.value
  privileges = ["USE_CATALOG"]
}

# Schemas are ours, so their grants are authoritative: anything not listed here is revoked.
resource "databricks_grants" "bronze" {
  schema = "${var.catalog}.${databricks_schema.layer["bronze"].name}"
  grant {
    principal  = var.pipeline_principal
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
  grant {
    principal  = var.engineers_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "silver" {
  schema = "${var.catalog}.${databricks_schema.layer["silver"].name}"
  grant {
    principal  = var.pipeline_principal
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
  grant {
    principal  = var.engineers_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
  # No analyst or app access: PII stays here.
}

resource "databricks_grants" "gold" {
  schema = "${var.catalog}.${databricks_schema.layer["gold"].name}"
  grant {
    principal  = var.pipeline_principal
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
  grant {
    principal  = var.engineers_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
  grant {
    principal  = var.analysts_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
  grant {
    principal  = var.app_principal
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "landing" {
  volume = databricks_volume.landing.id
  grant {
    principal  = var.ingestion_principal
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
  grant {
    principal  = var.pipeline_principal
    privileges = ["READ_VOLUME"]
  }
}

resource "databricks_grants" "raw" {
  schema = "${var.catalog}.${databricks_schema.layer["raw"].name}"
  grant {
    principal  = var.ingestion_principal
    privileges = ["USE_SCHEMA"]
  }
  grant {
    principal  = var.pipeline_principal
    privileges = ["USE_SCHEMA"]
  }
}

# Small serverless warehouse for the app: scales to zero after 10 idle minutes.
resource "databricks_sql_endpoint" "app" {
  name                      = "utilization-app-${var.environment}"
  cluster_size              = "2X-Small"
  max_num_clusters          = 1
  auto_stop_mins            = 10
  enable_serverless_compute = true
  warehouse_type            = "PRO"

  tags {
    dynamic "custom_tags" {
      for_each = local.tags
      content {
        key   = custom_tags.key
        value = custom_tags.value
      }
    }
  }
}

resource "databricks_permissions" "app_warehouse" {
  sql_endpoint_id = databricks_sql_endpoint.app.id
  access_control {
    service_principal_name = var.app_principal
    permission_level       = "CAN_USE"
  }
  access_control {
    group_name       = var.engineers_group
    permission_level = "CAN_MANAGE"
  }
}
