output "warehouse_id" {
  description = "Pass to the bundle as --var warehouse_id=..."
  value       = databricks_sql_endpoint.app.id
}

output "landing_volume_path" {
  value = "/Volumes/${var.catalog}/${databricks_schema.layer["raw"].name}/${databricks_volume.landing.name}"
}
