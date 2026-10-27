variable "environment" {
  description = "dev | prod. Used in names and tags."
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "catalog" {
  description = "Existing Unity Catalog catalog (catalogs are owned by the platform team)."
  type        = string
}

variable "pipeline_principal" {
  description = "Application id of the service principal that runs the bundle's job."
  type        = string
}

variable "app_principal" {
  description = "Application id of the Databricks App's service principal (created on first bundle deploy)."
  type        = string
}

variable "ingestion_principal" {
  description = "Principal the time-tracking SaaS connector uses to drop exports into the landing volume."
  type        = string
}

variable "engineers_group" {
  type    = string
  default = "data-engineering"
}

variable "analysts_group" {
  type    = string
  default = "staffing-analysts"
}
