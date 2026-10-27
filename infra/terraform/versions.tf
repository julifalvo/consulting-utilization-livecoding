terraform {
  required_version = ">= 1.6"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }

  # Remote state, one key per environment. Supplied at init time:
  #   terraform init -backend-config=envs/prod.backend.hcl
  backend "azurerm" {}
}

# Auth from the environment: DATABRICKS_HOST plus a service principal
# (ARM_* / DATABRICKS_CLIENT_ID + secret, or GitHub OIDC in CI).
provider "databricks" {}
