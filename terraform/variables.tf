variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "West Europe"
}

variable "resource_group_name" {
  type    = string
  default = "rg-churn-pipeline"
}

variable "storage_account_name" {
  description = "Doit être globalement unique, 3-24 caractères, minuscules et chiffres uniquement"
  type        = string
}

variable "eventhub_namespace_name" {
  type    = string
  default = "evhns-churn-pipeline"
}

variable "databricks_workspace_name" {
  type    = string
  default = "dbws-churn-pipeline"
}

variable "adf_name" {
  type    = string
  default = "adf-churn-pipeline"
}
