variable "region" {
  description = "AWS Region. The account and the proof are restricted to Sydney."
  type        = string
  default     = "ap-southeast-2"

  validation {
    condition     = var.region == "ap-southeast-2"
    error_message = "The AWS proof runs only in ap-southeast-2 (Sydney)."
  }
}

variable "name_prefix" {
  description = "Prefix of every resource name (lowercase letters, digits and hyphens)."
  type        = string
  default     = "deliverypulse-proof"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.name_prefix))
    error_message = "name_prefix must be 3-31 lowercase letters, digits or hyphens."
  }
}

variable "object_retention_days" {
  description = "Days before lake objects expire (safety net if teardown is forgotten)."
  type        = number
  default     = 7
}

variable "result_retention_days" {
  description = "Days before Athena query results expire."
  type        = number
  default     = 1
}

variable "per_query_scan_limit_bytes" {
  description = "Athena cancels any query scanning more than this (minimum 10 MB)."
  type        = number
  default     = 104857600 # 100 MiB: at most about USD 0.0005 per query at USD 5 per TB

  validation {
    condition     = var.per_query_scan_limit_bytes >= 10485760 && var.per_query_scan_limit_bytes <= 104857600
    error_message = "per_query_scan_limit_bytes must be between 10 MiB and 100 MiB."
  }
}

variable "projection_date_range" {
  description = "Partition projection range of cleaned_orders (the demo spans Sep 10-30, 2026)."
  type        = string
  default     = "2026-09-01,2026-10-31"
}
