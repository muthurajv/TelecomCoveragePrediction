variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP region for all resources"
}

variable "environment" {
  type        = string
  default     = "prod"
  description = "Deployment environment (dev | staging | prod)"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "bq_base_slot_count" {
  type        = number
  default     = 500
  description = "Total BigQuery flex slots to commit"
}

variable "streaming_slot_capacity" {
  type        = number
  default     = 100
  description = "Slots reserved for the streaming-ingest reservation"
}

variable "batch_slot_capacity" {
  type        = number
  default     = 400
  description = "Slots reserved for the batch-analytics reservation"
}
