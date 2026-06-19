terraform {
  required_version = ">= 1.7"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }
  backend "gcs" {
    bucket = "telco-pci-tf-state"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ─── GCS Buckets ────────────────────────────────────────────────────────────────

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = false

  versioning { enabled = true }

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }
  lifecycle_rule {
    condition { age = 730 }
    action    { type = "SetStorageClass"; storage_class = "COLDLINE" }
  }
}

resource "google_storage_bucket" "ml_artifacts" {
  name     = "${var.project_id}-ml-artifacts"
  location = var.region
  versioning { enabled = true }
}

resource "google_storage_bucket" "dataflow_temp" {
  name     = "${var.project_id}-dataflow-temp"
  location = var.region
  lifecycle_rule {
    condition { age = 7 }
    action    { type = "Delete" }
  }
}

# ─── BigQuery Datasets ──────────────────────────────────────────────────────────

locals {
  bq_datasets = {
    pci_raw       = "Raw ingested data — immutable"
    pci_curated   = "Validated, schema-normalized data"
    pci_features  = "H3-partitioned feature snapshots"
    pci_scoring   = "Model scoring outputs and SHAP values"
    pci_reporting = "Business KPI and executive reporting tables"
  }
}

resource "google_bigquery_dataset" "datasets" {
  for_each    = local.bq_datasets
  dataset_id  = each.key
  description = each.value
  location    = var.region

  delete_contents_on_destroy = false

  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bq_key.id
  }
}

# ─── BigQuery Reservations (prevents slot contention) ───────────────────────────

resource "google_bigquery_capacity_commitment" "base_slots" {
  location     = var.region
  slot_count   = var.bq_base_slot_count
  plan         = "FLEX"
  renewal_plan = "NONE"
}

resource "google_bigquery_reservation" "streaming_ingest" {
  name          = "streaming-ingest"
  location      = var.region
  slot_capacity = var.streaming_slot_capacity
  ignore_idle_slots = false
}

resource "google_bigquery_reservation" "batch_analytics" {
  name          = "batch-analytics"
  location      = var.region
  slot_capacity = var.batch_slot_capacity
  ignore_idle_slots = true
}

resource "google_bigquery_reservation_assignment" "streaming_assignment" {
  reservation = google_bigquery_reservation.streaming_ingest.id
  assignee    = "projects/${var.project_id}"
  job_type    = "PIPELINE"
}

resource "google_bigquery_reservation_assignment" "batch_assignment" {
  reservation = google_bigquery_reservation.batch_analytics.id
  assignee    = "projects/${var.project_id}"
  job_type    = "QUERY"
}

# ─── Pub/Sub ────────────────────────────────────────────────────────────────────

resource "google_pubsub_topic" "rf_telemetry" {
  name = "rf-telemetry-ingest"
  message_retention_duration = "86400s"
}

resource "google_pubsub_topic" "rf_telemetry_deadletter" {
  name = "rf-telemetry-deadletter"
  message_retention_duration = "604800s"  # 7 days
}

resource "google_pubsub_subscription" "rf_telemetry_dataflow" {
  name  = "rf-telemetry-dataflow-sub"
  topic = google_pubsub_topic.rf_telemetry.name

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"
  retain_acked_messages      = false

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.rf_telemetry_deadletter.id
    max_delivery_attempts = 5
  }

  expiration_policy { ttl = "" }
}

# ─── Dataplex ───────────────────────────────────────────────────────────────────

resource "google_dataplex_lake" "pci_lake" {
  name     = "pci-data-lake"
  location = var.region
  labels   = { env = var.environment }
}

resource "google_dataplex_zone" "raw_zone" {
  lake     = google_dataplex_lake.pci_lake.name
  name     = "raw-zone"
  location = var.region
  type     = "RAW"
  resource_spec { type = "STORAGE_BUCKET" }
  discovery_spec { enabled = true }
}

resource "google_dataplex_zone" "curated_zone" {
  lake     = google_dataplex_lake.pci_lake.name
  name     = "curated-zone"
  location = var.region
  type     = "CURATED"
  resource_spec { type = "BIGQUERY_DATASET" }
  discovery_spec { enabled = true }
}

resource "google_dataplex_asset" "raw_gcs" {
  lake          = google_dataplex_lake.pci_lake.name
  dataplex_zone = google_dataplex_zone.raw_zone.name
  name          = "raw-gcs-asset"
  location      = var.region

  resource_spec {
    name = "projects/${var.project_id}/buckets/${google_storage_bucket.data_lake.name}"
    type = "STORAGE_BUCKET"
  }
  discovery_spec { enabled = true }
}

# ─── KMS for BigQuery Encryption ────────────────────────────────────────────────

resource "google_kms_key_ring" "pci_keyring" {
  name     = "pci-keyring"
  location = var.region
}

resource "google_kms_crypto_key" "bq_key" {
  name            = "bq-encryption-key"
  key_ring        = google_kms_key_ring.pci_keyring.id
  rotation_period = "7776000s"  # 90 days
}

# ─── Service Accounts ───────────────────────────────────────────────────────────

resource "google_service_account" "dataflow_sa" {
  account_id   = "pci-dataflow-sa"
  display_name = "PCI Dataflow Service Account"
}

resource "google_service_account" "vertex_sa" {
  account_id   = "pci-vertex-sa"
  display_name = "PCI Vertex AI Service Account"
}

resource "google_service_account" "api_sa" {
  account_id   = "pci-api-sa"
  display_name = "PCI FastAPI Service Account"
}

# IAM bindings — principle of least privilege
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "vertex_aiplatform" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.vertex_sa.email}"
}

resource "google_project_iam_member" "vertex_bq_reader" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.vertex_sa.email}"
}

resource "google_project_iam_member" "api_bq_reader" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.api_sa.email}"
}

# ─── Cloud Run (FastAPI) ─────────────────────────────────────────────────────────

resource "google_cloud_run_v2_service" "pci_api" {
  name     = "pci-api"
  location = var.region

  template {
    service_account = google_service_account.api_sa.email
    scaling {
      min_instance_count = 1
      max_instance_count = 20
    }
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/pci/api:latest"
      resources {
        limits = { cpu = "2", memory = "2Gi" }
      }
      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "BQ_DATASET_SCORING"
        value = "pci_scoring"
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "api_invoker" {
  location = google_cloud_run_v2_service.pci_api.location
  name     = google_cloud_run_v2_service.pci_api.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.api_sa.email}"
}
