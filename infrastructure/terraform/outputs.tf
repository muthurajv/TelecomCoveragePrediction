output "data_lake_bucket" {
  value = google_storage_bucket.data_lake.name
}

output "ml_artifacts_bucket" {
  value = google_storage_bucket.ml_artifacts.name
}

output "rf_telemetry_topic" {
  value = google_pubsub_topic.rf_telemetry.id
}

output "rf_deadletter_topic" {
  value = google_pubsub_topic.rf_telemetry_deadletter.id
}

output "pci_api_url" {
  value = google_cloud_run_v2_service.pci_api.uri
}

output "dataflow_sa_email" {
  value = google_service_account.dataflow_sa.email
}

output "vertex_sa_email" {
  value = google_service_account.vertex_sa.email
}
