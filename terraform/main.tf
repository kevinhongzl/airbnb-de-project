terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.7.0"
    }
  }
}

provider "google" {
  project     = var.PROJECT // "dbt-cloud-bigquery-450116"
  credentials = "/root/.config/gcloud/application_default_credentials.json"
  // the credential directory bind-mounted in the docker container
}

// Resources
resource "google_storage_bucket" "bucket" {
  name                     = var.BUCKET_NAME // "test-bucket-12352"
  location                 = "US"
  force_destroy            = true
  public_access_prevention = "enforced"
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.DATASET_ID // "dataset_from_client_1234"
  description                = "This is a test description"
  location                   = "US"
  delete_contents_on_destroy = true
  // default_table_expiration_ms = 3600000 // tables removed after an hour
}
