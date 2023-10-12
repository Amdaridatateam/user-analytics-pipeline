

resource "google_storage_bucket" "storage_bucket" {
  name = var.bucket_name
  location = var.bucket_location
  storage_class = var.storage_class
  force_destroy = true

  labels = {
    environment = var.environment
  }
}