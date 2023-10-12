output "bucket_id" {
    value = google_storage_bucket.storage_bucket.id
}

output "bucket_name" {
    value = google_storage_bucket.storage_bucket.name
}

output "bucket_location" {
    value = google_storage_bucket.storage_bucket.location
}


output "bucket_project" {
  value = var.project_id
}

output "premier_pro" {
    value = "premier"
}