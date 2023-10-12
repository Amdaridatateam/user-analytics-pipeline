


variable "project_id" {
  type = string
}

variable "bucket_location" {
  type = string
}


variable "bucket_name" {
    type = string
    description = "Cloud storage bucket name"
}

variable "storage_class" {
  type = string
  default = "STANDARD"
}

variable "environment" {
    type = string
    default = "development"
}