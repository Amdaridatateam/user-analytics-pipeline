variable "project_id" {
  type        = string
  description = "GCP Project id"
}

variable "bucket_name" {
  type        = string
  description = "(optional) describe your variable"
}

variable "environment" {
  type        = string
  description = "(optional) describe your variable"
}

variable "cluster_name" {
  type        = string
  description = "K8s Cluster Name"
}

# variable "cluster_location" {
#     type = string
#     description = "Location for k8s cluster"
#     default = "us-central1"
# }

variable "vpc_id" {
  type        = string
  description = "Identifier for vpc"
}

variable "subnet_id" {
  type        = string
  description = "Subnet Identifier"
}

variable "cluster_num_nodes" {
  type        = number
  description = "Number of nodes in k8s cluster"
  default     = 2
}

variable "instance_type" {
  type        = string
  description = "Instance type of each node in k8s cluster"
  default     = "n1-standard-2"
}


# CLOUDSQL
variable "instance_name" {
  description = "DB Instance name"
}

variable "location" {
  type        = string
  description = "Preferred DB location"
}

variable "region" {
  type        = string
  description = "Preferred region"
}

variable "database_version" {
  description = "MySQL, Postgres or SQLServer"
}

variable "instance_tier" {
  description = "SQL Instance type"
  default     = "db-f1-micro"
}

variable "disk_space" {
  description = "Size of disk"
}

variable "database_name" {
  description = "Identifier for database"
}

variable "database_region" {
  type        = string
  description = "region for database"
}

variable "database_location" {
  type        = string
  description = "region for database"
}

variable "db_username" {
  type        = string
  description = "Username for database"
}

variable "db_password" {
  type        = string
  description = "Password for database"
}

variable "zone" {
  type = string
}

variable "bucket_location" {
  type        = string
  description = "Location of bucket"
  default     = "US"
}

variable "cluster_location" {
  description = "location for k8s"
  default     = "us-east4-a"
}
