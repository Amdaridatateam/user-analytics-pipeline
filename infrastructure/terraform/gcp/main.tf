terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.80.0"
    }
  }

  required_version = ">=0.13.0"
}

module "storage" {
  source          = "./modules/storage"
  project_id      = var.project_id
  bucket_name     = var.bucket_name
  bucket_location = var.bucket_location

}

module "vpc" {
  source     = "./modules/network"
  project_id = var.project_id
}

module "gke_cluster" {
  source            = "./modules/gke"
  cluster_name      = var.cluster_name
  cluster_location  = var.cluster_location
  cluster_num_nodes = var.cluster_num_nodes
  instance_type     = var.instance_type
  project_id        = var.project_id
  vpc_id            = module.vpc.vpc
  subnet_id         = module.vpc.private_subnets[0]
}

module "cloudsql" {
  source = "./modules/cloudsql"

  region           = var.database_region
  location         = var.database_location
  database_name    = var.database_name
  instance_name    = var.instance_name
  database_version = var.database_version
  disk_space       = var.disk_space
  instance_tier    = var.instance_tier
  db_username      = var.db_username
  db_password      = var.db_password
}


module "bigquery_dwh" {
  source            = "./modules/bigquery"
  project_id        = var.project_id
  dataset_file_path = "${path.module}/resources/dataset.json"
}