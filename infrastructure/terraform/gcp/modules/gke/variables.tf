variable "project_id" {
    type = string
    description = "GCP Project id"
}

variable "cluster_name" {
    type = string
    description = "K8s Cluster Name"
}

variable "cluster_location" {
    type = string
    description = "Location for k8s cluster"
    default = "us-east1-a"
}

variable "vpc_id" {
    type = string
    description = "Identifier for vpc"
}

variable "subnet_id" {
    type = string
    description = "Subnet Identifier"
}

variable "cluster_num_nodes" {
    type = number
    description = "Number of nodes in k8s cluster"
    default = 2
}

variable "instance_type" {
    type = string
    description = "Instance type of each node in k8s cluster"
    default = "n1-standard-2"
}