
variable "instance_name" {
  description = "DB Instance name"
}

variable "location" {
    type = string
    description = "Preferred DB location"
}

variable "region" {
    type = string
    description = "Preferred region"
}

# variable "zone" {
#   type = string
# }

variable "database_version" {
  description = "MySQL, Postgres or SQLServer"
}

variable "instance_tier" {
  description = "SQL Instance type"
  default = "db-f1-micro"
}

variable "disk_space" {
    description = "Size of disk"
}

variable "database_name" {
  description = "Identifier for database"
}

variable "db_username" {
    type = string
    description = "Username for database"
}

variable "db_password" {
    type = string
    description = "Password for database"
}