variable "region" {
  default = "us-east4"
}

variable "project_id" {
    type = string
    description = "GCP Project ID"
}

variable "private_subnet_names" {
    type = list(string)
    description = "(optional) describe your variable"
    default = [ "private0", "private1", "private2" ]
}

variable "public_subnet_names" {
    type = list(string)
    description = "(optional) describe your variable"
    default = [ "public0", "public1", "public2" ]
}

variable "private_subnets" {
  description = "Private Subnets IP addresses"
  type        = list(string)
  default     = [
    "10.0.1.0/24",
    "10.0.2.0/24",
    "10.0.3.0/24"]
}

variable "public_subnets" {
  description = "Public Subnets IP addresses"
  type        = list(string)
  default     = [
    "10.0.4.0/24",
    "10.0.5.0/24",
    "10.0.6.0/24"]
}