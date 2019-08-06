variable "region" {}

variable "environment" {}

variable "role_arn" {}

variable "terraform_workspace" {
  description = "The workspace to refrence for state files"
  type        = "string"
}

variable "state_bucket_name" {
  description = "The bucket in which our state files are stored."
  type        = "string"
}

variable "state_key_vpc" {
  description = "The key name for the VPC root's state file."
  type        = "string"
}
