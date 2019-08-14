variable "region" {}

variable "environment" {}

variable "project" {}

variable "terraform_workspace" {
  description = "The workspace to refrence for state files"
  type        = "string"
}

variable "emr_subnet" {
  description = "The workspace to refrence for state files"
  type        = "string"
}

variable "emr_master_sg" {
  description = "The workspace to refrence for state files"
  type        = "string"
}

variable "emr_slave_sg" {
  description = "The workspace to refrence for state files"
  type        = "string"
}

variable "emr_service_sg" {
  description = "The workspace to refrence for state files"
  type        = "string"
}
