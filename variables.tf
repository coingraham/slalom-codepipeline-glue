variable "region" {}

variable "environment" {}

variable "project" {}

variable "terraform_workspace" {
  description = "The workspace to refrence for state files"
  type        = "string"
}
