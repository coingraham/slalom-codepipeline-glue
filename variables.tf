variable "region" {}

variable "environment" {}

variable "project" {}

variable "role_arn" {}

variable "emr_subnet" {
  description = "The workspace to refrence for state files"
  type        = "string"
}

variable "sqoop_emr_subnet" {
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

variable "aurora_instance_count" {
  description = "The aurora rds instance count"
  type        = "string"
}

variable "aurora_subnet_group" {
  description = "The aurora rds subnet group"
  type        = "string"
}

variable "aurora_security_groups" {
  description = "The aurora rds security groups"
  type        = "list"
}

variable "aurora_instance_class" {
  description = "The aurora rds instance class"
  type        = "string"
}

variable "aurora_cluster_parameter_group_name" {
  description = "The aurora rds cluster parameter group name"
  type        = "string"
}

variable "aurora_backup_retention_window" {
  description = "The workspace to refrence for state files"
  type        = "string"
}
