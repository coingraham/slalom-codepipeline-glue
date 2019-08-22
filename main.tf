###############
# Data Sources
###############

#This Data Source will read in the current AWS Region
data "aws_region" "current" {}

# This Data source will provide the account number if needed
data "aws_caller_identity" "current" {}

# Get the ssm kms key for IAM permissions
data "aws_kms_key" "ssm_key" {
  key_id = "alias/aws/ssm"
}

# Get the ssm kms key for IAM permissions
data "aws_kms_key" "rds_key" {
  key_id = "alias/aws/rds"
}

# Existing Buckets since I can't build more.
data "aws_s3_bucket" "project_datalake_bucket" {
  bucket = "wrk-datalake-${var.project}-dev"
}

data "aws_s3_bucket" "project_ingest_bucket" {
  bucket = "wrk-ingest-${var.project}-dev"
}

data "aws_s3_bucket" "project_system_bucket" {
  bucket = "wrk-system-${var.project}-dev"
}

############
# Resources
############

# # Bucket for datalake files
# resource "aws_s3_bucket" "project_datalake_bucket" {
#   bucket = "wrk-${var.project}-datalake-${var.environment}"
#   acl    = "private"

#   server_side_encryption_configuration {
#     rule {
#       apply_server_side_encryption_by_default {
#         sse_algorithm     = "AES256"
#       }
#     }
#   }
# }

# # Bucket for ingest files
# resource "aws_s3_bucket" "project_ingest_bucket" {
#   bucket = "wrk-${var.project}-ingest-${var.environment}"
#   acl    = "private"

#   server_side_encryption_configuration {
#     rule {
#       apply_server_side_encryption_by_default {
#         sse_algorithm     = "AES256"
#       }
#     }
#   }
# }

# # Bucket for system files
# resource "aws_s3_bucket" "project_system_bucket" {
#   bucket = "wrk-${var.project}-system-${var.environment}"
#   acl    = "private"

#   server_side_encryption_configuration {
#     rule {
#       apply_server_side_encryption_by_default {
#         sse_algorithm     = "AES256"
#       }
#     }
#   }

#   # Example Temp rule for S3
#   lifecycle_rule {
#     id      = "Temp"
#     prefix  = "Temp/"
#     enabled = true

#     expiration {
#       days = 7
#     }
#   }

#     # Example Archive rule for S3
#     lifecycle_rule {
#     id      = "Archive"
#     enabled = true

#     prefix = "Archive/"

#     transition {
#       days          = 30
#       storage_class = "STANDARD_IA" # or "ONEZONE_IA"
#     }

#     transition {
#       days          = 90
#       storage_class = "GLACIER"
#     }

#     expiration {
#       days = 180
#     }
#   }

# }

resource "aws_rds_cluster" "aurora_rds_cluster" {
  cluster_identifier      = "wrk-${var.project}-rps-${var.environment}"
  engine                  = "aurora-postgresql"
  database_name           = "postgres"
  master_username         = "postgres"
  master_password         = "${aws_ssm_parameter.ppm_aurora_database_password.value}"
  backup_retention_period = var.aurora_backup_retention_window
  preferred_backup_window = "07:00-09:00"
  storage_encrypted = true
  kms_key_id = "${data.aws_kms_key.rds_key.arn}"
  db_cluster_parameter_group_name = var.aurora_cluster_parameter_group_name
  db_subnet_group_name = var.aurora_subnet_group
  vpc_security_group_ids = var.aurora_security_groups
}

resource "aws_rds_cluster_instance" "aurora_cluster_instances" {
  count              = var.aurora_instance_count
  identifier         = "wrk-${var.project}-rps-${var.environment}-instance-${count.index + 1}"
  cluster_identifier = "${aws_rds_cluster.aurora_rds_cluster.id}"
  engine             = "aurora-postgresql"
  db_subnet_group_name = var.aurora_subnet_group
  instance_class     = var.aurora_instance_class
}