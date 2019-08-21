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

# # Sample Glue job with S3 reference
# resource "aws_glue_job" "this" {
#   name     = "this"
#   role_arn = "${aws_iam_role.glue_role.arn}"
#   max_capacity = 2

#   command {
#     script_location = "s3://${aws_s3_bucket.codepipeline_bucket.bucket}/gluecode/helloworld.py"
#   }
# }

resource "aws_glue_catalog_database" "ods_database" {
  name = "wrk-${var.project}-ods-${var.environment}"
}

resource "aws_glue_crawler" "ods_transform" {
  database_name = "${aws_glue_catalog_database.ods_database.name}"
  name          = "wrk-${var.project}-ods-transform-${var.environment}"
  role          = "${aws_iam_role.glue_role.arn}"

  s3_target {
    # path = "s3://${aws_s3_bucket.project_datalake_bucket.bucket}/Transformed"
    path = "s3://wrk-datalake-ppm-dev/Transformed"
    exclusions = [
      "**/_SUCCESS"
    ]
  }

#   configuration = <<EOF
# {
#   "Version":1.0,
#   "Grouping": {
#     "TableGroupingPolicy": "CombineCompatibleSchemas"
#   }
# }
# EOF

}

resource "aws_glue_crawler" "ctrl_tables" {
  database_name = "${aws_glue_catalog_database.ods_database.name}"
  name          = "wrk-${var.project}-ctrl-tables-${var.environment}"
  role          = "${aws_iam_role.glue_role.arn}"

  s3_target {
    # path = "s3://${aws_s3_bucket.project_datalake_bucket.bucket}/Logs/AuroraDB/PPMODS"
    path = "s3://wrk-system-ppm-dev/Logs/AuroraDB/PPMODS"
    exclusions = [
      "**/_SUCCESS"
    ]
  }

#   configuration = <<EOF
# {
#   "Version":1.0,
#   "Grouping": {
#     "TableGroupingPolicy": "CombineCompatibleSchemas"
#   }
# }
# EOF

}
