# Terraform configuration 
terraform {
  required_version = ">= 0.12"

  # Using S3 for the backend state storage
  backend "s3" {
    region         = "us-east-1"
    bucket         = "coin-terraform-state"
    key            = "pipeline/terraform.tfstate" #The key name before the / needs to be changed. This needs to be a unique name
  }
}

#Provider configuration. Typically there will only be one provider config, unless working with multi account and / or multi region resources
provider "aws" {
  region = var.region
  version = "~> 2.22"
  profile = "ex"
}

###############
# Data Sources
###############

#This Data Source will read in the current AWS Region
data "aws_region" "current" {}

# This Data source will provide the account number if needed
data "aws_caller_identity" "current" {}

# Use the standard s3 kms key
data "aws_kms_alias" "s3kmskey" {
  name = "alias/aws/s3"
}

data "aws_iam_policy" "AWSGlueMasterPolicy" {
  arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy" "AWSDataPipelineRole" {
  arn = "arn:aws:iam::aws:policy/service-role/AWSDataPipelineRole"
}

data "aws_iam_policy" "AWSGlueServiceRole" {
  arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy" "AmazonEC2RoleforDataPipelineRole" {
  arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2RoleforDataPipelineRole"
}

############
# Resources
############

# # Create Code Commit Repository for loading data for glue/datapipeline
# resource "aws_codecommit_repository" "this" {
#   repository_name = "${var.project}-${var.environment}-repository"
#   description     = "Code repository for Project: ${var.project} and Environment: ${var.environment}"
# }

# # Bucket for pipeline code to be loaded to
# resource "aws_s3_bucket" "codepipeline_bucket" {
#   bucket = "${var.project}-${var.environment}-codepipeline-${var.region}"
#   acl    = "private"
# }

# # Role for codepipeline to assume
# resource "aws_iam_role" "codepipeline_role" {
#   name = "${var.project}-${var.environment}-codepipeline-role"

#   assume_role_policy = <<EOF
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "Principal": {
#         "Service": "codepipeline.amazonaws.com"
#       },
#       "Action": "sts:AssumeRole"
#     }
#   ]
# }
# EOF
# }

# # Policy for codepipeline to allow action to S3 and pull from codecommit
# resource "aws_iam_role_policy" "codepipeline_policy" {
#   name = "${var.project}-${var.environment}-codepipeline-policy"
#   role = "${aws_iam_role.codepipeline_role.id}"

#   policy = <<EOF
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect":"Allow",
#       "Action": [
#         "s3:*"
#       ],
#       "Resource": [
#         "${aws_s3_bucket.codepipeline_bucket.arn}",
#         "${aws_s3_bucket.codepipeline_bucket.arn}/*"
#       ]
#     },
#     {
#       "Action": [
#         "codecommit:CancelUploadArchive",
#         "codecommit:GetBranch",
#         "codecommit:GetCommit",
#         "codecommit:GetUploadArchiveStatus",
#         "codecommit:UploadArchive"
#       ],
#       "Resource": "${aws_codecommit_repository.this.arn}",
#       "Effect": "Allow"
#     },
#     {
#       "Action": [
#         "codebuild:StartBuild",
#         "codebuild:BatchGetBuilds"
#       ],
#       "Resource": "*",
#       "Effect": "Allow"
#     }
#   ]
# }
# EOF
# }

# # CodePipeline job to pull from codecommit and push to S3 for Glue and DataPipeline to consume
# resource "aws_codepipeline" "codepipeline_glue_dp_jobs" {
#   name     = "${var.project}-${var.environment}-code-deployment"
#   role_arn = aws_iam_role.codepipeline_role.arn

#   artifact_store {
#     location = aws_s3_bucket.codepipeline_bucket.id
#     type     = "S3"

#     encryption_key {
#       id   = "${data.aws_kms_alias.s3kmskey.arn}"
#       type = "KMS"
#     }
#   }

#   stage {
#     name = "Source"

#     action {
#       name             = "Source"
#       category         = "Source"
#       owner            = "AWS"
#       provider         = "CodeCommit"
#       version          = "1"
#       output_artifacts = ["source_output"]

#       configuration = {
#         RepositoryName  = "${var.project}-${var.environment}-repository"
#         BranchName   = "master"
#       }
#     }
#   }

# # TODO: need to code out this trim codebuild project
#   stage {
#     name = "Build"

#     action {
#       name            = "Build"
#       category        = "Build"
#       owner           = "AWS"
#       provider        = "CodeBuild"
#       input_artifacts = ["source_output"]
#       output_artifacts = ["build_output"]
#       version         = "1"

#       configuration = {
#         ProjectName = "trim"
#       }
#     }
#   }

#   stage {
#     name = "Deploy"

#     action {
#       name            = "Deploy"
#       category        = "Deploy"
#       owner           = "AWS"
#       provider        = "S3"
#       input_artifacts = ["build_output"]
#       version         = "1"

#       configuration = {
#         BucketName = aws_s3_bucket.codepipeline_bucket.id
#         Extract = true
#         ObjectKey = "gluecode"
#       }
#     }
#   }
# }

# Bucket for datalake files
resource "aws_s3_bucket" "project_datalake_bucket" {
  bucket = "wrk-${var.project}-datalake-${var.environment}"
  acl    = "private"
}

# Bucket for ingest files
resource "aws_s3_bucket" "project_ingest_bucket" {
  bucket = "wrk-${var.project}-ingest-${var.environment}"
  acl    = "private"
}

# Bucket for system files
resource "aws_s3_bucket" "project_system_bucket" {
  bucket = "wrk-${var.project}-system-${var.environment}"
  acl    = "private"

  # # Example Temp rule for S3
  # lifecycle_rule {
  #   id      = "Temp"
  #   prefix  = "Temp/"
  #   enabled = true

  #   expiration {
  #     days = 7
  #   }
  # }

  #   # Example Archive rule for S3
  #   lifecycle_rule {
  #   id      = "Archive"
  #   enabled = true

  #   prefix = "Archive/"

  #   transition {
  #     days          = 30
  #     storage_class = "STANDARD_IA" # or "ONEZONE_IA"
  #   }

  #   transition {
  #     days          = 90
  #     storage_class = "GLACIER"
  #   }

  #   expiration {
  #     days = 180
  #   }
  # }

}

# IAM role for Glue jobs to assume roles 
resource "aws_iam_role" "glue_role" {
  name = "${var.project}-${var.environment}-glue-job-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

# IAM Policy for glue to consume python in S3 for spark job
resource "aws_iam_role_policy" "glue_job_policy" {
  name = "${var.project}-${var.environment}-glue-job-policy"
  role = "${aws_iam_role.glue_role.id}"

  policy = data.aws_iam_policy.AWSGlueMasterPolicy.policy
}

# # Sample Glue job with S3 reference
# resource "aws_glue_job" "this" {
#   name     = "this"
#   role_arn = "${aws_iam_role.glue_role.arn}"
#   max_capacity = 2

#   command {
#     script_location = "s3://${aws_s3_bucket.codepipeline_bucket.bucket}/gluecode/helloworld.py"
#   }
# }

# Role for datapipeline's emr to assume
resource "aws_iam_role" "datapipeline_emr_role" {
  name = "${var.project}-${var.environment}-datapipeline-emr-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "elasticmapreduce.amazonaws.com",
          "datapipeline.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

# Policy for datapipeline's emr to allow action to EC2 and S3
resource "aws_iam_role_policy_attachment" "datapipeline_emr_attachment1" {
  role       = aws_iam_role.datapipeline_emr_role.name
  policy_arn = data.aws_iam_policy.AWSDataPipelineRole.arn
}

# Policy for datapipeline's emr to allow action to Glue
resource "aws_iam_role_policy_attachment" "datapipeline_emr_attachment2" {
  role       = aws_iam_role.datapipeline_emr_role.name
  policy_arn = data.aws_iam_policy.AWSGlueServiceRole.arn
}

# Role for datapipeline's emr resource to assume
resource "aws_iam_role" "datapipeline_emr_resource_role" {
  name = "${var.project}-${var.environment}-datapipeline-emr-resource-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "ec2.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_instance_profile" "datapipeline_emr_resource_role" {
  name = "${var.project}-${var.environment}-datapipeline-emr-resource-role"
  role = "${aws_iam_role.datapipeline_emr_resource_role.name}"
}

# Policy for datapipeline's emr resource to allow Glue access
resource "aws_iam_role_policy" "datapipeline_emr_resource_policy" {
  name = "${var.project}-${var.environment}-datapipeline-emr-resource-policy"
  role = "${aws_iam_role.datapipeline_emr_resource_role.id}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect":"Allow",
      "Action": [
        "glue:*Database*",
        "glue:*Table*",
        "glue:*Partition*"
      ],
      "Resource": [
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
        "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:userDefinedFuntion/*"
      ]
    }
  ]
}
EOF
}

# Policy for datapipeline's emr to allow action to EC2 and S3
resource "aws_iam_role_policy_attachment" "datapipeline_emr_resource_attachment1" {
  role       = aws_iam_role.datapipeline_emr_resource_role.name
  policy_arn = data.aws_iam_policy.AmazonEC2RoleforDataPipelineRole.arn
}

resource "aws_datapipeline_pipeline" "JE" {
    name        = "ppm_controller_JE_tf_${var.environment}"
}

# resource "null_resource" "update_JE_datapipeline_definition" {
#   triggers = {
#     keys = "${uuid()}"
#   }

#   provisioner "local-exec" {
#     command = <<EOF
# aws datapipeline put-pipeline-definition \
# --pipeline-id ${aws_datapipeline_pipeline.JE.id} \
# --pipeline-definition file://ppm_controller_JE_test.json \
# EOF
#   }
# }

resource "aws_datapipeline_pipeline" "MQ" {
    name        = "ppm_controller_MQ_tf_${var.environment}"
}

# resource "null_resource" "update_JE_datapipeline_definition" {
#   triggers = {
#     keys = "${uuid()}"
#   }

#   provisioner "local-exec" {
#     command = <<EOF
# aws datapipeline put-pipeline-definition \
# --pipeline-id ${aws_datapipeline_pipeline.JE.id} \
# --pipeline-definition file://ppm_controller_JE_test.json \
# EOF
#   }
# }
