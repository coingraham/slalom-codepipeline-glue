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

# Use the standard s3 kms key
data "aws_kms_alias" "s3kmskey" {
  name = "alias/aws/s3"
}

# Use the existing code commit repo
data "aws_codecommit_repository" "glue_repo" {
  repository_name = "codepipeline"
}

# Use the existing codepipeline role
data "aws_iam_role" "CodePipelineGlue" {
  name = "AWSCodePipelineServiceRole-us-east-1-GlueDeploy"
}

data "aws_iam_policy" "AWSGlueMasterPolicy" {
  arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
############
# Resources
############

# Create Code Commit Repository for loading data for glue/datapipeline
resource "aws_codecommit_repository" "this" {
  repository_name = "${var.project}-${var.environment}-repository"
  description     = "Code repository for Project: ${var.project} and Environment: ${var.environment}"
}

# Bucket for pipeline code to be loaded to
resource "aws_s3_bucket" "codepipeline_bucket" {
  bucket = "${var.project}-${var.environment}-codepipeline-${var.region}"
  acl    = "private"
}

# Role for codepipeline to assume
resource "aws_iam_role" "codepipeline_role" {
  name = "${var.project}-${var.environment}-codepipeline-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "codepipeline.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

# Policy for codepipeline to allow action to S3 and pull from codecommit
resource "aws_iam_role_policy" "codepipeline_policy" {
  name = "codepipeline_policy"
  role = "${aws_iam_role.codepipeline_role.id}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect":"Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "${aws_s3_bucket.codepipeline_bucket.arn}",
        "${aws_s3_bucket.codepipeline_bucket.arn}/*"
      ]
    },
    {
      "Action": [
        "codecommit:CancelUploadArchive",
        "codecommit:GetBranch",
        "codecommit:GetCommit",
        "codecommit:GetUploadArchiveStatus",
        "codecommit:UploadArchive"
      ],
      "Resource": "${aws_codecommit_repository.this.arn}",
      "Effect": "Allow"
    },
    {
      "Action": [
        "codebuild:StartBuild",
        "codebuild:BatchGetBuilds"
      ],
      "Resource": "*",
      "Effect": "Allow"
    }
  ]
}
EOF
}

# CodePipeline job to pull from codecommit and push to S3 for Glue and DataPipeline to consume
resource "aws_codepipeline" "codepipeline_glue_dp_jobs" {
  name     = "glue_jobs_datapipeline_deployment_pipeline"
  role_arn = aws_iam_role.codepipeline_role.arn

  artifact_store {
    location = aws_s3_bucket.codepipeline_bucket.id
    type     = "S3"

    encryption_key {
      id   = "${data.aws_kms_alias.s3kmskey.arn}"
      type = "KMS"
    }
  }

  stage {
    name = "Source"

    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeCommit"
      version          = "1"
      output_artifacts = ["source_output"]

      configuration = {
        RepositoryName  = "${var.project}-${var.environment}-repository"
        BranchName   = "master"
      }
    }
  }

  stage {
    name = "Build"

    action {
      name            = "Build"
      category        = "Build"
      owner           = "AWS"
      provider        = "CodeBuild"
      input_artifacts = ["source_output"]
      output_artifacts = ["build_output"]
      version         = "1"

      configuration = {
        ProjectName = "trim"
      }
    }
  }

  stage {
    name = "Deploy"

    action {
      name            = "Deploy"
      category        = "Deploy"
      owner           = "AWS"
      provider        = "S3"
      input_artifacts = ["build_output"]
      version         = "1"

      configuration = {
        BucketName = aws_s3_bucket.codepipeline_bucket.id
        Extract = true
        ObjectKey = "gluecode"
      }
    }
  }
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

# Sample Glue job with S3 reference
resource "aws_glue_job" "this" {
  name     = "this"
  role_arn = "${aws_iam_role.glue_role.arn}"
  max_capacity = 2

  command {
    script_location = "s3://${aws_s3_bucket.codepipeline_bucket.bucket}/gluecode/helloworld.py"
  }
}

# S3 Bucket for EMR Job logging.
resource "aws_s3_bucket" "emr_job_logging_bucket" {
  bucket = "${var.project}-${var.environment}-emr-${var.region}"
  acl    = "private"
}

# # Sample Datapipeline with S3 reference
# resource "aws_datapipeline_pipeline" "this" {
#     name        = "aws_datapipeline_job"
# }