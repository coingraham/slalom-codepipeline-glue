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

resource "aws_codecommit_repository" "this" {
  repository_name = "${var.project}-${var.environment}-repository"
  description     = "Code repository for Project: ${var.project} and Environment: ${var.environment}"
}

resource "aws_s3_bucket" "codepipeline_bucket" {
  bucket = "${var.project}-${var.environment}-codepipeline-${var.region}"
  acl    = "private"
}

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
    }
  ]
}
EOF
}

# data "aws_kms_alias" "s3kmskey" {
#   name = "alias/myKmsKey"
# }

resource "aws_codepipeline" "codepipeline_glue_jobs" {
  name     = "glue_jobs_deployment_pipeline"
  # role_arn = data.aws_iam_role.CodePipelineGlue.arn
  role_arn = aws_iam_role.codepipeline_role.arn

  artifact_store {
    location = aws_s3_bucket.codepipeline_bucket.id
    type     = "S3"

  #   encryption_key {
  #     id   = "${data.aws_kms_alias.s3kmskey.arn}"
  #     type = "KMS"
  #   }
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
        RepositoryName  = "codepipeline"
        BranchName   = "master"
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
      input_artifacts = ["source_output"]
      version         = "1"

      configuration = {
        BucketName = aws_s3_bucket.codepipeline_bucket.id
        Extract = true
        ObjectKey = "gluecode"
      }
    }
  }
}

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

resource "aws_iam_role_policy" "glue_job_policy" {
  name = "${var.project}-${var.environment}-glue-job-policy"
  role = "${aws_iam_role.glue_role.id}"

  policy = data.aws_iam_policy.AWSGlueMasterPolicy.policy
}
resource "aws_glue_job" "example" {
  name     = "example"
  role_arn = "${aws_iam_role.glue_role.arn}"
  max_capacity = 2

  command {
    script_location = "s3://${aws_s3_bucket.codepipeline_bucket.bucket}/gluecode/helloworld.py"
  }
}