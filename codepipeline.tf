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