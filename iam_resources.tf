###############
# Data Sources
###############

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

# Policy for glue's access to S3 data buckets
resource "aws_iam_role_policy" "glue_s3_access_policy" {
  name = "${var.project}-${var.environment}-glue-s3-policy"
  role = "${aws_iam_role.glue_role.id}"

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
        "arn:aws:s3:::${data.aws_s3_bucket.project_ingest_bucket.id}",
        "arn:aws:s3:::${data.aws_s3_bucket.project_ingest_bucket.id}/*",
        "arn:aws:s3:::${data.aws_s3_bucket.project_system_bucket.id}",
        "arn:aws:s3:::${data.aws_s3_bucket.project_system_bucket.id}/*",
        "arn:aws:s3:::${data.aws_s3_bucket.project_datalake_bucket.id}",
        "arn:aws:s3:::${data.aws_s3_bucket.project_datalake_bucket.id}/*"
      ]
    }
  ]
}
EOF
}

# IAM Policy for glue to consume python in S3 for spark job
resource "aws_iam_role_policy_attachment" "glue_job_policy" {
  role = "${aws_iam_role.glue_role.name}"
  policy_arn = "${data.aws_iam_policy.AWSGlueMasterPolicy.arn}"
}

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
        "glue:*"
      ],
      "Resource": [
        "*"
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

