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
  profile = "ex"
}

###############
# Data Sources
###############

#This Data Source will read in the current AWS Region
data "aws_region" "current" {}

############
# Resources
############

# Create a VPC
resource "aws_vpc" "example" {
  cidr_block = "10.0.0.0/16"
}