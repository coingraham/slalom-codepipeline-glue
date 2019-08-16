# Terraform configuration 
terraform {
  required_version = ">= 0.12"

  # Using S3 for the backend state storage
  backend "s3" {
    region         = "us-east-1"
    bucket         = "wrk-terraform-state-dev"
    key            = "ppm-pipeline/terraform.tfstate" #The key name before the / needs to be changed. This needs to be a unique name
  }
}

#Provider configuration. Typically there will only be one provider config, unless working with multi account and / or multi region resources
provider "aws" {
  region = var.region
  version = "~> 2.22"
  profile = "mwv"
}