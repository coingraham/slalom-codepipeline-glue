# Terraform configuration 
terraform {
  required_version = ">= 0.12"

  # Using S3 for the backend state storage
  backend "s3" {
    region         = "us-east-1"
    bucket         = "techservices-us-east-1-sharedservices-state-bucket"
    key            = "templaterepo/terraform.tfstate" #The key name before the / needs to be changed. This needs to be a unique name
    dynamodb_table = "techservices-sharedservices-state-table"
  }
}

#Provider configuration. Typically there will only be one provider config, unless working with multi account and / or multi region resources
provider "aws" {
  region = var.region

  assume_role {
    role_arn     = var.role_arn
    session_name = "terraform"
  }
}

###############
# Data Sources
###############

#This Data Source will read in the current AWS Region
data "aws_region" "current" {}

############
# Resources
############

#creates an EC2 instance
resource "aws_instance" "helloworld" {
  ami           = data.aws_ami.amazon-linux-2-ami.id
  instance_type = "t2.micro"
  subnet_id     = data.terraform_remote_state.vpc.outputs.private_subnets[0]

  tags = {
    Name = "helloworld-${data.aws_region.current.name}-ec2"
  }
}
