terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "yodlee-ops-tfstate-bucket-store-test"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "yodlee-ops-tfstate-lock-table-store-test"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      Project     = "yodlee-ops"
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}
