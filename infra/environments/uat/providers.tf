terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # kafka = {
    #   source  = "Mongey/kafka"
    #   version = "~> 0.7"
    # }
  }

  backend "s3" {
    bucket         = "yodlee-ops-tfstate-bucket-store-uat"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "yodlee-ops-tfstate-lock-table-store-uat"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      Project     = "yodlee-ops"
      Environment = "uat"
      ManagedBy   = "terraform"
    }
  }
}

# you must comment this out the very first time `tfe deploy` is executed, it needs the msk cluster to be created on a previous deployment
# provider "kafka" {
#   bootstrap_servers = [aws_msk_cluster.main.bootstrap_brokers_sasl_iam]
#   tls_enabled       = true
#   sasl_mechanism    = "aws-iam"
# }

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}
