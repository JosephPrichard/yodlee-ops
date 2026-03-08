variable "project" {
  description = "Project name, used as a prefix for all resources"
  type        = string
  default     = "yodlee-ops"
}

variable "environment" {
  description = "Environment name (e.g. test, staging, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.10.0/24", "10.0.20.0/24"]
}

variable "container_port" {
  description = "Port the container listens on"
  type        = number
}

variable "container_image" {
  description = "Docker image to run in ECS (e.g. 123456789.dkr.ecr.us-east-1.amazonaws.com/myapp)"
  type        = string
}

variable "image_tag" {
  description = "Container image SHA for this specific deployment"
  type        = string
}

variable "health_check_path" {
  description = "Path for the ALB health check"
  type        = string
}

variable "task_cpu" {
  description = "Fargate task CPU units (256, 512, 1024, 2048, 4096)"
  type        = number
}

variable "task_memory" {
  description = "Fargate task memory in MB"
  type        = number
}

variable "desired_count" {
  description = "Number of ECS tasks to run"
  type        = number
}

variable "buckets" {
  description = "Names of S3 buckets to create"
  type        = map(string)
  default     = {
    CNCT_BUCKET = "yodlee-cncts"
    ACCT_BUCKET = "yodlee-accts"
    HOLD_BUCKET = "yodlee-holds"
    TXN_BUCKET  = "yodlee-txns"
  }
}

variable "kafka_topics" {
  description = "Kafka topics to create"
  type        = list(string)
  default     = [
    "cnct-refreshes",
    "acct-refreshes",
    "hold-refreshes",
    "txn-refreshes",
    "cnct-responses",
    "acct-responses",
    "hold-responses",
    "txn-responses",
    "delete-retry",
    "broadcast"
  ]
}