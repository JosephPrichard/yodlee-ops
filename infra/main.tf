terraform {
  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "0.8.3"
    }
  }
}
# ── VPC ─────────────────────────────────────────────────────────────────────
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.project}-${var.environment}-vpc"
  cidr = var.vpc_cidr

  azs             = ["us-east-1a", "us-east-1b"]
  public_subnets  = var.public_subnet_cidrs
  private_subnets = var.private_subnet_cidrs

  enable_nat_gateway   = true
  single_nat_gateway   = true  # cost-saving for test env
  enable_dns_hostnames = true
}

# S3
locals {
  buckets = { for env_var_name, topic_name in var.buckets : env_var_name => "${var.project}-${var.environment}-${topic_name}" }
}

resource "aws_s3_bucket" "main" {
  for_each = local.buckets
  bucket   = each.value
}

resource "aws_s3_bucket_public_access_block" "main" {
  for_each                = local.buckets
  bucket                  = aws_s3_bucket.main[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# MSK
resource "aws_security_group" "msk" {
  name   = "${var.project}-${var.environment}-msk-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    description     = "Kafka IAM/TLS from ECS"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

locals {
  msk_cluster_name = "${var.project}-${var.environment}"
}

resource "aws_msk_cluster" "main" {
  cluster_name           = local.msk_cluster_name
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 2  # one per AZ (2 AZs)

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }
}

resource "aws_msk_configuration" "main" {
  name           = "${local.msk_cluster_name}-config"
  kafka_versions = ["3.5.1"]

  server_properties = <<-PROPS
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=1
    num.partitions=64
  PROPS
}

# you must comment this out the very first time `tfe deploy` is executed, it needs the msk cluster to be created on a previous deployment
resource "kafka_topic" "main" {
  for_each           = toset(var.kafka_topics)
  name               = each.key
  replication_factor = var.topic_replication
  partitions         = var.topic_partitions
}

# ALB
resource "aws_security_group" "alb" {
  name   = "${var.project}-${var.environment}-alb-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_lb" "main" {
  name               = "${var.project}-${var.environment}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = module.vpc.public_subnets
}

resource "aws_lb_target_group" "app" {
  name        = "${var.project}-${var.environment}-tg"
  port        = var.container_port
  protocol    = "HTTP"
  vpc_id      = module.vpc.vpc_id
  target_type = "ip"  # required for Fargate

  health_check {
    path                = var.health_check_path
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 30
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.app.arn
  }
}

# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = "${var.project}-${var.environment}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# IAM
resource "aws_iam_role" "ecs_task_execution" {
  name = "${var.project}-${var.environment}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name = "${var.project}-${var.environment}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Allow the task to access all S3 buckets for project
locals {
  s3_buckets_access = [
    for _, bucket in aws_s3_bucket.main : {
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [bucket.arn, "${bucket.arn}/*"]
    }
  ]
}

resource "aws_iam_role_policy" "ecs_task_s3" {
  name = "s3-access"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = local.s3_buckets_access
  })
}

# Allow the task to access all MSK topics for project
# you must comment this out the very first time `tfe deploy` is executed, topics cannot be created on first deployment and policy requires ARN for topics.
locals {
  kafka_topics_access = [
    for topic_name in var.kafka_topics : {
      Effect = "Allow"
      Action = [
        "kafka-cluster:*Topic*",
        "kafka-cluster:ReadData",
        "kafka-cluster:WriteData"
      ]
      # Dynamically construct the ARN from the kafka_topics list
      Resource = "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:topic/${local.msk_cluster_name}/*/${topic_name}"
    }
  ]
}

resource "aws_iam_role_policy" "ecs_task_msk" {
  name = "msk-access"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        # kafka-cluster access
        {
          Effect = "Allow"
          Action = [
            "kafka-cluster:Connect",
            "kafka-cluster:AlterCluster",
            "kafka-cluster:DescribeCluster"
          ]
          Resource = aws_msk_cluster.main.arn
        },
        # kafka group access
        {
          Effect = "Allow"
          Action = [
            "kafka-cluster:AlterGroup",
            "kafka-cluster:DescribeGroup"
          ]
          Resource = "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:group/${local.msk_cluster_name}/*/*"
        }
      ],
      local.kafka_topics_access
    )
  })
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "app" {
  name              = "/ecs/${var.project}-${var.environment}"
  retention_in_days = 30
}

# ECS Task Definition
locals {
  container_env_vars = [
    for k, v in merge(
      local.buckets,
      # { KAFKA_BROKERS = aws_msk_cluster.main.bootstrap_brokers_sasl_iam }
    ) : {
      name = k, value = v
    }
  ]
}

resource "aws_ecs_task_definition" "app" {
  family                   = "${var.project}-${var.environment}-task-definition"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = var.project
    image = "${var.container_image}:${var.image_tag}"

    portMappings = [{
      containerPort = var.container_port
      protocol      = "tcp"
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.app.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "ecs"
      }
    }

    environment = local.container_env_vars
  }])
}

# ECS Security Group
resource "aws_security_group" "ecs" {
  name   = "${var.project}-${var.environment}-ecs-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port       = var.container_port
    to_port         = var.container_port
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]  # only from ALB
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

#  ECS Service
resource "aws_ecs_service" "app" {
  name            = "${var.project}-${var.environment}-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.app.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.private_subnets  # tasks run in private subnets
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.app.arn
    container_name   = var.project
    container_port   = var.container_port
  }

  depends_on = [aws_lb_listener.http]
}