module "infra" {
  source = "../../"

  project           = "yodlee-ops"
  environment       = "test"
  aws_region        = "us-east-1"
  container_port    = 8080
  desired_count     = 4
  task_cpu          = 256
  task_memory       = 512
  health_check_path = "/ping"
  container_image   = "938864279852.dkr.ecr.us-east-1.amazonaws.com/development/yodlee-ops"
  image_tag         =  var.commit_sha
}