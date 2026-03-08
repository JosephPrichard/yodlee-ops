data "aws_caller_identity" "current" {}

# you must comment this out the very first time `tfe deploy` is executed, it needs the msk cluster to be created on a previous deployment
provider "kafka" {
  bootstrap_servers = split(",", aws_msk_cluster.main.bootstrap_brokers_sasl_iam)
  tls_enabled       = true
  sasl_mechanism    = "aws-iam"
}