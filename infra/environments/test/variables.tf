variable "commit_sha" {
  description = "Commit SHA for this specific deployment"
  type        = string
  default     = "" # overriden by github actions deployment
}