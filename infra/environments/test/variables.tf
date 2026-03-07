variable "commit_sha" {
  description = "Commit SHA for this specific deployment"
  type        = string
  default     = "ddee415dcf05f9e6d8fb1287c48be0f98831c86a" # overriden by github actions deployment
}