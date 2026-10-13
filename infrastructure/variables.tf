variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "data_lake_bucket_name" {
  description = "Name of the main S3 bucket for Bronze and Silver data"
  type        = string
  default     = "khaint17-fast-track-capstone-project-data-bucket"
}

variable "glue_scripts_bucket_name" {
  description = "Name of the S3 bucket to hold PySpark scripts"
  type        = string
  default     = "khaint17-fast-track-capstone-project-glue-scripts-bucket"
}