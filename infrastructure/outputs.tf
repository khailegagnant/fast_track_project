output "data_lake_bucket_name" {
  description = "S3 bucket for Bronze and Silver layers"
  value       = aws_s3_bucket.data_lake.id
}

output "glue_scripts_bucket_name" {
  description = "S3 bucket storing Glue transformation scripts"
  value       = aws_s3_bucket.glue_scripts.id
}

output "glue_job_name" {
  description = "Name of the provisioned Glue transformation job"
  value       = aws_glue_job.f1_bronze_to_silver.name
}

output "glue_role_arn" {
  description = "ARN of the IAM role assumed by Glue"
  value       = aws_iam_role.glue_execution_role.arn
}

output "snowflake_role_arn" {
  value = aws_iam_role.snowflake_storage_role.arn
}