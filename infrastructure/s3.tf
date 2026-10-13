# Main Data Lake Bucket (Bronze & Silver)
resource "aws_s3_bucket" "data_lake" {
  bucket        = var.data_lake_bucket_name
  force_destroy = true
}

# Bucket for Glue job scripts and auxiliary artifacts
resource "aws_s3_bucket" "glue_scripts" {
  bucket        = var.glue_scripts_bucket_name
  force_destroy = true
}

# Upload PySpark transformation script to S3
resource "aws_s3_object" "glue_transform_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/glue_transform.py"
  source = "${path.module}/../src/glue/glue_transform.py"
  etag   = filemd5("${path.module}/../src/glue/glue_transform.py")
}