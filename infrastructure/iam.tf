# Execution Role for AWS Glue
resource "aws_iam_role" "glue_execution_role" {
  name = "f1_capstone_glue_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}




# 1. IAM Role for Snowflake S3 Access
resource "aws_iam_role" "snowflake_storage_role" {
  name = "f1_capstone_snowflake_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        # This replaces the data.aws_caller_identity line
        AWS = "arn:aws:iam::203883638957:user/adzb2000-s" 
      }
      Condition = {
        StringEquals = {
          "sts:ExternalId" = "UK56841_SFCRole=4_KqUbYhU4Mykb1TMbxjNMCDcmB7g="
        }
      }
    }]
  })
}

# 2. Least-privilege S3 Read-Only Policy for Snowflake
resource "aws_iam_role_policy" "snowflake_s3_access" {
  name = "snowflake_silver_s3_read"
  role = aws_iam_role.snowflake_storage_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket"
      ]
      Effect = "Allow"
      Resource = [
        aws_s3_bucket.data_lake.arn,
        "${aws_s3_bucket.data_lake.arn}/f1/silver/*"
      ]
    }]
  })
}


# AWS Managed Policy for basic Glue runtime capabilities
resource "aws_iam_role_policy_attachment" "glue_service_basic" {
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Least-privilege S3 access policy scoped to the capstone buckets
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue_s3_data_lake_access"
  role = aws_iam_role.glue_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Effect = "Allow"
      Resource = [
        aws_s3_bucket.data_lake.arn,
        "${aws_s3_bucket.data_lake.arn}/*",
        aws_s3_bucket.glue_scripts.arn,
        "${aws_s3_bucket.glue_scripts.arn}/*"
      ]
    }]
  })
}