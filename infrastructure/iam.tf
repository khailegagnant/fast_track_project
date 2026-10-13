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