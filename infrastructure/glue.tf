resource "aws_glue_job" "f1_bronze_to_silver" {
  name              = "f1_bronze_to_silver_job"
  role_arn          = aws_iam_role.glue_execution_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  # Fix for Airflow Dynamic Mapping: Allow up to 10 parallel job runs
  execution_property {
    max_concurrent_runs = 10
  }
  
  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts.id}/${aws_s3_object.glue_transform_script.key}"
    python_version  = "3"
  }

  # default arguments  as environment variables for the PySpark script
  default_arguments = {
    "--job-language" = "python"
    "--bronze_bucket" = aws_s3_bucket.data_lake.id
    "--silver_bucket" = aws_s3_bucket.data_lake.id
    "--run_season" = "global"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui" = "true"
  }
}