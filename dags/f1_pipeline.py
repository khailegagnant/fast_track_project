import os
from datetime import datetime
from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
# Load variables from .env
load_dotenv()

# Get environment variables (with fallbacks just in case)
S3_BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET", "khaint17-fast-track-capstone-project-data-bucket")
S3_SILVER_BUCKET = os.getenv("S3_SILVER_BUCKET", "khaint17-fast-track-capstone-project-data-bucket")
S3_BRONZE_PREFIX = os.getenv("S3_BRONZE_PREFIX", "f1/bronze ") 
S3_SILVER_PREFIX = os.getenv("S3_SILVER_PREFIX", "f1/silver")

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
}

@dag(
    dag_id='f1_bronze_to_silver_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['capstone', 'f1', 'glue'],
    params={
        "seasons": Param(["global", "2026"], type="array", items={"type": "string"}, description="Seasons to extract and process")
    }
)
def f1_pipeline():

    extract_to_bronze = BashOperator(
        task_id='extract_api_to_bronze',
        bash_command='python /opt/airflow/src/scraper/extract_jolci_api.py --seasons "{{ params.seasons | join(\',\') }}"'
    )

    # 1. Update this function to include the bucket and prefix arguments
    @task
    def generate_glue_args(**context):
        seasons_list = context['params']['seasons']
        
        # Build the argument dictionary for EACH season run
        return [
            {
                "--run_season": season,
                "--bronze_bucket": S3_BRONZE_BUCKET,
                "--silver_bucket": S3_SILVER_BUCKET,
                "--bronze_prefix": S3_BRONZE_PREFIX,
                "--silver_prefix": S3_SILVER_PREFIX
            } 
            for season in seasons_list
        ]

    glue_mapped_args = generate_glue_args()

    # 2. Dynamic Task Mapping will automatically pass the newly structured dictionary
    transform_to_silver = GlueJobOperator.partial(
        task_id='transform_silver',
        job_name='f1_bronze_to_silver_job',
        aws_conn_id='aws_default', 
        region_name='us-east-1',
        wait_for_completion=True,
    ).expand(
        script_args=glue_mapped_args
    )

    @task
    def generate_snowflake_queries(**context):
        seasons_list = context['params']['seasons']
        queries = []
        
        for season in seasons_list:
            if season == "global":
                sql = """
                BEGIN;
                TRUNCATE TABLE raw_circuits;
                COPY INTO raw_circuits 
                    FROM @f1_silver_stage/circuits/season=global/
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                COMMIT;
                """
                queries.append({"sql": sql})
            else:
                # Group all seasonal tables into one transaction block
                sql = f"""
                BEGIN;
                
                -- 1. Tables with a 'season' column (Idempotent Overwrite)
                DELETE FROM raw_races WHERE season = {season};
                COPY INTO raw_races FROM @f1_silver_stage/races/season={season}/ MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                
                DELETE FROM raw_qualifying WHERE season = {season};
                COPY INTO raw_qualifying FROM @f1_silver_stage/qualifying/season={season}/ MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                
                DELETE FROM raw_results WHERE season = {season};
                COPY INTO raw_results FROM @f1_silver_stage/results/season={season}/ MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                
                -- 2. Tables without a 'season' column (Snowflake tracks loaded files automatically)
                COPY INTO raw_drivers FROM @f1_silver_stage/drivers/season={season}/ MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                
                COPY INTO raw_constructors FROM @f1_silver_stage/constructors/season={season}/ MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                
                COMMIT;
                """
                queries.append({"sql": sql})
                
        return queries

    snowflake_mapped_queries = generate_snowflake_queries()

# Change SnowflakeOperator to SQLExecuteQueryOperator
    load_to_snowflake = SQLExecuteQueryOperator.partial(
        task_id='load_to_snowflake',
        conn_id='snowflake_default',  
    ).expand(
        sql=snowflake_mapped_queries.map(lambda x: x["sql"])
    )




    with TaskGroup("dbt_transformations", tooltip="Run dbt models sequentially") as dbt_group:
        
        
        DBT_PROJECT_DIR = "/opt/airflow/dbt/fast_track" 
        
        dbt_commands = [
            ("dbt_deps", 'dbt deps'),
            ("dbt_test_sources", 'dbt test --select "source:*"'),
            ("dbt_build_staging", 'dbt build --select staging --exclude "tag:cross_layer"'),
            ("dbt_build_silver", 'dbt build --select silver --exclude "tag:cross_layer"'),
            ("dbt_snapshot", 'dbt snapshot'),
            ("dbt_test_snapshots", 'dbt test --select "snap_f1__drivers snap_f1__constructors"'),
            ("dbt_build_mart", 'dbt build --select mart --exclude "tag:cross_layer"'),
            ("dbt_test_cross_layer", 'dbt test --select "tag:cross_layer"')
        ]

        previous_task = None
        
        for task_id, command in dbt_commands:
            dbt_task = BashOperator(
                task_id=task_id,
                bash_command=f"cd {DBT_PROJECT_DIR} && {command}",
                env={
                    **os.environ,
                    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
                    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
                    "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE"),
                    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
                    "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
                    # Updated to point to fast_track_code/.ssh/snowflake_key.p8
                    "SNOWFLAKE_PRIVATE_KEY_PATH": "/opt/airflow/.ssh/snowflake_key.p8"
                }
            )
            
            if previous_task:
                previous_task >> dbt_task
            previous_task = dbt_task

    # ==========================================
    # Final Dependency Flow
    # ==========================================
    extract_to_bronze >> transform_to_silver >> load_to_snowflake >> dbt_group

f1_pipeline()