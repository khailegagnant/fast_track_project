# End-to-End F1 Data Pipeline

This repository contains an end-to-end ELT (Extract, Load, Transform) pipeline that ingests Formula 1 data from the Jolpica API, processes it through an AWS Data Lake, and loads it into Snowflake for dimensional modeling using dbt. 

The entire workflow is orchestrated using Apache Airflow and containerized via Docker, with infrastructure provisioned as code using Terraform.

##  Pipeline Architecture

The pipeline follows a modern ELT architecture:

1. **Extract (Python to S3 Bronze):** A custom Python scraper extracts JSON data from the Jolpica F1 API (handling seasons, circuits, constructors, drivers, qualifying, races, and results) and lands the raw JSON files into an AWS S3 Bronze bucket (`bronze/f1`).
2. **Transform (AWS Glue to S3 Silver):** An AWS Glue (PySpark) job reads the raw JSON, flattens the nested structures, and writes the output as Snappy-compressed Parquet files into an S3 Silver bucket (`f1/silver`).
3. **Load (Snowflake RAW):** Snowflake natively ingests the Parquet files from the Silver S3 bucket using an external stage and a secure Storage Integration. The load is idempotent (`DELETE` followed by `COPY INTO`) to ensure data consistency during reruns.
4. **Transform (dbt):** Data transformations (cleansing, deduplication, dimensional modeling, and SCD Type 2 snapshots) are handled by dbt within Snowflake.
    * *Note: For detailed documentation on the dbt architecture, models, and testing strategy, please refer to [`fast_track_code/dbt/fast_track/dbt_docs.md`](dbt/fast_track/dbt_docs.md).*
5. **Orchestrate (Airflow):** Apache Airflow (v3.0) manages the entire execution graph, passing dynamic parameters (like which F1 season to process) downstream through dynamic task mapping and sequential `TaskGroups`.

##  Prerequisites

* **Docker & Docker Compose:** For running the local Airflow environment.
* **Terraform:** For provisioning AWS infrastructure.
* **AWS Account:** With credentials configured locally (or via `.env`).
* **Snowflake Account:** With `ACCOUNTADMIN` access to create integrations and roles.
* **RSA Key Pair:** Generated for Snowflake service account authentication (`snowflake_key.p8`).

##  Project Setup

### 1. Infrastructure Provisioning (Terraform)
Navigate to the `infrastructure/` directory to deploy the AWS resources (S3 buckets, Glue jobs, IAM roles).

*Note on Snowflake/AWS Handshake:* 
1. Run a preliminary `terraform apply` to create the S3 buckets.
2. Create the Storage Integration in Snowflake pointing to the S3 bucket.
3. Run `DESC INTEGRATION <integration_name>;` in Snowflake to retrieve the `STORAGE_AWS_EXTERNAL_ID`.
4. Update the `iam.tf` file with this External ID and run `terraform apply` again to finalize the secure handshake.

### 2. Environment Variables
Create a `.env` file in your Airflow project root with the following variables:
```env
S3_BRONZE_BUCKET="<your-bucket-name>"
S3_SILVER_BUCKET="<your-bucket-name>"
S3_BRONZE_PREFIX="bronze/f1"
S3_SILVER_PREFIX="f1/silver"

SNOWFLAKE_ACCOUNT="<your-account-id>"
SNOWFLAKE_USER="SVC_F1_PIPELINE"
SNOWFLAKE_ROLE="F1_DATA_ENGINEER_ROLE"
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
SNOWFLAKE_DATABASE="F1_CAPSTONE"
```

### 3. Start Airflow (Docker)
Ensure your `docker-compose.yaml` has the necessary volume mounts for your DAGs, dbt models, and SSH keys:
```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./dbt:/opt/airflow/dbt
  - ./.ssh:/opt/airflow/.ssh
```
Spin up the cluster:
```bash
docker compose up -d
```

### 4. Configure Airflow Connections
Open the Airflow UI (typically `http://localhost:8080`) and configure the Snowflake connection:
* **Connection ID:** `snowflake_default`
* **Connection Type:** `Snowflake`
* **Login:** `SVC_F1_PIPELINE`
* **Password:** *(Leave blank)*
* **Extra (JSON):**
```json
{
  "account": "<your-account-id>",
  "database": "F1_CAPSTONE",
  "schema": "RAW",
  "warehouse": "COMPUTE_WH",
  "role": "F1_DATA_ENGINEER_ROLE",
  "private_key_file": "/opt/airflow/.ssh/snowflake_key.p8"
}
```

## ▶ How to Run the Pipeline

1. Navigate to the Airflow Web UI.
2. Unpause the `f1_bronze_to_silver_pipeline` DAG.
3. Click the **Trigger DAG w/ config** button (the play icon).
4. Provide the seasons you wish to process as a JSON array. For example:
```json
{
  "seasons": ["global", "2022", "2026"]
}
```
5. Click **Trigger**. 

The DAG will dynamically map the extraction, Glue transformations, and Snowflake loads for each season provided in the configuration. Once the raw data is loaded, it will sequentially execute the dbt `TaskGroup` to build the staging views, silver tables, snapshots, and final data marts.