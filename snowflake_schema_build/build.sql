CREATE ROLE IF NOT EXISTS F1_DATA_ENGINEER_ROLE;

-- Grant role to yourself (SYSADMIN) for management
GRANT ROLE F1_DATA_ENGINEER_ROLE TO ROLE SYSADMIN;

-- 2. Create Compute Warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

GRANT USAGE, OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE F1_DATA_ENGINEER_ROLE;

-- 3. Create Database & Schemas
CREATE DATABASE IF NOT EXISTS F1_CAPSTONE;
CREATE SCHEMA IF NOT EXISTS F1_CAPSTONE.RAW;
CREATE SCHEMA IF NOT EXISTS F1_CAPSTONE.MARTS;
CREATE SCHEMA IF NOT EXISTS F1_CAPSTONE.SNAPSHOTS;

-- Grant database & schema privileges to the role
GRANT ALL PRIVILEGES ON DATABASE F1_CAPSTONE TO ROLE F1_DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA F1_CAPSTONE.RAW TO ROLE F1_DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA F1_CAPSTONE.MARTS TO ROLE F1_DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA F1_CAPSTONE.SNAPSHOTS TO ROLE F1_DATA_ENGINEER_ROLE;

-- Grant privileges for future tables/views created by dbt or scripts
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA F1_CAPSTONE.RAW TO ROLE F1_DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA F1_CAPSTONE.MARTS TO ROLE F1_DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA F1_CAPSTONE.SNAPSHOTS TO ROLE F1_DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA F1_CAPSTONE.RAW TO ROLE F1_DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA F1_CAPSTONE.MARTS TO ROLE F1_DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA F1_CAPSTONE.SNAPSHOTS TO ROLE F1_DATA_ENGINEER_ROLE;

-- 4. Create Dedicated Service User with Key-Pair Auth
-- (Replace the RSA_PUBLIC_KEY string with the single-line string from snowflake_key.pub)
CREATE USER IF NOT EXISTS SVC_F1_PIPELINE
    TYPE = SERVICE
    DEFAULT_ROLE = F1_DATA_ENGINEER_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH
    RSA_PUBLIC_KEY = '<redacted>' --I ain't falling for this :)
    COMMENT = 'Service account for F1 Capstone Airflow and dbt pipeline';

-- 5. Grant Role to the Service User
GRANT ROLE F1_DATA_ENGINEER_ROLE TO USER SVC_F1_PIPELINE;







USE DATABASE F1_CAPSTONE;
USE SCHEMA RAW;

-- 1. Ensure the file format exists and grant access
CREATE OR REPLACE FILE FORMAT f1_parquet_format
  TYPE = PARQUET
  COMPRESSION = SNAPPY;
  
GRANT USAGE ON FILE FORMAT f1_parquet_format TO ROLE F1_DATA_ENGINEER_ROLE;

-- 2. Ensure the stage exists and grant access
CREATE OR REPLACE STAGE f1_silver_stage
  URL = 's3://khaint17-fast-track-capstone-project-data-bucket/f1/silver'
  STORAGE_INTEGRATION = s3_f1_integration
  FILE_FORMAT = f1_parquet_format;

GRANT USAGE, READ ON STAGE f1_silver_stage TO ROLE F1_DATA_ENGINEER_ROLE;





-- ==========================================
-- 2. CREATE RAW TABLES
-- ==========================================

CREATE TABLE IF NOT EXISTS raw_circuits (
    circuit_id STRING,
    circuit_name STRING,
    locality STRING,
    country STRING,
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE IF NOT EXISTS raw_constructors (
    constructor_id STRING,
    name STRING,
    nationality STRING
);

CREATE TABLE IF NOT EXISTS raw_drivers (
    driver_id STRING,
    permanent_number INT,
    driver_code STRING,
    given_name STRING,
    family_name STRING,
    date_of_birth DATE,
    nationality STRING
);

CREATE TABLE IF NOT EXISTS raw_races (
    season INT,
    round INT,
    race_name STRING,
    circuit_id STRING,
    race_date DATE,
    race_time STRING,
    race_timestamp_utc TIMESTAMP,
    race_url STRING
);

CREATE TABLE IF NOT EXISTS raw_qualifying (
    season INT,
    round INT,
    driver_id STRING,
    constructor_id STRING,
    position INT,
    q1_time_str STRING,
    q2_time_str STRING,
    q3_time_str STRING
);

CREATE TABLE IF NOT EXISTS raw_results (
    season INT,
    round INT,
    driver_id STRING,
    constructor_id STRING,
    grid_position INT,
    finish_position INT,
    position_text STRING,
    points_scored FLOAT,
    laps_completed INT,
    status STRING,
    race_time_millis INT,
    fastest_lap_rank INT,
    fastest_lap_time_str STRING
);