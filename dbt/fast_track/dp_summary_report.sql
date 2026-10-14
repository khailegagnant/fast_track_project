-- dq_summary_report.sql
USE ROLE F1_DATA_ENGINEER_ROLE;
USE DATABASE F1_CAPSTONE;
USE SCHEMA RAW;

WITH dq_checks AS (
    -- 1. DQ Check for Races
    SELECT 
        'RAW_RACES' AS table_name,
        COUNT(*) AS total_rows,
        COUNT_IF(season IS NULL OR round IS NULL OR circuit_id IS NULL) AS failed_rows
    FROM raw_races

    UNION ALL
    
    -- 2. DQ Check for Drivers
    SELECT 
        'RAW_DRIVERS' AS table_name,
        COUNT(*) AS total_rows,
        COUNT_IF(driver_id IS NULL OR given_name IS NULL OR family_name IS NULL) AS failed_rows
    FROM raw_drivers

    UNION ALL

    -- 3. DQ Check for Results
    SELECT 
        'RAW_RESULTS' AS table_name,
        COUNT(*) AS total_rows,
        COUNT_IF(driver_id IS NULL OR constructor_id IS NULL OR season IS NULL) AS failed_rows
    FROM raw_results

    UNION ALL
    
    -- 4. DQ Check for Constructors
    SELECT 
        'RAW_CONSTRUCTORS' AS table_name,
        COUNT(*) AS total_rows,
        COUNT_IF(constructor_id IS NULL OR name IS NULL) AS failed_rows
    FROM raw_constructors
    
    UNION ALL

    -- 5. DQ Check for Qualifying
    SELECT 
        'RAW_QUALIFYING' AS table_name,
        COUNT(*) AS total_rows,
        COUNT_IF(driver_id IS NULL OR constructor_id IS NULL OR position IS NULL) AS failed_rows
    FROM raw_qualifying
)
SELECT 
    table_name,
    total_rows,
    failed_rows,
    (total_rows - failed_rows) AS passed_rows,
    ROUND((total_rows - failed_rows) * 100.0 / NULLIF(total_rows, 0), 2) AS pass_rate_percentage,
    CASE 
        WHEN failed_rows = 0 THEN '✅ PASS'
        WHEN (failed_rows * 100.0 / NULLIF(total_rows, 0)) <= 2 THEN '⚠️ WARNING (< 2% Fail)'
        ELSE '❌ FAIL'
    END AS dq_status
FROM dq_checks
ORDER BY pass_rate_percentage ASC, table_name;