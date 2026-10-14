{#
    Staging: 1:1 passthrough over F1_RAW.RAW_RACES.

    Select and rename only - RACE_TIMESTAMP_UTC is renamed to the projects
    `_at` timestamp convention. Casting and the race surrogate key are
    applied in slv_f1__races.
#}

WITH SOURCE AS (

    SELECT
        SEASON,
        ROUND,
        RACE_NAME,
        CIRCUIT_ID,
        RACE_DATE,
        RACE_TIME,
        RACE_TIMESTAMP_UTC,
        RACE_URL
    FROM {{ source('F1_RAW', 'RAW_RACES') }}

),

RENAMED AS (

    SELECT
        ---------- ids
        SEASON,
        ROUND,
        CIRCUIT_ID,

        ---------- strings
        RACE_NAME,
        RACE_TIME,
        RACE_URL,

        ---------- dates
        RACE_DATE,

        ---------- timestamps
        RACE_TIMESTAMP_UTC AS RACE_STARTED_AT,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM SOURCE

)

SELECT * FROM RENAMED
