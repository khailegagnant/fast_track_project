{#
    Staging: 1:1 passthrough over F1_RAW.RAW_RESULTS.

    Select only - every column already carries its business name in RAW.
    Lap-time strings are kept verbatim; casting, classification, status
    categorisation, and derived measures all happen in slv_f1__results.
#}

WITH SOURCE AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        GRID_POSITION,
        FINISH_POSITION,
        POSITION_TEXT,
        POINTS_SCORED,
        LAPS_COMPLETED,
        STATUS,
        RACE_TIME_MILLIS,
        FASTEST_LAP_RANK,
        FASTEST_LAP_TIME_STR
    FROM {{ source('F1_RAW', 'RAW_RESULTS') }}

),

RENAMED AS (

    SELECT
        ---------- ids
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,

        ---------- strings
        POSITION_TEXT,
        STATUS,
        FASTEST_LAP_TIME_STR,

        ---------- numerics
        GRID_POSITION,
        FINISH_POSITION,
        POINTS_SCORED,
        LAPS_COMPLETED,
        RACE_TIME_MILLIS,
        FASTEST_LAP_RANK,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM SOURCE

)

SELECT * FROM RENAMED
