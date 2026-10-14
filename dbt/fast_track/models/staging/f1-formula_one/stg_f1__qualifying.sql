{#
    Staging: 1:1 passthrough over F1_RAW.RAW_QUALIFYING.

    Select and rename only - POSITION is qualified as QUALIFYING_POSITION.
    Lap-time strings are kept verbatim; conversion to seconds happens in
    slv_f1__qualifying.
#}

WITH SOURCE AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        POSITION,
        Q1_TIME_STR,
        Q2_TIME_STR,
        Q3_TIME_STR
    FROM {{ source('F1_RAW', 'RAW_QUALIFYING') }}

),

RENAMED AS (

    SELECT
        ---------- ids
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,

        ---------- strings
        Q1_TIME_STR,
        Q2_TIME_STR,
        Q3_TIME_STR,

        ---------- numerics
        POSITION AS QUALIFYING_POSITION,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM SOURCE

)

SELECT * FROM RENAMED
