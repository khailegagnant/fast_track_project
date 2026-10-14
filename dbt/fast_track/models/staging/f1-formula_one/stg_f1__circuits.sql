{#
    Staging: 1:1 passthrough over F1_RAW.RAW_CIRCUITS.

    Select and rename only. Types are inherited from RAW as landed by the
    Glue/parquet COPY INTO. Casting, trimming, and deduplication all happen
    in slv_f1__circuits.
#}

WITH SOURCE AS (

    SELECT
        CIRCUIT_ID,
        CIRCUIT_NAME,
        LOCALITY,
        COUNTRY,
        LATITUDE,
        LONGITUDE
    FROM {{ source('F1_RAW', 'RAW_CIRCUITS') }}

),

RENAMED AS (

    SELECT
        ---------- ids
        CIRCUIT_ID,

        ---------- strings
        CIRCUIT_NAME,
        LOCALITY,
        COUNTRY,

        ---------- numerics
        LATITUDE,
        LONGITUDE,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM SOURCE

)

SELECT * FROM RENAMED
