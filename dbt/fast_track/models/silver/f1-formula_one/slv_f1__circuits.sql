{#
    Silver: typed, cleaned, conformed circuit entity. One row per CIRCUIT_ID.

    Responsibilities:
      - Cast every column to its contracted type.
      - Trim text and collapse blanks to NULL.
      - Reject coordinates outside valid lat/long bounds so downstream
        mapping never plots a bad point.
      - Guard the grain with a defensive QUALIFY in case the API re-lands a
        circuit with corrected detail. Ordering uses business columns, never
        DBT_LOADED_AT, which is constant across a run and would order
        nothing.
#}

WITH CIRCUITS AS (

    SELECT
        CIRCUIT_ID,
        CIRCUIT_NAME,
        LOCALITY,
        COUNTRY,
        LATITUDE,
        LONGITUDE
    FROM {{ ref('stg_f1__circuits') }}

),

TYPED AS (

    SELECT
        NULLIF(TRIM(CIRCUIT_ID), '')::VARCHAR AS CIRCUIT_ID,
        NULLIF(TRIM(CIRCUIT_NAME), '')::VARCHAR AS CIRCUIT_NAME,
        NULLIF(TRIM(LOCALITY), '')::VARCHAR AS LOCALITY,
        NULLIF(TRIM(COUNTRY), '')::VARCHAR AS COUNTRY,
        LATITUDE::FLOAT AS LATITUDE,
        LONGITUDE::FLOAT AS LONGITUDE
    FROM CIRCUITS
    WHERE CIRCUIT_ID IS NOT NULL

),

DEDUPLICATED AS (

    SELECT
        CIRCUIT_ID,
        CIRCUIT_NAME,
        LOCALITY,
        COUNTRY,
        LATITUDE,
        LONGITUDE
    FROM TYPED
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CIRCUIT_ID
        ORDER BY
            CIRCUIT_NAME DESC NULLS LAST,
            LOCALITY DESC NULLS LAST,
            LATITUDE DESC NULLS LAST
    ) = 1

),

CONFORMED AS (

    SELECT
        ---------- ids
        {{ dbt_utils.generate_surrogate_key(circuit_sk()) }} AS CIRCUIT_KEY,
        CIRCUIT_ID,

        ---------- strings
        CIRCUIT_NAME,
        LOCALITY,
        COUNTRY,

        ---------- numerics
        CASE
            WHEN LATITUDE BETWEEN -90 AND 90 THEN LATITUDE
        END AS LATITUDE,
        CASE
            WHEN LONGITUDE BETWEEN -180 AND 180 THEN LONGITUDE
        END AS LONGITUDE,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM DEDUPLICATED

)

SELECT * FROM CONFORMED
