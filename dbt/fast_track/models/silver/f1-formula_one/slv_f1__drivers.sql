{#
    Silver: typed, cleaned, conformed driver entity. One row per DRIVER_ID.

    Responsibilities:
      - Cast and trim.
      - Resolve the duplicate rows in RAW: the API is queried per season, so
        a driver is landed once per season they competed in.
      - Identity attributes (names, date of birth, nationality) are stable
        across seasons. PERMANENT_NUMBER and DRIVER_CODE can change, so the
        highest permanent number wins, which in practice selects the most
        recent entry. DBT_LOADED_AT is deliberately not a tie-break: it is
        stamped from run_started_at and is identical for every row in a run.
      - Build DRIVER_FULL_NAME once so every consumer formats names the same.

    PERMANENT_NUMBER is intentionally not unique - retired numbers are
    reassigned, so two drivers can legitimately share one.
#}

WITH DRIVERS AS (

    SELECT
        DRIVER_ID,
        DRIVER_CODE,
        GIVEN_NAME,
        FAMILY_NAME,
        DRIVER_NATIONALITY,
        PERMANENT_NUMBER,
        DATE_OF_BIRTH
    FROM {{ ref('stg_f1__drivers') }}

),

TYPED AS (

    SELECT
        NULLIF(TRIM(DRIVER_ID), '')::VARCHAR AS DRIVER_ID,
        NULLIF(TRIM(DRIVER_CODE), '')::VARCHAR AS DRIVER_CODE,
        NULLIF(TRIM(GIVEN_NAME), '')::VARCHAR AS GIVEN_NAME,
        NULLIF(TRIM(FAMILY_NAME), '')::VARCHAR AS FAMILY_NAME,
        NULLIF(TRIM(DRIVER_NATIONALITY), '')::VARCHAR AS DRIVER_NATIONALITY,
        TRY_TO_NUMBER(PERMANENT_NUMBER::VARCHAR) AS PERMANENT_NUMBER,
        DATE_OF_BIRTH::DATE AS DATE_OF_BIRTH
    FROM DRIVERS
    WHERE DRIVER_ID IS NOT NULL

),

DEDUPLICATED AS (

    SELECT
        DRIVER_ID,
        DRIVER_CODE,
        GIVEN_NAME,
        FAMILY_NAME,
        DRIVER_NATIONALITY,
        PERMANENT_NUMBER,
        DATE_OF_BIRTH
    FROM TYPED
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY DRIVER_ID
        ORDER BY
            PERMANENT_NUMBER DESC NULLS LAST,
            DRIVER_CODE DESC NULLS LAST,
            DATE_OF_BIRTH DESC NULLS LAST,
            FAMILY_NAME DESC NULLS LAST,
            GIVEN_NAME DESC NULLS LAST
    ) = 1

),

CONFORMED AS (

    SELECT
        ---------- ids
        {{ dbt_utils.generate_surrogate_key(driver_sk()) }} AS DRIVER_KEY,
        DRIVER_ID,

        ---------- strings
        DRIVER_CODE,
        GIVEN_NAME,
        FAMILY_NAME,
        TRIM(COALESCE(GIVEN_NAME, '') || ' ' || COALESCE(FAMILY_NAME, ''))
            AS DRIVER_FULL_NAME,
        DRIVER_NATIONALITY,

        ---------- numerics
        PERMANENT_NUMBER,

        ---------- dates
        DATE_OF_BIRTH,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM DEDUPLICATED

)

SELECT * FROM CONFORMED
