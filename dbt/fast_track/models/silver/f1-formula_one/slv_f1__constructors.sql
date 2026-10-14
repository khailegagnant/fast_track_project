{#
    Silver: typed, cleaned, conformed constructor entity.
    One row per CONSTRUCTOR_ID.

    Responsibilities:
      - Cast and trim.
      - Resolve the duplicate rows in RAW: the API is queried per season, so
        a constructor is landed once per season it competed in.
      - When a rebrand gives the same ID differing names, the alphabetically
        last name wins so repeated runs are byte-for-byte stable.
#}

WITH CONSTRUCTORS AS (

    SELECT
        CONSTRUCTOR_ID,
        CONSTRUCTOR_NAME,
        CONSTRUCTOR_NATIONALITY
    FROM {{ ref('stg_f1__constructors') }}

),

TYPED AS (

    SELECT
        NULLIF(TRIM(CONSTRUCTOR_ID), '')::VARCHAR AS CONSTRUCTOR_ID,
        NULLIF(TRIM(CONSTRUCTOR_NAME), '')::VARCHAR AS CONSTRUCTOR_NAME,
        NULLIF(TRIM(CONSTRUCTOR_NATIONALITY), '')::VARCHAR
            AS CONSTRUCTOR_NATIONALITY
    FROM CONSTRUCTORS
    WHERE CONSTRUCTOR_ID IS NOT NULL

),

DEDUPLICATED AS (

    SELECT
        CONSTRUCTOR_ID,
        CONSTRUCTOR_NAME,
        CONSTRUCTOR_NATIONALITY
    FROM TYPED
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CONSTRUCTOR_ID
        ORDER BY
            CONSTRUCTOR_NAME DESC NULLS LAST,
            CONSTRUCTOR_NATIONALITY DESC NULLS LAST
    ) = 1

),

CONFORMED AS (

    SELECT
        ---------- ids
        {{ dbt_utils.generate_surrogate_key(constructor_sk()) }}
            AS CONSTRUCTOR_KEY,
        CONSTRUCTOR_ID,

        ---------- strings
        CONSTRUCTOR_NAME,
        CONSTRUCTOR_NATIONALITY,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM DEDUPLICATED

)

SELECT * FROM CONFORMED
