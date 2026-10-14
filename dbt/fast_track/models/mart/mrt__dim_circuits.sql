{#
    Gold: presentation circuit dimension.
    Adds a display label and a single GEO_COORDINATES string for mapping
    tools that expect a 'lat,long' pair.
#}

WITH CIRCUITS AS (

    SELECT
        CIRCUIT_KEY,
        CIRCUIT_ID,
        CIRCUIT_NAME,
        LOCALITY,
        COUNTRY,
        LATITUDE,
        LONGITUDE
    FROM {{ ref('slv_f1__circuits') }}

),

PRESENTED AS (

    SELECT
        ---------- ids
        CIRCUIT_KEY,
        CIRCUIT_ID,

        ---------- strings
        CIRCUIT_NAME,
        LOCALITY,
        COUNTRY,
        CIRCUIT_NAME || ' (' || COALESCE(LOCALITY, COUNTRY) || ')'
            AS CIRCUIT_LABEL,
        CASE
            WHEN LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
                THEN LATITUDE::VARCHAR || ',' || LONGITUDE::VARCHAR
        END AS GEO_COORDINATES,

        ---------- numerics
        LATITUDE,
        LONGITUDE,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM CIRCUITS

)

SELECT * FROM PRESENTED
