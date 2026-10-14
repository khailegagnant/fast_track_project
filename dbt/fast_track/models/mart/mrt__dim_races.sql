{#
    Gold: presentation race dimension, denormalised with circuit attributes so
    consumers can filter by country or circuit without a second join.
#}

WITH RACES AS (

    SELECT
        RACE_KEY,
        CIRCUIT_KEY,
        SEASON,
        ROUND,
        CIRCUIT_ID,
        RACE_NAME,
        RACE_DATE,
        RACE_STARTED_AT,
        RACE_URL
    FROM {{ ref('slv_f1__races') }}

),

CIRCUITS AS (

    SELECT
        CIRCUIT_KEY,
        CIRCUIT_NAME,
        LOCALITY,
        COUNTRY
    FROM {{ ref('slv_f1__circuits') }}

),

SEASON_BOUNDS AS (

    SELECT
        SEASON,
        MAX(ROUND) AS ROUNDS_IN_SEASON
    FROM RACES
    GROUP BY 1

),

DENORMALISED AS (

    SELECT
        ---------- ids
        races.RACE_KEY,
        races.CIRCUIT_KEY,
        races.SEASON,
        races.ROUND,
        races.CIRCUIT_ID,

        ---------- strings
        races.RACE_NAME,
        circuits.CIRCUIT_NAME,
        circuits.LOCALITY,
        circuits.COUNTRY,
        races.SEASON::VARCHAR || ' R' || LPAD(races.ROUND::VARCHAR, 2, '0')
            || ' ' || races.RACE_NAME AS RACE_LABEL,
        races.RACE_URL,

        ---------- numerics
        bounds.ROUNDS_IN_SEASON,

        ---------- booleans
        (races.ROUND = 1) AS IS_SEASON_OPENER,
        (races.ROUND = bounds.ROUNDS_IN_SEASON) AS IS_SEASON_FINALE,

        ---------- dates
        races.RACE_DATE,

        ---------- timestamps
        races.RACE_STARTED_AT,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM RACES races
    LEFT JOIN CIRCUITS circuits
        ON races.CIRCUIT_KEY = circuits.CIRCUIT_KEY
    INNER JOIN SEASON_BOUNDS bounds
        ON races.SEASON = bounds.SEASON

)

SELECT * FROM DENORMALISED
