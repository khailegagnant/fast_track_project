{#
    Silver: typed, cleaned, conformed race entity.
    Grain: SEASON + ROUND.

    Responsibilities:
      - Cast and trim.
      - Assign RACE_KEY, the surrogate over (SEASON, ROUND).
      - Resolve CIRCUIT_KEY here so no downstream model re-derives it.
      - Drop rows that cannot be keyed. Any resulting calendar gap is caught
        by the assert_race_round_sequence_is_contiguous singular test.
#}

WITH RACES AS (

    SELECT
        SEASON,
        ROUND,
        CIRCUIT_ID,
        RACE_NAME,
        RACE_TIME,
        RACE_URL,
        RACE_DATE,
        RACE_STARTED_AT
    FROM {{ ref('stg_f1__races') }}

),

TYPED AS (

    SELECT
        TRY_TO_NUMBER(SEASON::VARCHAR) AS SEASON,
        TRY_TO_NUMBER(ROUND::VARCHAR) AS ROUND,
        NULLIF(TRIM(CIRCUIT_ID), '')::VARCHAR AS CIRCUIT_ID,
        NULLIF(TRIM(RACE_NAME), '')::VARCHAR AS RACE_NAME,
        NULLIF(TRIM(RACE_TIME), '')::VARCHAR AS RACE_TIME,
        NULLIF(TRIM(RACE_URL), '')::VARCHAR AS RACE_URL,
        RACE_DATE::DATE AS RACE_DATE,
        RACE_STARTED_AT::TIMESTAMP_NTZ AS RACE_STARTED_AT
    FROM RACES

),

FILTERED AS (

    SELECT
        SEASON,
        ROUND,
        CIRCUIT_ID,
        RACE_NAME,
        RACE_TIME,
        RACE_URL,
        RACE_DATE,
        RACE_STARTED_AT
    FROM TYPED
    WHERE SEASON IS NOT NULL
      AND ROUND IS NOT NULL

),

DEDUPLICATED AS (

    SELECT
        SEASON,
        ROUND,
        CIRCUIT_ID,
        RACE_NAME,
        RACE_TIME,
        RACE_URL,
        RACE_DATE,
        RACE_STARTED_AT
    FROM FILTERED
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SEASON, ROUND
        ORDER BY
            RACE_STARTED_AT DESC NULLS LAST,
            RACE_DATE DESC NULLS LAST,
            RACE_NAME DESC NULLS LAST
    ) = 1

),

CONFORMED AS (

    SELECT
        ---------- ids
        {{ dbt_utils.generate_surrogate_key(race_sk()) }} AS RACE_KEY,
        {{ dbt_utils.generate_surrogate_key(circuit_sk()) }} AS CIRCUIT_KEY,
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
        RACE_STARTED_AT,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM DEDUPLICATED

)

SELECT * FROM CONFORMED
