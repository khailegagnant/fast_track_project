{#
    Silver: typed, cleaned qualifying entity.
    Grain: SEASON + ROUND + DRIVER_ID.

    Business rules:
      - Q1/Q2/Q3 strings are converted to seconds via f1_lap_time_to_seconds.
      - A NULL Q2 or Q3 time is expected, not a defect: it means the driver
        was eliminated in the previous segment. FINAL_SEGMENT_REACHED records
        how far the driver progressed.
      - BEST_QUALIFYING_TIME_SECONDS is the fastest lap across every segment
        the driver actually set a time in. A plain LEAST() collapses to NULL
        as soon as one segment is NULL, so a sentinel is substituted and then
        stripped back to NULL for drivers with no time at all.
#}

WITH QUALIFYING AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        Q1_TIME_STR,
        Q2_TIME_STR,
        Q3_TIME_STR,
        QUALIFYING_POSITION
    FROM {{ ref('stg_f1__qualifying') }}

),

TYPED AS (

    SELECT
        TRY_TO_NUMBER(SEASON::VARCHAR) AS SEASON,
        TRY_TO_NUMBER(ROUND::VARCHAR) AS ROUND,
        NULLIF(TRIM(DRIVER_ID), '')::VARCHAR AS DRIVER_ID,
        NULLIF(TRIM(CONSTRUCTOR_ID), '')::VARCHAR AS CONSTRUCTOR_ID,
        NULLIF(TRIM(Q1_TIME_STR), '')::VARCHAR AS Q1_TIME_STR,
        NULLIF(TRIM(Q2_TIME_STR), '')::VARCHAR AS Q2_TIME_STR,
        NULLIF(TRIM(Q3_TIME_STR), '')::VARCHAR AS Q3_TIME_STR,
        TRY_TO_NUMBER(QUALIFYING_POSITION::VARCHAR) AS QUALIFYING_POSITION
    FROM QUALIFYING

),

FILTERED AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        Q1_TIME_STR,
        Q2_TIME_STR,
        Q3_TIME_STR,
        QUALIFYING_POSITION
    FROM TYPED
    WHERE SEASON IS NOT NULL
      AND ROUND IS NOT NULL
      AND DRIVER_ID IS NOT NULL

),

DEDUPLICATED AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        Q1_TIME_STR,
        Q2_TIME_STR,
        Q3_TIME_STR,
        QUALIFYING_POSITION
    FROM FILTERED
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SEASON, ROUND, DRIVER_ID
        ORDER BY
            QUALIFYING_POSITION ASC NULLS LAST,
            Q3_TIME_STR ASC NULLS LAST,
            Q2_TIME_STR ASC NULLS LAST,
            Q1_TIME_STR ASC NULLS LAST
    ) = 1

),

TIMES_CONVERTED AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        QUALIFYING_POSITION,
        {{ f1_lap_time_to_seconds('Q1_TIME_STR') }} AS Q1_TIME_SECONDS,
        {{ f1_lap_time_to_seconds('Q2_TIME_STR') }} AS Q2_TIME_SECONDS,
        {{ f1_lap_time_to_seconds('Q3_TIME_STR') }} AS Q3_TIME_SECONDS
    FROM DEDUPLICATED

),

DERIVED AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        QUALIFYING_POSITION,
        Q1_TIME_SECONDS,
        Q2_TIME_SECONDS,
        Q3_TIME_SECONDS,

        -- sentinel keeps LEAST from collapsing to NULL on eliminated drivers
        NULLIF(
            LEAST(
                COALESCE(Q1_TIME_SECONDS, 999999),
                COALESCE(Q2_TIME_SECONDS, 999999),
                COALESCE(Q3_TIME_SECONDS, 999999)
            ),
            999999
        ) AS BEST_QUALIFYING_TIME_SECONDS,

        CASE
            WHEN Q3_TIME_SECONDS IS NOT NULL THEN 'Q3'
            WHEN Q2_TIME_SECONDS IS NOT NULL THEN 'Q2'
            WHEN Q1_TIME_SECONDS IS NOT NULL THEN 'Q1'
            ELSE 'NO_TIME'
        END AS FINAL_SEGMENT_REACHED,

        (CASE WHEN Q1_TIME_SECONDS IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN Q2_TIME_SECONDS IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN Q3_TIME_SECONDS IS NOT NULL THEN 1 ELSE 0 END)
            AS SEGMENTS_WITH_A_TIME
    FROM TIMES_CONVERTED

),

CONFORMED AS (

    SELECT
        ---------- ids
        {{ dbt_utils.generate_surrogate_key(qualifying_sk()) }}
            AS QUALIFYING_KEY,
        {{ dbt_utils.generate_surrogate_key(race_sk()) }} AS RACE_KEY,
        {{ dbt_utils.generate_surrogate_key(driver_sk()) }} AS DRIVER_KEY,
        {{ dbt_utils.generate_surrogate_key(constructor_sk()) }}
            AS CONSTRUCTOR_KEY,
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,

        ---------- strings
        FINAL_SEGMENT_REACHED,

        ---------- numerics
        QUALIFYING_POSITION,
        Q1_TIME_SECONDS,
        Q2_TIME_SECONDS,
        Q3_TIME_SECONDS,
        BEST_QUALIFYING_TIME_SECONDS,
        SEGMENTS_WITH_A_TIME,

        ---------- booleans
        COALESCE(QUALIFYING_POSITION = 1, FALSE) AS IS_POLE_POSITION,
        COALESCE(QUALIFYING_POSITION <= 10, FALSE) AS IS_TOP_TEN_QUALIFIER,
        (FINAL_SEGMENT_REACHED = 'NO_TIME') AS IS_NO_TIME_SET,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM DERIVED

)

SELECT * FROM CONFORMED
