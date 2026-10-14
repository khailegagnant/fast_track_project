{#
    Gold: consumption-ready race-result fact, grain SEASON + ROUND + DRIVER_ID.

    Adds within-race analytics that silver deliberately leaves out because
    they depend on other rows in the same race:
      - GAP_TO_WINNER_MILLIS, only meaningful between classified finishers
        who both have a recorded race time.
      - FASTEST_LAP_GAP_SECONDS against the quickest lap of the race.
      - FIELD_SIZE and the drivers percentile finish within it.
#}

WITH RESULTS AS (

    SELECT
        RACE_RESULT_KEY,
        RACE_KEY,
        DRIVER_KEY,
        CONSTRUCTOR_KEY,
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        POSITION_TEXT,
        STATUS,
        FINISH_STATUS_CATEGORY,
        STARTING_GRID_POSITION,
        FINISH_POSITION,
        CLASSIFIED_POSITION,
        POINTS_SCORED,
        LAPS_COMPLETED,
        RACE_TIME_MILLIS,
        FASTEST_LAP_RANK,
        FASTEST_LAP_TIME_SECONDS,
        POSITIONS_GAINED,
        IS_CLASSIFIED,
        IS_PIT_LANE_START,
        IS_RACE_WINNER,
        IS_PODIUM_FINISH,
        IS_POINTS_FINISH,
        IS_FASTEST_LAP
    FROM {{ ref('slv_f1__results') }}

),

RACE_CONTEXT AS (

    SELECT
        RACE_KEY,
        COUNT(*) AS FIELD_SIZE,
        MAX(LAPS_COMPLETED) AS RACE_DISTANCE_LAPS,
        MIN(FASTEST_LAP_TIME_SECONDS) AS RACE_FASTEST_LAP_SECONDS,
        MIN(
            CASE WHEN IS_RACE_WINNER THEN RACE_TIME_MILLIS END
        ) AS WINNER_RACE_TIME_MILLIS
    FROM RESULTS
    GROUP BY 1

),

ENRICHED AS (

    SELECT
        ---------- ids
        results.RACE_RESULT_KEY,
        results.RACE_KEY,
        results.DRIVER_KEY,
        results.CONSTRUCTOR_KEY,
        results.SEASON,
        results.ROUND,
        results.DRIVER_ID,
        results.CONSTRUCTOR_ID,

        ---------- strings
        results.POSITION_TEXT,
        results.STATUS,
        results.FINISH_STATUS_CATEGORY,

        ---------- numerics
        results.STARTING_GRID_POSITION,
        results.FINISH_POSITION,
        results.CLASSIFIED_POSITION,
        results.POINTS_SCORED,
        results.LAPS_COMPLETED,
        results.RACE_TIME_MILLIS,
        results.FASTEST_LAP_RANK,
        results.FASTEST_LAP_TIME_SECONDS,
        results.POSITIONS_GAINED,
        context.FIELD_SIZE,
        context.RACE_DISTANCE_LAPS,

        -- only comparable when both the winner and this driver were timed
        CASE
            WHEN results.IS_CLASSIFIED
                AND results.RACE_TIME_MILLIS IS NOT NULL
                AND context.WINNER_RACE_TIME_MILLIS IS NOT NULL
                THEN results.RACE_TIME_MILLIS - context.WINNER_RACE_TIME_MILLIS
        END AS GAP_TO_WINNER_MILLIS,

        results.FASTEST_LAP_TIME_SECONDS - context.RACE_FASTEST_LAP_SECONDS
            AS FASTEST_LAP_GAP_SECONDS,

        ROUND(
            100.0 * results.LAPS_COMPLETED
            / NULLIF(context.RACE_DISTANCE_LAPS, 0),
            2
        ) AS PCT_RACE_DISTANCE_COMPLETED,

        ---------- booleans
        results.IS_CLASSIFIED,
        results.IS_PIT_LANE_START,
        results.IS_RACE_WINNER,
        results.IS_PODIUM_FINISH,
        results.IS_POINTS_FINISH,
        results.IS_FASTEST_LAP,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM RESULTS results
    INNER JOIN RACE_CONTEXT context
        ON results.RACE_KEY = context.RACE_KEY

)

SELECT * FROM ENRICHED
