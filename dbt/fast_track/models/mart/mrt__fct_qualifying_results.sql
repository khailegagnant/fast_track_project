{#
    Gold: consumption-ready qualifying fact, grain SEASON + ROUND + DRIVER_ID.

    Adds within-session analytics that depend on the rest of the field:
      - GAP_TO_POLE_SECONDS and PCT_OFF_POLE, the standard way qualifying
        pace is compared.
      - GAP_TO_TEAMMATE_SECONDS, the usual intra-team benchmark. NULL when a
        constructor fielded only one timed car in that session.
#}

WITH QUALIFYING AS (

    SELECT
        QUALIFYING_KEY,
        RACE_KEY,
        DRIVER_KEY,
        CONSTRUCTOR_KEY,
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        FINAL_SEGMENT_REACHED,
        QUALIFYING_POSITION,
        Q1_TIME_SECONDS,
        Q2_TIME_SECONDS,
        Q3_TIME_SECONDS,
        BEST_QUALIFYING_TIME_SECONDS,
        SEGMENTS_WITH_A_TIME,
        IS_POLE_POSITION,
        IS_TOP_TEN_QUALIFIER,
        IS_NO_TIME_SET
    FROM {{ ref('slv_f1__qualifying') }}

),

SESSION_CONTEXT AS (

    SELECT
        RACE_KEY,
        COUNT(*) AS QUALIFYING_FIELD_SIZE,
        MIN(BEST_QUALIFYING_TIME_SECONDS) AS POLE_TIME_SECONDS
    FROM QUALIFYING
    GROUP BY 1

),

TEAMMATE_CONTEXT AS (

    SELECT
        QUALIFYING_KEY,
        MIN(BEST_QUALIFYING_TIME_SECONDS) OVER (
            PARTITION BY RACE_KEY, CONSTRUCTOR_KEY
        ) AS TEAM_BEST_TIME_SECONDS,
        COUNT(BEST_QUALIFYING_TIME_SECONDS) OVER (
            PARTITION BY RACE_KEY, CONSTRUCTOR_KEY
        ) AS TEAM_TIMED_CAR_COUNT
    FROM QUALIFYING

),

ENRICHED AS (

    SELECT
        ---------- ids
        qualifying.QUALIFYING_KEY,
        qualifying.RACE_KEY,
        qualifying.DRIVER_KEY,
        qualifying.CONSTRUCTOR_KEY,
        qualifying.SEASON,
        qualifying.ROUND,
        qualifying.DRIVER_ID,
        qualifying.CONSTRUCTOR_ID,

        ---------- strings
        qualifying.FINAL_SEGMENT_REACHED,

        ---------- numerics
        qualifying.QUALIFYING_POSITION,
        qualifying.Q1_TIME_SECONDS,
        qualifying.Q2_TIME_SECONDS,
        qualifying.Q3_TIME_SECONDS,
        qualifying.BEST_QUALIFYING_TIME_SECONDS,
        qualifying.SEGMENTS_WITH_A_TIME,
        context.QUALIFYING_FIELD_SIZE,
        context.POLE_TIME_SECONDS,

        qualifying.BEST_QUALIFYING_TIME_SECONDS - context.POLE_TIME_SECONDS
            AS GAP_TO_POLE_SECONDS,

        ROUND(
            100.0
            * (
                qualifying.BEST_QUALIFYING_TIME_SECONDS
                - context.POLE_TIME_SECONDS
            )
            / NULLIF(context.POLE_TIME_SECONDS, 0),
            3
        ) AS PCT_OFF_POLE,

        -- NULL when the constructor had only one timed car in the session
        CASE
            WHEN teammate.TEAM_TIMED_CAR_COUNT > 1
                THEN qualifying.BEST_QUALIFYING_TIME_SECONDS
                    - teammate.TEAM_BEST_TIME_SECONDS
        END AS GAP_TO_TEAMMATE_SECONDS,

        ---------- booleans
        qualifying.IS_POLE_POSITION,
        qualifying.IS_TOP_TEN_QUALIFIER,
        qualifying.IS_NO_TIME_SET,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM QUALIFYING qualifying
    INNER JOIN SESSION_CONTEXT context
        ON qualifying.RACE_KEY = context.RACE_KEY
    INNER JOIN TEAMMATE_CONTEXT teammate
        ON qualifying.QUALIFYING_KEY = teammate.QUALIFYING_KEY

)

SELECT * FROM ENRICHED
