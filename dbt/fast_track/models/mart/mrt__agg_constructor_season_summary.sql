{#
    Gold metrics: one row per constructor per season. Final championship
    position and points come from the last rounds standings snapshot so this
    aggregate can never disagree with mrt__fct_constructor_standings.
#}

WITH STANDINGS AS (

    SELECT
        SEASON,
        ROUND,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_KEY,
        CHAMPIONSHIP_POSITION,
        POINTS_TO_DATE,
        WINS_TO_DATE,
        PODIUMS_TO_DATE,
        POINTS_FINISHES_TO_DATE,
        RETIREMENTS_TO_DATE,
        CAR_ENTRIES_TO_DATE,
        BEST_FINISH_TO_DATE
    FROM {{ ref('mrt__fct_constructor_standings') }}

),

FINAL_STANDING AS (

    SELECT
        SEASON,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_KEY,
        CHAMPIONSHIP_POSITION AS FINAL_CHAMPIONSHIP_POSITION,
        POINTS_TO_DATE AS TOTAL_POINTS,
        WINS_TO_DATE AS TOTAL_WINS,
        PODIUMS_TO_DATE AS TOTAL_PODIUMS,
        POINTS_FINISHES_TO_DATE AS TOTAL_POINTS_FINISHES,
        RETIREMENTS_TO_DATE AS TOTAL_RETIREMENTS,
        CAR_ENTRIES_TO_DATE AS TOTAL_CAR_ENTRIES,
        BEST_FINISH_TO_DATE AS BEST_FINISH
    FROM STANDINGS
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SEASON, CONSTRUCTOR_ID
        ORDER BY ROUND DESC
    ) = 1

),

DRIVER_COUNT AS (

    SELECT
        SEASON,
        CONSTRUCTOR_ID,
        COUNT(DISTINCT DRIVER_ID) AS DRIVERS_FIELDED
    FROM {{ ref('mrt__fct_race_results') }}
    GROUP BY 1, 2

),

QUALIFYING_SUMMARY AS (

    SELECT
        SEASON,
        CONSTRUCTOR_ID,
        SUM(IFF(IS_POLE_POSITION, 1, 0)) AS TOTAL_POLE_POSITIONS,
        AVG(QUALIFYING_POSITION) AS AVG_QUALIFYING_POSITION,
        AVG(GAP_TO_POLE_SECONDS) AS AVG_GAP_TO_POLE_SECONDS
    FROM {{ ref('mrt__fct_qualifying_results') }}
    GROUP BY 1, 2

),

RACE_SUMMARY AS (

    SELECT
        SEASON,
        CONSTRUCTOR_ID,
        AVG(CLASSIFIED_POSITION) AS AVG_FINISH_POSITION,
        SUM(IFF(IS_FASTEST_LAP, 1, 0)) AS TOTAL_FASTEST_LAPS,
        SUM(LAPS_COMPLETED) AS TOTAL_LAPS_COMPLETED
    FROM {{ ref('mrt__fct_race_results') }}
    GROUP BY 1, 2

),

COMBINED AS (

    SELECT
        ---------- ids
        final_standing.SEASON,
        final_standing.CONSTRUCTOR_ID,
        final_standing.CONSTRUCTOR_KEY,

        ---------- numerics
        final_standing.FINAL_CHAMPIONSHIP_POSITION,
        final_standing.TOTAL_POINTS,
        final_standing.TOTAL_WINS,
        final_standing.TOTAL_PODIUMS,
        final_standing.TOTAL_POINTS_FINISHES,
        final_standing.TOTAL_RETIREMENTS,
        final_standing.TOTAL_CAR_ENTRIES,
        final_standing.BEST_FINISH,
        drivers.DRIVERS_FIELDED,
        qualifying.TOTAL_POLE_POSITIONS,
        races.TOTAL_FASTEST_LAPS,
        races.TOTAL_LAPS_COMPLETED,

        ROUND(qualifying.AVG_QUALIFYING_POSITION, 2)
            AS AVG_QUALIFYING_POSITION,
        ROUND(qualifying.AVG_GAP_TO_POLE_SECONDS, 3)
            AS AVG_GAP_TO_POLE_SECONDS,
        ROUND(races.AVG_FINISH_POSITION, 2) AS AVG_FINISH_POSITION,

        ROUND(
            final_standing.TOTAL_POINTS
            / NULLIF(final_standing.TOTAL_CAR_ENTRIES, 0),
            2
        ) AS AVG_POINTS_PER_CAR_ENTRY,

        ROUND(
            100.0 * final_standing.TOTAL_RETIREMENTS
            / NULLIF(final_standing.TOTAL_CAR_ENTRIES, 0),
            1
        ) AS PCT_ENTRIES_RETIRED,

        ---------- booleans
        (final_standing.FINAL_CHAMPIONSHIP_POSITION = 1)
            AS IS_SEASON_CHAMPION,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM FINAL_STANDING final_standing
    LEFT JOIN DRIVER_COUNT drivers
        ON final_standing.SEASON = drivers.SEASON
        AND final_standing.CONSTRUCTOR_ID = drivers.CONSTRUCTOR_ID
    LEFT JOIN QUALIFYING_SUMMARY qualifying
        ON final_standing.SEASON = qualifying.SEASON
        AND final_standing.CONSTRUCTOR_ID = qualifying.CONSTRUCTOR_ID
    LEFT JOIN RACE_SUMMARY races
        ON final_standing.SEASON = races.SEASON
        AND final_standing.CONSTRUCTOR_ID = races.CONSTRUCTOR_ID

)

SELECT * FROM COMBINED
