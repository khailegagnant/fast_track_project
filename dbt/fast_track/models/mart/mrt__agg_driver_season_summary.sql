{#
    Gold metrics: one row per driver per season, summarising the full
    campaign. Final championship position and points are taken from the last
    rounds standings snapshot rather than recomputed, so this aggregate can
    never disagree with mrt__fct_driver_standings.
#}

WITH STANDINGS AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        DRIVER_KEY,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_KEY,
        CHAMPIONSHIP_POSITION,
        POINTS_TO_DATE,
        WINS_TO_DATE,
        PODIUMS_TO_DATE,
        POINTS_FINISHES_TO_DATE,
        RETIREMENTS_TO_DATE,
        RACES_ENTERED_TO_DATE,
        BEST_FINISH_TO_DATE
    FROM {{ ref('mrt__fct_driver_standings') }}

),

FINAL_STANDING AS (

    SELECT
        SEASON,
        DRIVER_ID,
        DRIVER_KEY,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_KEY,
        CHAMPIONSHIP_POSITION AS FINAL_CHAMPIONSHIP_POSITION,
        POINTS_TO_DATE AS TOTAL_POINTS,
        WINS_TO_DATE AS TOTAL_WINS,
        PODIUMS_TO_DATE AS TOTAL_PODIUMS,
        POINTS_FINISHES_TO_DATE AS TOTAL_POINTS_FINISHES,
        RETIREMENTS_TO_DATE AS TOTAL_RETIREMENTS,
        RACES_ENTERED_TO_DATE AS TOTAL_RACES_ENTERED,
        BEST_FINISH_TO_DATE AS BEST_FINISH
    FROM STANDINGS
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SEASON, DRIVER_ID
        ORDER BY ROUND DESC
    ) = 1

),

QUALIFYING_SUMMARY AS (

    SELECT
        SEASON,
        DRIVER_ID,
        COUNT(*) AS TOTAL_QUALIFYING_SESSIONS,
        SUM(IFF(IS_POLE_POSITION, 1, 0)) AS TOTAL_POLE_POSITIONS,
        AVG(QUALIFYING_POSITION) AS AVG_QUALIFYING_POSITION,
        AVG(GAP_TO_POLE_SECONDS) AS AVG_GAP_TO_POLE_SECONDS
    FROM {{ ref('mrt__fct_qualifying_results') }}
    GROUP BY 1, 2

),

RACE_SUMMARY AS (

    SELECT
        SEASON,
        DRIVER_ID,
        AVG(CLASSIFIED_POSITION) AS AVG_FINISH_POSITION,
        AVG(STARTING_GRID_POSITION) AS AVG_GRID_POSITION,
        SUM(POSITIONS_GAINED) AS TOTAL_POSITIONS_GAINED,
        SUM(IFF(IS_FASTEST_LAP, 1, 0)) AS TOTAL_FASTEST_LAPS,
        SUM(LAPS_COMPLETED) AS TOTAL_LAPS_COMPLETED
    FROM {{ ref('mrt__fct_race_results') }}
    GROUP BY 1, 2

),

COMBINED AS (

    SELECT
        ---------- ids
        final_standing.SEASON,
        final_standing.DRIVER_ID,
        final_standing.DRIVER_KEY,
        final_standing.CONSTRUCTOR_ID,
        final_standing.CONSTRUCTOR_KEY,

        ---------- numerics
        final_standing.FINAL_CHAMPIONSHIP_POSITION,
        final_standing.TOTAL_POINTS,
        final_standing.TOTAL_WINS,
        final_standing.TOTAL_PODIUMS,
        final_standing.TOTAL_POINTS_FINISHES,
        final_standing.TOTAL_RETIREMENTS,
        final_standing.TOTAL_RACES_ENTERED,
        final_standing.BEST_FINISH,
        qualifying.TOTAL_QUALIFYING_SESSIONS,
        qualifying.TOTAL_POLE_POSITIONS,
        races.TOTAL_FASTEST_LAPS,
        races.TOTAL_POSITIONS_GAINED,
        races.TOTAL_LAPS_COMPLETED,

        ROUND(qualifying.AVG_QUALIFYING_POSITION, 2)
            AS AVG_QUALIFYING_POSITION,
        ROUND(qualifying.AVG_GAP_TO_POLE_SECONDS, 3)
            AS AVG_GAP_TO_POLE_SECONDS,
        ROUND(races.AVG_FINISH_POSITION, 2) AS AVG_FINISH_POSITION,
        ROUND(races.AVG_GRID_POSITION, 2) AS AVG_GRID_POSITION,

        ROUND(
            final_standing.TOTAL_POINTS
            / NULLIF(final_standing.TOTAL_RACES_ENTERED, 0),
            2
        ) AS AVG_POINTS_PER_RACE,

        ROUND(
            100.0 * final_standing.TOTAL_POINTS_FINISHES
            / NULLIF(final_standing.TOTAL_RACES_ENTERED, 0),
            1
        ) AS PCT_RACES_IN_POINTS,

        ROUND(
            100.0 * final_standing.TOTAL_RETIREMENTS
            / NULLIF(final_standing.TOTAL_RACES_ENTERED, 0),
            1
        ) AS PCT_RACES_RETIRED,

        ---------- booleans
        (final_standing.FINAL_CHAMPIONSHIP_POSITION = 1) AS IS_SEASON_CHAMPION,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM FINAL_STANDING final_standing
    LEFT JOIN QUALIFYING_SUMMARY qualifying
        ON final_standing.SEASON = qualifying.SEASON
        AND final_standing.DRIVER_ID = qualifying.DRIVER_ID
    LEFT JOIN RACE_SUMMARY races
        ON final_standing.SEASON = races.SEASON
        AND final_standing.DRIVER_ID = races.DRIVER_ID

)

SELECT * FROM COMBINED
