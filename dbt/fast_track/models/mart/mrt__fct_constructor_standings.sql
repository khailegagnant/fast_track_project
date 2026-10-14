{#
    Gold: constructor championship standings, modelled as a periodic snapshot
    fact.

    Grain: SEASON + ROUND + CONSTRUCTOR_ID. One row per constructor per
    completed round, carrying cumulative championship state *after* that
    round.

    Design notes:
      - Constructor points are the sum of all their drivers' points in a
        round, so ROUND_RESULT_TOTALS aggregates results up from driver level
        before the cumulative window is applied.
      - A spine of every season entrant crossed with every round keeps the
        snapshot dense, so a constructor that sits out a round still has a
        row with unchanged cumulative totals.
      - CHAMPIONSHIP_POSITION implements the same full FIA countback as
        mrt__fct_driver_standings:
            points
              -> number of 1st places, then 2nds, then 3rds, ... (race)
              -> number of poles, then 2nds, then 3rds, ... (qualifying)
        Every car counts towards the constructor's tally, so a 1-2 finish
        contributes one 1st place and one 2nd place.
      - SCOPE: as with the driver standings, only main-race results are
        landed upstream. Sprint points are absent, so totals read lower than
        the official constructors championship for seasons that ran sprints.
#}

WITH RACES AS (

    SELECT
        RACE_KEY,
        SEASON,
        ROUND,
        RACE_DATE
    FROM {{ ref('slv_f1__races') }}

),

RESULTS AS (

    SELECT
        SEASON,
        ROUND,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_KEY,
        DRIVER_ID,
        POINTS_SCORED,
        CLASSIFIED_POSITION,
        IS_RACE_WINNER,
        IS_PODIUM_FINISH,
        IS_POINTS_FINISH,
        IS_CLASSIFIED
    FROM {{ ref('slv_f1__results') }}

),

QUALIFYING AS (

    SELECT
        SEASON,
        ROUND,
        CONSTRUCTOR_ID,
        QUALIFYING_POSITION
    FROM {{ ref('slv_f1__qualifying') }}

),

SEASON_ENTRANTS AS (

    SELECT DISTINCT
        SEASON,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_KEY
    FROM RESULTS

),

STANDINGS_SPINE AS (

    SELECT
        races.RACE_KEY,
        races.SEASON,
        races.ROUND,
        races.RACE_DATE,
        entrants.CONSTRUCTOR_ID,
        entrants.CONSTRUCTOR_KEY
    FROM RACES races
    INNER JOIN SEASON_ENTRANTS entrants
        ON races.SEASON = entrants.SEASON

),

ROUND_RESULT_TOTALS AS (

    SELECT
        SEASON,
        ROUND,
        CONSTRUCTOR_ID,
        SUM(POINTS_SCORED) AS ROUND_POINTS,
        SUM(IFF(IS_RACE_WINNER, 1, 0)) AS ROUND_WINS,
        SUM(IFF(IS_PODIUM_FINISH, 1, 0)) AS ROUND_PODIUMS,
        SUM(IFF(IS_POINTS_FINISH, 1, 0)) AS ROUND_POINTS_FINISHES,
        SUM(IFF(IS_CLASSIFIED, 0, 1)) AS ROUND_RETIREMENTS,
        MIN(CLASSIFIED_POSITION) AS ROUND_BEST_POSITION,
        COUNT(DISTINCT DRIVER_ID) AS ROUND_CARS_ENTERED,

        -- per-position finish counts across both cars
        {{ f1_position_count_columns('CLASSIFIED_POSITION', 'ROUND_FINISH') }}
    FROM RESULTS
    GROUP BY 1, 2, 3

),

ROUND_QUALIFYING_TOTALS AS (

    SELECT
        SEASON,
        ROUND,
        CONSTRUCTOR_ID,

        -- per-position qualifying counts across both cars
        {{ f1_position_count_columns('QUALIFYING_POSITION', 'ROUND_QUALI') }}
    FROM QUALIFYING
    GROUP BY 1, 2, 3

),

SPINE_WITH_ROUND_TOTALS AS (

    SELECT
        spine.RACE_KEY,
        spine.SEASON,
        spine.ROUND,
        spine.RACE_DATE,
        spine.CONSTRUCTOR_ID,
        spine.CONSTRUCTOR_KEY,
        COALESCE(results.ROUND_POINTS, 0) AS ROUND_POINTS,
        COALESCE(results.ROUND_WINS, 0) AS ROUND_WINS,
        COALESCE(results.ROUND_PODIUMS, 0) AS ROUND_PODIUMS,
        COALESCE(results.ROUND_POINTS_FINISHES, 0) AS ROUND_POINTS_FINISHES,
        COALESCE(results.ROUND_RETIREMENTS, 0) AS ROUND_RETIREMENTS,
        COALESCE(results.ROUND_CARS_ENTERED, 0) AS ROUND_CARS_ENTERED,
        results.ROUND_BEST_POSITION,

        {% for position in range(1, f1_countback_max_position() + 1) -%}
        COALESCE(results.ROUND_FINISH_P{{ position }}, 0)
            AS ROUND_FINISH_P{{ position }},
        {% endfor -%}

        {% for position in range(1, f1_countback_max_position() + 1) -%}
        COALESCE(qualifying.ROUND_QUALI_P{{ position }}, 0)
            AS ROUND_QUALI_P{{ position }},
        {% endfor %}
    FROM STANDINGS_SPINE spine
    LEFT JOIN ROUND_RESULT_TOTALS results
        ON spine.SEASON = results.SEASON
        AND spine.ROUND = results.ROUND
        AND spine.CONSTRUCTOR_ID = results.CONSTRUCTOR_ID
    LEFT JOIN ROUND_QUALIFYING_TOTALS qualifying
        ON spine.SEASON = qualifying.SEASON
        AND spine.ROUND = qualifying.ROUND
        AND spine.CONSTRUCTOR_ID = qualifying.CONSTRUCTOR_ID

),

CUMULATIVE AS (

    SELECT
        RACE_KEY,
        SEASON,
        ROUND,
        RACE_DATE,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_KEY,
        ROUND_POINTS,
        ROUND_WINS,
        ROUND_CARS_ENTERED,
        ROUND_BEST_POSITION,

        SUM(ROUND_POINTS) OVER (
            PARTITION BY SEASON, CONSTRUCTOR_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS POINTS_TO_DATE,

        SUM(ROUND_WINS) OVER (
            PARTITION BY SEASON, CONSTRUCTOR_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS WINS_TO_DATE,

        SUM(ROUND_PODIUMS) OVER (
            PARTITION BY SEASON, CONSTRUCTOR_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS PODIUMS_TO_DATE,

        SUM(ROUND_POINTS_FINISHES) OVER (
            PARTITION BY SEASON, CONSTRUCTOR_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS POINTS_FINISHES_TO_DATE,

        SUM(ROUND_RETIREMENTS) OVER (
            PARTITION BY SEASON, CONSTRUCTOR_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS RETIREMENTS_TO_DATE,

        SUM(ROUND_CARS_ENTERED) OVER (
            PARTITION BY SEASON, CONSTRUCTOR_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS CAR_ENTRIES_TO_DATE,

        MIN(ROUND_BEST_POSITION) OVER (
            PARTITION BY SEASON, CONSTRUCTOR_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS BEST_FINISH_TO_DATE,

        {{ f1_cumulative_position_counts('ROUND_FINISH', 'FINISH', 'SEASON, CONSTRUCTOR_ID') }}
        {{ f1_cumulative_position_counts('ROUND_QUALI', 'QUALI', 'SEASON, CONSTRUCTOR_ID') }}
    FROM SPINE_WITH_ROUND_TOTALS

),

COUNTBACK AS (

    SELECT
        CUMULATIVE.*,
        {{ f1_countback_key('FINISH') }} AS RACE_COUNTBACK_KEY,
        {{ f1_countback_key('QUALI') }} AS QUALIFYING_COUNTBACK_KEY
    FROM CUMULATIVE

),

RANKED AS (

    SELECT
        COUNTBACK.*,

        -- points, then race countback, then qualifying countback
        RANK() OVER (
            PARTITION BY SEASON, ROUND
            ORDER BY
                POINTS_TO_DATE DESC,
                RACE_COUNTBACK_KEY DESC,
                QUALIFYING_COUNTBACK_KEY DESC
        ) AS CHAMPIONSHIP_POSITION,

        MAX(POINTS_TO_DATE) OVER (
            PARTITION BY SEASON, ROUND
        ) AS LEADER_POINTS_TO_DATE
    FROM COUNTBACK

),

CONFORMED AS (

    SELECT
        ---------- ids
        {{ dbt_utils.generate_surrogate_key(constructor_standing_sk()) }}
            AS CONSTRUCTOR_STANDING_KEY,
        RACE_KEY,
        CONSTRUCTOR_KEY,
        SEASON,
        ROUND,
        CONSTRUCTOR_ID,

        ---------- strings
        RACE_COUNTBACK_KEY,
        QUALIFYING_COUNTBACK_KEY,

        ---------- numerics
        CHAMPIONSHIP_POSITION,
        POINTS_TO_DATE,
        WINS_TO_DATE,
        PODIUMS_TO_DATE,
        POINTS_FINISHES_TO_DATE,
        RETIREMENTS_TO_DATE,
        CAR_ENTRIES_TO_DATE,
        BEST_FINISH_TO_DATE,
        FINISH_P2 AS SECOND_PLACES_TO_DATE,
        FINISH_P3 AS THIRD_PLACES_TO_DATE,
        QUALI_P1 AS POLE_POSITIONS_TO_DATE,
        ROUND_POINTS,
        ROUND_WINS,
        ROUND_BEST_POSITION,
        LEADER_POINTS_TO_DATE - POINTS_TO_DATE AS POINTS_BEHIND_LEADER,

        ---------- booleans
        (CHAMPIONSHIP_POSITION = 1) AS IS_CHAMPIONSHIP_LEADER,
        (ROUND_CARS_ENTERED > 0) AS HAS_RACED_THIS_ROUND,

        ---------- dates
        RACE_DATE AS SNAPSHOT_DATE,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM RANKED

)

SELECT * FROM CONFORMED
