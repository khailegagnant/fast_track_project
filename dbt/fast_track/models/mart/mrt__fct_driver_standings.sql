{#
    Gold: driver championship standings, modelled as a periodic snapshot fact.

    Grain: SEASON + ROUND + DRIVER_ID. One row per driver per completed round,
    carrying the cumulative championship state *after* that round.

    Design notes:
      - A spine of every season entrant crossed with every round of that
        season guarantees a dense snapshot. Drivers who miss a round still
        get a row with zero round points and unchanged cumulative totals,
        which is what makes this a periodic snapshot rather than a
        transaction fact.
      - Cumulative measures use ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT
        ROW so each snapshot reflects only rounds up to and including itself.
      - CHAMPIONSHIP_POSITION implements the full FIA countback:
            points
              -> number of 1st places, then 2nds, then 3rds, ... (race)
              -> number of poles, then 2nds, then 3rds, ... (qualifying)
        The position counts are folded into two zero-padded sortable strings
        by the f1_countback macros, so a plain DESC sort on those strings is
        equivalent to comparing position by position. Qualifying is joined in
        purely to supply the final tier of that countback.
      - CONSTRUCTOR_ID is carried forward with LAST_VALUE(... IGNORE NULLS)
        so a driver who misses a round keeps their most recent team. For
        rounds before a drivers season debut there is no earlier team, so
        the value falls back to the first team they appear with.
      - SCOPE: RAW_RESULTS contains only main-race results. Sprint races are
        not landed by the upstream pipeline, so points here are main-race
        points only and will read lower than the official championship
        tables for any season that ran sprints (2021 onward). For example
        2022 Verstappen totals 433 here against an official 454, the 21
        point difference being his three sprint results. This is a source
        coverage limitation, not a calculation error - add the sprint
        endpoint upstream to close the gap.
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
        DRIVER_ID,
        DRIVER_KEY,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_KEY,
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
        DRIVER_ID,
        QUALIFYING_POSITION
    FROM {{ ref('slv_f1__qualifying') }}

),

SEASON_ENTRANTS AS (

    SELECT DISTINCT
        SEASON,
        DRIVER_ID,
        DRIVER_KEY
    FROM RESULTS

),

STANDINGS_SPINE AS (

    SELECT
        races.RACE_KEY,
        races.SEASON,
        races.ROUND,
        races.RACE_DATE,
        entrants.DRIVER_ID,
        entrants.DRIVER_KEY
    FROM RACES races
    INNER JOIN SEASON_ENTRANTS entrants
        ON races.SEASON = entrants.SEASON

),

ROUND_RESULT_TOTALS AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        SUM(POINTS_SCORED) AS ROUND_POINTS,
        SUM(IFF(IS_RACE_WINNER, 1, 0)) AS ROUND_WINS,
        SUM(IFF(IS_PODIUM_FINISH, 1, 0)) AS ROUND_PODIUMS,
        SUM(IFF(IS_POINTS_FINISH, 1, 0)) AS ROUND_POINTS_FINISHES,
        SUM(IFF(IS_CLASSIFIED, 0, 1)) AS ROUND_RETIREMENTS,
        MIN(CLASSIFIED_POSITION) AS ROUND_BEST_POSITION,
        MAX(CONSTRUCTOR_ID) AS ROUND_CONSTRUCTOR_ID,
        MAX(CONSTRUCTOR_KEY) AS ROUND_CONSTRUCTOR_KEY,
        COUNT(*) AS ROUND_ENTRIES,

        -- per-position finish counts feeding the race countback
        {{ f1_position_count_columns('CLASSIFIED_POSITION', 'ROUND_FINISH') }}
    FROM RESULTS
    GROUP BY 1, 2, 3

),

ROUND_QUALIFYING_TOTALS AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,

        -- per-position qualifying counts feeding the qualifying countback
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
        spine.DRIVER_ID,
        spine.DRIVER_KEY,
        COALESCE(results.ROUND_POINTS, 0) AS ROUND_POINTS,
        COALESCE(results.ROUND_WINS, 0) AS ROUND_WINS,
        COALESCE(results.ROUND_PODIUMS, 0) AS ROUND_PODIUMS,
        COALESCE(results.ROUND_POINTS_FINISHES, 0) AS ROUND_POINTS_FINISHES,
        COALESCE(results.ROUND_RETIREMENTS, 0) AS ROUND_RETIREMENTS,
        COALESCE(results.ROUND_ENTRIES, 0) AS ROUND_ENTRIES,
        results.ROUND_BEST_POSITION,
        results.ROUND_CONSTRUCTOR_ID,
        results.ROUND_CONSTRUCTOR_KEY,

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
        AND spine.DRIVER_ID = results.DRIVER_ID
    LEFT JOIN ROUND_QUALIFYING_TOTALS qualifying
        ON spine.SEASON = qualifying.SEASON
        AND spine.ROUND = qualifying.ROUND
        AND spine.DRIVER_ID = qualifying.DRIVER_ID

),

CUMULATIVE AS (

    SELECT
        RACE_KEY,
        SEASON,
        ROUND,
        RACE_DATE,
        DRIVER_ID,
        DRIVER_KEY,
        ROUND_POINTS,
        ROUND_WINS,
        ROUND_ENTRIES,
        ROUND_BEST_POSITION,

        SUM(ROUND_POINTS) OVER (
            PARTITION BY SEASON, DRIVER_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS POINTS_TO_DATE,

        SUM(ROUND_WINS) OVER (
            PARTITION BY SEASON, DRIVER_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS WINS_TO_DATE,

        SUM(ROUND_PODIUMS) OVER (
            PARTITION BY SEASON, DRIVER_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS PODIUMS_TO_DATE,

        SUM(ROUND_POINTS_FINISHES) OVER (
            PARTITION BY SEASON, DRIVER_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS POINTS_FINISHES_TO_DATE,

        SUM(ROUND_RETIREMENTS) OVER (
            PARTITION BY SEASON, DRIVER_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS RETIREMENTS_TO_DATE,

        SUM(ROUND_ENTRIES) OVER (
            PARTITION BY SEASON, DRIVER_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS RACES_ENTERED_TO_DATE,

        MIN(ROUND_BEST_POSITION) OVER (
            PARTITION BY SEASON, DRIVER_ID
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS BEST_FINISH_TO_DATE,

        {{ f1_cumulative_position_counts('ROUND_FINISH', 'FINISH', 'SEASON, DRIVER_ID') }}
        {{ f1_cumulative_position_counts('ROUND_QUALI', 'QUALI', 'SEASON, DRIVER_ID') }}

        -- Carry the most recent team forward across missed rounds. For
        -- rounds *before* a driver's season debut there is nothing earlier
        -- to carry, so fall back to the first team they are seen with.
        -- Both are guaranteed non-NULL because the spine is built from
        -- drivers who have at least one result in the season.
        COALESCE(
            LAST_VALUE(ROUND_CONSTRUCTOR_ID IGNORE NULLS) OVER (
                PARTITION BY SEASON, DRIVER_ID
                ORDER BY ROUND
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            FIRST_VALUE(ROUND_CONSTRUCTOR_ID IGNORE NULLS) OVER (
                PARTITION BY SEASON, DRIVER_ID
                ORDER BY ROUND
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        ) AS CONSTRUCTOR_ID,

        COALESCE(
            LAST_VALUE(ROUND_CONSTRUCTOR_KEY IGNORE NULLS) OVER (
                PARTITION BY SEASON, DRIVER_ID
                ORDER BY ROUND
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            FIRST_VALUE(ROUND_CONSTRUCTOR_KEY IGNORE NULLS) OVER (
                PARTITION BY SEASON, DRIVER_ID
                ORDER BY ROUND
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        ) AS CONSTRUCTOR_KEY
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
        {{ dbt_utils.generate_surrogate_key(driver_standing_sk()) }}
            AS DRIVER_STANDING_KEY,
        RACE_KEY,
        DRIVER_KEY,
        CONSTRUCTOR_KEY,
        SEASON,
        ROUND,
        DRIVER_ID,
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
        RACES_ENTERED_TO_DATE,
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
        (ROUND_ENTRIES > 0) AS HAS_RACED_THIS_ROUND,

        ---------- dates
        RACE_DATE AS SNAPSHOT_DATE,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM RANKED

)

SELECT * FROM CONFORMED
