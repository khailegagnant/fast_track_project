{{
    config(
        tags = ['cross_layer', 'reconciliation']
    )
}}

{#
    Business rule: the final driver standings snapshot of a season must equal
    the sum of every point that driver actually scored in that season.

    This is the single most important guard on the standings models - it
    proves the cumulative window and the dense spine did not double-count or
    drop a round. Returns one row per driver whose totals disagree.

    Tagged cross_layer because it reads from both silver and gold. Layer-by-
    layer orchestration should exclude tag:cross_layer from the per-layer
    tasks and run these once at the end, when every layer exists.
#}

WITH FINAL_STANDINGS AS (

    SELECT
        SEASON,
        DRIVER_ID,
        POINTS_TO_DATE,
        WINS_TO_DATE
    FROM {{ ref('mrt__fct_driver_standings') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SEASON, DRIVER_ID
        ORDER BY ROUND DESC
    ) = 1

),

ACTUAL_TOTALS AS (

    SELECT
        SEASON,
        DRIVER_ID,
        SUM(POINTS_SCORED) AS ACTUAL_POINTS,
        SUM(IFF(IS_RACE_WINNER, 1, 0)) AS ACTUAL_WINS
    FROM {{ ref('slv_f1__results') }}
    GROUP BY 1, 2

),

MISMATCHED AS (

    SELECT
        standings.SEASON,
        standings.DRIVER_ID,
        standings.POINTS_TO_DATE,
        actual.ACTUAL_POINTS,
        standings.WINS_TO_DATE,
        actual.ACTUAL_WINS
    FROM FINAL_STANDINGS standings
    INNER JOIN ACTUAL_TOTALS actual
        ON standings.SEASON = actual.SEASON
        AND standings.DRIVER_ID = actual.DRIVER_ID
    WHERE standings.POINTS_TO_DATE != actual.ACTUAL_POINTS
       OR standings.WINS_TO_DATE != actual.ACTUAL_WINS

)

SELECT * FROM MISMATCHED
