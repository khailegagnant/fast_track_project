{{
    config(
        tags = ['cross_layer', 'reconciliation']
    )
}}

{#
    Business rule: a constructor's championship points for a season must equal
    the sum of the points scored by every car it fielded that season.

    Guards the driver-level -> constructor-level rollup in
    mrt__fct_constructor_standings.

    Tagged cross_layer because it reads from both silver and gold. Layer-by-
    layer orchestration should exclude tag:cross_layer from the per-layer
    tasks and run these once at the end, when every layer exists.
#}

WITH FINAL_STANDINGS AS (

    SELECT
        SEASON,
        CONSTRUCTOR_ID,
        POINTS_TO_DATE
    FROM {{ ref('mrt__fct_constructor_standings') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SEASON, CONSTRUCTOR_ID
        ORDER BY ROUND DESC
    ) = 1

),

ACTUAL_TOTALS AS (

    SELECT
        SEASON,
        CONSTRUCTOR_ID,
        SUM(POINTS_SCORED) AS ACTUAL_POINTS
    FROM {{ ref('slv_f1__results') }}
    GROUP BY 1, 2

),

MISMATCHED AS (

    SELECT
        standings.SEASON,
        standings.CONSTRUCTOR_ID,
        standings.POINTS_TO_DATE,
        actual.ACTUAL_POINTS
    FROM FINAL_STANDINGS standings
    INNER JOIN ACTUAL_TOTALS actual
        ON standings.SEASON = actual.SEASON
        AND standings.CONSTRUCTOR_ID = actual.CONSTRUCTOR_ID
    WHERE standings.POINTS_TO_DATE != actual.ACTUAL_POINTS

)

SELECT * FROM MISMATCHED
