{#
    Business rule: cumulative championship points can never go down as the
    season progresses.

    A decrease would mean the cumulative window frame is wrong (for example
    RANGE instead of ROWS, or a missing PARTITION BY). Returns the offending
    round alongside the preceding one.
#}

WITH STANDINGS AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        POINTS_TO_DATE,
        LAG(POINTS_TO_DATE) OVER (
            PARTITION BY SEASON, DRIVER_ID
            ORDER BY ROUND
        ) AS PREVIOUS_POINTS_TO_DATE
    FROM {{ ref('mrt__fct_driver_standings') }}

),

DECREASED AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        PREVIOUS_POINTS_TO_DATE,
        POINTS_TO_DATE
    FROM STANDINGS
    WHERE PREVIOUS_POINTS_TO_DATE IS NOT NULL
      AND POINTS_TO_DATE < PREVIOUS_POINTS_TO_DATE

)

SELECT * FROM DECREASED
