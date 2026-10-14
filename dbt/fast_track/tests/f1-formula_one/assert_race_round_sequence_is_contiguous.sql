{#
    Business rule: every season's calendar must be a contiguous run of rounds
    starting at 1, with no gaps.

    A gap means a race failed to land from the API, which would silently
    distort every standings snapshot after it. Returns one row per season
    whose round sequence is broken.
#}

WITH SEASON_ROUNDS AS (

    SELECT
        SEASON,
        MIN(ROUND) AS FIRST_ROUND,
        MAX(ROUND) AS LAST_ROUND,
        COUNT(DISTINCT ROUND) AS DISTINCT_ROUNDS
    FROM {{ ref('slv_f1__races') }}
    GROUP BY 1

),

BROKEN_SEQUENCES AS (

    SELECT
        SEASON,
        FIRST_ROUND,
        LAST_ROUND,
        DISTINCT_ROUNDS
    FROM SEASON_ROUNDS
    WHERE FIRST_ROUND != 1
       OR DISTINCT_ROUNDS != LAST_ROUND

)

SELECT * FROM BROKEN_SEQUENCES
