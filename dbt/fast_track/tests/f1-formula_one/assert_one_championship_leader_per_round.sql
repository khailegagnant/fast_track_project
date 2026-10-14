{{
    config(
        severity = 'warn'
    )
}}

{#
    Business rule: each round should normally resolve to exactly one
    championship leader.

    CHAMPIONSHIP_POSITION applies the full FIA countback - points, then every
    finishing position in turn, then every qualifying position in turn - so a
    shared lead now means two competitors are genuinely identical on all of
    those. That is possible but vanishingly rare, and when it happens the
    real championship would be decided by a tie-break beyond what this
    dataset carries. The test is therefore a warning: it surfaces shared
    leads for review without failing the build.
#}

WITH LEADER_COUNTS AS (

    SELECT
        SEASON,
        ROUND,
        COUNT(*) AS LEADER_COUNT
    FROM {{ ref('mrt__fct_driver_standings') }}
    WHERE IS_CHAMPIONSHIP_LEADER
    GROUP BY 1, 2

),

NOT_EXACTLY_ONE_LEADER AS (

    SELECT
        SEASON,
        ROUND,
        LEADER_COUNT
    FROM LEADER_COUNTS
    WHERE LEADER_COUNT != 1

)

SELECT * FROM NOT_EXACTLY_ONE_LEADER
