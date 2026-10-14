{#
    Dashboard widget 1: Driver championship leaderboard (top 10).
 
    Simulates a BI tools "current standings" table. Pulls the most recent
    round of the most recent season from the periodic-snapshot standings fact
    and joins it to the driver/constructor dimensions for display labels.
#}
 
WITH LATEST_SNAPSHOT AS (
 
    SELECT
        SEASON,
        MAX(ROUND) AS ROUND
    FROM {{ ref('mrt__fct_driver_standings') }}
    WHERE SEASON = (
        SELECT MAX(SEASON) FROM {{ ref('mrt__fct_driver_standings') }}
    )
    GROUP BY 1
 
),
 
STANDINGS AS (
 
    SELECT
        standings.CHAMPIONSHIP_POSITION,
        standings.DRIVER_KEY,
        standings.CONSTRUCTOR_KEY,
        standings.POINTS_TO_DATE,
        standings.WINS_TO_DATE,
        standings.PODIUMS_TO_DATE,
        standings.POINTS_BEHIND_LEADER
    FROM {{ ref('mrt__fct_driver_standings') }} standings
    INNER JOIN LATEST_SNAPSHOT snapshot
        ON standings.SEASON = snapshot.SEASON
        AND standings.ROUND = snapshot.ROUND
 
)
 
SELECT
    standings.CHAMPIONSHIP_POSITION AS POSITION,
    drivers.DRIVER_LABEL AS DRIVER,
    constructors.CONSTRUCTOR_LABEL AS CONSTRUCTOR,
    standings.POINTS_TO_DATE AS POINTS,
    standings.WINS_TO_DATE AS WINS,
    standings.PODIUMS_TO_DATE AS PODIUMS,
    standings.POINTS_BEHIND_LEADER AS GAP_TO_LEADER
FROM STANDINGS standings
INNER JOIN {{ ref('mrt__dim_drivers') }} drivers
    ON standings.DRIVER_KEY = drivers.DRIVER_KEY
INNER JOIN {{ ref('mrt__dim_constructors') }} constructors
    ON standings.CONSTRUCTOR_KEY = constructors.CONSTRUCTOR_KEY
ORDER BY standings.CHAMPIONSHIP_POSITION
LIMIT 10