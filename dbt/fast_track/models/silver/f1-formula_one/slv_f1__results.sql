{#
    Silver: typed, cleaned race-result entity.
    Grain: SEASON + ROUND + DRIVER_ID.

    Business rules:
      - POSITION_TEXT is the authority on whether a driver was classified.
        Ergast writes a numeric string for classified finishers and a letter
        code otherwise ('R' retired, 'D' disqualified, 'E' excluded,
        'W' withdrew, 'F' failed to qualify, 'N' not classified).
        FINISH_POSITION is still populated for unclassified drivers, so
        IS_CLASSIFIED must come from POSITION_TEXT, not FINISH_POSITION.
      - CLASSIFIED_POSITION is populated only for classified finishers so
        podium and win counts never pick up retirements.
      - GRID_POSITION = 0 means a pit-lane start. It is normalised to NULL
        for the positions-gained calculation, since "gained 19 places from
        grid 0" is meaningless.
      - FASTEST_LAP_TIME_STR is converted to seconds via the
        f1_lap_time_to_seconds macro.
      - Raw STATUS strings are grouped into reportable categories so
        dashboards stay stable when the API adds a new failure description.
#}

WITH RESULTS AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        POSITION_TEXT,
        STATUS,
        FASTEST_LAP_TIME_STR,
        GRID_POSITION,
        FINISH_POSITION,
        POINTS_SCORED,
        LAPS_COMPLETED,
        RACE_TIME_MILLIS,
        FASTEST_LAP_RANK
    FROM {{ ref('stg_f1__results') }}

),

TYPED AS (

    SELECT
        TRY_TO_NUMBER(SEASON::VARCHAR) AS SEASON,
        TRY_TO_NUMBER(ROUND::VARCHAR) AS ROUND,
        NULLIF(TRIM(DRIVER_ID), '')::VARCHAR AS DRIVER_ID,
        NULLIF(TRIM(CONSTRUCTOR_ID), '')::VARCHAR AS CONSTRUCTOR_ID,
        NULLIF(TRIM(POSITION_TEXT), '')::VARCHAR AS POSITION_TEXT,
        NULLIF(TRIM(STATUS), '')::VARCHAR AS STATUS,
        NULLIF(TRIM(FASTEST_LAP_TIME_STR), '')::VARCHAR
            AS FASTEST_LAP_TIME_STR,
        TRY_TO_NUMBER(GRID_POSITION::VARCHAR) AS GRID_POSITION,
        TRY_TO_NUMBER(FINISH_POSITION::VARCHAR) AS FINISH_POSITION,
        TRY_TO_DECIMAL(POINTS_SCORED::VARCHAR, 6, 2) AS POINTS_SCORED,
        TRY_TO_NUMBER(LAPS_COMPLETED::VARCHAR) AS LAPS_COMPLETED,
        TRY_TO_NUMBER(RACE_TIME_MILLIS::VARCHAR) AS RACE_TIME_MILLIS,
        TRY_TO_NUMBER(FASTEST_LAP_RANK::VARCHAR) AS FASTEST_LAP_RANK
    FROM RESULTS

),

FILTERED AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        POSITION_TEXT,
        STATUS,
        FASTEST_LAP_TIME_STR,
        GRID_POSITION,
        FINISH_POSITION,
        POINTS_SCORED,
        LAPS_COMPLETED,
        RACE_TIME_MILLIS,
        FASTEST_LAP_RANK
    FROM TYPED
    WHERE SEASON IS NOT NULL
      AND ROUND IS NOT NULL
      AND DRIVER_ID IS NOT NULL

),

DEDUPLICATED AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        POSITION_TEXT,
        STATUS,
        FASTEST_LAP_TIME_STR,
        GRID_POSITION,
        FINISH_POSITION,
        POINTS_SCORED,
        LAPS_COMPLETED,
        RACE_TIME_MILLIS,
        FASTEST_LAP_RANK
    FROM FILTERED
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SEASON, ROUND, DRIVER_ID
        ORDER BY
            POINTS_SCORED DESC NULLS LAST,
            FINISH_POSITION ASC NULLS LAST,
            LAPS_COMPLETED DESC NULLS LAST
    ) = 1

),

CLASSIFIED AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        POSITION_TEXT,
        STATUS,
        FASTEST_LAP_TIME_STR,
        FINISH_POSITION,
        POINTS_SCORED,
        LAPS_COMPLETED,
        RACE_TIME_MILLIS,
        FASTEST_LAP_RANK,

        -- pit-lane starts are recorded as grid 0; treat as unknown grid slot
        NULLIF(GRID_POSITION, 0) AS STARTING_GRID_POSITION,
        (GRID_POSITION = 0) AS IS_PIT_LANE_START,

        -- numeric position text == classified finisher
        (POSITION_TEXT REGEXP '^[0-9]+$') AS IS_CLASSIFIED
    FROM DEDUPLICATED

),

DERIVED AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,
        POSITION_TEXT,
        STATUS,
        STARTING_GRID_POSITION,
        FINISH_POSITION,
        POINTS_SCORED,
        LAPS_COMPLETED,
        RACE_TIME_MILLIS,
        FASTEST_LAP_RANK,
        IS_PIT_LANE_START,
        IS_CLASSIFIED,

        CASE WHEN IS_CLASSIFIED THEN FINISH_POSITION END
            AS CLASSIFIED_POSITION,

        {{ f1_lap_time_to_seconds('FASTEST_LAP_TIME_STR') }}
            AS FASTEST_LAP_TIME_SECONDS,

        CASE
            WHEN STATUS = 'Finished' THEN 'FINISHED'
            WHEN STATUS ILIKE '+%Lap%' THEN 'LAPPED'
            WHEN STATUS ILIKE 'Disqualified' THEN 'DISQUALIFIED'
            WHEN STATUS IN (
                'Accident', 'Collision', 'Collision damage', 'Spun off'
            ) THEN 'ACCIDENT'
            ELSE 'MECHANICAL_OR_OTHER'
        END AS FINISH_STATUS_CATEGORY
    FROM CLASSIFIED

),

CONFORMED AS (

    SELECT
        ---------- ids
        {{ dbt_utils.generate_surrogate_key(race_result_sk()) }}
            AS RACE_RESULT_KEY,
        {{ dbt_utils.generate_surrogate_key(race_sk()) }} AS RACE_KEY,
        {{ dbt_utils.generate_surrogate_key(driver_sk()) }} AS DRIVER_KEY,
        {{ dbt_utils.generate_surrogate_key(constructor_sk()) }}
            AS CONSTRUCTOR_KEY,
        SEASON,
        ROUND,
        DRIVER_ID,
        CONSTRUCTOR_ID,

        ---------- strings
        POSITION_TEXT,
        STATUS,
        FINISH_STATUS_CATEGORY,

        ---------- numerics
        STARTING_GRID_POSITION,
        FINISH_POSITION,
        CLASSIFIED_POSITION,
        POINTS_SCORED,
        LAPS_COMPLETED,
        RACE_TIME_MILLIS,
        FASTEST_LAP_RANK,
        FASTEST_LAP_TIME_SECONDS,

        -- positive = places gained from the starting grid
        STARTING_GRID_POSITION - CLASSIFIED_POSITION AS POSITIONS_GAINED,

        ---------- booleans
        IS_CLASSIFIED,
        IS_PIT_LANE_START,
        COALESCE(CLASSIFIED_POSITION = 1, FALSE) AS IS_RACE_WINNER,
        COALESCE(CLASSIFIED_POSITION <= 3, FALSE) AS IS_PODIUM_FINISH,
        COALESCE(POINTS_SCORED > 0, FALSE) AS IS_POINTS_FINISH,
        COALESCE(FASTEST_LAP_RANK = 1, FALSE) AS IS_FASTEST_LAP,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM DERIVED

)

SELECT * FROM CONFORMED
