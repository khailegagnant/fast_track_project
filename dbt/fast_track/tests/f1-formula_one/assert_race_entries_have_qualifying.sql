{#
    Business rule: a driver's grid position for a race should be reconcilable
    with their qualifying classification.

    Grid penalties, pit-lane starts, and failures to qualify all legitimately
    break the 1:1 relationship, so this test only asserts the much weaker
    rule that every race entry has a matching qualifying row. A missing
    qualifying row usually means the API pull dropped a session.

    Severity is warn because historical rounds occasionally have genuinely
    missing qualifying data in the source.
#}

{{
    config(
        severity = 'warn'
    )
}}

WITH RACE_ENTRIES AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID
    FROM {{ ref('slv_f1__results') }}

),

QUALIFYING_ENTRIES AS (

    SELECT
        SEASON,
        ROUND,
        DRIVER_ID
    FROM {{ ref('slv_f1__qualifying') }}

),

RACED_WITHOUT_QUALIFYING AS (

    SELECT
        race_entries.SEASON,
        race_entries.ROUND,
        race_entries.DRIVER_ID
    FROM RACE_ENTRIES race_entries
    LEFT JOIN QUALIFYING_ENTRIES qualifying_entries
        ON race_entries.SEASON = qualifying_entries.SEASON
        AND race_entries.ROUND = qualifying_entries.ROUND
        AND race_entries.DRIVER_ID = qualifying_entries.DRIVER_ID
    WHERE qualifying_entries.DRIVER_ID IS NULL

)

SELECT * FROM RACED_WITHOUT_QUALIFYING
