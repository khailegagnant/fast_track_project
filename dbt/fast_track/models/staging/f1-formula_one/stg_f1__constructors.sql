{#
    Staging: 1:1 passthrough over F1_RAW.RAW_CONSTRUCTORS.

    Select and rename only - NAME and NATIONALITY are qualified so they do
    not collide downstream. The duplicate rows present in RAW (the API is
    pulled per season) are intentionally preserved here and removed in
    slv_f1__constructors.
#}

WITH SOURCE AS (

    SELECT
        CONSTRUCTOR_ID,
        NAME,
        NATIONALITY
    FROM {{ source('F1_RAW', 'RAW_CONSTRUCTORS') }}

),

RENAMED AS (

    SELECT
        ---------- ids
        CONSTRUCTOR_ID,

        ---------- strings
        NAME AS CONSTRUCTOR_NAME,
        NATIONALITY AS CONSTRUCTOR_NATIONALITY,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM SOURCE

)

SELECT * FROM RENAMED
