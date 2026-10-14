{#
    Staging: 1:1 passthrough over F1_RAW.RAW_DRIVERS.

    Select and rename only. The duplicate rows present in RAW (the API is
    pulled per season) are intentionally preserved here and removed in
    slv_f1__drivers.
#}

WITH SOURCE AS (

    SELECT
        DRIVER_ID,
        PERMANENT_NUMBER,
        DRIVER_CODE,
        GIVEN_NAME,
        FAMILY_NAME,
        DATE_OF_BIRTH,
        NATIONALITY
    FROM {{ source('F1_RAW', 'RAW_DRIVERS') }}

),

RENAMED AS (

    SELECT
        ---------- ids
        DRIVER_ID,

        ---------- strings
        DRIVER_CODE,
        GIVEN_NAME,
        FAMILY_NAME,
        NATIONALITY AS DRIVER_NATIONALITY,

        ---------- numerics
        PERMANENT_NUMBER,

        ---------- dates
        DATE_OF_BIRTH,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM SOURCE

)

SELECT * FROM RENAMED
