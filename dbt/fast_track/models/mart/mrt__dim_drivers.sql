{#
    Gold: presentation driver dimension.
    Adds a broadcast-style display label and the drivers current age.
#}

WITH DRIVERS AS (

    SELECT
        DRIVER_KEY,
        DRIVER_ID,
        DRIVER_CODE,
        GIVEN_NAME,
        FAMILY_NAME,
        DRIVER_FULL_NAME,
        DRIVER_NATIONALITY,
        PERMANENT_NUMBER,
        DATE_OF_BIRTH
    FROM {{ ref('slv_f1__drivers') }}

),

PRESENTED AS (

    SELECT
        ---------- ids
        DRIVER_KEY,
        DRIVER_ID,

        ---------- strings
        DRIVER_CODE,
        GIVEN_NAME,
        FAMILY_NAME,
        DRIVER_FULL_NAME,
        DRIVER_NATIONALITY,
        DRIVER_FULL_NAME || COALESCE(' (' || DRIVER_CODE || ')', '')
            AS DRIVER_LABEL,

        ---------- numerics
        PERMANENT_NUMBER,
        DATEDIFF('year', DATE_OF_BIRTH, CURRENT_DATE()) AS DRIVER_AGE_YEARS,

        ---------- dates
        DATE_OF_BIRTH,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM DRIVERS

)

SELECT * FROM PRESENTED
