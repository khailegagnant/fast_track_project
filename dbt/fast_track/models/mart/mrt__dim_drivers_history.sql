{#
    Mart: full SCD Type 2 history of the driver entity, for ad hoc "what was
    true about this driver as of date X" lookups.

    Standalone - deliberately not joined to any fact. DRIVER_KEY is a hash of
    DRIVER_ID alone, so it is identical across every historical version;
    joining this table to a fact on DRIVER_KEY would fan out one fact row
    into N historical versions. Use mrt__dim_drivers for all fact joins.
#}

WITH HISTORY AS (

    SELECT
        DRIVER_KEY,
        DRIVER_ID,
        DRIVER_CODE,
        GIVEN_NAME,
        FAMILY_NAME,
        DRIVER_FULL_NAME,
        DRIVER_NATIONALITY,
        PERMANENT_NUMBER,
        DATE_OF_BIRTH,
        DBT_SCD_ID,
        DBT_VALID_FROM,
        DBT_VALID_TO
    FROM {{ ref('snap_f1__drivers') }}

),

PRESENTED AS (

    SELECT
        ---------- ids
        DBT_SCD_ID AS DRIVER_VERSION_KEY,
        DRIVER_KEY,
        DRIVER_ID,

        ---------- strings
        DRIVER_CODE,
        GIVEN_NAME,
        FAMILY_NAME,
        DRIVER_FULL_NAME,
        DRIVER_NATIONALITY,

        ---------- numerics
        PERMANENT_NUMBER,

        ---------- booleans
        (DBT_VALID_TO IS NULL) AS IS_CURRENT,

        ---------- dates
        DATE_OF_BIRTH,

        ---------- timestamps
        DBT_VALID_FROM,
        DBT_VALID_TO
    FROM HISTORY

)

SELECT * FROM PRESENTED
