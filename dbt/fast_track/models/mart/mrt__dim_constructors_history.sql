{#
    Mart: full SCD Type 2 history of the constructor entity, for ad hoc "what
    was true about this constructor as of date X" lookups.

    Standalone - deliberately not joined to any fact. CONSTRUCTOR_KEY is a
    hash of CONSTRUCTOR_ID alone, so it is identical across every historical
    version; joining this table to a fact on CONSTRUCTOR_KEY would fan out
    one fact row into N historical versions. Use mrt__dim_constructors for
    all fact joins.
#}

WITH HISTORY AS (

    SELECT
        CONSTRUCTOR_KEY,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_NAME,
        CONSTRUCTOR_NATIONALITY,
        DBT_SCD_ID,
        DBT_VALID_FROM,
        DBT_VALID_TO
    FROM {{ ref('snap_f1__constructors') }}

),

PRESENTED AS (

    SELECT
        ---------- ids
        DBT_SCD_ID AS CONSTRUCTOR_VERSION_KEY,
        CONSTRUCTOR_KEY,
        CONSTRUCTOR_ID,

        ---------- strings
        CONSTRUCTOR_NAME,
        CONSTRUCTOR_NATIONALITY,

        ---------- booleans
        (DBT_VALID_TO IS NULL) AS IS_CURRENT,

        ---------- timestamps
        DBT_VALID_FROM,
        DBT_VALID_TO
    FROM HISTORY

)

SELECT * FROM PRESENTED
