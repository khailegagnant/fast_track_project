{#
    Gold: presentation constructor dimension.
#}

WITH CONSTRUCTORS AS (

    SELECT
        CONSTRUCTOR_KEY,
        CONSTRUCTOR_ID,
        CONSTRUCTOR_NAME,
        CONSTRUCTOR_NATIONALITY
    FROM {{ ref('slv_f1__constructors') }}

),

PRESENTED AS (

    SELECT
        ---------- ids
        CONSTRUCTOR_KEY,
        CONSTRUCTOR_ID,

        ---------- strings
        CONSTRUCTOR_NAME,
        CONSTRUCTOR_NATIONALITY,
        CONSTRUCTOR_NAME || ' (' || CONSTRUCTOR_NATIONALITY || ')'
            AS CONSTRUCTOR_LABEL,

        -- dbt audit columns using macro
        {{ audit_columns() }}

    FROM CONSTRUCTORS

)

SELECT * FROM PRESENTED
