{% macro f1_lap_time_to_seconds(column_name) %}
    {#-
        Converts an F1 lap or qualifying time string formatted as 'M:SS.sss'
        (for example '1:31.471') into NUMBER(9,3) seconds (91.471).

        Returns NULL when the input is NULL, blank, or does not contain the
        ':' separator, so malformed source values degrade to NULL rather than
        raising a cast error.
    -#}
    CASE
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN TRIM({{ column_name }}) = '' THEN NULL
        WHEN POSITION(':' IN {{ column_name }}) = 0 THEN NULL
        ELSE
            (TRY_TO_NUMBER(SPLIT_PART({{ column_name }}, ':', 1)) * 60)
            + TRY_TO_DECIMAL(SPLIT_PART({{ column_name }}, ':', 2), 9, 3)
    END::NUMBER(9, 3)
{% endmacro %}
