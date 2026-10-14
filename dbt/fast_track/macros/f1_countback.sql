{#
    Countback helpers for championship tie-breaks.

    The FIA tie-break is a lexicographic comparison of how many times a
    competitor achieved each finishing position, best first:

        points
          -> number of 1st places
          -> number of 2nd places
          -> number of 3rd places
          -> ... through the whole field ...
          -> number of pole positions
          -> number of 2nd places in qualifying
          -> ... and so on

    Rather than emit one ORDER BY term per position, these macros build a
    single zero-padded, fixed-width string per competitor - for example
    '015' || '004' || '002' ... for 15 wins, 4 seconds, 2 thirds. Comparing
    those strings with a plain DESC sort is exactly equivalent to the
    position-by-position countback, because every count occupies the same
    number of characters and positions are concatenated best-first.

    Counts are padded to 3 digits, which covers any realistic career total.
#}

{% macro f1_countback_pad_width() %}
    {{ return(3) }}
{% endmacro %}


{% macro f1_countback_max_position() %}
    {{ return(var('f1_countback_max_position')) }}
{% endmacro %}


{% macro f1_position_count_columns(position_column, prefix) %}
    {#-
        Per-round count of finishes (or qualifying classifications) at each
        position. Intended for use inside a GROUP BY aggregate CTE.
    -#}
    {%- set max_position = f1_countback_max_position() -%}
    {%- for position in range(1, max_position + 1) %}
        SUM(IFF({{ position_column }} = {{ position }}, 1, 0))
            AS {{ prefix }}_P{{ position }},
    {%- endfor %}
{% endmacro %}


{% macro f1_cumulative_position_counts(source_prefix, target_prefix, partition_by) %}
    {#-
        Season-to-date running totals of each position count, matching the
        cumulative frame used by the rest of the standings measures.
    -#}
    {%- set max_position = f1_countback_max_position() -%}
    {%- for position in range(1, max_position + 1) %}
        SUM({{ source_prefix }}_P{{ position }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY ROUND
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS {{ target_prefix }}_P{{ position }},
    {%- endfor %}
{% endmacro %}


{% macro f1_countback_key(prefix) %}
    {#-
        Concatenates the cumulative position counts into one sortable string.
        Sorting this DESC reproduces the position-by-position countback.
    -#}
    {%- set max_position = f1_countback_max_position() -%}
    {%- set pad_width = f1_countback_pad_width() -%}
    {%- for position in range(1, max_position + 1) -%}
        LPAD(COALESCE({{ prefix }}_P{{ position }}, 0)::VARCHAR, {{ pad_width }}, '0')
        {{- " || " if not loop.last }}
    {%- endfor -%}
{% endmacro %}
