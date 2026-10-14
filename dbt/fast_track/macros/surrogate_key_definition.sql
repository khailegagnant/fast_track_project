{#
    Centralised surrogate-key column definitions so every layer builds the same
    key from the same business columns. Mirrors the surrogate_key_definition
    pattern used in the ACA project.
#}

{% macro circuit_sk() %}
    {{ return(['CIRCUIT_ID']) }}
{% endmacro %}


{% macro constructor_sk() %}
    {{ return(['CONSTRUCTOR_ID']) }}
{% endmacro %}


{% macro driver_sk() %}
    {{ return(['DRIVER_ID']) }}
{% endmacro %}


{% macro race_sk() %}
    {{ return(['SEASON', 'ROUND']) }}
{% endmacro %}


{% macro race_result_sk() %}
    {{ return(['SEASON', 'ROUND', 'DRIVER_ID']) }}
{% endmacro %}


{% macro qualifying_sk() %}
    {{ return(['SEASON', 'ROUND', 'DRIVER_ID']) }}
{% endmacro %}


{% macro driver_standing_sk() %}
    {{ return(['SEASON', 'ROUND', 'DRIVER_ID']) }}
{% endmacro %}


{% macro constructor_standing_sk() %}
    {{ return(['SEASON', 'ROUND', 'CONSTRUCTOR_ID']) }}
{% endmacro %}
