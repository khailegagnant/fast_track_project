{% macro drop_renamed_orphans() %}
    {#-
        One-off maintenance helper. Renaming a model does not drop the
        relation built under its previous name, so the gold -> mart and
        silver dim_/fct_ -> entity renames left orphaned objects behind.

        Run once per target after the rename:
            dbt run-operation drop_renamed_orphans
    -#}
    {% set orphan_tables = [
        'SLV_F1__DIM_CIRCUITS',
        'SLV_F1__DIM_CONSTRUCTORS',
        'SLV_F1__DIM_DRIVERS',
        'SLV_F1__DIM_RACES',
        'SLV_F1__FCT_QUALIFYING',
        'SLV_F1__FCT_RESULTS'
    ] %}

    {% set pipeline_schema = target.schema ~ '_F1_PIPELINE' %}
    {% set old_mart_schema = target.schema ~ '_F1_ANALYTICS' %}

    {% for relation_name in orphan_tables %}
        {% set sql %}
            DROP TABLE IF EXISTS
                {{ target.database }}.{{ pipeline_schema }}.{{ relation_name }}
        {% endset %}
        {% do run_query(sql) %}
        {% do log('dropped ' ~ pipeline_schema ~ '.' ~ relation_name, info=True) %}
    {% endfor %}

    {% set drop_schema_sql %}
        DROP SCHEMA IF EXISTS
            {{ target.database }}.{{ old_mart_schema }} CASCADE
    {% endset %}
    {% do run_query(drop_schema_sql) %}
    {% do log('dropped schema ' ~ old_mart_schema, info=True) %}
{% endmacro %}
