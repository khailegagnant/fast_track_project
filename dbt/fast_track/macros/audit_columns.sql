{% macro audit_columns(inherit=False) %}
    {#-
        Standard dbt audit columns appended to every staging, silver, and gold
        model. Pass inherit=True to carry forward the upstream model's audit
        lineage instead of stamping the current run.
    -#}
        '{{ run_started_at.isoformat() }}'::TIMESTAMP_NTZ AS DBT_LOADED_AT,

    {% if inherit %}
        DBT_LOADED_BY,
        RUN_ID

    {% else %}
        CURRENT_USER() AS DBT_LOADED_BY,
        '{{ env_var("DBT_ENV_RUN_ID", invocation_id) }}'::VARCHAR AS RUN_ID

    {% endif %}
{% endmacro %}
