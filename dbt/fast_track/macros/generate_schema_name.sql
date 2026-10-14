{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {# If no custom +schema is provided, use target.schema (DBT_DEV) #}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {# If +schema is provided, use the exact custom name #}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}

{%- endmacro %}