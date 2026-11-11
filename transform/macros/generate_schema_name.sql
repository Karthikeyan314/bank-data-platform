{#
    Use the configured schema name as-is (staging, core, risk ...) instead of dbt's
    default "<target_schema>_<custom_schema>", so the warehouse layout reads like the
    architecture diagram. Models without a custom schema land in the target schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
