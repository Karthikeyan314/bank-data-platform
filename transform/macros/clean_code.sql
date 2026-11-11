{#
    Source code columns arrive padded ("  "), blank ("") or NULL for the same meaning
    ("no value"). Normalise all three to NULL and upper-case the rest, so accepted-value
    tests and joins to the code seeds see one representation.
#}
{% macro clean_code(column) -%}
    nullif(upper(trim({{ column }})), '')
{%- endmacro %}


{# Cast text to a decimal amount; empty strings become NULL rather than failing. #}
{% macro to_amount(column, precision=14, scale=2) -%}
    cast(nullif(trim({{ column }}), '') as decimal({{ precision }}, {{ scale }}))
{%- endmacro %}
