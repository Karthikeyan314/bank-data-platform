{#
    Fails for every row where the expression is false. Written in-project instead of
    pulling dbt_utils, so the project builds with no package downloads.
#}
{% test expression_is_true(model, expression, column_name=none, where=none) %}
select *
from {{ model }}
where not ({{ expression }})
{% if where %}and ({{ where }}){% endif %}
{% endtest %}
