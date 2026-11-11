{#
    Source-to-target reconciliation: a measure summed on this model must equal the same
    measure on another relation, overall or per group. This is the test that catches a
    join that fanned out (inflated totals) or a filter that silently dropped rows.

    Arguments:
      compare_to:       relation to reconcile against, e.g. ref('stg_bank__transactions')
      measure:          SQL expression on this model, e.g. "count(*)" or "sum(amount)"
      compare_measure:  expression on compare_to (defaults to measure)
      group_by:         optional list of columns present in both relations
      tolerance:        allowed absolute difference (default 0)
#}
{% test reconciles_with(model, compare_to, measure, compare_measure=none, group_by=none, tolerance=0) %}
{%- set compare_measure = compare_measure or measure -%}
{%- set keys = group_by or [] -%}
with target_side as (
    select
        {% for k in keys %}{{ k }}, {% endfor %}
        {{ measure }} as measure_value
    from {{ model }}
    {% if keys %}group by {{ keys | join(', ') }}{% endif %}
),

source_side as (
    select
        {% for k in keys %}{{ k }}, {% endfor %}
        {{ compare_measure }} as measure_value
    from {{ compare_to }}
    {% if keys %}group by {{ keys | join(', ') }}{% endif %}
)

select
    {% for k in keys %}coalesce(t.{{ k }}, s.{{ k }}) as {{ k }}, {% endfor %}
    t.measure_value as model_value,
    s.measure_value as compare_value
from target_side as t
full outer join source_side as s
    {% if keys %}
    on {% for k in keys %}t.{{ k }} = s.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
    {% else %}
    on 1 = 1
    {% endif %}
where t.measure_value is null
   or s.measure_value is null
   or abs(t.measure_value - s.measure_value) > {{ tolerance }}
{% endtest %}
