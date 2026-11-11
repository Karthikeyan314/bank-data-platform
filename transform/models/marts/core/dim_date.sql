{#
    Calendar dimension. Built with a recursive CTE so it compiles unchanged on DuckDB
    and Snowflake; no package dependency.
#}

with recursive spine (calendar_date) as (
    select cast('2013-01-01' as date)
    union all
    select cast({{ dbt.dateadd('day', 1, 'calendar_date') }} as date)
    from spine
    where calendar_date < cast('2018-12-31' as date)
)

select
    calendar_date as date_day,
    extract(year from calendar_date) as year_number,
    extract(quarter from calendar_date) as quarter_number,
    extract(month from calendar_date) as month_number,
    extract(day from calendar_date) as day_of_month,
    cast({{ dbt.date_trunc('month', 'calendar_date') }} as date) as month_start,
    cast({{ dbt.last_day('calendar_date', 'month') }} as date) as month_end,
    cast(extract(year from calendar_date) as varchar) || '-Q'
    || cast(extract(quarter from calendar_date) as varchar) as year_quarter,
    extract(isodow from calendar_date) in (6, 7) as is_weekend
from spine
