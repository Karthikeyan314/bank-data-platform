{#
    End-to-end reconciliation: per month, the published fact must hold exactly the rows
    and the money that arrived in raw. Any difference means a model dropped, duplicated
    or mis-typed a posting somewhere between landing and the mart.
#}

with raw_side as (
    select
        cast({{ dbt.date_trunc('month', 'cast(trans_date as date)') }} as date) as month_start,
        count(*) as row_count,
        sum(cast(amount as decimal(14, 2))) as total_amount
    from {{ source('bank', 'transaction') }}
    group by 1
),

mart_side as (
    select
        cast({{ dbt.date_trunc('month', 'posted_on') }} as date) as month_start,
        count(*) as row_count,
        sum(amount) as total_amount
    from {{ ref('fct_transactions') }}
    group by 1
)

select
    coalesce(r.month_start, m.month_start) as month_start,
    r.row_count as raw_rows,
    m.row_count as mart_rows,
    r.total_amount as raw_amount,
    m.total_amount as mart_amount
from raw_side as r
full outer join mart_side as m on r.month_start = m.month_start
where r.row_count is distinct from m.row_count
    or r.total_amount is distinct from m.total_amount
