{#
    Periodic snapshot fact: one row per account per month from the month it opened to the
    last month in the data, including months with no activity (balance carried forward).

    Closing balance is the bank's reported balance after the month's last posting, not a
    running sum, so it never drifts. Opening balance is the previous month's closing.
    `rounding_adjustment` is what is left after opening + net flow; the mirror rounds
    amounts and balances separately, so it is a few cents at most, and a test bounds it.
#}

with transactions as (
    select * from {{ ref('int_transactions_sequenced') }}
),

months as (
    select distinct
        month_start,
        month_end
    from {{ ref('dim_date') }}
),

data_end as (
    select cast({{ dbt.date_trunc('month', 'max(posted_on)') }} as date) as last_month
    from transactions
),

account_months as (
    select
        a.account_id,
        m.month_start,
        m.month_end
    from {{ ref('stg_bank__accounts') }} as a
    inner join months as m
        on m.month_start >= cast({{ dbt.date_trunc('month', 'a.opened_on') }} as date)
    cross join data_end as e
    where m.month_start <= e.last_month
),

monthly_activity as (
    select
        account_id,
        cast({{ dbt.date_trunc('month', 'posted_on') }} as date) as month_start,
        count(*) as transaction_count,
        sum(case when amount >= 0 then amount else 0 end) as credits,
        sum(case when amount < 0 then -amount else 0 end) as debits,
        sum(amount) as net_flow,
        min(balance_after) as min_balance,
        max(balance_after) as max_balance,
        sum(case when category_code = 'IC' then amount else 0 end) as interest_credited,
        sum(case when category_code = 'IO' then -amount else 0 end) as overdraft_interest,
        count(case when not is_balance_continuous then 1 end) as balance_breaks
    from transactions
    group by 1, 2
),

month_close as (
    select
        account_id,
        cast({{ dbt.date_trunc('month', 'posted_on') }} as date) as month_start,
        balance_after as closing_balance
    from transactions
    qualify row_number() over (
            partition by account_id, {{ dbt.date_trunc('month', 'posted_on') }}
            order by account_sequence desc
        ) = 1
),

joined as (
    select
        am.account_id,
        am.month_start,
        am.month_end,
        coalesce(ma.transaction_count, 0) as transaction_count,
        coalesce(ma.credits, 0) as credits,
        coalesce(ma.debits, 0) as debits,
        coalesce(ma.net_flow, 0) as net_flow,
        coalesce(ma.interest_credited, 0) as interest_credited,
        coalesce(ma.overdraft_interest, 0) as overdraft_interest,
        coalesce(ma.balance_breaks, 0) as balance_breaks,
        ma.min_balance,
        ma.max_balance,
        last_value(mc.closing_balance ignore nulls) over (
            partition by am.account_id
            order by am.month_start
            rows between unbounded preceding and current row
        ) as closing_balance
    from account_months as am
    left join monthly_activity as ma
        on am.account_id = ma.account_id and am.month_start = ma.month_start
    left join month_close as mc
        on am.account_id = mc.account_id and am.month_start = mc.month_start
),

with_opening as (
    select
        *,
        coalesce(
            lag(closing_balance) over (
                partition by account_id
                order by month_start
            ),
            -- First month: the balance before the account's first posting (usually 0).
            closing_balance - net_flow
        ) as opening_balance
    from joined
)

select
    account_id,
    month_start,
    month_end,
    opening_balance,
    credits,
    debits,
    net_flow,
    closing_balance,
    closing_balance - opening_balance - net_flow as rounding_adjustment,
    coalesce(min_balance, closing_balance) as min_balance,
    coalesce(max_balance, closing_balance) as max_balance,
    coalesce(min_balance, closing_balance) < 0 as was_overdrawn,
    transaction_count,
    transaction_count > 0 as is_active,
    interest_credited,
    overdraft_interest,
    balance_breaks
from with_opening
