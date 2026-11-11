{{ config(contract={'enforced': true}) }}

{#
    Board-level monthly KPIs. One row per month. This is the table the refresh-over-refresh
    QC compares between runs: closed months must not move, and the newest month must sit
    inside its expected band. The contract stops a refactor from silently renaming or
    retyping a reported column.
#}

with account_months as (
    select * from {{ ref('fct_account_monthly') }}
),

new_accounts as (
    select
        cast({{ dbt.date_trunc('month', 'opened_on') }} as date) as month_start,
        count(*) as n
    from {{ ref('dim_account') }}
    group by month_start
),

loans as (
    select * from {{ ref('fct_loans') }}
),

loans_granted as (
    select
        cast({{ dbt.date_trunc('month', 'granted_on') }} as date) as month_start,
        count(*) as n,
        sum(principal_amount) as principal
    from loans
    group by month_start
),

monthly as (
    select
        month_start,
        count(*) as open_accounts,
        count(case when is_active then 1 end) as active_accounts,
        sum(closing_balance) as total_deposits,
        sum(credits) as credits,
        sum(debits) as debits,
        sum(transaction_count) as transaction_count,
        count(case when was_overdrawn then 1 end) as overdrawn_accounts,
        sum(interest_credited) as interest_credited
    from account_months
    group by month_start
)

select
    m.month_start,
    cast(m.open_accounts as bigint) as open_accounts,
    cast(m.active_accounts as bigint) as active_accounts,
    cast(coalesce(na.n, 0) as bigint) as new_accounts,
    cast(m.total_deposits as decimal(18, 2)) as total_deposits,
    cast(m.credits as decimal(18, 2)) as credits,
    cast(m.debits as decimal(18, 2)) as debits,
    cast(m.transaction_count as bigint) as transaction_count,
    cast(m.overdrawn_accounts as bigint) as overdrawn_accounts,
    cast(m.interest_credited as decimal(18, 2)) as interest_credited,
    cast(coalesce(lg.n, 0) as bigint) as loans_granted,
    cast(coalesce(lg.principal, 0) as decimal(18, 2)) as principal_granted
from monthly as m
left join new_accounts as na on m.month_start = na.month_start
left join loans_granted as lg on m.month_start = lg.month_start
