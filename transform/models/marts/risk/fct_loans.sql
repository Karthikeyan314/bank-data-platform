{{ config(contract={'enforced': true}) }}

{#
    One row per loan with its status, risk flags, repayments seen in the ledger and an
    outstanding-principal estimate. The contract fixes names and types, because risk
    and regulatory reporting read this table directly.
#}

with loans as (
    select * from {{ ref('stg_bank__loans') }}
),

statuses as (
    select * from {{ ref('loan_statuses') }}
),

repayments as (
    select
        t.account_id,
        sum(-t.amount) as repaid_amount,
        count(*) as repayment_count,
        max(t.posted_on) as last_repayment_on
    from {{ ref('int_transactions_sequenced') }} as t
    inner join loans as l on t.account_id = l.account_id
    where t.category_code = 'LO' and t.posted_on >= l.granted_on
    group by t.account_id
),

as_of as (
    select max(posted_on) as as_of_date from {{ ref('stg_bank__transactions') }}
)

select
    l.loan_id,
    l.account_id,
    a.owner_client_id,
    a.branch_district_id,
    l.granted_on,
    cast(extract(year from l.granted_on) as varchar) || '-Q'
    || cast(extract(quarter from l.granted_on) as varchar) as vintage_quarter,
    l.principal_amount,
    l.term_months,
    l.monthly_payment,
    l.loan_status_code,
    s.loan_status,
    s.risk_bucket,
    s.is_contract_finished,
    s.is_in_default,
    cast(coalesce(r.repaid_amount, 0) as decimal(14, 2)) as repaid_amount,
    cast(coalesce(r.repayment_count, 0) as integer) as repayment_count,
    r.last_repayment_on,
    cast(
        case
            when l.loan_status_code = 'A' then 0
            else greatest(l.principal_amount - coalesce(r.repaid_amount, 0), 0)
        end as decimal(14, 2)
    ) as outstanding_principal,
    cast(
        least({{ dbt.datediff('l.granted_on', 'o.as_of_date', 'month') }}, l.term_months)
        as integer
    ) as months_on_book
from loans as l
inner join statuses as s on l.loan_status_code = s.loan_status_code
left join {{ ref('dim_account') }} as a on l.account_id = a.account_id
left join repayments as r on l.account_id = r.account_id
cross join as_of as o
