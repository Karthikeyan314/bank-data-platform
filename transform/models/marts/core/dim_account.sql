with accounts as (
    select * from {{ ref('stg_bank__accounts') }}
),

parties as (
    select * from {{ ref('int_account_parties') }}
),

loans as (
    select
        account_id,
        loan_id
    from {{ ref('stg_bank__loans') }}
),

orders as (
    select
        account_id,
        count(*) as standing_orders
    from {{ ref('stg_bank__permanent_orders') }}
    group by account_id
)

select
    a.account_id,
    a.opened_on,
    a.statement_frequency,
    a.branch_district_id,
    p.owner_client_id,
    p.disponent_client_id,
    p.has_disponent,
    p.card_type,
    p.card_issued_on,
    l.loan_id is not null as has_loan,
    coalesce(o.standing_orders, 0) as standing_orders
from accounts as a
left join parties as p on a.account_id = p.account_id
left join loans as l on a.account_id = l.account_id
left join orders as o on a.account_id = o.account_id
