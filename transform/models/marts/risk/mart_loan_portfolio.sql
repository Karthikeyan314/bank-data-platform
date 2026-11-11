{#
    Loan book by vintage and region: the grain a credit-risk committee reviews.
    Default rate counts loans in status B (written off) or D (in arrears).
#}

with loans as (
    select * from {{ ref('fct_loans') }}
),

districts as (
    select
        district_id,
        region
    from {{ ref('dim_district') }}
)

select
    l.vintage_quarter,
    d.region,
    count(*) as loans,
    sum(l.principal_amount) as principal_amount,
    sum(l.outstanding_principal) as outstanding_principal,
    count(case when l.is_in_default then 1 end) as defaulted_loans,
    sum(case when l.is_in_default then l.principal_amount else 0 end) as defaulted_principal,
    round(count(case when l.is_in_default then 1 end) * 1.0 / count(*), 4) as default_rate,
    round(avg(l.term_months), 1) as avg_term_months
from loans as l
left join districts as d on l.branch_district_id = d.district_id
group by l.vintage_quarter, d.region
