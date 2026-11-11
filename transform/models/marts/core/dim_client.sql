with clients as (
    select * from {{ ref('stg_bank__clients') }}
),

-- Age is measured at the end of the observed period, not today, so it stays stable.
reference_date as (
    select max(posted_on) as as_of_date from {{ ref('stg_bank__transactions') }}
)

select
    c.client_id,
    c.gender,
    c.birth_date,
    {{ dbt.datediff('c.birth_date', 'r.as_of_date', 'year') }}
    - case
        when extract(month from c.birth_date) * 100 + extract(day from c.birth_date)
            > extract(month from r.as_of_date) * 100 + extract(day from r.as_of_date)
            then 1
        else 0
    end as age_at_period_end,
    c.home_district_id,
    d.region as home_region
from clients as c
cross join reference_date as r
left join {{ ref('stg_bank__districts') }} as d on c.home_district_id = d.district_id
