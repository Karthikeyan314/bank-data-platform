with source as (
    select * from {{ source('bank', 'account') }}
)

select
    cast(account_id as integer) as account_id,
    cast(district_id as integer) as branch_district_id,
    cast(create_date as date) as opened_on,
    case {{ clean_code('frequency') }}
        when 'M' then 'monthly'
        when 'W' then 'weekly'
        when 'T' then 'per_transaction'
    end as statement_frequency,
    _loaded_at
from source
