with source as (
    select * from {{ source('bank', 'client') }}
)

select
    cast(client_id as integer) as client_id,
    cast(birth_date as date) as birth_date,
    case {{ clean_code('gender') }}
        when 'F' then 'female'
        when 'M' then 'male'
    end as gender,
    cast(district_id as integer) as home_district_id,
    _loaded_at
from source
