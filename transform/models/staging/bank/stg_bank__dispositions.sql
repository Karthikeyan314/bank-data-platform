with source as (
    select * from {{ source('bank', 'disposition') }}
)

select
    cast(disp_id as integer) as disposition_id,
    cast(client_id as integer) as client_id,
    cast(account_id as integer) as account_id,
    case {{ clean_code('disp_type') }}
        when 'O' then 'owner'
        when 'D' then 'disponent'
    end as disposition_role,
    _loaded_at
from source
