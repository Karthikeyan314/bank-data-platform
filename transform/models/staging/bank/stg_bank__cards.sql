with source as (
    select * from {{ source('bank', 'card') }}
)

select
    cast(card_id as integer) as card_id,
    cast(disp_id as integer) as disposition_id,
    {{ clean_code('card_type') }} as card_type_code,
    cast(issued_date as date) as issued_on,
    _loaded_at
from source
