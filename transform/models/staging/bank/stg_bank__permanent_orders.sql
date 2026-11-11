with source as (
    select * from {{ source('bank', 'permanent_order') }}
)

select
    cast(order_id as integer) as permanent_order_id,
    cast(account_id as integer) as account_id,
    {{ clean_code('bank_to') }} as recipient_bank_code,
    cast(nullif(trim(account_to), '') as bigint) as recipient_account_number,
    -- Orders are debits, so they are stored negative to match transaction signs.
    -1 * {{ to_amount('amount') }} as amount,
    {{ clean_code('category') }} as category_code,
    _loaded_at
from source
