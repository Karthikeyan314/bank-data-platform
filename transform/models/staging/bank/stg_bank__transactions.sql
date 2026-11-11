with source as (
    select * from {{ source('bank', 'transaction') }}
)

select
    cast(trans_id as integer) as transaction_id,
    cast(account_id as integer) as account_id,
    cast(trans_date as date) as posted_on,
    -- Amounts arrive signed: credits positive, debits and withdrawals negative.
    {{ to_amount('amount') }} as amount,
    {{ to_amount('balance') }} as balance_after,
    {{ clean_code('trans_type') }} as transaction_type_code,
    {{ clean_code('operation') }} as operation_code,
    {{ clean_code('category') }} as category_code,
    {{ clean_code('other_bank_id') }} as counterparty_bank_code,
    cast(nullif(trim(other_account_id), '') as bigint) as counterparty_account_number,
    _loaded_at
from source
