with source as (
    select * from {{ source('bank', 'loan') }}
)

select
    cast(loan_id as integer) as loan_id,
    cast(account_id as integer) as account_id,
    cast(granted_date as date) as granted_on,
    {{ to_amount('amount') }} as principal_amount,
    cast(duration as integer) as term_months,
    {{ to_amount('payments') }} as monthly_payment,
    {{ clean_code('status') }} as loan_status_code,
    _loaded_at
from source
