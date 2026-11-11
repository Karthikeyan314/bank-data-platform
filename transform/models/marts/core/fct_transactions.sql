{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

{#
    Transaction fact at posting grain, with codes decoded through the seed. Incremental:
    only the lookback window and new postings are rewritten on each run.
#}

with sequenced as (
    select * from {{ ref('int_transactions_sequenced') }}
    {% if is_incremental() %}
        where posted_on > (
                select {{ dbt.dateadd('day', -1 * var('incremental_lookback_days'), 'max(posted_on)') }}
                from {{ this }}
            )
    {% endif %}
),

codes as (
    select * from {{ ref('transaction_codes') }}
)

select
    s.transaction_id,
    s.account_id,
    s.posted_on,
    s.account_sequence,
    s.amount,
    case when s.amount >= 0 then s.amount else 0 end as credit_amount,
    case when s.amount < 0 then -s.amount else 0 end as debit_amount,
    s.balance_before_reported,
    s.balance_after,
    s.transaction_type_code,
    tt.description as transaction_type,
    s.operation_code,
    op.description as operation_name,
    s.category_code,
    coalesce(cat.description, 'Uncategorised') as category,
    s.counterparty_bank_code,
    s.counterparty_account_number,
    s.is_balance_continuous
from sequenced as s
left join codes as tt on tt.code_type = 'trans_type' and s.transaction_type_code = tt.code
left join codes as op on op.code_type = 'operation' and s.operation_code = op.code
left join codes as cat on cat.code_type = 'category' and s.category_code = cat.code
