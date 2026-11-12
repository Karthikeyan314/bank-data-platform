{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

{#
    Recovers the order in which transactions actually posted.

    The source has no posting time, and transaction_id is not chronological within a
    day, so ordering by (date, id) breaks the running balance on ~50% of rows. The
    bank posted credits before debits within a day; inside each group the reported
    balance itself gives the order (credits raise it, debits lower it). Ordering by
    (date, direction, balance path) reconciles all but a handful of 1.05M steps.

    Each row is then checked: previous balance + amount must equal the reported
    balance within a cent, because the mirror rounded amount and balance separately.

    Incremental: each run re-sequences the last `incremental_lookback_days` of every
    account plus anything new, and seeds the window from the last balance before it.
#}

with new_rows as (
    select *
    from {{ ref('stg_bank__transactions') }}
    {% if is_incremental() %}
        where posted_on > (
                select {{ dbt.dateadd('day', -1 * var('incremental_lookback_days'), 'max(posted_on)') }}
                from {{ this }}
            )
    {% endif %}
),

{% if is_incremental() %}
-- The last already-sequenced row before the window anchors each account's balance chain.
    anchor as (
        select
            account_id,
            balance_after as anchor_balance,
            account_sequence as anchor_sequence
        from {{ this }}
        where posted_on <= (
                select {{ dbt.dateadd('day', -1 * var('incremental_lookback_days'), 'max(posted_on)') }}
                from {{ this }}
            )
        qualify row_number() over (
                partition by account_id
                order by account_sequence desc
            ) = 1
    ),
{% endif %}

ordered as (
    select
        *,
        row_number() over (
            partition by account_id
            order by
                posted_on,
                case when amount >= 0 then 0 else 1 end,
                case when amount >= 0 then balance_after else -balance_after end,
                transaction_id
        ) as window_sequence
    from new_rows
),

chained as (
    select
        o.*,
        {% if is_incremental() %}
            coalesce(a.anchor_sequence, 0) + o.window_sequence as account_sequence,
            coalesce(
                lag(o.balance_after) over (
                    partition by o.account_id
                    order by o.window_sequence
                ),
                a.anchor_balance
            ) as balance_before_reported
        {% else %}
        o.window_sequence as account_sequence,
        lag(o.balance_after) over (partition by o.account_id order by o.window_sequence)
            as balance_before_reported
        {% endif %}
    from ordered as o
    {% if is_incremental() %}
        left join anchor as a on o.account_id = a.account_id
    {% endif %}
)

select
    transaction_id,
    account_id,
    posted_on,
    account_sequence,
    amount,
    balance_before_reported,
    balance_after,
    transaction_type_code,
    operation_code,
    category_code,
    counterparty_bank_code,
    counterparty_account_number,
    -- First transaction of an account opens it, so there is no previous balance to check.
    case
        when balance_before_reported is null then true
        else abs(balance_before_reported + amount - balance_after) <= {{ var('balance_tolerance') }}
    end as is_balance_continuous,
    _loaded_at
from chained
