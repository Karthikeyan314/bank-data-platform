{#
    The ledger must reconcile: every posting's previous balance plus its amount equals
    the balance the bank reported. A handful of same-day ties cannot be ordered from
    the data alone, so the test warns on any break and fails the build only if breaks
    exceed `balance_break_error_share` of postings, which would mean the sequencing
    logic, or the source, is wrong.
#}
{{
    config(
        severity='error',
        warn_if='> 0',
        error_if='> ' ~ (var('balance_break_error_share') * 1056320) | int
    )
}}

select
    transaction_id,
    account_id,
    posted_on,
    account_sequence,
    balance_before_reported,
    amount,
    balance_after,
    balance_before_reported + amount - balance_after as gap
from {{ ref('int_transactions_sequenced') }}
where not is_balance_continuous
