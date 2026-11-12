{#
    Flattens the client-to-account bridge to one row per account: the owner, the
    optional disponent and the account's best card. Built here once so every mart
    joins accounts to people the same way and none of them fans out on dispositions.
#}

with dispositions as (
    select * from {{ ref('stg_bank__dispositions') }}
),

cards as (
    select
        c.disposition_id,
        c.card_id,
        c.issued_on,
        t.card_type,
        t.card_tier_rank
    from {{ ref('stg_bank__cards') }} as c
    inner join {{ ref('card_types') }} as t on c.card_type_code = t.card_type_code
),

owners as (
    select
        account_id,
        client_id as owner_client_id,
        disposition_id as owner_disposition_id
    from dispositions
    where disposition_role = 'owner'
),

disponents as (
    select
        account_id,
        client_id as disponent_client_id
    from dispositions
    where disposition_role = 'disponent'
),

best_card as (
    select
        d.account_id,
        cards.card_id,
        cards.card_type,
        cards.issued_on as card_issued_on
    from cards
    inner join dispositions as d on cards.disposition_id = d.disposition_id
    qualify row_number() over (
            partition by d.account_id
            order by cards.card_tier_rank desc, cards.issued_on asc
        ) = 1
)

select
    o.account_id,
    o.owner_client_id,
    o.owner_disposition_id,
    dp.disponent_client_id,
    dp.disponent_client_id is not null as has_disponent,
    bc.card_id,
    bc.card_type,
    bc.card_issued_on
from owners as o
left join disponents as dp on o.account_id = dp.account_id
left join best_card as bc on o.account_id = bc.account_id
