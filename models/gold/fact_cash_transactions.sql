with s1 as (
    select
        *,
        to_date(transaction_timestamp) sk_transaction_date
    from
        {{ ref('cash_transactions') }}
)
select
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    transaction_timestamp,
    amount,
    description
from
    s1
join
    {{ ref('dim_account') }} a
on
    s1.account_id = a.account_id
and
    s1.transaction_timestamp between a.effective_timestamp and a.end_timestamp