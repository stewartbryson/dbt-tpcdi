SELECT
    {{dbt_utils.generate_surrogate_key(['account_id','a.effective_timestamp'])}} sk_account_id,
    a.account_id,
    sk_broker_id,
    sk_customer_id,
    a.status,
    account_desc,
    tax_status,
    a.effective_timestamp,
    a.end_timestamp,
    a.is_current
from
    {{ ref('accounts') }} a
join
    {{ ref('dim_customer') }} c
on a.customer_id = c.customer_id
and a.effective_timestamp between c.effective_timestamp and c.end_timestamp
join
    {{ ref('dim_broker') }} b 
using (broker_id)