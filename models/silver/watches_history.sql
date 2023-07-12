
with s1 as (
    select
        w_c_id customer_id,
        w_s_symb symbol,
        w_dts watch_timestamp,
        case w_action
        when 'ACTV' then 'Activate'
        when 'CNCL' then 'Cancelled'
        else null end action_type
    from
        {{ ref('brokerage_watch_history') }}
)
select 
    s1.*,
    company_id,
    company_name,
    exchange_id,
    status security_status
from 
    s1
join
    {{ ref('securities') }} s
using (symbol)