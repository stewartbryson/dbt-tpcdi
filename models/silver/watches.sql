with s1 as (
    select
        customer_id,
        symbol,
        watch_timestamp,
        action_type,
        company_id,
        company_name,
        exchange_id,
        security_status,
        case action_type
            when 'Activate' then watch_timestamp 
            else null 
        end placed_timestamp,
        case action_type
            when 'Cancelled' then watch_timestamp 
            else null 
        end removed_timestamp
    from
        {{ ref('watches_history') }}
),
s2 as (
    select
        customer_id,
        symbol,
        company_id,
        company_name,
        exchange_id,
        security_status,
        min(placed_timestamp) placed_timestamp, 
        max(removed_timestamp) removed_timestamp
    from s1
    group by all
)
select 
    *,
    case
        when removed_timestamp is null then 'Active'
        else 'Inactive'
    end watch_status
from s2
