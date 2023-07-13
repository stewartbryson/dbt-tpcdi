select *
from {{ source('brokerage', 'trade_history') }}
