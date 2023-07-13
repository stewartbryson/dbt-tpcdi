select *
from {{ source('brokerage', 'holding_history') }}