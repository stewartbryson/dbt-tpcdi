select *
from {{ source('syndicated', 'prospect') }}