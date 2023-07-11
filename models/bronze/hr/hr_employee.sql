select *
from {{ source('hr', 'hr') }}