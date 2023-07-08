select
    sk_company_id,
    f.company_id,
    QUARTER_START_DATE,
    sum(eps) over (
        partition by f.company_id
        order by QUARTER_START_DATE
        rows between 4 preceding and current row
    ) - eps sum_basic_eps
from {{ ref("financials") }} f
join {{ ref("dim_company") }} c 
on f.company_id = c.company_id
and f.effective_timestamp between c.effective_timestamp and c.end_timestamp