
select
    to_timestamp(pts, 'yyyymmdd-hhmiss') as pts,
    company_name,
    to_number(cik) as cik,
    status,
    industry_id,
    sp_rating,
    to_date(nullif(trim(founding_date), ''), 'yyyymmdd') as founding_date,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_province,
    country,
    ceo_name,
    description
from {{ source("finwire", "cmp") }}
