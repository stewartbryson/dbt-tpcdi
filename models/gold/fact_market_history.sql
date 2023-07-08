SELECT
    s.security_key,
    s.company_key,
    dm_date date_key,
    dmh.dm_close / sum_fi_basic_eps AS peratio,
    (s.dividend / dmh.dm_close) / 100 yield,
    fifty_two_week_high,
    fifty_two_week_high_date_key,
    fifty_two_week_low,
    fifty_two_week_low_date_key,
    dm_close closeprice,
    dm_high dayhigh,
    dm_low daylow,
    dm_vol volume,
    dmh.batchid
FROM {{ ref("daily_market") }} dmh
JOIN {{ ref("dim_security") }} s
ON 
    s.symbol = dmh.dm_s_symb
    AND dmh.dim_date between start_time and end_time
LEFT JOIN {{ ref("wrk_company_financials")}} f
ON 
    f.company_key = s.company_key
    AND quarter(dmh.dm_date) = quarter(fi_qtr_start_date)
    AND year(dmh.dm_date) = year(fi_qtr_start_date)