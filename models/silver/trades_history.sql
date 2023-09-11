select
    t_id trade_id,
    t_dts trade_timestamp,
    t_ca_id account_id,
    ts.st_name trade_status,
    tt_name trade_type,
    case t_is_cash
        when true then 'Cash'
        when false then 'Margin'
    end transaction_type,
    t_s_symb symbol,
    t_exec_name executor_name,
    t_qty quantity,
    t_bid_price bid_price,
    t_trade_price trade_price,
    t_chrg fee,
    t_comm commission,
    t_tax tax,
    us.st_name update_status,
    th_dts effective_timestamp,
    ifnull(
        timestampadd(
        'millisecond',
        -1,
        lag(th_dts) over (
            partition by t_id
            order by
            th_dts desc
        )
        ),
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
        WHEN (
            row_number() over (
                partition by t_id
                order by
                th_dts desc
            ) = 1
        ) THEN TRUE
        ELSE FALSE
    END as IS_CURRENT
from
    {{ ref('brokerage_trade') }} 
join
    {{ ref('brokerage_trade_history') }}  
on
    t_id = th_t_id
join
    {{ ref('reference_trade_type') }} 
on
    t_tt_id = tt_id
join
    {{ ref('reference_status_type') }} ts
on
    t_st_id = ts.st_id
join
    {{ ref('reference_status_type') }} us
on th_st_id = us.st_id