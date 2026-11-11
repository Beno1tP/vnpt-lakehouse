-- UC5 understood / not understood per day x traffic_class (not understood = fallback or bot_khong_hieu, dim_intent).
-- Not-understood rate over a period = sum(not_understood_cnt) / sum(understood_cnt + not_understood_cnt).
{{ config(table_type='DUPLICATE', keys=['event_date', 'traffic_class'], distributed_by=['event_date'], buckets=1) }}
select
    f.event_date,
    f.traffic_class,
    sum(case when i.is_understood then 1 else 0 end) as understood_cnt,
    sum(case when i.is_understood then 0 else 1 end) as not_understood_cnt
from {{ ref('fct_prediction') }} f
join {{ ref('dim_intent') }} i on i.final_intent = f.final_intent
group by f.event_date, f.traffic_class
