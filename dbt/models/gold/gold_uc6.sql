-- UC6 intents per day x final_intent x intent_group x traffic_class. Top N over a period = order by sum(message_cnt);
-- intent_group separates business intents from fallback and bot_khong_hieu (generic top-N, leader 10-23).
{{ config(table_type='DUPLICATE', keys=['event_date', 'final_intent'], distributed_by=['final_intent'], buckets=1) }}
select
    f.event_date,
    f.final_intent,
    i.intent_group,
    f.traffic_class,
    count(*) as message_cnt
from {{ ref('fct_prediction') }} f
join {{ ref('dim_intent') }} i on i.final_intent = f.final_intent
group by f.event_date, f.final_intent, i.intent_group, f.traffic_class
