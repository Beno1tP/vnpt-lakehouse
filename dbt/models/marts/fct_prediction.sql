-- Fact: one NLU prediction (10,729). Base of UC4 (step), UC5 (understood), UC6 (intents). traffic_class, first match
-- wins (metrics.md): internal > unmatched (no chat row, M1) > home > real. Primary Key (prediction_id, event_date):
-- StarRocks requires the partition column in the key; partitioned by day (expression partitioning).
-- partition_by is a LIST: a string is split at its comma (E73). No comments inside the Jinja config call (E74).
{{ config(
    table_type='PRIMARY',
    keys=['prediction_id', 'event_date'],
    partition_by=["date_trunc('day', event_date)"],
    partition_type='Expr',
    distributed_by=['prediction_id'],
    buckets=1
) }}
select
    p.prediction_id,
    p.event_date,
    p.event_ts,
    p.step,
    p.final_intent,
    p.confidence,
    p.has_chat_row,
    s.sender_key,
    case
        when p.sender_id like '%@%' then 'internal'
        when not p.has_chat_row then 'unmatched'
        when c.is_home then 'home'
        else 'real'
    end as traffic_class
from {{ ref('stg_nlu_prediction') }} p
left join {{ ref('stg_chat_message') }} c on c.message_id = p.chat_id
left join {{ ref('dim_sender') }} s on s.sender_id = p.sender_id
