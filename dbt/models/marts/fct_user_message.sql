-- Fact: one client message (10,613). Base of UC7/UC8 (users) via sender_key. traffic_class: internal > home > real
-- (a client message always has its chat row). Primary Key (message_id, event_date), partitioned by day.
-- partition_by is a LIST: a string is split at its comma (E73). No comments inside the Jinja config call (E74).
{{ config(
    table_type='PRIMARY',
    keys=['message_id', 'event_date'],
    partition_by=["date_trunc('day', event_date)"],
    partition_type='Expr',
    distributed_by=['message_id'],
    buckets=1
) }}
select
    u.message_id,
    u.event_date,
    u.event_ts,
    s.sender_key,
    u.input_channel,
    u.is_home,
    u.is_internal_sender,
    u.has_prediction,
    u.final_intent,
    u.step,
    case when u.is_internal_sender then 'internal' when u.is_home then 'home' else 'real' end as traffic_class
from {{ ref('stg_user_message') }} u
left join {{ ref('dim_sender') }} s on s.sender_id = u.sender_id
