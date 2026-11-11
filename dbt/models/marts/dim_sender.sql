-- One row per user (distinct sender of client messages). sender_key = a STABLE integer (bitmaps in 4.4 need an
-- integer key, and a key must never move: 01_knowledge.md A2). Incremental: a full build numbers all senders; later
-- runs only append senders not seen before, numbered after the current max. Primary Key table on sender_id (D18).
{{ config(
    materialized='incremental',
    table_type='PRIMARY',
    keys=['sender_id'],
    distributed_by=['sender_id'],
    buckets=1
) }}
with src as (
    select
        sender_id,
        min(event_date) as first_event_date,
        max(case when is_internal_sender then 1 else 0 end) = 1 as is_internal
    from {{ ref('stg_chat_message') }}
    where is_client
    group by sender_id
)
{% if is_incremental() %}
, base as (select coalesce(max(sender_key), 0) as max_key from {{ this }})
{% endif %}
select
    src.sender_id,
    {% if is_incremental() %}base.max_key + {% endif %}row_number() over (order by src.first_event_date, src.sender_id) as sender_key,
    src.first_event_date,
    src.is_internal,
    case when src.is_internal then 'internal' else 'external' end as user_class
from src
{% if is_incremental() %}
cross join base
where src.sender_id not in (select sender_id from {{ this }})
{% endif %}
