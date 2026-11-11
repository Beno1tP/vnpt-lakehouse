-- Silver gate (4.5, gold mode): Silver read through StarRocks = tests/expected/silver.json totals (pandas = Spark =
-- Trino in Task 3). Runs on the staging views, so `dbt build` skips every Gold model when Silver is off.
with got as (
    select 'chat_rows' as k, count(*) as n from {{ ref('stg_chat_message') }}
    union all select 'client_rows', count(*) from {{ ref('stg_chat_message') }} where is_client
    union all select 'client_home_rows', count(*) from {{ ref('stg_chat_message') }} where is_client and is_home
    union all select 'internal_sender_rows', count(*) from {{ ref('stg_chat_message') }} where is_internal_sender
    union all select 'predict_rows', count(*) from {{ ref('stg_nlu_prediction') }}
    union all select 'predict_without_chat_row', count(*) from {{ ref('stg_nlu_prediction') }} where not has_chat_row
    union all select 'user_message_rows', count(*) from {{ ref('stg_user_message') }}
    union all select 'user_message_with_prediction', count(*) from {{ ref('stg_user_message') }} where has_prediction
    union all select concat('final_intent.', final_intent), count(*) from {{ ref('stg_nlu_prediction') }} group by final_intent
    union all select concat('step.', step), count(*) from {{ ref('stg_nlu_prediction') }} group by step
    union all select 'event_dates', count(distinct event_date) from {{ ref('stg_chat_message') }}
), want as (
    select 'chat_rows' as k, 21224 as n
    union all select 'client_rows', 10613
    union all select 'client_home_rows', 10378
    union all select 'internal_sender_rows', 32
    union all select 'predict_rows', 10729
    union all select 'predict_without_chat_row', 116
    union all select 'user_message_rows', 10613
    union all select 'user_message_with_prediction', 10613
    union all select 'final_intent.xin_chao', 10588
    union all select 'final_intent.fallback', 107
    union all select 'final_intent.bot_khong_hieu', 34
    union all select 'step.406665', 10494
    union all select 'step.406666', 235
    union all select 'event_dates', 18
)
select coalesce(w.k, g.k) as k, w.n as expected, g.n as actual
from want w full outer join got g on g.k = w.k
where w.n is null or g.n is null or w.n <> g.n
