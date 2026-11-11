-- One row per final_intent. UC5: not understood = fallback or bot_khong_hieu; UC6 separates business intents.
{{ config(table_type='DUPLICATE', keys=['final_intent'], distributed_by=['final_intent'], buckets=1) }}
select
    final_intent,
    case when final_intent in ('fallback', 'bot_khong_hieu') then final_intent else 'business' end as intent_group,
    final_intent not in ('fallback', 'bot_khong_hieu') as is_understood
from (select distinct final_intent from {{ ref('stg_nlu_prediction') }}) i
