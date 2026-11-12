-- One client message + its prediction (left join in Silver): the base of UC7/UC8 (users).
-- E77: a StarRocks view drops FOR VERSION AS OF from its stored definition, so a tag rebuild inlines staging (ephemeral)
{{ config(materialized=('ephemeral' if var('silver_version', none) else 'view')) }}
select
    id as message_id,
    sender_id,
    is_home,
    is_internal_sender,
    input_channel,
    event_ts,
    event_date,
    prediction_id,
    final_intent,
    step,
    has_prediction
from {{ source('silver', 'user_message') }}{{ silver_version() }}
