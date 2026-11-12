-- One client message + its prediction (left join in Silver): the base of UC7/UC8 (users).
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
