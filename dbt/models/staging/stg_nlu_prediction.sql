-- One NLU prediction (10,729 incl. the 116 without a chat row, kept and flagged: docs/metrics.md base of UC4-UC6).
select
    id as prediction_id,
    chat_id,
    sender_id,
    step,
    intent_name,
    final_intent,
    confidence,
    nlu_threshold,
    has_chat_row,
    event_ts,
    event_date
from {{ source('silver', 'nlu_prediction') }}
