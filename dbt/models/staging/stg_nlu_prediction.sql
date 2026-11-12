-- One NLU prediction (10,729 incl. the 116 without a chat row, kept and flagged: docs/metrics.md base of UC4-UC6).
-- E77: a StarRocks view drops FOR VERSION AS OF from its stored definition, so a tag rebuild inlines staging (ephemeral)
{{ config(materialized=('ephemeral' if var('silver_version', none) else 'view')) }}
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
from {{ source('silver', 'nlu_prediction') }}{{ silver_version() }}
