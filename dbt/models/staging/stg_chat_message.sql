-- One chat row (client or bot), typed and flagged by the Silver job (Task 3). Read through the external catalog.
-- E77: a StarRocks view drops FOR VERSION AS OF from its stored definition, so a tag rebuild inlines staging (ephemeral)
{{ config(materialized=('ephemeral' if var('silver_version', none) else 'view')) }}
select
    id as message_id,
    sender_id,
    source,
    is_client,
    is_home,
    is_internal_sender,
    input_channel,
    event_ts,
    event_date
from {{ source('silver', 'chat_message') }}{{ silver_version() }}
