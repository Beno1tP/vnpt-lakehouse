-- One row per calendar day of the period (24 days; days without messages included: metrics.md M3).
{{ config(table_type='DUPLICATE', keys=['date_day'], distributed_by=['date_day'], buckets=1) }}
select
    date_add(cast('{{ var("period_start") }}' as date), generate_series) as date_day,
    dayofweek(date_add(cast('{{ var("period_start") }}' as date), generate_series)) as day_of_week,  -- 1 = Sunday
    dayofweek(date_add(cast('{{ var("period_start") }}' as date), generate_series)) in (1, 7) as is_weekend
from table(generate_series(0, datediff(cast('{{ var("period_end") }}' as date), cast('{{ var("period_start") }}' as date))))
