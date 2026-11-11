-- UC4 scenarios (docs/metrics.md): predicted messages per day x scenario (step) x traffic_class. Share and ranking
-- over a period are computed at query time from these counts (ratio of sums). Scenario names: seed scenario_labels
-- (inferred from the data, leader question).
{{ config(table_type='DUPLICATE', keys=['event_date', 'step'], distributed_by=['step'], buckets=1) }}
select
    f.event_date,
    f.step,
    coalesce(l.scenario, concat('step ', f.step)) as scenario,
    f.traffic_class,
    count(*) as message_cnt
from {{ ref('fct_prediction') }} f
left join {{ ref('scenario_labels') }} l on l.step = f.step
group by f.event_date, f.step, coalesce(l.scenario, concat('step ', f.step)), f.traffic_class
