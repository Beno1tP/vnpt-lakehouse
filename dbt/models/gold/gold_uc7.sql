-- UC7 total users up to each calendar day x user_class: a user counts once (first chat <= day, dim_sender), change vs
-- 30 days earlier (M2): users before the lake = 0, so change_pct is NULL instead of a division by 0.
{{ config(table_type='DUPLICATE', keys=['event_date', 'user_class'], distributed_by=['event_date'], buckets=1) }}
with days as (
    select d.date_day, c.user_class
    from {{ ref('dim_date') }} d
    cross join (select 'external' as user_class union all select 'internal') c
), counted as (
    select
        days.date_day,
        days.user_class,
        sum(case when s.first_event_date <= days.date_day then 1 else 0 end) as cumulative_users,
        sum(case when s.first_event_date <= date_sub(days.date_day, interval 30 day) then 1 else 0 end) as users_30d_before
    from days
    left join {{ ref('dim_sender') }} s on s.user_class = days.user_class
    group by days.date_day, days.user_class
)
select
    date_day as event_date,
    user_class,
    cumulative_users,
    users_30d_before,
    cumulative_users - users_30d_before as change_abs,
    case when users_30d_before = 0 then null
         else round(100.0 * (cumulative_users - users_30d_before) / users_30d_before, 2) end as change_pct
from counted
