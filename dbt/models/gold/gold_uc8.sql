-- UC8 new and active users per calendar day x user_class (days without messages = 0, M3). users = BITMAP of the
-- stable sender_key: BITMAP_UNION_COUNT(users) over any range of days gives EXACT distinct users (distinct counts are
-- not additive: summing active_users double-counts people active on several days; 01_knowledge.md A2).
-- Primary Key table: a BITMAP value column is not allowed in a Duplicate table (E75; tested in a scratch database).
{{ config(table_type='PRIMARY', keys=['event_date', 'user_class'], distributed_by=['event_date'], buckets=1) }}
with days as (
    select d.date_day, c.user_class
    from {{ ref('dim_date') }} d
    cross join (select 'external' as user_class union all select 'internal') c
), active as (
    select f.event_date, s.user_class, count(distinct f.sender_key) as active_users,
           bitmap_union(to_bitmap(f.sender_key)) as users
    from {{ ref('fct_user_message') }} f
    join {{ ref('dim_sender') }} s on s.sender_key = f.sender_key
    group by f.event_date, s.user_class
), new_users as (
    select first_event_date as event_date, user_class, count(*) as new_users
    from {{ ref('dim_sender') }}
    group by first_event_date, user_class
)
select
    days.date_day as event_date,
    days.user_class,
    coalesce(n.new_users, 0) as new_users,
    coalesce(a.active_users, 0) as active_users,
    coalesce(a.users, bitmap_empty()) as users
from days
left join active a on a.event_date = days.date_day and a.user_class = days.user_class
left join new_users n on n.event_date = days.date_day and n.user_class = days.user_class
