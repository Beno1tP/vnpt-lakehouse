-- UC7/UC8: the bitmap of gold_uc8 gives EXACT distinct users over any range: its union over all days must equal the
-- users of the last day in gold_uc7 (87 external + 2 internal = 89, metrics.md) and the sum of new users. Returns
-- one row per user_class that disagrees.
with b as (
    select user_class, bitmap_union_count(users) as bitmap_users, sum(new_users) as sum_new_users
    from {{ ref('gold_uc8') }}
    group by user_class
), last_day as (
    select user_class, cumulative_users
    from {{ ref('gold_uc7') }}
    where event_date = (select max(event_date) from {{ ref('gold_uc7') }})
)
select b.user_class, b.bitmap_users, b.sum_new_users, l.cumulative_users
from b join last_day l on l.user_class = b.user_class
where b.bitmap_users <> l.cumulative_users or b.sum_new_users <> l.cumulative_users
