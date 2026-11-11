-- Singular test (4.3): the core Gold tables have exactly the expected sizes. Returns one row per mismatch (dbt fails
-- the test when rows come back). Expected: tests/expected/silver.json (predict_rows 10,729, user_message_rows 10,613),
-- docs/metrics.md (89 users = Task 1's distinct senders, 24 calendar days, traffic split of the 10,729 predictions).
with got as (
    select 'fct_prediction rows' as k, count(*) as n from {{ ref('fct_prediction') }}
    union all select 'fct_user_message rows', count(*) from {{ ref('fct_user_message') }}
    union all select 'dim_sender rows', count(*) from {{ ref('dim_sender') }}
    union all select 'dim_date rows', count(*) from {{ ref('dim_date') }}
    union all select 'fct_prediction home', count(*) from {{ ref('fct_prediction') }} where traffic_class = 'home'
    union all select 'fct_prediction real', count(*) from {{ ref('fct_prediction') }} where traffic_class = 'real'
    union all select 'fct_prediction unmatched', count(*) from {{ ref('fct_prediction') }} where traffic_class = 'unmatched'
    union all select 'fct_prediction internal', count(*) from {{ ref('fct_prediction') }} where traffic_class = 'internal'
), want as (
    select 'fct_prediction rows' as k, 10729 as n
    union all select 'fct_user_message rows', 10613
    union all select 'dim_sender rows', 89
    union all select 'dim_date rows', 24
    union all select 'fct_prediction home', 10378
    union all select 'fct_prediction real', 219
    union all select 'fct_prediction unmatched', 116
    union all select 'fct_prediction internal', 16
)
select want.k, want.n as expected, got.n as actual
from want left join got on got.k = want.k
where got.n is null or got.n <> want.n
