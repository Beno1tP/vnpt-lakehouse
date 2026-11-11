-- UC7 Gold = pandas expected values per calendar day and user_class (change_pct NULL when the base is 0, M2).
{{ reconcile(ref('gold_uc7'), ref('expected_gold_uc7'), ['event_date', 'user_class'], ['cumulative_users', 'users_30d_before', 'change_abs', 'change_pct']) }}
