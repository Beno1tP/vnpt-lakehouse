-- UC8 Gold = pandas expected values per calendar day and user_class (zero days included, M3).
{{ reconcile(ref('gold_uc8'), ref('expected_gold_uc8'), ['event_date', 'user_class'], ['new_users', 'active_users']) }}
