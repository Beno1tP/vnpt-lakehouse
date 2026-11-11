-- UC5 Gold = pandas expected values at the full grain.
{{ reconcile(ref('gold_uc5'), ref('expected_gold_uc5'), ['event_date', 'traffic_class'], ['understood_cnt', 'not_understood_cnt']) }}
