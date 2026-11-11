-- UC4 Gold = pandas expected values at the full grain (docs/metrics.md).
{{ reconcile(ref('gold_uc4'), ref('expected_gold_uc4'), ['event_date', 'step', 'traffic_class'], ['message_cnt']) }}
