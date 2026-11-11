-- UC6 Gold = pandas expected values at the full grain.
{{ reconcile(ref('gold_uc6'), ref('expected_gold_uc6'), ['event_date', 'final_intent', 'traffic_class'], ['intent_group', 'message_cnt']) }}
