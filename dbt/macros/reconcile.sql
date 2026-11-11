{#- Task 4 step 4.4: compare a Gold model with its expected seed (pandas, tools/expected_gold.py) row by row.
    Returns one row per difference: a key only in the model, only in the seed, or a value that differs (null-safe
    <=>). A singular test fails when it returns rows. Filter "on" (one traffic_class / user_class) and "off" (all)
    are both covered because the grain includes the class. -#}
{% macro reconcile(model, seed, keys, values) %}
select
    {% for k in keys %}coalesce(m.{{ k }}, e.{{ k }}) as {{ k }}, {% endfor %}
    case when m.{{ keys[0] }} is null then 'only in expected'
         when e.{{ keys[0] }} is null then 'only in model'
         else 'value differs' end as problem
    {% for v in values %}, m.{{ v }} as model_{{ v }}, e.{{ v }} as expected_{{ v }}{% endfor %}
from {{ model }} m
full outer join {{ seed }} e
  on {% for k in keys %}m.{{ k }} = e.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
where m.{{ keys[0] }} is null or e.{{ keys[0] }} is null
   {% for v in values %} or not (m.{{ v }} <=> e.{{ v }}){% endfor %}
{% endmacro %}
