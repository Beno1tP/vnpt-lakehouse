{#- Task 4 freeze: read Silver at an Iceberg tag instead of the current snapshot, e.g.
      dbt build --vars '{silver_version: task4_v1}'
    StarRocks 3.5.21 resolves tags in FOR VERSION AS OF and reads old snapshots with the current schema (checked
    11-11, 07_dbt_and_plans.md §4). Only tag names of the freeze pattern are accepted (they end up in SQL). Without
    the var: current Silver (normal runs, gold_build). A later normal build puts the staging views back on current. -#}
{% macro silver_version() %}
  {%- set v = var('silver_version', none) -%}
  {%- if v -%}
    {%- if not modules.re.match('^task[2-5]_v[0-9]{1,3}$', v) -%}
      {{ exceptions.raise_compiler_error("silver_version must look like task4_v1, got " ~ v) }}
    {%- endif -%}
    {{ " FOR VERSION AS OF '" ~ v ~ "'" }}
  {%- endif -%}
{% endmacro %}
