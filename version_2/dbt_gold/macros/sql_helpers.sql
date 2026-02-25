{% macro incremental_distinct_keys(
    relation,
    key_col,
    source_watermark_col,
    target_relation,
    target_watermark_col,
    lookback_hours=0
) -%}
{% if is_incremental() %}
select distinct {{ key_col }}
from {{ relation }}
where {{ source_watermark_col }} > (
    select
        {% if lookback_hours | int > 0 %}
            coalesce(max({{ target_watermark_col }}), timestamp('1900-01-01')) - interval {{ lookback_hours | int }} hours
        {% else %}
            coalesce(max({{ target_watermark_col }}), timestamp('1900-01-01'))
        {% endif %}
    from {{ target_relation }}
)
{% else %}
select distinct {{ key_col }}
from {{ relation }}
{% endif %}
{%- endmacro %}

{% macro sum_when(condition_sql, value_sql) -%}
sum(case when {{ condition_sql }} then {{ value_sql }} else 0 end)
{%- endmacro %}

{% macro safe_divide(numerator_sql, denominator_sql) -%}
case when {{ denominator_sql }} > 0 then {{ numerator_sql }} / {{ denominator_sql }} else null end
{%- endmacro %}

{% macro count_distinct_when(value_sql, condition_sql) -%}
count(distinct case when {{ condition_sql }} then {{ value_sql }} end)
{%- endmacro %}

{% macro is_weekend_from_date(date_col) -%}
case when dayofweek({{ date_col }}) in (1, 7) then true else false end
{%- endmacro %}
