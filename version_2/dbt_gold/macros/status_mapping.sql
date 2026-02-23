{% macro status_category_from_severity(status_severity_col) -%}
case
    when {{ status_severity_col }} >= 10 then 'Good Service'
    when {{ status_severity_col }} = 9 then 'Minor Delays'
    when {{ status_severity_col }} = 8 then 'Severe Delays'
    when {{ status_severity_col }} in (7, 6, 5) then 'Suspended / Closed'
    else 'Other'
end
{%- endmacro %}

{% macro severity_weight_from_severity(status_severity_col) -%}
case
    when {{ status_severity_col }} >= 10 then 0.0
    when {{ status_severity_col }} = 9 then 1.0
    when {{ status_severity_col }} = 8 then 2.0
    when {{ status_severity_col }} = 7 then 3.0
    when {{ status_severity_col }} in (6, 5) then 4.0
    else 2.0
end
{%- endmacro %}

{% macro is_good_service_from_severity(status_severity_col) -%}
case when {{ status_severity_col }} >= 10 then true else false end
{%- endmacro %}

{% macro recovery_flag_from_severity(prev_status_severity_col, new_status_severity_col) -%}
case
    when {{ prev_status_severity_col }} is not null
      and not {{ is_good_service_from_severity(prev_status_severity_col) }}
      and {{ is_good_service_from_severity(new_status_severity_col) }}
    then true
    else false
end
{%- endmacro %}
