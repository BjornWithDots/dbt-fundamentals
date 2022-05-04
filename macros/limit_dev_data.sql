-- Macro to filter out data when in dev environment
{% macro limit_dev_data(filter_column, numberofdays=3) %}
{%- if target.name == 'dev' -%}
where {{ filter_column }} >= dateadd('day',- {{ numberofdays }}, current_timestamp)
{%- endif %}
{%- endmacro %}