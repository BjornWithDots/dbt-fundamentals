-- Macro to filter out data when in dev environment
--where {{ filter_column }} >= dateadd('day',- {{ numberofdays }}, current_timestamp) --MSSQL
{% macro limit_dev_data(filter_column, numberofdays=3) %}
{%- if target.name == 'dev' -%}
where {{ filter_column }} >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ numberofdays }} DAY) --BQ
{%- endif %}
{%- endmacro %}