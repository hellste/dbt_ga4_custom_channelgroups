{% macro get_last_n_days_date_range(number_of_days) %}
    date_sub(current_date(), interval {{ number_of_days }} day)
    and date_sub(current_date(), interval 1 day)
{% endmacro %}