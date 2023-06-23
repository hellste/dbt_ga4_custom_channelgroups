{% macro get_last_n_days_prev_year(number_of_days) %}
    -- get date n days ago of last year
    date_sub(date_sub(current_date(), interval 1 year), interval {{ number_of_days }} day)
    -- get yesterday last year
    and date_sub(date_sub(current_date(), interval 1 year), interval 0 day)
{% endmacro %}
