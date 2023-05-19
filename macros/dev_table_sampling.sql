{% macro dev_table_sampling(sampling_percent = var('dev_table_sampling')) %}
    {% if (env_var('DBT_ENVIRON_ANALYTICS') != 'ci') %}
        {% if target.name == 'dev' %}
            TABLESAMPLE SYSTEM ({{ sampling_percent }} PERCENT)
        {%- else -%}
            --no sampling
        {%- endif -%}
        
    {%- endif -%}
{% endmacro %}