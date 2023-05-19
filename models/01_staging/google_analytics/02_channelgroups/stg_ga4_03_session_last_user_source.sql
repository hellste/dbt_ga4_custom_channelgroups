
{% if (env_var('DBT_ENVIRON_ANALYTICS') != 'ci') %}
    {{
        config(
            materialized='table',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
            cluster_by = "session_key"
        )
    }}
{% endif %}

with session_source_data as(

    select 
        event_date_dt,
        session_key,
        user_key,
        session_start_time,
        first_value_session,
        last_non_null_value_session
    from {{ ref('stg_ga4_02_session_first_and_last_source') }}

        {% if target.name == 'dev' %}
            where event_date_dt between {{ get_last_n_days_date_range(2) }}
        {% else %}

        {% endif %}

),

last_value_per_user_last_30_days as(

    select
        event_date_dt,
        user_key,
        session_key,
        session_start_time,
        ifnull(first_value_session,
                last_value(last_non_null_value_session ignore nulls) over(sessions_30day_window))
         as last_non_direct_source_user
    from session_source_data
    window sessions_30day_window as (partition by user_key order by session_start_time asc range between 2592000 preceding and 1 preceding)
    /* day window in seconds */
    /* 2592000 = 30 days */
    /* 1209600 = 14 days */

)

select * from last_value_per_user_last_30_days
