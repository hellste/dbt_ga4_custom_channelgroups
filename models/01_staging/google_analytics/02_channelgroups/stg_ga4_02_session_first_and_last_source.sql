
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

with sessions_pageviews as(

    select 
        event_date_dt,
        session_key,
        user_key,
        session_start_time,
        event_timestamp,
        traffic_source_struct,
        /* flag the sessions first event */
        row_number() over(
            partition by session_key
            order by
                event_timestamp asc
        ) as event_no
    from {{ ref('stg_ga4_01_events_pageviews_adjust_google_params') }}

        {% if target.name == 'dev' %}
            where event_date_dt between {{ get_last_n_days_date_range(2) }}
        {% else %}
            /*select all data */
        {% endif %}

),

first_and_last_session_source as(

    select
        event_date_dt,
        session_key,
        user_key,
        session_start_time,
        event_no,
        first_value(traffic_source_struct) over (session_window) as first_value_session,
        last_value(traffic_source_struct ignore nulls) over (session_window) as last_non_null_value_session
    from sessions_pageviews
    window session_window as (partition by session_key order by event_timestamp asc rows between unbounded preceding and unbounded following)

)

select * from first_and_last_session_source
/* reduce the dataset to one row per session_key */
where event_no = 1
