
{% if (env_var('DBT_ENVIRON_ANALYTICS') != 'ci') %}
    {{
        config(
            materialized='incremental',
            incremental_strategy='insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
            cluster_by = "session_key"
        )
    }}
{% endif %}

with pageview_events_per_session_key as(
    select
        event_date_dt,
        event_timestamp,
        session_key,
        user_key,
        ga_session_id as session_start_time,
        if(event_name = 'page_view', event_name, null) as page_views
    from {{ ref('base_ga4_events') }}
    where
    
        {% if target.name == 'dev' %}
            event_date_dt between {{ get_last_n_days_date_range(2) }}
        {% else %}
            {% if is_incremental() %}
                event_date_dt between _dbt_max_partition and date_sub(current_date(), interval 1 day)
            {% else %}
                event_date_dt between {{ get_last_n_days_date_range(30) }}
            {% endif %}
        {% endif %}
    
),

/* columns with null values are required to union the data to the 01 Model */
get_sessions_with_no_pageview as(
    select
        min(event_date_dt) as event_date_dt,
        session_key,
        user_key, 
        min(event_timestamp) as event_timestamp,
        cast(null as string) as event_name,
        cast(null as string) as page_pagetype,
        session_start_time,
        cast(null as string) as gclid,
        cast(null as string) as session_gadscampaign,
        cast(null as string) as session_gadsbraid,
        1 as entrances,
        cast(null as string) as page_referrer,
        cast(null as string) as page_location,
        cast(null as string) as source,
        cast(null as string) as medium,
        cast(null as string) as campaign,
        coalesce(STRING_AGG(page_views ORDER BY event_timestamp)) AS has_pageview
    from pageview_events_per_session_key
    group by 2,3,5,6,7,8,9,10,11,12,13,14,15,16
)

/* only get sessions without any page_view event; as soon as a session has one page_view it is covered by the 01 Model*/
select * except(has_pageview) from get_sessions_with_no_pageview
where has_pageview is null
