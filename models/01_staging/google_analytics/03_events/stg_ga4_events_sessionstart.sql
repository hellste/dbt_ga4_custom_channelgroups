
{% if (env_var('DBT_ENVIRON_ANALYTICS') != 'ci') %}
    {{
        config(
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
            cluster_by = "session_key"
        )
    }}
{% endif %}

with source_data as (

    select
        event_date_dt,
        event_timestamp,
        event_name,
        event_params,
        ga_session_id,
        page_location,
        page_path,
        page_path_and_querystring,
        ga_session_number,
        session_engaged,
        engagement_time_msec,
        page_title,
        page_referrer,
        page_device,
        page_hostname,
        user_key,
        session_key,
        sk_id
    from {{ ref('base_ga4_events') }}
    where event_name = 'session_start'

        {% if target.name == 'dev' %}
            and event_date_dt between {{ get_last_n_days_date_range(2) }}
        {% else %}
            {% if is_incremental() %}
                and event_date_dt between _dbt_max_partition and date_sub(current_date(), interval 1 day)
            {% else %}
                and event_date_dt between {{ get_last_n_days_date_range(30) }}
            {% endif %}
        {% endif %}

),

session_source_data as(

    select
        session_key,
        source as session_source,
        medium as session_medium,
        campaign as session_campaign,
        channel_group as session_channelgroup
    from {{ ref('stg_ga4_04_session_channelgroups') }}
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

join_session_source_to_data as(

    select * from source_data
    left join session_source_data using(session_key)

)

select * from join_session_source_to_data
