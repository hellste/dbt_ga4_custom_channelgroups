
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

with sessions_start_source as(

    select 
        event_date_dt,
        session_key,
        user_key,
        session_start_time,
        first_value_session
    from {{ ref('stg_ga4_02_session_first_and_last_source') }}
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

last_user_source_30_day_window as(

    select
        event_date_dt,
        session_key,
        user_key,
        session_start_time,
        last_non_direct_source_user
    from {{ ref('stg_ga4_03_session_last_user_source') }}
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

join_last_user_source as(

    select * from sessions_start_source
    left join last_user_source_30_day_window using(event_date_dt, session_key, user_key, session_start_time)
    
),

unnest_parameters as(
/* if the session has a first non-direct traffic source use that one */
/* if there are no parameters found use the last traffic-source parameters found for this user within the last 30 days */
    select
        *,
        case when first_value_session is not null then first_value_session.source
            when first_value_session is null and last_non_direct_source_user is not null then last_non_direct_source_user.source
            else null 
        end as session_source,
        case when first_value_session is not null then first_value_session.medium
            when first_value_session is null and last_non_direct_source_user is not null then last_non_direct_source_user.medium
            else null 
        end as session_medium,
        case when first_value_session is not null then first_value_session.campaign
            when first_value_session is null and last_non_direct_source_user is not null then last_non_direct_source_user.campaign
            else null 
        end as session_campaign,
        case when first_value_session is not null then first_value_session.page_pagetype
            when first_value_session is null and last_non_direct_source_user is not null then last_non_direct_source_user.page_pagetype
            else null 
        end as session_page_pagetype
    from join_last_user_source
        
),

add_channel_group as(

    select
        *,
        {{ custom_ga4_channelgroups('session_source', 'session_medium', 'session_campaign', 'session_page_pagetype') }} as session_channel_group
    from unnest_parameters
    
)

select *
from add_channel_group
