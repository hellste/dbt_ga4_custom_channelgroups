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



with ga4_unnested_pageview_event_data as(

    select
        event_date_dt,
        session_key,
        user_pseudo_id,
        event_timestamp,
        event_name,
        page_pagetype,
        ga_session_id as session_start_time,
        {{ unnest_key_with_null_default('event_params', 'source') }},
        {{ unnest_key_with_null_default('event_params', 'medium') }},
        {{ unnest_key_with_null_default('event_params', 'campaign') }},
        {{ unnest_key_with_null_default('event_params', 'gclid') }},
        {{ unnest_key_with_null_default('event_params', 'session_gadscampaign') }},
        {{ unnest_key_with_null_default('event_params', 'session_gadsbraid') }},
        {{ unnest_key_with_null_default('event_params', 'entrances', value_type = "int_value") }},
        page_referrer,
        page_location
    from {{ ref('base_ga4_events') }}
    where
        event_name = 'page_view'

        {% if target.name == 'dev' %}
            and event_date_dt = date_sub(current_date(), interval 2 day)
        {% else %}
            {% if is_incremental() %}
                and(
                    event_date_dt between _dbt_max_partition and date_sub(current_date(), interval 1 day)
                    or
                    event_date_dt between date_sub(_dbt_max_partition, interval 1 year) and date_sub(date_sub(current_date(), interval 1 year), interval 1 day)
                )
            {% else %}
                and(
                    event_date_dt between {{ get_last_n_days_date_range(120) }}
                    or
                    event_date_dt between {{ get_last_n_days_prev_year(120) }}
                )
            {% endif %}
        {% endif %}

),

/*
- in the GA4 raw data google / cpc is not correctly assigned as source / medium
- instead it appears as direct or organic. Hence if a gclid or braid parameter is present, source / medium must be overwritten with google / cpc
- for the campaign parameter the gads transfer data contains the correct campaign name for each gclid which can be used for matching
*/
gclid_campaign_name_matching_table as(

    select distinct
        gclid,
        campaign_name as gads_campaign_name
    from {{ ref('int_gads_gclids_and_campaigns') }}
    where

        {% if target.name == 'dev' %}
            date = date_sub(current_date(), interval 2 day)
        {% else %}
            {% if is_incremental() %}               
                date between _dbt_max_partition and date_sub(current_date(), interval 1 day)
                or
                date between date_sub(_dbt_max_partition, interval 1 year) and date_sub(date_sub(current_date(), interval 1 year), interval 1 day)               
            {% else %}
                date between {{ get_last_n_days_date_range(120) }}
                or
                date between {{ get_last_n_days_prev_year(120) }}           
            {% endif %}
        {% endif %} 
),

join_campaign_name_via_gclid as(

    select * from ga4_unnested_pageview_event_data
    left join gclid_campaign_name_matching_table using(gclid)

),

adjust_source_medium_parameters_for_google_cpc as(

    select
        * except(source, medium),
        case 
            when (gclid is not null
                    or session_gadsbraid is not null) 
                and (source is null 
                    or source like('%google%') or gads_campaign_name is not null) then 'google'
            else source
        end as source,
        case 
            when (gclid is not null 
                    or session_gadsbraid is not null) 
                and ((source is null
                    or source like('%google%') or gads_campaign_name is not null)) then 'cpc'
            else medium
        end as medium
    from join_campaign_name_via_gclid

),

merge_gclid_campaign_into_campaign as(

    select 
        event_date_dt,
        session_key,
        user_pseudo_id, 
        event_timestamp,
        event_name,
        page_pagetype,
        session_start_time,
        gclid,
        session_gadscampaign,
        session_gadsbraid,
        entrances,
        page_referrer,
        page_location,
        source,
        medium,
        case
            /* If you have a campaign name from the matching table use it */
            when source = 'google' and medium = 'cpc' and gads_campaign_name is not null then gads_campaign_name
            /* In the Bergzeit project not all brand accounts have a Gads Transfer set up, hence we need to use the url parameter or pagetype method for brand campaigns*/
            when source = 'google' and medium = 'cpc' and gads_campaign_name is null and session_gadscampaign is not null then session_gadscampaign
            when source = 'google' and medium = 'cpc' and gads_campaign_name is null and session_gadscampaign is null and page_pagetype like('start') then 'brand_paid_search'
            else campaign
        end as campaign
    from adjust_source_medium_parameters_for_google_cpc

),

sessions_with_no_pageview as(

    select
        event_date_dt,
        session_key,
        user_pseudo_id, 
        event_timestamp,
        event_name,
        page_pagetype,
        session_start_time,
        gclid,
        session_gadscampaign,
        session_gadsbraid,
        entrances,
        page_referrer,
        page_location,
        source,
        medium,
        campaign
    from {{ ref('stg_ga4_00_sessions_non_pageview') }}
    where

        {% if target.name == 'dev' %}
            event_date_dt = date_sub(current_date(), interval 2 day)
        {% else %}
            {% if is_incremental() %}
                event_date_dt between _dbt_max_partition and date_sub(current_date(), interval 1 day)
                or
                event_date_dt between date_sub(_dbt_max_partition, interval 1 year) and date_sub(date_sub(current_date(), interval 1 year), interval 1 day)
            {% else %}
                event_date_dt between {{ get_last_n_days_date_range(30) }}
                or
                event_date_dt between {{ get_last_n_days_prev_year(30) }}
            {% endif %}
        {% endif %}

),

join_non_pageview_sessions_to_data as(

    select * from merge_gclid_campaign_into_campaign
    union all 
    select * from sessions_with_no_pageview 

),

traffic_source_struct as(

    select 
        *,
        if(
            coalesce(source,medium) is not null,
            (select as struct source, medium, campaign, page_pagetype),
            null
        ) as traffic_source_struct
    from join_non_pageview_sessions_to_data

)

select * from traffic_source_struct
