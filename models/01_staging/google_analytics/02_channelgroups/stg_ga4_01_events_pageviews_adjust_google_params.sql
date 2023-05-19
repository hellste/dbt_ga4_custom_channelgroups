
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
        user_key,
        event_timestamp,
        event_name,
        page_pagetype,
        ga_session_id as session_start_time,
        {{ unnest_key_with_null_default('event_params', 'source') }} as source,
        {{ unnest_key_with_null_default('event_params', 'medium') }} as medium,
        {{ unnest_key_with_null_default('event_params', 'campaign') }} as campaign,
        {{ unnest_key_with_null_default('event_params', 'gclid') }} as gclid,
        {{ unnest_key_with_null_default('event_params', 'session_gadscampaign') }} as session_gadscampaign, /* custom parameter to distinguish Google Ads campaign types */ 
        {{ unnest_key_with_null_default('event_params', 'session_gadsbraid') }} as session_gadsbraid, /* gbraid and wbraid parameters as custom parameter extracted from the URL */
        {{ unnest_key_with_null_default('event_params', 'entrances', value_type = "int_value") }} as entrances,
        page_referrer,
        page_location
    from {{ ref('base_ga4_events') }}
    where

        {% if target.name == 'dev' %}
            event_date_dt between {{ get_last_n_days_date_range(2) }}
        {% else %}
            {% if is_incremental() %}
                event_date_dt between _dbt_max_partition and date_sub(current_date(), interval 1 day) 
            {% else %}
                event_date_dt between {{ get_last_n_days_date_range(60) }}
            {% endif %}
        {% endif %}

),

adjust_source_medium_parameters_for_google_cpc as(
/* in the GA4 raw data google / cpc is not correctly assigned as source / medium */
/* instead it appears as direct or organic. Hence if a gclid or braid parameter is present, source / medium must be overwritten with google / cpc */
/* the reassignment is simplyfied and only changes source / medium if they are null or already include google in the source (e.g. for organic) */
/* if the source / medium parameters are populated otherwise but still have a gclid no reassignment happens */

    select
        * except(source, medium),
        case 
            when (gclid is not null
                    or session_gadsbraid is not null) 
                and (source is null 
                    or source like('%google%')) then 'google'
            else source
        end as source,
        case 
            when (gclid is not null 
                    or session_gadsbraid is not null) 
                and ((source is null
                    or source like('%google%'))) then 'cpc'
            else medium
        end as medium
    from ga4_unnested_pageview_event_data

),


/* this step may not be neccessary if you don't distinguish between different Google Ads campaigns */
/* the future solution will be to match the correct campaign names via the gclid with the campaign information from the Google Ads BigQuery Transfer */

adjust_campaign_paramter_for_google_cpc as(

    select
        * except(campaign),
        case
            /* if you have the custom gadscampaign url parameter to distinguish Google Ads campaigns use it */
            when source = 'google' and medium = 'cpc' and session_gadscampaign is not null then session_gadscampaign
            /* only since the gadscampaign parameter is present in all countries we can identifie pmax campaigns that don't come from the feed */
            when event_date_dt >= '2023-04-08'and source = 'google' and medium = 'cpc' and session_gadscampaign is null then 'generic_paid_pmax'
            /* before we have our custom campaign parameter use the landing page pagetype as proxy for the campaign */
            when event_date_dt < '2023-04-08' and source = 'google' and medium = 'cpc' and page_pagetype like('productdetail') then 'generic_paid_shopping'
            when event_date_dt < '2023-04-08' and source = 'google' and medium = 'cpc' and page_pagetype like('start') then 'brand_paid_search'
            when event_date_dt < '2023-04-08' and source = 'google' and medium = 'cpc' and page_pagetype not in('productdetail', 'start') then 'generic_paid_search'
            else campaign
        end as campaign
    from adjust_source_medium_parameters_for_google_cpc

),

sessions_with_no_pageview as(

    select
        event_date_dt,
        session_key,
        user_key, 
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
            event_date_dt between {{ get_last_n_days_date_range(2) }}
        {% else %}
            {% if is_incremental() %}
                event_date_dt between _dbt_max_partition and date_sub(current_date(), interval 1 day) 
            {% else %}
                event_date_dt between {{ get_last_n_days_date_range(30) }}
            {% endif %}
        {% endif %}

),

join_non_pageview_sessions_to_data as(

    select * from adjust_campaign_paramter_for_google_cpc
    union all 
    select * from sessions_with_no_pageview 

),

traffic_source_struct as(

    select 
        *,
        if(
            coalesce(source,medium) is not null,
            /* select all parameters that are relevant for our custom channelgroups */
            (select as struct source, medium, campaign, page_pagetype),
            null
        ) as traffic_source_struct
    from join_non_pageview_sessions_to_data

)

select * from traffic_source_struct
