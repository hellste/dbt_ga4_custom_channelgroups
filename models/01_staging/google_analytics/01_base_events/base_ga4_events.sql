{% set partitions_to_replace =  ['date(date_sub(current_date(), interval 1 day))','date(date_sub(current_date(), interval 2 day))'] %}
      
{% if (env_var('DBT_ENVIRON_ANALYTICS') != 'ci') %}
    {{
        config(   
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
            partitions = partitions_to_replace,  
            cluster_by = "event_name"
        )
    }}
{% endif %}

/* Incrementally load the raw data from the events_ and events_intraday tables */
with source_data as (

    select
        parse_date('%Y%m%d', event_date) as event_date_dt,
        event_timestamp,
        event_name,
        event_params,
        user_id,
        user_pseudo_id,
        device,
        device.category as page_device,
        device.web_info.hostname as page_hostname,
        stream_id,
        items,
        ecommerce,
        {{ unnest_key_with_null_default('event_params', 'ga_session_id', 'int_value') }} as ga_session_id,
        {{ unnest_key_with_null_default('event_params', 'page_location') }} as page_location,
        {{ unnest_key_with_null_default('event_params', 'ga_session_number',  'int_value') }} as ga_session_number,
        (case when (select value.string_value 
                    from unnest(event_params) 
                    where key = 'session_engaged') = '1' then 1 
            else null 
            end) as session_engaged,
        {{ unnest_key_with_null_default('event_params', 'engagement_time_msec', 'int_value') }} as engagement_time_msec,
        {{ unnest_key_with_null_default('event_params', 'page_title') }} as page_title,
        {{ unnest_key_with_null_default('event_params', 'page_pagetype') }} as page_pagetype,
        {{ unnest_key_with_null_default('event_params', 'page_referrer') }} as page_referrer,
    from {{ source('ga4_bz_overall', 'ga4_bz_overall_events') }} {{ dev_table_sampling() }}
    where

    {% if target.name == 'dev' %}
            parse_date('%Y%m%d', regexp_extract(_table_suffix,'[0-9]+')) between {{ get_last_n_days_date_range(2) }}
    {% else %}
        {% if is_incremental() %}
            parse_date('%Y%m%d', regexp_extract(_table_suffix,'[0-9]+')) in ( {{ partitions_to_replace | join(',') }}  )
        {% else %}
            parse_date('%Y%m%d', regexp_extract(_table_suffix,'[0-9]+')) between {{ get_last_n_days_date_range(60) }}
        {% endif %}
    {% endif %}

),

/* Add a unique key for the user that checks for user_id and then user_pseudo_id */
add_user_key as (

    select
        *,
        to_json_string(items) as hashable_items,
        case
            when user_id is not null then to_base64(md5(user_id))
            when user_pseudo_id is not null then to_base64(md5(user_pseudo_id))
            else null -- this case is reached when privacy settings are enabled
        end as user_key
    from source_data

), 

/* Add unique keys for sessions */
include_session_key as (

    select
        *,
        -- Surrogate key to determine unique session across users. Sessions do NOT reset after midnight in GA4
        to_base64(md5(CONCAT(CAST(user_key as STRING), cast(ga_session_id as STRING)))) as session_key
    from add_user_key

),

include_event_number as (

    select include_session_key.*,
        row_number() over(partition by session_key order by event_timestamp asc) as session_event_number
    from include_session_key

),

/* create a unique identifier for each event */
create_surrogate_key as(

    select
        * except(hashable_items),
        {{ dbt_utils.generate_surrogate_key(['event_timestamp', 'ga_session_id', 'event_name','engagement_time_msec', 'hashable_items'])}}  as sk_id
    from include_event_number
    
),

/* strip the page location to page path and path+querystring */
add_stripped_page_path as(
    
    select
        *,
        REGEXP_REPLACE((split(page_location,'?')[safe_offset(0)]), r'(^http[s]?://www.bergzeit.[a-z][a-z][.]?[a-z]?[a-z]?)','') as page_path,
        REGEXP_REPLACE(page_location, r'(^http[s]?://www.bergzeit.[a-z][a-z][.]?[a-z]?[a-z]?)','') as page_path_and_querystring
    from create_surrogate_key
    
)

select * from add_stripped_page_path










