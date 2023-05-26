with ga4_sessions_daily as (

    select
        event_date_dt,
        page_device,
        page_hostname,
        session_channel_group,
        session_source,
        session_medium,
        session_campaign,
        count(event_name) as ga4_sessions
    from {{ ref('stg_ga4_events_sessionstart') }}
    {{dbt_utils.group_by(7)}}

)

select * from ga4_sessions_daily
