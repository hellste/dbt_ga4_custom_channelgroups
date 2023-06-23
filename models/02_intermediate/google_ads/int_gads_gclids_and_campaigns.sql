/* For better Google Ads and GA4 data matching, the campaign name is needed per gclid */

with click_stats as (

    select
        account,
        date,
        account_id,
        campaign_id,
        adgroup_id,
        gclid
    from {{ ref('stg_gads_all_clickids_stats') }}
    where gclid is not null

), 

campaign_ids_and_names as (

    select
        date,
        campaign_id,
        campaign_name,
        campaign_name_last_value
    from {{ ref('stg_gads_all_campaign_names_historic') }}

),

click_stats_and_campaign_names_joined as (

    select
        click.account,
        click.date,
        click.account_id,
        click.campaign_id,
        click.adgroup_id,
        click.gclid,
        /* joined attributes. Campaign names. Coalesce included to avoid null values */
        coalesce(camp.campaign_name, camp.campaign_name_last_value) as campaign_name
    from click_stats as click
    left join campaign_ids_and_names as camp using (date, campaign_id)

)

select * from click_stats_and_campaign_names_joined
