/* collect campaigns ids and names from all countries */
{% set brand_account_countries= ['de', 'at', 'ch'] %}


with all_campaign_names_historic as (

    /* Performance account countries */
    {% for country in var('gads_countries') %}
        {% set country_table = 'gads_campaigns_' + country %}

        select
            date(_PARTITIONTIME) as date,
            customer_id,
            campaign_id,
            campaign_name,
            last_value(campaign_name) over (partition by campaign_id order by date(_PARTITIONTIME) asc) as campaign_name_last_value
        from {{ source('google_ads_transfer_v2', country_table) }}
        where
            date(_PARTITIONTIME) between {{ get_last_n_days_date_range(150) }}
            or date(_PARTITIONTIME) between {{ get_last_n_days_prev_year(150) }}

        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}

    union all

    /* Brand account countries */
    {% for country in brand_account_countries %}
        {% set country_table = 'gads_campaigns_' + country + '_brand' %}

        select
            date(_PARTITIONTIME) as date,
            customer_id,
            campaign_id,
            campaign_name,
            last_value(campaign_name) over (partition by campaign_id order by date(_PARTITIONTIME) asc) as campaign_name_last_value
        from {{ source('google_ads_transfer_v2', country_table) }}
        where
            date(_PARTITIONTIME) between {{ get_last_n_days_date_range(120) }}
            or date(_PARTITIONTIME) between {{ get_last_n_days_prev_year(120) }}

        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}

)

select
    {{ dbt_utils.generate_surrogate_key(['date','campaign_id', 'campaign_name']) }} as sk_id,
    * 
from all_campaign_names_historic
