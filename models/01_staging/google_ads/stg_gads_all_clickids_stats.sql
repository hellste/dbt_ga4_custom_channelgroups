{% set partitions_to_replace =  
    [
        'date(date_sub(current_date(), interval 1 day))',
        'date(date_sub(current_date(), interval 2 day))'
    ] 
%}  

{% if (env_var('DBT_ENVIRON_ANALYTICS') != 'ci') %}
    {{
        config(  
            materialized='incremental',
            incremental_strategy='insert_overwrite', 
            partition_by={
                "field": "date",
                "data_type": "date",
            },
            partitions = partitions_to_replace,  
            cluster_by = "account"
        )
    }}
{% endif %}

/* Collect click stats from performance country accounts */

with clickstats_perf as (

    {% for country in var('gads_countries') %}
        {% set country_table = 'gads_click_stats_' + country %}

        select
            '{{ country }}' as account,
            segments_date as date,
            customer_id as account_id,
            campaign_id,
            ad_group_id as adgroup_id,
            segments_ad_network_type as ad_network,
            click_view_gclid as gclid
        from {{ source('google_ads_transfer_v2', country_table) }}
        where

        {% if target.name == 'dev' %}

            date(_PARTITIONTIME) between {{ get_last_n_days_date_range(1) }}

        {% else %}

            {% if is_incremental() %}

                date(_PARTITIONTIME) in ( {{ partitions_to_replace | join(',') }}  )
                
            {% else %}

                date(_PARTITIONTIME) between {{ get_last_n_days_date_range(120) }}
                or date(_PARTITIONTIME) between {{ get_last_n_days_prev_year(120) }}

            {% endif %}

        {% endif %}

        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}

),

{% set brand_account_countries= ['de', 'at', 'ch'] %}

clickstats_brand as (

    {% for country in brand_account_countries %}
        {% set country_table = 'gads_click_stats_' + country + '_brand' %}

        select
            concat('{{ country }}', '_brand') as account,
            segments_date as date,
            customer_id as account_id,
            campaign_id,
            ad_group_id as adgroup_id,
            segments_ad_network_type as ad_network,
            click_view_gclid as gclid
        from {{ source('google_ads_transfer_v2', country_table) }}
        where

        {% if target.name == 'dev' %}

            date(_PARTITIONTIME) between {{ get_last_n_days_date_range(1) }}

        {% else %}

            {% if is_incremental() %}

                date(_PARTITIONTIME) in ( {{ partitions_to_replace | join(',') }}  )
                
            {% else %}

                date(_PARTITIONTIME) between {{ get_last_n_days_date_range(120) }}
                or date(_PARTITIONTIME) between {{ get_last_n_days_prev_year(120) }}

            {% endif %}

        {% endif %}

        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}

),

all_clickstats as (

    select * from clickstats_perf
    union all
    select * from clickstats_brand

)

select 
    *,
    {{ dbt_utils.generate_surrogate_key(['account_id', 'gclid']) }} as sk_id
from all_clickstats
