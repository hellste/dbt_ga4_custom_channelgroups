/* adapted form the dbt-ga4 package https://github.com/Velir/dbt-ga4/blob/3.2.1/macros/default_channel_grouping.sql */

{% macro default_channel_grouping(source, medium, source_category) %}
    case 
        when {{source}} is null and {{medium}} is null 
            then 'Direct'
        when {{source}} = '(direct)'
                and ({{medium}} = '(none)' or {{medium}} = '(not set)')
            then 'Direct'
        when REGEXP_CONTAINS({{source}}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
                and REGEXP_CONTAINS({{medium}}, r"^(cpc|ppc|paid)") = true
            then 'Paid Social'
        when REGEXP_CONTAINS({{source}}, r"^(facebook|instagram|pinterest|reddit|twitter|linkedin)") = true
                or REGEXP_CONTAINS({{medium}}, r"^(social|social-network|social-media|sm|social network|social media)") = true
                or {{source_category}} = 'SOURCE_CATEGORY_SOCIAL' 
            then 'Organic Social'
        when REGEXP_CONTAINS({{medium}}, r"email|e-mail|e_mail|e mail") = true
                or REGEXP_CONTAINS({{source}}, r"email|e-mail|e_mail|e mail") = true
            then 'Email'
        when REGEXP_CONTAINS({{medium}}, r"affiliate|affiliates") = true 
            then 'Affiliates'
        when {{source_category}} = 'SOURCE_CATEGORY_SHOPPING' and REGEXP_CONTAINS({{medium}},r"^(.*cp.*|ppc|paid.*)$") 
            then 'Paid Shopping'
        when ({{source_category}} = 'SOURCE_CATEGORY_VIDEO' AND REGEXP_CONTAINS({{medium}},r"^(.*cp.*|ppc|paid.*)$"))
                or {{source}} = 'dv360_video'
            then 'Paid Video'
        when REGEXP_CONTAINS({{medium}}, r"^(display|cpm|banner)$")
                or {{source}} = 'dv360_display'
            then 'Display'
        when REGEXP_CONTAINS({{medium}}, r"^(cpc|ppc|paidsearch)$") 
            then 'Paid Search'
        when REGEXP_CONTAINS({{medium}}, r"^(cpv|cpa|cpp|content-text)$") 
            then 'Other Advertising'
        when {{medium}} = 'organic' or {{source_category}} = 'SOURCE_CATEGORY_SEARCH' 
            then 'Organic Search'
        when REGEXP_CONTAINS({{medium}}, r"^(.*video.*)$") or {{source_category}} = 'SOURCE_CATEGORY_VIDEO' 
            then 'Organic Video'
        when {{source_category}} = 'SOURCE_CATEGORY_SHOPPING' 
            then 'Organic Shopping'
        when {{medium}} = 'referral' 
            then 'Referral'
        when {{medium}} = 'audio' 
            then 'Audio'
        when {{medium}} = 'sms' 
            then 'SMS'
        when REGEXP_CONTAINS({{medium}}, r"(mobile|notification|push$)") or {{source}} = 'firebase' 
            then 'Push Notifications'
        else '(Other)' 
    end 

{% endmacro %}