
{% macro custom_ga4_channel_groups(source, medium, campaign, pagetype="'none'") %}

    case 
        when {{ source }} = 'google' and {{ medium }} = 'cpc' and (regexp_contains({{campaign}},  '_2_|_3_') or {{ campaign }} = 'generic_paid_search')
            then 'Generic Paid Search Google'
        when {{ source }} = 'google' and {{ medium }} = 'cpc' and (regexp_contains({{campaign}},  '_4_|_6_') or {{ campaign }} in('generic_paid_shopping','generic_paid_pmax'))
            then 'Generic Paid Shopping Google'
        when {{ source }} = 'bing' and {{ medium }} = 'cpc' and not regexp_contains({{ campaign }},  '_5_|_1_')
            then 'Generic Paid Search Bing'
        when {{ source }} in ('google', 'bing') and {{ medium }} = 'cpc' and (regexp_contains({{ campaign }},  '_1_') or {{ campaign }} = 'brand_paid_search')
            then 'Brand Paid Search'
        when (regexp_contains({{ source }}, 'suche.t-online.|qwant.|suche.web.|search.yahoo.|baidu.|suche.gmx.|suche.1und1.|suche.aol.|google.com / referral|startpage.com|search.brave.|yandex.') 
            or {{ medium }} = 'organic')
            and {{ pagetype }} = 'start' 
            then 'Organic Search Home'
        when (regexp_contains({{ source }}, 'suche.t-online.|qwant.|suche.web.|search.yahoo.|baidu.|suche.gmx.|suche.1und1.|suche.aol.|google.com / referral|startpage.com|search.brave.|yandex.') 
            or {{ medium }} = 'organic'
            or {{ campaign }} = 'google_shopping_organic')
            and ({{ pagetype }} not in('start','magazine') or {{ pagetype }} is null)
            then 'Organic Search Non-Home'
        when (regexp_contains({{ source }}, 'suche.t-online.|qwant.|suche.web.|search.yahoo.|baidu.|suche.gmx.|suche.1und1.|suche.aol.|google.com / referral|startpage.com|search.brave.|yandex.') 
            or {{ medium }} = 'organic')
            and {{ pagetype }} = 'magazine' 
            then 'Organic Search Magazin'
        when regexp_contains({{ source }},'newsletter|Newsletter|postwurfsendung') or regexp_contains({{ campaign }}, 'y??_kw??_??_ns_')
            then 'Newsletter' 
        when regexp_contains({{ source }},'parcellab') or regexp_contains({{ medium }},'parcellab') 
            then 'Triggermail Parcellab'
        when {{ source }} = 'triggermail' and {{ medium }} = 'email'
            then 'Triggermail CRM'
        when regexp_contains({{ source }}, 'facebook|instagram|pinterest|youtube') and {{ medium }} in('cpc')
            then 'Social Paid'
        when regexp_contains({{ source }}, 'facebook|instagram|pinterest|youtube|strava|linkedin|IGShopping') and {{ medium }} in('social', 'referral','Social')
            then 'Social Organic'
        when regexp_contains({{ source }}, 'psm|mydealz|beslist|trovaprezzi|shoptail|kelkoo') or {{campaign}} like('%_outdoordeals')
            then 'PSM'
        when regexp_contains({{ source }}, 'affiliate|awin|redbrain|ulligunde|bike-magazin|outdoor-professionell')
            then 'Affiliate'
        when regexp_contains({{ source }}, 'manuf|edelrid|redchiliclimbing.com|sn-supernatural.com|komperdell.com|vaude.com|trollkids.com|houdinisportswear.com|locator.suunto.com|martini-sportswear.com|mountain-equipment.de|maier-sports.com|gonso.de|brandwidgets.outtra.com|chillaz.com|alpina-sports.com')
            then 'Hersteller Links'
        when regexp_contains({{ source }}, 'criteo') or {{ source }} in('bsmart') or regexp_contains({{ campaign }}, '_5_')
            or ({{ source }} = 'google' and {{ medium }} = 'cpc' and {{ campaign }} = 'display')
            then 'Display'
        when {{ medium }} = 'referral' 
            then 'Referral'
        when {{ source }} in ('taboola', 'outbrain','partner') or {{ medium }} = 'cpm' or {{campaign}} = '_branding_'
            then 'Branding'
        when {{ source }} is null and {{ medium }} is null then 'Direct' 
        else 'Unassigned'
    end

{% endmacro %}
