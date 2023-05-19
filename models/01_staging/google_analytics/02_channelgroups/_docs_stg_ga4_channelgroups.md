
{% docs description_ga4_session_channelgroups %}
### Data Origin
The data is transferred from GA4 to BigQuery via the automatic data transfer that can be configured in the GA4 UI.

### Data Processing in dbt

#### 00_sessions_non_pageview
There are sessions in the raw data that don't have a page_view but only a session_start event and one or more other events.
These sessions can only exist because the user has been on our page before, started a session and then came back with a direct entrance later where he didn't trigger a new page load.
To include these sessions in the channel groups logic, this model extracts all sessions without a page_view event. They are later joined to the 01 Model.
As the user has been on our site before these sessions will be assigned a source through the last-non-direct-user-source logic.

#### 01_events_pageviews_adjust_google_params
This model incrementally pulls all page_view events from the base_ga4_events model and unnests the parameters that are required for the session source assignment.
The data on this stage is still on the event level.
In the GA4 raw data source, medium and campaign parameters are not correctly assigned if the user arrives via a Google Ads Campaign. 
This is considered a bug and will hopefully be resolved in the future. In the meantime source, medium and campaign must be reassigned correctly for Google Ads traffic.
The model checks if there is a gclid, wbraid or gbraid parameter present in the page_view event. 
If one of these parameters is present, and if the source is empty or already contains 'google', the source is set to 'google' and the medium is set to 'cpc'. This way other ad partners who also utilize the gclid are unaffected by the reassignment.
The session_gadscampaign parameter is a custom parameter added to the url by us to distinguish the types of Google Ads campaigns (shopping, search or brand paid).
If it is present it is set as campaign parameter. For historical data, before the session_gadscampaign parameter is present, the page_pagetype is used as proxy. 
If the user arrived on a PDP it is considered a shopping entrance, if he arrived on the homepage a brand paid entrance and else a generic paid search entrance.

#### 02_session_first_and_last_source
Model 02 finds the first session source and the last non-direct session source for each session_key.

#### 03_session_last_user_source
For sessions that start with a direct entrance, the last source that can be found for this user within the last 30 days is extracted.

#### 04_session_channelgroups
This final model combines the information of the previous models to finally assign the session traffic source to each session_key.
If a session starts with a traffic source this source is assigned. 
If there is no session traffic source, the last-non-direct-user-source is assigned to a session_key.

### Who oversees the data pipeline and how? 
The responsibility for the tables lies with Helena Steurer as owner of the BigQuery dbt pipeline.


{% enddocs %}
