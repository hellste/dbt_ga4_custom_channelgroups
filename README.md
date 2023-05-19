# Welcome to the GA4 Custom Channel Groups Repository

This repository contains one possible solution to rebuild custom session level channel groups with GA4 raw data.
The goal of the contained code is to bring the GA4 raw data as close as possible to the session level custom channel groups displayed in the GA4 UI.

## How can this repository help me?
If you see shortcomings in the GA4 default channel groups in the GA4 UI, or if you are in need of session level channel groups in your GA4 raw data for reporting or other purposes this repository may be for you.
If you use dbt you can use the documented code blocks with some minor adjustments. If you do not use dbt the code may still give you some inspiration on the process.

## How good are the results?
When using this model within the Bergzeit GmbH setting, the differences between the GA4 UI and the modelled raw data are between 1-5% on a single channel group level. Important to note is that the GA4 UI estimates session counts which in consequence will always result in small differences between the UI and the modelled raw data.

## Important considerations
1. At the time of creation of this code and repository there is still no session level traffic information in the GA4 raw data export and 'google / cpc' is still not assigned correctly. Also the traffic source parameters are contained in the event_params record and not yet in their own traffic parameter record.
2. The assignment of 'google / cpc' traffic in this code is simplified to only reassign 'google / cpc' if there is a gclid, gbraid or wbraid parameter present in the event, AND if the traffic source is empty or alrady contains 'google' in the source. This way other ads partners that also utilize the gclid are not affected by the change. In consequence the raw data models assign a bit more traffic to channels such as 'social organic' or 'referral' that are contained in a google paid channel in the GA4 UI.
