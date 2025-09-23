-- This intermediate model provides user-level behavioral metrics.
-- Note: The purchase-related columns are not included due to lack of usable data.

-- ✅ Features:
-- User-level grain: one row per user_pseudo_id.
-- Counts per user: each column counts only the events that occurred for that user.
-- Flexible pivoting: adding or removing events from event_names automatically updates the output columns.
-- Browser-level counts: counts of events per user by browser are also included.
-- Intermediate model: designed to feed into the core data marts for analytics.

{% set event_names = [
    'add_payment_info', 'add_shipping_info', 'add_to_cart', 'begin_checkout',
    'click', 'first_visit', 'page_view', 'purchase', 'scroll', 'select_item',
    'select_promotion', 'session_start', 'user_engagement', 'view_item',
    'view_promotion', 'view_search_results'
] %}

{% set browsers = [
    'chrome',
    'firefox',
    'safari',
    'edge',
    'android_webview',
    '<other>'
] %}


select 
    e.user_pseudo_id,
    count(distinct e.event_timestamp) as num_events,

    -- pivoted counts per event type on user-level
    {{ count_events_by_name(event_names) }},

    --pivoted counts per browser
    {{ count_by_browser(browsers) }}

from {{ ref('stg_ga4__events') }} e
left join {{ ref('stg_ga4__users') }} u
  on e.user_pseudo_id = u.user_pseudo_id
group by 1
