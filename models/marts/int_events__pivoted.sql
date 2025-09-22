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

    {{ count_events_by_name(event_names) }},
    {{ count_by_browser(browsers) }}

from {{ ref('stg_ga4__events') }} e
left join {{ ref('stg_ga4__users') }} u
  on e.user_pseudo_id = u.user_pseudo_id
group by 1
