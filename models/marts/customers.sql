-- Customers mart: one row per user

-- ✅ Features:
-- - User-level grain: one row per user_pseudo_id
-- - Includes event activity metrics from int_events__pivoted
-- - Enriched with traffic source, geo, and device context
-- - Useful for customer segmentation & lifetime behavior analysis

with events as (
    select *
    from {{ ref('int_events__pivoted') }}
),

users as (
    select *
    from {{ ref('stg_ga4__users') }}
),

traffic as (
    select *
    from {{ ref('stg_ga4__traffic_sources') }}
)

select
    e.user_pseudo_id,

    -- fct
    e.num_events,
    e.num_add_payment_info_events,
    e.num_add_shipping_info_events,
    e.num_add_to_cart_events,
    e.num_begin_checkout_events,
    e.num_click_events,
    e.num_first_visit_events,
    e.num_page_view_events,
    e.num_scroll_events,
    e.num_select_item_events,
    e.num_select_promotion_events,
    e.num_session_start_events,
    e.num_user_engagement_events,
    e.num_view_item_events,
    e.num_view_promotion_events,
    e.num_view_search_results_events,

    -- dim
    t.traffic_source,
    t.traffic_medium,
    t.traffic_campaign,
    u.user_country,
    u.user_region,
    u.user_city,
    u.user_os,
    u.user_device_browser,
    u.user_device_category

from events e
left join users u
    on e.user_pseudo_id = u.user_pseudo_id
left join traffic t
    on e.user_pseudo_id = t.user_pseudo_id
