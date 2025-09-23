-- Sessions mart: one row per session

-- ✅ Features:
-- - Session-level grain: user_pseudo_id + ga_session_id
-- - Includes session-level event counts from int_sessions__pivoted
-- - Enriched with traffic source & device/geo context
-- - Useful for session funnel analysis & attribution

with sessions as (
    select *
    from {{ ref('int_sessions__pivoted') }}
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
    s.user_pseudo_id,
    s.ga_session_id,

    -- activity metrics
    s.num_events,
    s.num_add_payment_info_events,
    s.num_add_shipping_info_events,
    s.num_add_to_cart_events,
    s.num_begin_checkout_events,
    s.num_click_events,
    s.num_first_visit_events,
    s.num_page_view_events,
    s.num_scroll_events,
    s.num_select_item_events,
    s.num_select_promotion_events,
    s.num_session_start_events,
    s.num_user_engagement_events,
    s.num_view_item_events,
    s.num_view_promotion_events,
    s.num_view_search_results_events,

    -- traffic
    t.traffic_source,
    t.traffic_medium,
    t.traffic_campaign,

    -- geography
    u.user_country,
    u.user_region,
    u.user_city,

    -- device
    u.user_os,
    u.user_device_browser,
    u.user_device_category

from sessions s
left join users u
    on s.user_pseudo_id = u.user_pseudo_id
left join traffic t
    on s.user_pseudo_id = t.user_pseudo_id
