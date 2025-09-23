-- This model provides session-level behavioral metrics

-- ✅ Features:
-- Session-level grain: user_pseudo_id + ga_session_id.
-- Counts per session: Each column counts only the events within that session.
-- Flexible pivoting: You can add more events to session_events and the macro loop will automatically create the columns.
-- Intermediate model: Can feed into session-level metrics in the core sessions.sql mart.

{% set session_event_names = [
    'add_payment_info', 'add_shipping_info', 'add_to_cart', 'begin_checkout',
    'click', 'first_visit', 'page_view', 'purchase', 'scroll', 'select_item',
    'select_promotion', 'session_start', 'user_engagement', 'view_item',
    'view_promotion', 'view_search_results'
] %}

with events as (
    select 
        e.user_pseudo_id,
        s.ga_session_id,
        e.event_name,
        e.event_timestamp
    from {{ ref('stg_ga4__events') }} e
    left join {{ ref('stg_ga4__sessions') }} s
      on e.user_pseudo_id = s.user_pseudo_id
)

select
    user_pseudo_id,
    ga_session_id,
    
    -- total events per session
    count(distinct event_timestamp) as num_events,

    -- pivoted counts per event type
    {{ count_events_by_name(session_event_names) }}

from events
group by 1, 2

