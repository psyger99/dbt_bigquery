with source as (
    select *
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_20210131') }}
),

renamed as (
    select  
        user_pseudo_id,
        user_first_touch_timestamp,
        geo.country as user_country,
        geo.region as user_region,
        geo.city as user_city,
        device.operating_system as user_os,
        device.web_info.browser as user_device_browser,
        device.category as user_device_category
    from source
)

select *
from renamed