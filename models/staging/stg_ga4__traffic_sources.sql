with source as (
    select *
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_20210131') }}
),

renamed as (
    select 
        user_pseudo_id,
        traffic_source.source as traffic_source,
        traffic_source.medium as traffic_medium,
        traffic_source.name as traffic_campaign
    from source
)

select *
from renamed
