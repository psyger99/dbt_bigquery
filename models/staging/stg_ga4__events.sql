with source as (
    select *
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_20210131') }}
),

renamed as (
    select
        user_pseudo_id,
        parse_date('%Y%m%d', event_date) as event_date,
        event_name,
        event_bundle_sequence_id,
        event_timestamp,
        event_previous_timestamp,
    from source
)

select *
from renamed