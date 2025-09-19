with source as (
    select *
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_20210131') }}
),

renamed as (
    select
        user_pseudo_id,

        -- session identifiers from event_params
        (select value.int_value
         from unnest(event_params)
         where key = "ga_session_id") as ga_session_id,

        (select value.int_value 
         from unnest(event_params)
         where key = "ga_session_number") as ga_session_number,

        -- engagement
        (select value.int_value
         from unnest(event_params)
         where key = "engaged_session_event") as engaged_session_event
    from source
)

select *
from renamed