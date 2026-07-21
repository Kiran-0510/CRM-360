with source as (
    select * from {{ source('raw', 'loyalty_events') }}
),

renamed as (
    select
        customer_id,
        lower(event_type)                           as event_type,
        upper(new_tier)                             as new_tier,
        try_to_date(event_date, 'YYYY-MM-DD')       as event_date,
        '{{ run_started_at }}'::timestamp as _loaded_at
    from source
    where customer_id is not null
      and try_to_date(event_date, 'YYYY-MM-DD') is not null
)

select * from renamed
