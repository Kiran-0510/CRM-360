with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        customer_id,
        first_name,
        last_name,
        lower(email)                    as email,
        signup_date::date               as signup_date,
        upper(loyalty_tier)             as loyalty_tier,
        upper(state)                    as state,
        is_likely_duplicate,
        '{{ run_started_at }}'::timestamp as _loaded_at
    from source
    where customer_id is not null
)

select * from renamed
