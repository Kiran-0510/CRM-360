with source as (
    select * from {{ source('raw', 'support_ticket_messages') }}
),

renamed as (
    select
        ticket_id,
        customer_id,
        opened_at::timestamp            as opened_at,
        lower(category)                 as category,
        lower(message_sender)           as message_sender,
        message_text,
        message_sent_at::timestamp      as message_sent_at,
        '{{ run_started_at }}'::timestamp as _loaded_at
    from source
    where ticket_id is not null
      and customer_id is not null
)

select * from renamed
