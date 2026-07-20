with source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        customer_id,
        round(amount, 2)                as amount,
        event_timestamp::timestamp      as event_timestamp,
        lower(channel)                  as channel,
        round(rolling_90d_spend, 2)     as rolling_90d_spend,
        rolling_90d_txn_count,
        current_timestamp()             as _loaded_at
    from source
    where transaction_id is not null
      and customer_id is not null
      and amount > 0
)

select * from renamed
