
{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customer_versions as (
    select * from {{ ref('dim_customer') }}
),

joined as (
    select
        -- keys
        t.transaction_id,
        t.customer_id,
        c.customer_sk,

        -- transaction facts
        t.amount,
        t.event_timestamp,
        t.channel,
        t.rolling_90d_spend,
        t.rolling_90d_txn_count,

        -- customer attributes at time of transaction (point-in-time correct)
        c.loyalty_tier,
        c.customer_segment,
        c.state,
        c.signup_date,

        -- derived
        datediff(
            'day',
            c.signup_date,
            t.event_timestamp::date
        )                                   as days_since_signup,
        case
            when t.amount >= 500  then 'high_value'
            when t.amount >= 100  then 'mid_value'
            else                       'low_value'
        end                                 as transaction_value_band,

        -- metadata
        t._loaded_at

    from transactions t
    left join customer_versions c
        on t.customer_id = c.customer_id
        and t.event_timestamp between c.valid_from and c.valid_to
)

select * from joined
{% if is_incremental() %}
where event_timestamp >= (
    select dateadd('day', -3, max(event_timestamp))
    from {{ this }}
)
{% endif %}
