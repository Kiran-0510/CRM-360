with customers as (
    select * from {{ ref('stg_customers') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

joined as (
    select
        -- transaction keys
        t.transaction_id,
        t.customer_id,

        -- transaction facts
        t.amount,
        t.event_timestamp,
        t.channel,
        t.rolling_90d_spend,
        t.rolling_90d_txn_count,

        -- customer attributes at time of transaction
        -- (static for now, SCD2 will handle history later)
        c.first_name,
        c.last_name,
        c.email,
        c.loyalty_tier,
        c.state,
        c.signup_date,
        c.is_likely_duplicate,

        -- derived fields — business logic starts here
        t.amount / nullif(t.rolling_90d_spend, 0)  as pct_of_90d_spend,
        datediff('day', c.signup_date, t.event_timestamp::date) as days_since_signup,
        case
            when datediff('day', c.signup_date, t.event_timestamp::date) <= 30
                then 'new_customer'
            when datediff('day', c.signup_date, t.event_timestamp::date) <= 180
                then 'early_customer'
            else 'established_customer'
        end                                         as customer_lifecycle_stage,

        -- metadata
        t._loaded_at

    from transactions t
    left join customers c
        on t.customer_id = c.customer_id
)

select * from joined
