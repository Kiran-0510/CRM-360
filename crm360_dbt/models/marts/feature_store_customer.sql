
{{
    config(materialized='table')
}}

with customers as (
    select
        customer_id,
        loyalty_tier,
        customer_segment,
        state,
        signup_date,
        is_likely_duplicate
    from {{ ref('dim_customer') }}
    where is_current = true
),

-- 30-day transaction features
rolling_30d as (
    select
        customer_id,
        SUM(amount)                     as total_spend_30d,
        COUNT(transaction_id)           as transaction_count_30d,
        AVG(amount)                     as avg_transaction_amount_30d
    from {{ ref('fact_transactions') }}
    where event_timestamp >= dateadd('day', -30, current_date)
    group by customer_id
),

-- 90-day transaction features
rolling_90d as (
    select
        customer_id,
        SUM(amount)                     as total_spend_90d,
        COUNT(transaction_id)           as transaction_count_90d,
        AVG(amount)                     as avg_transaction_amount_90d
    from {{ ref('fact_transactions') }}
    where event_timestamp >= dateadd('day', -90, current_date)
    group by customer_id
),

-- lifetime transaction features
lifetime as (
    select
        customer_id,
        SUM(amount)                     as total_spend_lifetime,
        COUNT(transaction_id)           as transaction_count_lifetime,
        MAX(event_timestamp)            as last_transaction_at,
        datediff('day',
            MAX(event_timestamp),
            current_date
        )                               as days_since_last_transaction
    from {{ ref('fact_transactions') }}
    group by customer_id
),

-- most used channel in last 90 days (mode)
channel_ranked as (
    select
        customer_id,
        channel,
        COUNT(*)                        as channel_count,
        ROW_NUMBER() over (
            partition by customer_id
            order by COUNT(*) desc
        )                               as channel_rank
    from {{ ref('fact_transactions') }}
    where event_timestamp >= dateadd('day', -90, current_date)
    group by customer_id, channel
),

most_used_channel as (
    select customer_id, channel as most_used_channel_90d
    from channel_ranked
    where channel_rank = 1
),

-- support features
support_features as (
    select
        customer_id,
        COUNT(ticket_id)                as total_tickets_lifetime,
        AVG(resolution_hours)           as avg_resolution_hours
    from {{ ref('fact_support') }}
    group by customer_id
),

-- most common support category (mode)
category_ranked as (
    select
        customer_id,
        category,
        COUNT(*)                        as category_count,
        ROW_NUMBER() over (
            partition by customer_id
            order by COUNT(*) desc
        )                               as category_rank
    from {{ ref('fact_support') }}
    group by customer_id, category
),

most_common_category as (
    select customer_id, category as most_common_support_category
    from category_ranked
    where category_rank = 1
),

-- join everything together
final as (
    select
        -- identity
        c.customer_id,
        c.loyalty_tier,
        c.customer_segment,
        c.state,
        c.signup_date,
        c.is_likely_duplicate,
        datediff('day', c.signup_date, current_date)    as days_since_signup,

        -- 30-day features
        coalesce(r30.total_spend_30d, 0)                as total_spend_30d,
        coalesce(r30.transaction_count_30d, 0)          as transaction_count_30d,
        coalesce(r30.avg_transaction_amount_30d, 0)     as avg_transaction_amount_30d,

        -- 90-day features
        coalesce(r90.total_spend_90d, 0)                as total_spend_90d,
        coalesce(r90.transaction_count_90d, 0)          as transaction_count_90d,
        coalesce(r90.avg_transaction_amount_90d, 0)     as avg_transaction_amount_90d,
        ch.most_used_channel_90d,

        -- lifetime features
        coalesce(l.total_spend_lifetime, 0)             as total_spend_lifetime,
        coalesce(l.transaction_count_lifetime, 0)       as transaction_count_lifetime,
        l.last_transaction_at,
        coalesce(l.days_since_last_transaction, 999)    as days_since_last_transaction,

        -- support features
        coalesce(s.total_tickets_lifetime, 0)           as total_tickets_lifetime,
        coalesce(s.avg_resolution_hours, 0)             as avg_resolution_hours,
        sc.most_common_support_category,

        -- ML signals
        case
            when coalesce(l.total_spend_lifetime, 0) > 500
                then true
            else false
        end                                             as is_high_value_customer,
        case
            when coalesce(l.days_since_last_transaction, 999) > 60
            and coalesce(l.total_spend_lifetime, 0) > 1000
                then true
            else false
        end                                             as is_at_churn_risk,

        -- metadata
        '{{ run_started_at }}'::timestamp               as _loaded_at

    from customers c
    left join rolling_30d r30       on c.customer_id = r30.customer_id
    left join rolling_90d r90       on c.customer_id = r90.customer_id
    left join lifetime l            on c.customer_id = l.customer_id
    left join most_used_channel ch  on c.customer_id = ch.customer_id
    left join support_features s    on c.customer_id = s.customer_id
    left join most_common_category sc on c.customer_id = sc.customer_id
)

select * from final
