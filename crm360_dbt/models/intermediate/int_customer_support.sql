with customers as (
    select * from {{ ref('stg_customers') }}
),

tickets as (
    select * from {{ ref('stg_support_ticket_messages') }}
),

-- Aggregate to one row per ticket first
-- (messages are at message grain, we need ticket grain)
ticket_summary as (
    select
        ticket_id,
        customer_id,
        opened_at,
        category,
        COUNT(*)                                as message_count,
        MIN(message_sent_at)                    as first_message_at,
        MAX(message_sent_at)                    as last_message_at,
        datediff(
            'hour',
            MIN(message_sent_at),
            MAX(message_sent_at)
        )                                       as resolution_hours,
        SUM(case when message_sender = 'agent' 
            then 1 else 0 end)                  as agent_message_count,
        SUM(case when message_sender = 'customer' 
            then 1 else 0 end)                  as customer_message_count
    from tickets
    group by 1, 2, 3, 4
),

-- Join customer attributes onto ticket summary
joined as (
    select
        ts.ticket_id,
        ts.customer_id,
        ts.opened_at,
        ts.category,
        ts.message_count,
        ts.first_message_at,
        ts.last_message_at,
        ts.resolution_hours,
        ts.agent_message_count,
        ts.customer_message_count,

        -- ratio of agent to customer messages
        -- high ratio = agent doing most of the work = complex issue
        round(
            ts.agent_message_count / nullif(ts.customer_message_count, 0),
            2
        )                                       as agent_to_customer_ratio,

        -- customer attributes
        c.loyalty_tier,
        c.state,
        c.signup_date

    from ticket_summary ts
    left join customers c
        on ts.customer_id = c.customer_id
)

select * from joined
