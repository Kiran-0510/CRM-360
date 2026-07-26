{{
    config(
        materialized='table'
    )
}}

with support_tickets as (
    select * from {{ ref('int_customer_support') }}
),

customer_versions as (
    select * from {{ ref('dim_customer') }}
),

joined as (
    select
        -- keys
        s.ticket_id,
        s.customer_id,
        c.customer_sk,

        -- ticket facts
        s.opened_at,
        s.category,
        s.message_count,
        s.resolution_hours,
        s.agent_to_customer_ratio,
        s.first_message_at,
        s.last_message_at,

        -- customer attributes at time of ticket (point-in-time correct)
        c.loyalty_tier,
        c.customer_segment,
        c.state,
        c.signup_date,

        -- derived
        datediff(
            'day',
            c.signup_date,
            s.opened_at::date
        )                           as days_since_signup_at_ticket,
        case
            when s.resolution_hours <= 24  then 'fast'
            when s.resolution_hours <= 72  then 'standard'
            else                                'slow'
        end                         as resolution_speed,

        -- metadata
        -- metadata
        '{{ run_started_at }}'::timestamp   as _loaded_at

    from support_tickets s
    left join customer_versions c
        on s.customer_id = c.customer_id
        and s.opened_at between c.valid_from and c.valid_to
)

select * from joined