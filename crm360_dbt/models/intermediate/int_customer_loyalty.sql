with customers as (
    select * from {{ ref('stg_customers') }}
),

loyalty_events as (
    select * from {{ ref('stg_loyalty_events') }}
),

-- For customers who HAVE loyalty events, build their tier history
tier_history as (
    select
        customer_id,
        new_tier                                as loyalty_tier,
        event_date                              as valid_from,
        lead(event_date) over (
            partition by customer_id
            order by event_date
        )                                       as valid_to_raw,
        row_number() over (
            partition by customer_id
            order by event_date
        )                                       as tier_sequence
    from loyalty_events
),

-- Replace NULL valid_to (current active record) with sentinel date
tier_history_clean as (
    select
        customer_id,
        loyalty_tier,
        valid_from,
        coalesce(valid_to_raw, '9999-12-31'::date) as valid_to,
        tier_sequence,
        case
            when valid_to_raw is null then true
            else false
        end                                     as is_current
    from tier_history
),

-- For customers with NO loyalty events, their signup tier is their only tier
-- valid from signup_date to forever
customers_without_events as (
    select
        c.customer_id,
        c.loyalty_tier,
        c.signup_date                           as valid_from,
        '9999-12-31'::date                      as valid_to,
        1                                       as tier_sequence,
        true                                    as is_current
    from customers c
    left join loyalty_events le
        on c.customer_id = le.customer_id
    where le.customer_id is null  -- only customers with no events
),

-- Union both sets together
final as (
    select * from tier_history_clean
    union all
    select * from customers_without_events
)

select * from final
