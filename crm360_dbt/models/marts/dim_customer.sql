with customers as (
    select * from {{ ref('stg_customers') }}
),

loyalty_history as (
    select * from {{ ref('int_customer_loyalty') }}
),

-- Join customer base attributes with their tier history
-- This gives us one row per customer per tier state
dim as (
    select
        -- surrogate key — unique identifier for this specific version
        -- of the customer record
        {{ dbt_utils.generate_surrogate_key([
            'lh.customer_id',
            'lh.valid_from'
        ]) }}                               as customer_sk,

        -- natural key
        lh.customer_id,

        -- customer attributes
        c.first_name,
        c.last_name,
        c.email,
        c.state,
        c.signup_date,
        c.is_likely_duplicate,

        -- slowly changing dimension fields
        lh.loyalty_tier,
        lh.valid_from,
        lh.valid_to,
        lh.is_current,
        lh.tier_sequence,

        -- derived attributes
        datediff('day', c.signup_date, current_date)    as customer_age_days,
        case
            when datediff('day', c.signup_date, current_date) <= 90
                then 'new'
            when datediff('day', c.signup_date, current_date) <= 365
                then 'developing'
            else 'established'
        end                                             as customer_segment,

        -- metadata
        c._loaded_at

    from loyalty_history lh
    inner join customers c
        on lh.customer_id = c.customer_id
)

select * from dim
