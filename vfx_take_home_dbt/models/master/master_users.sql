{{
    config(
        materialized='table',
        latest_version=1
    )
}}

with user_transactions as (
    -- Here I'm adding some relevant information to characterize the user
    select
        user_id,
        min(purchase_date) as first_purchase_date,
        max(purchase_date) as last_purchase_date,
        count(distinct transaction_id) as total_orders,
        sum(final_price) as lifetime_value,
        max(loaded_at) as last_loaded_at
    from {{ ref('stg_ecommerce__transactions') }}
    group by user_id
),

user_master as (
    select
        -- Deterministic master key
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_master_id,

        -- Natural key
        user_id as source_user_id,

        -- User metrics
        first_purchase_date,
        last_purchase_date,
        total_orders,
        round(lifetime_value, 2) as lifetime_value,

        -- User segmentation. I've added this just to make the data a bit more interesting
        case
            when total_orders >= 10 then 'VIP'
            when total_orders >= 5 then 'LOYAL'
            when total_orders >= 2 then 'REPEAT'
            else 'NEW'
        end as customer_segment,

        -- Metadata
        'RAW_ECOMMERCE_DATA' as source_system,
        last_loaded_at,
        -- I've added these as SCD2 support columns
        current_timestamp()::timestamp_ntz as dbt_valid_from,
        null::timestamp as dbt_valid_to,
        true as is_current,

        -- I've added these as GDPR support columns
        false as is_deleted,
        null::timestamp as deleted_at,
        null::varchar as deleted_reason

    from user_transactions
)

select * from user_master
