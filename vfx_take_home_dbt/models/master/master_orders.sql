-- Here are some sugestions on how to handle late-arriving updates:
-- 1. We could change the materialization of this table to incremental, this would avoid a full table rebuild
--    I would need to set the configs to something like
--        { {
--            config(
--                materialized='incremental',
--                unique_key='order_master_id',
--                incremental_strategy='merge',
--                merge_update_columns = ['order_status', 'final_price', 'dbt_updated_at']
--            )
--        } }
-- 2. It might also be useful to have an incremental table that we could use to keep the orders history, but instead of
--    having a merge strategy we would leave the default strategy of append.
-- 3. We could partition our orders master table by date as that way we could efficiently query our data
-- 4. If in our use case the order status could transition back and forward between status values, we could add an
--    order_version column to our data to keep track of this for auditing purposes


{{
    config(
        materialized='table',
        meta={
            'latest_version': 1
        }
    )
}}

with orders_base as (
    select
        transaction_id,
        user_id,
        product_id,
        purchase_date,
        payment_method,

        -- Monetary values in USD
        price,
        discount_percentage,
        final_price,

        loaded_at,
        dbt_processed_at
    from {{ ref('stg_ecommerce__transactions') }}
),

-- Currency conversion (USD to GBP)
-- Currency conversion (USD to GBP)
currency_conversion as (
    select
        o.*,

        -- Convert USD to GBP using rate from seed file
        {{ convert_amount('o.price', get_conversion_rate()) }} as price_gbp,
        {{ convert_amount('o.final_price', get_conversion_rate()) }} as final_price_gbp,

        {{ get_conversion_rate() }} as conversion_rate

    from orders_base o
),

orders_enriched as (
    select
        cc.*,

        -- Join master keys
        u.user_master_id,
        p.product_master_id,
        p.category as product_category

    from currency_conversion cc
    left join {{ ref('master_users') }} u
        on cc.user_id = u.source_user_id
    left join {{ ref('master_products') }} p
        on cc.product_id = p.source_product_id
),

orders_master as (
    select
        -- Deterministic master key
        {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as order_master_id,

        -- Natural key
        transaction_id as source_transaction_id,

        -- Foreign keys to master tables
        user_master_id,
        product_master_id,

        -- Order attributes
        purchase_date as order_date,
        payment_method,
        product_category,

        -- Monetary values - Multi-currency support
        price as original_price,
        final_price as original_final_price,
        discount_percentage,

        price_gbp,
        final_price_gbp,
        round(price_gbp - final_price_gbp, 2)::float as discount_amount_gbp,

        -- Standardized currency for reporting
        'GBP' as reporting_currency,

        -- Order status (This is simplified, in real scenario this would come from the source or a separate status table)
        'COMPLETED' as order_status,

        -- Metadata
        'RAW_ECOMMERCE_DATA' as source_system,
        loaded_at as source_loaded_at,
        dbt_processed_at,
        current_timestamp()::timestamp_ntz as dbt_updated_at

    from orders_enriched
)

select * from orders_master
