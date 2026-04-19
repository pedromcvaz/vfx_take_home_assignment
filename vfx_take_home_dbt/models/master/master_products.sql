{{
    config(
        materialized='table',
        meta={
            'latest_version': 1
        }
    )
}}

with product_metrics as (
    select
        product_id,
        category,
        coalesce(count(distinct s.transaction_id), 0)::number(38, 0) as times_sold,
        coalesce(count(distinct s.user_id), 0)::number(38, 0) as unique_customers,
        coalesce(avg(s.price), 0.0)::float as avg_price,  -- Default to 0 if no sales
        coalesce(avg(s.discount_percentage), 0)::number(5, 2) as avg_discount_pct,
        min(purchase_date) as first_sold_date,
        max(purchase_date) as last_sold_date
    from {{ ref('stg_ecommerce__transactions') }} s
    group by product_id, category
),

product_master as (
    select
        -- Deterministic master key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_master_id,

        -- Natural key
        product_id as source_product_id,

        -- Product attributes
        category,

        -- Product metrics.
        times_sold,
        unique_customers,
        round(avg_price, 2)::number(18,2) as avg_price,
        round(avg_discount_pct, 2)::number(38, 0) as avg_discount_pct,
        first_sold_date,
        last_sold_date,

        -- Metadata
        'RAW_ECOMMERCE_DATA' as source_system,
        -- Columns for SCD2
        current_timestamp()::timestamp_ntz as dbt_valid_from,
        null::timestamp_ntz as dbt_valid_to,
        true as is_current

        -- Note: For SCD2 implementation, I would've add track changes to:
        -- - category_standardized (if product gets recategorized)
        -- - avg_price (if pricing strategy changes significantly)

    from product_metrics
)

select * from product_master
