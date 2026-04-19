{{
    config(
        materialized='table',
        latest_version=1
    )
}}

with product_base as (
    select
        product_id,
        category
    from {{ ref('stg_ecommerce__transactions') }}
    group by product_id, category
),

product_metrics as ( -- I've decided to add some metrics
    select
        p.product_id,
        p.category,
        count(distinct s.transaction_id)::number(38, 0) as times_sold,
        count(distinct s.user_id)::number(38, 0) as unique_customers,
        avg(s.price) as avg_price,
        avg(s.discount_percentage) as avg_discount_pct,
        min(s.purchase_date) as first_sold_date,
        max(s.purchase_date) as last_sold_date
    from product_base p
    left join {{ ref('stg_ecommerce__transactions') }} s
        on p.product_id = s.product_id
    group by
        p.product_id,
        p.category
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
        round(avg_price, 2)::float as avg_price,
        round(avg_discount_pct, 2) as avg_discount_pct,
        first_sold_date,
        last_sold_date,

        -- Metadata
        'RAW_ECOMMERCE_DATA' as source_system,
        current_timestamp()::timestamp_ntz as dbt_valid_from,
        null::timestamp_ntz as dbt_valid_to,
        true as is_current

        -- Note: For SCD2 implementation, I would've add track changes to:
        -- - category_standardized (if product gets recategorized)
        -- - avg_price (if pricing strategy changes significantly)

    from product_metrics
)

select * from product_master
